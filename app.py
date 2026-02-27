from logging import getLogger
from log import init_logger
import yaml
import os
import requests
import json
import datetime
import re
import unicodedata

init_logger()

logger = getLogger("jha-summary")


def get_api_key():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        logger.warning("GOOGLE_API_KEY is not set in environment variables.")
        exit(1)
    return api_key


def load_config():
    with open("config.yml", "r") as f:
        config = yaml.safe_load(f)
    return config


def request_google_spread_sheet(url):
    try:
        response = requests.get(url, headers={"X-Goog-Api-Key": get_api_key()})
        if response.status_code != 200:
            raise Exception(f"Failed to fetch spreadsheet metadata")
        return json.loads(response.text)

    except requests.RequestException as e:
        logger.error(f"Request to failed")
        raise


def fetch_sheets(spreadsheet_id) -> list[dict[str, str]]:
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"

    parsed_response = request_google_spread_sheet(url)
    logger.debug(f"Successfully fetched spreadsheet metadata: {parsed_response}")
    sheet_ids = [
        {
            "sheetId": sheet["properties"]["sheetId"],
            "title": sheet["properties"]["title"],
        }
        for sheet in reversed(parsed_response["sheets"])
    ]
    logger.debug(f"Extracted sheet IDs: {sheet_ids}")
    return sheet_ids


# 変換する文字列のマッピング
FULL_WIDTH_TRANSLATION = {
    # 大文字の A-Z
    **{ord("Ａ") + i: ord("A") + i for i in range(26)},
    # 小文字の a-z
    **{ord("ａ") + i: ord("a") + i for i in range(26)},
    # 全角の数字 0-9
    **{ord("０") + i: ord("0") + i for i in range(10)},
    # 全角スペース
    ord("　"): ord(" "),
    # 全角のカッコ
    ord("（"): ord("("),
    ord("）"): ord(")"),
    # 全角のハイフン
    ord("－"): ord("-"),
    # マイナス
    ord("−"): ord("-"),
    # 全角のアンダースコア
    ord("＿"): ord("_"),
    # 全角のコロン
    ord("："): ord(":"),
    # 全角のセミコロン
    ord("；"): ord(";"),
    # 全角のクエスチョンマーク
    ord("？"): ord("?"),
    # 全角のエクスクラメーションマーク
    ord("！"): ord("!"),
    # 全角のスラッシュ
    ord("／"): ord("/"),
    # 全角チルダと全角波ダッシュ
    ord("〜"): ord("～"),
}


def validate_string(string) -> str:
    if string is None:
        return ""

    normalized = str(string).translate(FULL_WIDTH_TRANSLATION)
    normalized = unicodedata.normalize("NFC", normalized)
    normalized = re.sub(r" {2,}", " ", normalized)
    normalized = normalized.rstrip(" ")

    return normalized


def validate_game(config, game) -> str:
    game_alias_map = build_game_alias_map(config)
    validated_game = validate_string(game)
    if not game_alias_map:
        return validated_game
    return game_alias_map.get(validated_game, validated_game)


def validate_department(config, department) -> str:
    validated_department = validate_string(department)
    department_aliases = {
        "": "-",
        "部門なし": "-",
        "部門無し": "-",
        "なし": "-",
        "連なし": "連無し",
        "連付き": "連付き",
        "連付": "連付き",
        "連無": "連無し",
    }
    if validated_department in department_aliases:
        return department_aliases[validated_department]

    department_alias_map = build_department_alias_map(config)
    if not department_alias_map:
        return validated_department
    return department_alias_map.get(validated_department, validated_department)


def fetch_games(config, spread_sheet_id, sheets):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{spread_sheet_id}/values:batchGet"
    query_params = ""
    for sheet in sheets:
        query_params += f"ranges={sheet['title']}!A1:{config['max_range']}&"

    url = f"{url}?{query_params[:-1]}"  # Remove trailing '&'
    parsed_response = request_google_spread_sheet(url)
    output = {}
    for value_range in parsed_response["valueRanges"]:
        sheet_title = value_range["range"].split("!")[0]
        values = []
        for value in value_range["values"][3:]:
            if value == [] or len(value) < 2 or value[0] == "":
                logger.info(
                    f"Skipping empty or invalid row in sheet '{sheet_title}': {value}"
                )
                break
            values.append(
                {
                    "game_title": (
                        validate_game(config, value[0]) if len(value) > 0 else ""
                    ),
                    "department": (
                        validate_department(config, value[1]) if len(value) > 1 else ""
                    ),
                    "score": value[2] if len(value) > 2 else "",
                    "score_name": value[3] if len(value) > 3 else "",
                    "notes": value[4] if len(value) > 4 else "",
                    "game_center": value[5] if len(value) > 5 else "",
                }
            )
        output[sheet_title] = values
    return output


def build_game_alias_map(config: dict) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    for entry in config.get("game_specific", []):
        canonical_name = validate_string(entry.get("game", ""))
        for alias in entry.get("same_as", []) or []:
            alias_map[validate_string(alias)] = canonical_name
    return alias_map


def build_department_alias_map(config: dict) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    for entry in config.get("game_specific", []):
        for department in entry.get("departments", []) or []:
            canonical_name = validate_string(department.get("name", ""))
            for alias in department.get("same_as", []) or []:
                alias_map[validate_string(alias)] = canonical_name
    return alias_map


def insert_output_from_sheets(output, sheets):
    for date in sheets:
        logger.info(f"Processing sheet: {date}")
        logger.debug(f"Sheet data: {sheets[date]}")
        for entry in sheets[date]:
            game_title = entry["game_title"]
            if game_title not in output:
                output[game_title] = {}
            if entry["department"] not in output[game_title]:
                output[game_title][entry["department"]] = []
            output[game_title][entry["department"]].append(
                {
                    "score": entry["score"],
                    "score_name": entry["score_name"],
                    "notes": entry["notes"],
                    "game_center": entry["game_center"],
                    "date": date.replace("'", ""),
                }
            )


def sort_output(output):
    for game_title in output.keys():
        logger.debug(f"Sorting entries for game: {output[game_title]}")
        for department in output[game_title].keys():
            output[game_title][department] = sorted(
                output[game_title][department], key=lambda x: x["date"], reverse=True
            )
    return output


if __name__ == "__main__":
    config = load_config()
    logger.info(f"Loaded config: {config}")
    output = {}
    for spread_sheet_id in config["spread_sheet_ids"]:
        logger.info(f"Processing spreadsheet ID: {spread_sheet_id}")
        sheets = fetch_sheets(spread_sheet_id)
        response = fetch_games(config, spread_sheet_id, sheets)
        insert_output_from_sheets(output, response)
    output = sort_output(output)
    output = {
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "jha_summary": output,
    }
    with open(f"output/jha-scores.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(output, ensure_ascii=False, sort_keys=True))
