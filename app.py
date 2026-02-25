from logging import getLogger
from log import init_logger
import yaml
import os
import requests
import json
import datetime

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
                    "game_title": value[0] if len(value) > 0 else "",
                    "department": value[1] if len(value) > 1 else "",
                    "score": value[2] if len(value) > 2 else "",
                    "score_name": value[3] if len(value) > 3 else "",
                    "notes": value[4] if len(value) > 4 else "",
                    "game_center": value[5] if len(value) > 5 else "",
                }
            )
        output[sheet_title] = values
    return output


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
