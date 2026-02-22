from logging import getLogger
from log import init_logger
import yaml
import os
import requests
import json

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
    return parsed_response


if __name__ == "__main__":
    config = load_config()
    logger.info(f"Loaded config: {config}")
    for spread_sheet_id in config["spread_sheet_ids"]:
        logger.info(f"Processing spreadsheet ID: {spread_sheet_id}")
        sheets = fetch_sheets(spread_sheet_id)
        response = fetch_games(config, spread_sheet_id, sheets)
        with open(f"{spread_sheet_id}.json", "w", encoding="utf-8") as f:
            f.write(json.dumps(response, indent=2, ensure_ascii=False))
