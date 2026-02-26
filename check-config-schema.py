import json
from pathlib import Path

import yaml
from jsonschema import ValidationError, validate


def main() -> None:
    schema_path = Path("config-schema.json")
    config_path = Path("config.yml")

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    try:
        validate(instance=config, schema=schema)
    except ValidationError as err:
        message = f"Config validation failed: {err.message}"
        raise SystemExit(message) from err


if __name__ == "__main__":
    main()
