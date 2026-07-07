import json
import os

from jsonschema import Draft7Validator

SCHEMA_PATH = os.getenv("TRANSACTION_SCHEMA_PATH", "/app/schemas/transaction-schema.json")

with open(SCHEMA_PATH) as f:
    _schema = json.load(f)

_validator = Draft7Validator(_schema)


def validate_transaction(payload: dict) -> None:
    _validator.validate(payload)
