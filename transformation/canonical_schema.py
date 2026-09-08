"""Project and validate normalized records using a runtime JSON Schema."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


def load_canonical_schema() -> dict[str, Any] | None:
    path_value = os.getenv("CANONICAL_SCHEMA_PATH")
    if not path_value:
        return None
    path = Path(path_value)
    with path.open(encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    if not isinstance(schema, dict):
        raise ValueError("The canonical schema must be a JSON object.")
    Draft202012Validator.check_schema(schema)
    return schema


def _project(value: Any, schema: dict[str, Any], path: str) -> Any:
    schema_type = schema.get("type")
    if schema_type == "object" and isinstance(value, dict):
        properties = schema.get("properties", {})
        if not isinstance(properties, dict):
            raise ValueError(f"Invalid properties definition at {path or '$'}.")
        return {
            key: _project(value[key], child_schema, f"{path}.{key}".strip("."))
            for key, child_schema in properties.items()
            if key in value
        }
    if schema_type == "array" and isinstance(value, list):
        item_schema = schema.get("items", {})
        return [_project(item, item_schema, f"{path}[]") for item in value]
    return value


def apply_canonical_schema(record: dict[str, Any]) -> dict[str, Any]:
    schema = load_canonical_schema()
    if schema is None:
        return record
    projected = _project(record, schema, "")
    errors = sorted(Draft202012Validator(schema).iter_errors(projected), key=str)
    if errors:
        messages = "; ".join(error.message for error in errors[:5])
        raise ValueError(f"Canonical schema validation failed: {messages}")
    return projected
