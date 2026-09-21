from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import load_project_env

load_project_env()


def load_mapping_config(mapping_path: str | None = None) -> dict[str, Any] | None:
    """Load a source-to-canonical mapping from a JSON/YAML file if configured."""
    config_path = (
        mapping_path
        or os.getenv("SOURCE_MAPPING_PATH")
        or os.getenv("MAPPING_CONFIG_PATH")
        or str(Path(__file__).resolve().parent / "source_mapping.json")
    )
    path = Path(config_path)
    if not path.exists():
        return None

    content = path.read_text(encoding="utf-8")
    if not content.strip():
        return None

    if path.suffix.lower() == ".json":
        data = json.loads(content)
    else:
        try:
            import yaml
            data = yaml.safe_load(content)
        except Exception:
            data = json.loads(content)

    if not isinstance(data, dict):
        raise ValueError("The mapping config must be a JSON/YAML object.")
    return data


def resolve_path(data: Any, path: str) -> Any:
    if path in (None, ""):
        return data
    current = data
    for part in str(path).split("."):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            if part.isdigit():
                index = int(part)
                if index >= len(current):
                    return None
                current = current[index]
            else:
                return None
        else:
            return None
    return current


def _deep_set(target: dict[str, Any], dot_path: str, value: Any) -> None:
    parts = str(dot_path).split(".")
    current = target
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = value


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def map_payload_to_canonical(
    payload: dict[str, Any],
    *,
    mapping: dict[str, Any],
    source_system: str,
    source_entity: str,
    batch_id: str,
) -> dict[str, Any]:
    """Apply a declarative field map to any source payload and emit a generic
    canonical envelope. No entity shape (order/shipment/etc.) is assumed; the
    mapping config alone decides what attributes and relationships exist.

    Mapping config keys:
      - "source.entity_id": dotted path to the record's unique identifier.
      - "canonical": {dotted attribute key: source path} -> becomes entity attributes.
      - "entity_type": optional node label / entity.type (defaults to source_entity).
      - "relationships": optional list of
            {"type": "REL_TYPE", "from": "attr.path", "to": "attr.path"}
        where "from"/"to" reference keys already produced under "canonical"
        (or the literal "entity_id").
      - "literals": optional {attribute key: constant value}, for fields with
        no source field (e.g. a fixed process/process_type tag).
      - "schema_version": optional override for the canonical envelope version.
    """
    source_map = mapping.get("source", {})
    canonical_shape = mapping.get("canonical", {})
    if not isinstance(canonical_shape, dict):
        raise ValueError("The mapping config must contain a 'canonical' object.")

    entity_id = _coalesce(
        resolve_path(payload, source_map.get("entity_id")),
        resolve_path(payload, source_map.get("id")),
        "UNKNOWN_ENTITY",
    )

    attributes: dict[str, Any] = dict(mapping.get("literals") or {})
    for canonical_key, source_path in canonical_shape.items():
        value = resolve_path(payload, source_path)
        if value is None:
            continue
        _deep_set(attributes, canonical_key, value)

    relationships: list[dict[str, Any]] = []
    for rel in mapping.get("relationships") or []:
        if not isinstance(rel, dict):
            continue
        rel_type = rel.get("type")
        from_key = rel.get("from")
        to_key = rel.get("to")
        from_value = str(entity_id) if from_key == "entity_id" else resolve_path(attributes, from_key)
        to_value = str(entity_id) if to_key == "entity_id" else resolve_path(attributes, to_key)
        if not rel_type or from_value in (None, "") or to_value in (None, ""):
            continue
        relationships.append(
            {
                "type": str(rel_type),
                "from_id": str(from_value),
                "to_id": str(to_value),
                "attributes": {},
            }
        )

    return {
        "batch_id": str(batch_id),
        "erp_source": source_system.upper(),
        "entity_id": str(entity_id),
        "entity_type": str(mapping.get("entity_type") or source_entity or "record"),
        "schema_version": str(mapping.get("schema_version") or "custom.v1"),
        "attributes": attributes,
        "relationships": relationships,
        "derived": {
            "fields": {},
            "calculation": f"Mapped from {source_system}.{source_entity} using declarative source mapping.",
            "calculated_at": datetime.now(timezone.utc).isoformat(),
        },
    }
