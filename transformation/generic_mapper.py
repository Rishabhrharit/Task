from __future__ import annotations

import json
import os
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
    """Apply a declarative field map to a source payload and emit canonical attributes."""
    source_map = mapping.get("source", {})
    canonical_shape = mapping.get("canonical", {})
    if not isinstance(canonical_shape, dict):
        raise ValueError("The mapping config must contain a 'canonical' object.")

    result: dict[str, Any] = {
        "batch_id": batch_id,
        "erp_source": source_system.upper(),
        "entity_id": _coalesce(
            resolve_path(payload, source_map.get("entity_id")),
            resolve_path(payload, source_map.get("order_id")),
            resolve_path(payload, source_map.get("shipment_id")),
            "UNKNOWN_ENTITY",
        ),
    }

    for canonical_key, source_path in canonical_shape.items():
        value = resolve_path(payload, source_path)
        if value is None:
            continue
        _deep_set(result, canonical_key, value)

    if "order" not in result:
        result["order"] = {}
    if "shipment" not in result:
        result["shipment"] = {}
    if "facility" not in result:
        result["facility"] = {}
    if "carrier" not in result:
        result["carrier"] = {}
    if "delivery_partner" not in result:
        result["delivery_partner"] = {}
    if "route" not in result:
        result["route"] = {}
    if "event" not in result:
        result["event"] = {}

    if "order_id" in result and "order" in result:
        result["order"]["order_id"] = result["order_id"]
    if "shipment_id" in result and "shipment" in result:
        result["shipment"]["shipment_id"] = result["shipment_id"]
    if "carrier_name" in result and "carrier" in result:
        result["carrier"]["carrier_name"] = result["carrier_name"]
    if "partner_name" in result and "delivery_partner" in result:
        result["delivery_partner"]["partner_name"] = result["partner_name"]
    if "facility_id" in result and "facility" in result:
        result["facility"]["facility_id"] = result["facility_id"]
    if "route_origin" in result and "route" in result:
        result["route"]["origin_id"] = result["route_origin"]
    if "route_dest" in result and "route" in result:
        result["route"]["dest_id"] = result["route_dest"]

    result["derived"] = {
        "fields": {},
        "calculation": f"Mapped from {source_system}.{source_entity} using declarative source mapping.",
        "calculated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
    }
    return result
