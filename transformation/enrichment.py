import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

def enrich_order(payload: dict[str, Any]) -> dict[str, Any]:
    """Map ERP order fields into the canonical contract without Shippo assumptions."""
    order_value = float(payload["order_value"].replace("$", "").replace(",", ""))
    return {
        "batch_id": None,
        "erp_source": payload.get("erp_system", "ERP"),
        "entity_id": payload["shipment_id"],
        "order": {
            "order_id": payload["order_id"],
            "customer_id": payload["customer_id"],
            "order_value_usd": order_value,
            "currency": payload.get("currency", "USD"),
            "original_promise_dt": payload.get("promise_date"),
            "target_delivery_dt": payload.get("target_date"),
            "order_priority": payload.get("priority", "NORMAL"),
            "status": payload.get("order_status"),
        },
        "shipment": {
            "shipment_id": payload["shipment_id"],
            "tracking_number": payload.get("tracking_number"),
        },
        "facility": {"facility_id": payload.get("warehouse_id")},
        "carrier": {"carrier_name": payload.get("carrier")},
        "delivery_partner": {"partner_name": payload.get("carrier")},
        "route": {"origin_id": payload.get("warehouse_id"), "dest_id": payload["customer_id"]},
        "event": {
            "event_type": "ORDER_UPDATED",
            "timestamp_iso": payload.get("updated_at") or payload.get("promise_date"),
            "exception_reason": None,
        },
        "derived": {"fields": {}, "calculation": "ERP source values mapped directly"},
    }


def enrich_shipment(payload: dict[str, Any]) -> dict[str, Any]:
    """Return canonical attributes enriched from one Shippo shipment."""
    object_id = payload["object_id"]
    customer_tier = (
        "PREMIUM" if object_id.endswith(("0", "1", "2", "3")) else "STANDARD"
    )
    order_value_usd = 100.0 + (len(object_id) * 10)
    order_priority = 1 if customer_tier == "PREMIUM" else 2
    selected_rate = payload["rates"][0]
    address_from = payload["address_from"]
    seed = int(hashlib.sha256(object_id.encode("utf-8")).hexdigest()[:8], 16)
    transaction = payload.get("transaction") or {}
    tracking_number = (
        payload.get("tracking_number")
        or transaction.get("tracking_number")
        or payload["synthetic_tracking_number"]
    )
    latitude = address_from.get("latitude")
    longitude = address_from.get("longitude")
    if latitude is None or longitude is None:
        latitude = round(25.0 + (seed % 2500) / 100.0, 6)
        longitude = round(-124.0 + ((seed // 2500) % 5800) / 100.0, 6)
    shipment_date = datetime.fromisoformat(
        payload["shipment_date"].replace("Z", "+00:00")
    )
    estimated_delivery = shipment_date + timedelta(
        days=selected_rate["estimated_days"]
    )
    event_type = "SHIPMENT_CREATED"
    audit_hash = hashlib.sha256(
        f"{object_id}:{event_type}:{shipment_date.isoformat()}".encode("utf-8")
    ).hexdigest()
    extracted_at = datetime.now(timezone.utc)

    return {
        "batch_id": None,
        "erp_source": "SYNTHETIC_ERP",
        "entity_id": object_id,
        "order": {
            "customer_tier": customer_tier,
            "order_value_usd": order_value_usd,
            "original_promise_dt": estimated_delivery.isoformat(),
            "target_delivery_dt": estimated_delivery.isoformat(),
            "estimated_delivery_dt": estimated_delivery.isoformat(),
            "order_priority": order_priority,
            "sla_health": "ON_TRACK",
        },
        "shipment": {
            "shipment_id": object_id,
            "tracking_number": tracking_number,
            "dwell_time_sigma": 1.15,
            "arrival_eta": estimated_delivery.isoformat(),
        },
        "facility": {
            "facility_type": "ORIGIN_WAREHOUSE",
            "facility_name": payload["address_from"]["name"],
            "region_id": (
                f"{payload['address_from']['country']}-"
                f"{payload['address_from'].get('state', 'UNKNOWN')}"
            ),
            "geo_point": {"lat": latitude, "lon": longitude},
            "throughput_z": 0.65,
            "status_flag": "ACTIVE",
        },
        "carrier": {
            "carrier_name": selected_rate["provider"],
            "reliability_idx": 0.85,
            "cost_volatility": 0.10,
        },
        "delivery_partner": {
            "partner_name": selected_rate["provider"],
            "partner_display_name": selected_rate["provider"],
            "service_zone_id": selected_rate["zone"],
            "capacity_z": 0.80,
            "success_rate_z": 0.92,
        },
        "route": {
            "origin_id": payload["address_from"]["object_id"],
            "dest_id": payload["address_to"]["object_id"],
            "route_name": (
                f"{payload['address_from']['city']} to "
                f"{payload['address_to']['city']}"
            ),
            "risk_score": 0.10,
        },
        "event": {
            "event_type": event_type,
            "timestamp_iso": shipment_date.isoformat(),
            "audit_hash": audit_hash,
            "delta_t_hrs": 0.0,
            "location_id": payload["address_from"]["object_id"],
            "location_kind": "FACILITY",
            "exception_reason": None,
        },
        "derived": {
            "fields": {
                "estimated_delivery_dt": estimated_delivery.isoformat(),
                "sla_health": "ON_TRACK",
            },
            "calculation": "estimated_days added to shipment_date",
            "calculated_at": extracted_at.isoformat(),
        },
    }