import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

def enrich_shipment(payload: dict[str, Any]) -> dict[str, Any]:
    """Return canonical attributes enriched from one Shippo shipment."""
    object_id = payload["object_id"]
    customer_tier = (
        "PREMIUM" if object_id.endswith(("0", "1", "2", "3")) else "STANDARD"
    )
    order_value_usd = 100.0 + (len(object_id) * 10)
    order_priority = 1 if customer_tier == "PREMIUM" else 2
    selected_rate = payload["rates"][0]
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
            "tracking_number": payload.get("tracking_number"),
            "dwell_time_sigma": 0.0,
            "arrival_eta": estimated_delivery.isoformat(),
        },
        "facility": {
            "facility_type": "ORIGIN_WAREHOUSE",
            "facility_name": payload["address_from"]["name"],
            "region_id": (
                f"{payload['address_from']['country']}-"
                f"{payload['address_from'].get('state', 'UNKNOWN')}"
            ),
            "geo_point": {
                "lat": payload["address_from"].get("latitude"),
                "lon": payload["address_from"].get("longitude"),
            },
            "throughput_z": 0.0,
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
            "capacity_z": 0.0,
            "success_rate_z": 0.0,
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