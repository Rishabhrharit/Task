import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

def enrich_order(payload: dict[str, Any]) -> dict[str, Any]:
    """Map an ERP order and synthesize deterministic demo-only missing fields."""
    shipment_id = str(payload["shipment_id"])
    seed = int(hashlib.sha256(shipment_id.encode("utf-8")).hexdigest()[:8], 16)
    order_value = float(payload["order_value"].replace("$", "").replace(",", ""))
    customer_tier = "PREMIUM" if seed % 4 == 0 else "STANDARD"
    order_priority = 1 if customer_tier == "PREMIUM" else 2
    carrier = payload.get("carrier") or "UNKNOWN"
    warehouse_id = payload.get("warehouse_id") or f"WH-{seed % 1000:03d}"
    updated_at = payload.get("updated_at") or payload.get("promise_date")
    synthetic_latitude = round(25.0 + (seed % 2500) / 100.0, 6)
    synthetic_longitude = round(-124.0 + ((seed // 2500) % 5800) / 100.0, 6)
    audit_hash = hashlib.sha256(
        f"{shipment_id}:ORDER_UPDATED:{updated_at}".encode("utf-8")
    ).hexdigest()
    return {
        "batch_id": None,
        "erp_source": payload.get("erp_system", "ERP"),
        "entity_id": shipment_id,
        "order": {
            "order_id": payload["order_id"],
            "customer_id": payload["customer_id"],
            "order_value_usd": order_value,
            "currency": payload.get("currency", "USD"),
            "original_promise_dt": payload.get("promise_date"),
            "target_delivery_dt": payload.get("target_date"),
            "estimated_delivery_dt": payload.get("target_date"),
            "order_priority": payload.get("priority") or order_priority,
            "status": payload.get("order_status"),
            "customer_tier": customer_tier,
            "sla_health": "ON_TRACK",
        },
        "shipment": {
            "shipment_id": shipment_id,
            "tracking_number": payload.get("tracking_number") or f"syn_{seed:08x}",
            "dwell_time_sigma": round(0.8 + (seed % 80) / 100, 2),
            "arrival_eta": payload.get("target_date"),
        },
        "facility": {
            "facility_id": warehouse_id,
            "facility_type": "ORIGIN_WAREHOUSE",
            "facility_name": f"Warehouse {warehouse_id}",
            "region_id": f"REGION-{seed % 50:02d}",
            "geo_point": {"lat": synthetic_latitude, "lon": synthetic_longitude},
            "throughput_z": round(0.5 + (seed % 50) / 100, 2),
            "status_flag": "ACTIVE",
        },
        "carrier": {
            "carrier_name": carrier,
            "reliability_idx": round(0.75 + (seed % 20) / 100, 2),
            "cost_volatility": round(0.05 + (seed % 15) / 100, 2),
        },
        "delivery_partner": {
            "partner_name": carrier,
            "partner_display_name": carrier,
            "service_zone_id": f"ZONE-{seed % 10:02d}",
            "capacity_z": round(0.7 + (seed % 25) / 100, 2),
            "success_rate_z": round(0.85 + (seed % 14) / 100, 2),
        },
        "route": {
            "origin_id": warehouse_id,
            "dest_id": payload["customer_id"],
            "route_name": f"{warehouse_id} to {payload['customer_id']}",
            "risk_score": round(0.05 + (seed % 20) / 100, 2),
        },
        "event": {
            "event_type": "ORDER_UPDATED",
            "timestamp_iso": updated_at,
            "audit_hash": audit_hash,
            "delta_t_hrs": 0.0,
            "location_id": warehouse_id,
            "location_kind": "FACILITY",
            "exception_reason": "NONE",
        },
        "derived": {
            "fields": {
                "estimated_delivery_dt": payload.get("target_date"),
                "sla_health": "ON_TRACK",
            },
            "calculation": (
                "Source values used where available; missing canonical values "
                "generated deterministically for demonstration."
            ),
            "calculated_at": datetime.now(timezone.utc).isoformat(),
        },
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
            "exception_reason": "NONE",
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