from fastapi import FastAPI

app = FastAPI(title="Synthetic OTC API")


orders = [
    {
        "order_id": "ORD-10001",
        "customer_id": "CUST-001",
        "order_value": "$1,249.99",
        "promise_date": "09/15/2026",
        "target_date": "2026-09-17",
        "priority": "HIGH ",
        "shipment_id": "SHIP-10001",
        "warehouse_id": "WH-SF-01",
        "carrier": "USPS",
        "tracking_number": "9400111899223856928493",
        "order_status": "PROCESSING",
        "currency": "USD",
        "erp_system": "SAP",
        "updated_at": "2026-09-03T10:15:00Z"
    },
    {
        "order_id": "ORD-10002",
        "customer_id": "CUST-002",
        "order_value": "$849.50",
        "promise_date": "09/16/2026",
        "target_date": "2026-09-18",
        "priority": "MEDIUM",
        "shipment_id": "SHIP-10002",
        "warehouse_id": "WH-SF-01",
        "carrier": "UPS",
        "tracking_number": "1Z999AA10123456784",
        "order_status": "PROCESSING",
        "currency": "USD",
        "erp_system": "Oracle",
        "updated_at": "2026-09-03T10:20:00Z"
    },
    {
        "order_id": "ORD-10003",
        "customer_id": "CUST-001",
        "order_value": "$2,499.00",
        "promise_date": "09/14/2026",
        "target_date": "2026-09-16",
        "priority": "LOW",
        "shipment_id": "SHIP-10003",
        "warehouse_id": "WH-OAK-02",
        "carrier": "FedEx",
        "tracking_number": "784512345678",
        "order_status": "READY_TO_SHIP",
        "currency": "USD",
        "erp_system": "SAP",
        "updated_at": "2026-09-03T10:25:00Z"
    }
]


@app.get("/orders")
def get_orders():
    return orders