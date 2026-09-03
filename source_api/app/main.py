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
        "erp_system": "SAP",
        "updated_at": "2026-09-03T10:25:00Z"
    }
]


@app.get("/orders")
def get_orders():
    return orders