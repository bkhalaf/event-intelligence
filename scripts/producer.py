from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaProducer
from typing import List, Optional
import json
import time
import traceback
from dashboard_utils import (
    fetch_dashboard_data,
    calculate_metrics,
    get_latest_orders,
    get_sales_by_branch,
    get_branch_performance,
    get_sales_by_payment
)

app = FastAPI(
    title="Kafka Producer API",
    description="Receive customer orders and send them to Kafka"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TOPIC_NAME = "test-topic"
KAFKA_BROKER = "kafka:29092"

producer = None


class OrderItem(BaseModel):
    product_id: str
    product_name: str
    quantity: int
    unit_price: float


class OrderRequest(BaseModel):
    branch: str
    customer_name: str
    payment_method: str
    notes: Optional[str] = ""
    items: List[OrderItem]


def get_producer():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    return producer


def build_order_message(order: OrderRequest) -> str:
    items_summary = ", ".join(
        [f"{item.product_name} x{item.quantity}" for item in order.items]
    )

    notes_part = order.notes.strip() if order.notes else "No notes"

    return (
        f"Order from {order.customer_name} | "
        f"Branch: {order.branch} | "
        f"Payment: {order.payment_method} | "
        f"Items: {items_summary} | "
        f"Notes: {notes_part}"
    )


@app.post("/order")
def create_order(order: OrderRequest):
    try:
        message_text = build_order_message(order)

        message = {
            "data": {
                "message": message_text
            },
            "timestamp": time.time()
        }

        kafka_producer = get_producer()
        future = kafka_producer.send(TOPIC_NAME, value=message)
        result = future.get(timeout=10)

        return {
            "status": "success",
            "message": "Order sent to Kafka",
            "topic": TOPIC_NAME,
            "partition": result.partition,
            "offset": result.offset
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    return {"status": "healthy"}


from dashboard_utils import (
    fetch_dashboard_data,
    calculate_metrics,
    get_latest_orders,
    get_sales_by_branch,
    get_branch_performance,
)


@app.get("/get_metrics")
def get_metrics():
    try:
        orders = fetch_dashboard_data()
        return calculate_metrics(orders)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/latest_orders")
def latest_orders():
    try: 
        orders = fetch_dashboard_data()
        return get_latest_orders(orders)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sales_branch")
def sales_branch():
    try: 
        orders = fetch_dashboard_data()
        return get_sales_by_branch(orders)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/branch_performance")
def branch_performance():
    try:
        orders = fetch_dashboard_data()
        return get_branch_performance(orders)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
    
@app.get("/sales_by_payment")
def sales_by_payment():
    try:
         orders = fetch_dashboard_data()
         return get_sales_by_payment(orders)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))