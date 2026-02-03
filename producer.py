from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import time

app = FastAPI(title="Kafka Producer API", description="Send messages to Kafka")

TOPIC_NAME = 'test-topic'
KAFKA_BROKER = 'kafka:29092'

producer = None

class Message(BaseModel):
    message: str

def get_producer():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    return producer

@app.post("/send")
def send_message(data: Message):
    try:
        message = {
            'data': data.model_dump(),
            'timestamp': time.time()
        }

        kafka_producer = get_producer()
        future = kafka_producer.send(TOPIC_NAME, value=message)
        result = future.get(timeout=10)

        return {
            'status': 'success',
            'message': 'Message sent to Kafka',
            'topic': TOPIC_NAME,
            'partition': result.partition,
            'offset': result.offset
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {'status': 'healthy'}
