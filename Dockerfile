FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/producer.py scripts/seed.py dashboard_utils.py ./

EXPOSE 5000

CMD ["uvicorn", "producer:app", "--host", "0.0.0.0", "--port", "5000"]