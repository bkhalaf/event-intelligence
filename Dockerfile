FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts ./scripts
COPY config ./config
COPY scripts/dashboard_utils.py ./
COPY scripts/seed.py ./

EXPOSE 5000

CMD ["uvicorn", "scripts.producer:app", "--host", "0.0.0.0", "--port", "5000"]