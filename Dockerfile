FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app/backend

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts ./scripts
COPY backend ./backend
COPY config ./config
COPY scripts/seed.py ./

EXPOSE 5000

CMD ["uvicorn", "backend.api:app", "--host", "0.0.0.0", "--port", "5000"]