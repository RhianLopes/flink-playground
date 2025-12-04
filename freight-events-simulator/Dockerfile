FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY freight_events_simulator.py .

ENV KAFKA_BOOTSTRAP="kafka.kafka.svc.cluster.local:9092" \
    TOPIC_FREIGHT_EVENTS="freight.events" \
    TOPIC_TRACKING_EVENTS="tracking.events" \
    EVENT_INTERVAL_SECONDS="1" \
    CLIENT_ID="freight-events-simulator"

CMD ["python", "-u", "freight_events_simulator.py"]
