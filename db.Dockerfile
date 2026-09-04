FROM python:3.11-slim

WORKDIR /app

COPY db/requirements.txt /app/db/requirements.txt

RUN pip install --no-cache-dir -r /app/db/requirements.txt

COPY . /app

CMD ["python3", "db/scripts/ingest_taxi_trips.py"]