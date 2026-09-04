FROM python:3.11-slim

WORKDIR /app

COPY dbt/requirements.txt /app/dbt/requirements.txt

RUN pip install --no-cache-dir -r dbt/requirements.txt

WORKDIR /app/dbt/chicago_taxi

ENTRYPOINT ["dbt"]

CMD ["--version"]