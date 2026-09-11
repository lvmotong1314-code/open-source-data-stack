FROM apache/airflow:2.2.2-python3.8

RUN pip install dbt-postgres==1.1.0
RUN pip install great_expectations==0.15.5
RUN pip install "requests>=2,<3" "psycopg[binary]>=3,<4"

RUN pip install "jsonschema==3.2.0" "openapi-schema-validator==0.1.5" "openapi-spec-validator==0.3.1"