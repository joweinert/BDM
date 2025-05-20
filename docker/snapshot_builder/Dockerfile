FROM python:3.10-slim

RUN pip install --upgrade pip setuptools
COPY docker/snapshot_builder/requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app
COPY src/scripts/exploitation_zone /app/scripts

ENTRYPOINT ["python", "/app/scripts/populate_duckdb.py"]
