FROM python:3.12.6

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

ENV PYTHONPATH=/app/src

WORKDIR /app

RUN pip install --upgrade pip setuptools

COPY src/app_simulation /app/src
COPY docker/app_backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt