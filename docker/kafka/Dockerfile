FROM python:3.12.6

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

WORKDIR /app

COPY docker/kafka/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/streaming /app/streaming

