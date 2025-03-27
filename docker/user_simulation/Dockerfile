FROM python:3.12.6

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

ENV PYTHONPATH=/app/app_simulation


WORKDIR /app

RUN pip install --upgrade pip setuptools

COPY src/app_simulation /app/src
COPY docker/user_simulation/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt