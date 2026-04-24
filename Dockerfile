FROM python:3.12-slim

LABEL maintainer="IoT Platform Team"
LABEL description="IoT Management Platform - Production Ready"
LABEL version="1.0.0"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir \
        gunicorn \
        uvicorn[standard] \
        psycopg2-binary \
        python-multipart

COPY . .

RUN mkdir -p /app/reports /app/logs /app/audit_wal /app/static

RUN useradd -m -u 1000 iotuser && \
    chown -R iotuser:iotuser /app

ENV APP_ENV=production

ENV DATABASE_URL="sqlite:///./iot_devices.db"

ENV SECRET_KEY=""

ENV DEVICE_WEBHOOK_URL=""
ENV WEBHOOK_TIMEOUT=5

ENV HEARTBEAT_TIMEOUT=120
ENV OFFLINE_THRESHOLD=120
ENV PENDING_OFFLINE_THRESHOLD=60
ENV CHECK_INTERVAL=30

ENV COMMAND_TTL_SECONDS=600
ENV SIGNATURE_TIMESTAMP_TOLERANCE=60

ENV TEMPERATURE_THRESHOLD=50
ENV ALERT_CONSECUTIVE_THRESHOLD=3

ENV MAX_MEMORY_EVENTS=50

ENV REPORTS_DIR="/app/reports"
ENV EXPORT_BATCH_SIZE=1000
ENV DEFAULT_REPORT_RETENTION_HOURS=168

ENV AUDIT_WAL_DIR="/app/audit_wal"

ENV GUNICORN_BIND="0.0.0.0:8000"
ENV GUNICORN_WORKERS=2
ENV GUNICORN_WORKER_CLASS="uvicorn.workers.UvicornWorker"
ENV GUNICORN_WORKER_CONNECTIONS=1000
ENV GUNICORN_TIMEOUT=120
ENV GUNICORN_KEEPALIVE=5
ENV GUNICORN_BACKLOG=2048
ENV GUNICORN_MAX_REQUESTS=10000
ENV GUNICORN_MAX_REQUESTS_JITTER=1000
ENV GUNICORN_LOGLEVEL="info"
ENV GUNICORN_ACCESSLOG="-"
ENV GUNICORN_ERRORLOG="-"

USER iotuser

VOLUME ["/app/reports", "/app/logs", "/app/audit_wal"]

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; r = httpx.get('http://localhost:8000/'); exit(0 if r.status_code == 200 else 1)" || exit 1

CMD ["gunicorn", "server.main:app", "-c", "gunicorn_conf.py"]
