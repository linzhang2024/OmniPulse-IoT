import os
import multiprocessing
import logging

logger = logging.getLogger("gunicorn_config")

bind = os.getenv("GUNICORN_BIND", "0.0.0.0:8000")

workers = int(os.getenv("GUNICORN_WORKERS", max(2, multiprocessing.cpu_count())))

worker_class = os.getenv("GUNICORN_WORKER_CLASS", "uvicorn.workers.UvicornWorker")

worker_connections = int(os.getenv("GUNICORN_WORKER_CONNECTIONS", "5000"))

timeout = int(os.getenv("GUNICORN_TIMEOUT", "30"))

keepalive = int(os.getenv("GUNICORN_KEEPALIVE", "60"))

backlog = int(os.getenv("GUNICORN_BACKLOG", "4096"))

threads = int(os.getenv("GUNICORN_THREADS", "1"))

max_requests = int(os.getenv("GUNICORN_MAX_REQUESTS", "5000"))
max_requests_jitter = int(os.getenv("GUNICORN_MAX_REQUESTS_JITTER", "500"))

preload_app = os.getenv("GUNICORN_PRELOAD_APP", "false").lower() == "true"

reload = os.getenv("GUNICORN_RELOAD", "false").lower() == "true"

daemon = os.getenv("GUNICORN_DAEMON", "false").lower() == "true"

pidfile = os.getenv("GUNICORN_PIDFILE", "./gunicorn.pid")

accesslog = os.getenv("GUNICORN_ACCESSLOG", "-")
errorlog = os.getenv("GUNICORN_ERRORLOG", "-")

loglevel = os.getenv("GUNICORN_LOGLEVEL", "warning")

access_log_format = os.getenv(
    "GUNICORN_ACCESS_LOG_FORMAT",
    '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'
)

limit_request_line = int(os.getenv("GUNICORN_LIMIT_REQUEST_LINE", "8190"))
limit_request_fields = int(os.getenv("GUNICORN_LIMIT_REQUEST_FIELDS", "100"))
limit_request_field_size = int(os.getenv("GUNICORN_LIMIT_REQUEST_FIELD_SIZE", "8190"))

def post_fork(server, worker):
    server.log.info("Worker spawned (pid: %s)", worker.pid)
    
    try:
        import uvloop
        import asyncio
        
        policy = uvloop.EventLoopPolicy()
        asyncio.set_event_loop_policy(policy)
        
        server.log.info("[uvloop] Successfully enabled uvloop as default event loop")
    except ImportError:
        server.log.warning("[uvloop] uvloop not installed, using default asyncio event loop")
        server.log.info("[uvloop] Install with: pip install uvloop")
    except Exception as e:
        server.log.warning(f"[uvloop] Failed to enable uvloop: {e}")

def pre_fork(server, worker):
    pass

def pre_exec(server):
    server.log.info("Forked child, re-executing.")

def when_ready(server):
    server.log.info("Server is ready. Spawning workers")
    server.log.info(f"Bind: {bind}")
    server.log.info(f"Workers: {workers}")
    server.log.info(f"Worker class: {worker_class}")
    server.log.info(f"Worker connections: {worker_connections}")
    server.log.info(f"Timeout: {timeout}s")
    server.log.info(f"Max requests: {max_requests}")

def worker_int(worker):
    worker.log.info("worker received INT or QUIT signal")

def worker_abort(worker):
    worker.log.info("worker received SIGABRT signal")

_env_settings = {
    "APP_ENV": os.getenv("APP_ENV", "production"),
    "DATABASE_URL": os.getenv("DATABASE_URL", "sqlite:///./iot_devices.db"),
    "SECRET_KEY": os.getenv("SECRET_KEY", ""),
    "DEVICE_WEBHOOK_URL": os.getenv("DEVICE_WEBHOOK_URL", ""),
    "WEBHOOK_TIMEOUT": os.getenv("WEBHOOK_TIMEOUT", "5"),
    "HEARTBEAT_TIMEOUT": os.getenv("HEARTBEAT_TIMEOUT", "120"),
    "OFFLINE_THRESHOLD": os.getenv("OFFLINE_THRESHOLD", "120"),
    "PENDING_OFFLINE_THRESHOLD": os.getenv("PENDING_OFFLINE_THRESHOLD", "60"),
    "CHECK_INTERVAL": os.getenv("CHECK_INTERVAL", "30"),
    "COMMAND_TTL_SECONDS": os.getenv("COMMAND_TTL_SECONDS", "600"),
    "SIGNATURE_TIMESTAMP_TOLERANCE": os.getenv("SIGNATURE_TIMESTAMP_TOLERANCE", "60"),
    "TEMPERATURE_THRESHOLD": os.getenv("TEMPERATURE_THRESHOLD", "50"),
    "ALERT_CONSECUTIVE_THRESHOLD": os.getenv("ALERT_CONSECUTIVE_THRESHOLD", "3"),
    "MAX_MEMORY_EVENTS": os.getenv("MAX_MEMORY_EVENTS", "50"),
    "REPORTS_DIR": os.getenv("REPORTS_DIR", "./reports"),
    "EXPORT_BATCH_SIZE": os.getenv("EXPORT_BATCH_SIZE", "1000"),
    "DEFAULT_REPORT_RETENTION_HOURS": os.getenv("DEFAULT_REPORT_RETENTION_HOURS", "168"),
    "AUDIT_WAL_DIR": os.getenv("AUDIT_WAL_DIR", "./audit_wal"),
}

def on_starting(server):
    server.log.info("=" * 60)
    server.log.info("IoT Platform - Starting Gunicorn")
    server.log.info("=" * 60)
    server.log.info("Environment variables:")
    for key, value in _env_settings.items():
        if "SECRET" in key or "KEY" in key:
            if value:
                masked = value[:8] + "..." if len(value) > 8 else "***"
                server.log.info(f"  {key}: {masked}")
            else:
                server.log.info(f"  {key}: (not set)")
        else:
            server.log.info(f"  {key}: {value}")
    server.log.info("=" * 60)
