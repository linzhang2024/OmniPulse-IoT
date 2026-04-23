import csv
import io
import os
import uuid
import threading
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Generator, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc
from contextlib import contextmanager

logger = logging.getLogger("ReportEngine")

REPORTS_DIR = Path("./reports")
EXPORT_BATCH_SIZE = 1000
DEFAULT_REPORT_RETENTION_HOURS = 24
MAX_RECENT_TASKS = 100

from server.models import (
    ReportTask, ReportTaskStatus, 
    ScheduledReportConfig, ScheduledReportType,
    DeviceDataHistory, Device, User
)

class ReportExportEngine:
    def __init__(self):
        self._export_lock = threading.Lock()
        self._active_tasks: Dict[str, threading.Thread] = {}
        self._task_callbacks: Dict[str, List] = {}
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        self._scheduled_configs: Dict[str, Dict] = {}
        
    def ensure_reports_dir(self):
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    
    def _get_csv_headers(self, include_payload: bool = True) -> List[str]:
        headers = [
            "id",
            "device_id",
            "timestamp",
            "temperature",
            "humidity",
            "is_alert",
        ]
        if include_payload:
            headers.append("payload_json")
        return headers
    
    def _format_row(self, record: DeviceDataHistory, include_payload: bool = True) -> List[Any]:
        row = [
            record.id,
            record.device_id,
            record.timestamp.isoformat() if record.timestamp else "",
            record.temperature if record.temperature is not None else "",
            record.humidity if record.humidity is not None else "",
            "1" if record.is_alert else "0",
        ]
        if include_payload:
            import json
            row.append(json.dumps(record.payload, ensure_ascii=False) if record.payload else "")
        return row
    
    def stream_history_to_csv_generator(
        self,
        db: Session,
        device_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        include_payload: bool = True,
        batch_size: int = EXPORT_BATCH_SIZE
    ) -> Generator[Tuple[int, List[DeviceDataHistory]], None, None]:
        query = db.query(DeviceDataHistory).filter(
            DeviceDataHistory.device_id == device_id
        )
        
        if start_time:
            query = query.filter(DeviceDataHistory.timestamp >= start_time)
        if end_time:
            query = query.filter(DeviceDataHistory.timestamp <= end_time)
        
        total_count = query.count()
        logger.info(f"[ReportEngine] Starting export for device {device_id}: {total_count} records total")
        
        offset = 0
        while offset < total_count:
            batch = query.order_by(DeviceDataHistory.timestamp.asc()).offset(offset).limit(batch_size).all()
            if not batch:
                break
            
            yield total_count, batch
            offset += len(batch)
    
    def export_to_csv_file(
        self,
        db: Session,
        device_id: str,
        output_path: Path,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        include_payload: bool = True,
        progress_callback: Optional[callable] = None
    ) -> Tuple[int, int]:
        headers = self._get_csv_headers(include_payload)
        total_records = 0
        file_size = 0
        
        with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            
            for total_count, batch in self.stream_history_to_csv_generator(
                db, device_id, start_time, end_time, include_payload
            ):
                for record in batch:
                    row = self._format_row(record, include_payload)
                    writer.writerow(row)
                    total_records += 1
                
                if progress_callback:
                    progress = int((total_records / total_count) * 100) if total_count > 0 else 0
                    progress_callback(total_records, total_count, progress)
                
                f.flush()
                logger.debug(f"[ReportEngine] Export progress: {total_records}/{total_count} records")
        
        file_size = output_path.stat().st_size
        return total_records, file_size
    
    def export_to_csv_stream(
        self,
        db: Session,
        device_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        include_payload: bool = True
    ) -> Generator[str, None, None]:
        headers = self._get_csv_headers(include_payload)
        output = io.StringIO()
        writer = csv.writer(output)
        
        writer.writerow(headers)
        yield output.getvalue()
        output.seek(0)
        output.truncate()
        
        for total_count, batch in self.stream_history_to_csv_generator(
            db, device_id, start_time, end_time, include_payload
        ):
            for record in batch:
                row = self._format_row(record, include_payload)
                writer.writerow(row)
            yield output.getvalue()
            output.seek(0)
            output.truncate()
    
    def create_export_task(
        self,
        db: Session,
        device_id: str,
        task_name: Optional[str] = None,
        user_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        include_payload: bool = True,
        retention_hours: int = DEFAULT_REPORT_RETENTION_HOURS
    ) -> ReportTask:
        task_id = str(uuid.uuid4())
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_device_id = device_id.replace('/', '_').replace('\\', '_')
        file_name = f"report_{safe_device_id}_{timestamp}.csv"
        file_path = REPORTS_DIR / file_name
        
        expire_at = datetime.utcnow() + timedelta(hours=retention_hours)
        
        task = ReportTask(
            id=task_id,
            task_name=task_name or f"Export_{device_id}_{timestamp}",
            device_id=device_id,
            user_id=user_id,
            status=ReportTaskStatus.PENDING,
            file_name=file_name,
            file_path=str(file_path),
            expire_at=expire_at,
            filters={
                "start_time": start_time.isoformat() if start_time else None,
                "end_time": end_time.isoformat() if end_time else None,
                "include_payload": include_payload
            },
            export_format="csv"
        )
        
        db.add(task)
        db.commit()
        db.refresh(task)
        
        logger.info(f"[ReportEngine] Created export task: {task_id} for device {device_id}")
        return task
    
    def _update_task_status(
        self,
        db: Session,
        task_id: str,
        status: ReportTaskStatus,
        progress: Optional[int] = None,
        record_count: Optional[int] = None,
        file_size_bytes: Optional[int] = None,
        error_message: Optional[str] = None,
        download_url: Optional[str] = None
    ) -> Optional[ReportTask]:
        task = db.query(ReportTask).filter(ReportTask.id == task_id).first()
        if not task:
            return None
        
        task.status = status
        if progress is not None:
            task.progress = progress
        if record_count is not None:
            task.record_count = record_count
        if file_size_bytes is not None:
            task.file_size_bytes = file_size_bytes
        if error_message is not None:
            task.error_message = error_message
        if download_url is not None:
            task.download_url = download_url
        
        if status == ReportTaskStatus.PROCESSING and task.start_time is None:
            task.start_time = datetime.utcnow()
        if status in [ReportTaskStatus.COMPLETED, ReportTaskStatus.FAILED]:
            task.completed_at = datetime.utcnow()
        
        db.commit()
        db.refresh(task)
        return task
    
    def _execute_export_task(
        self,
        task_id: str,
        db_session_factory,
        device_id: str,
        file_path: Path,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
        include_payload: bool
    ):
        db = db_session_factory()
        try:
            self._update_task_status(db, task_id, ReportTaskStatus.PROCESSING, progress=0)
            
            def progress_callback(current: int, total: int, progress_pct: int):
                self._update_task_status(db, task_id, ReportTaskStatus.PROCESSING, progress=progress_pct)
            
            total_records, file_size = self.export_to_csv_file(
                db, device_id, file_path, start_time, end_time, include_payload,
                progress_callback
            )
            
            download_url = f"/api/reports/download/{task_id}"
            
            self._update_task_status(
                db, task_id, ReportTaskStatus.COMPLETED,
                progress=100,
                record_count=total_records,
                file_size_bytes=file_size,
                download_url=download_url
            )
            
            logger.info(f"[ReportEngine] Task {task_id} completed: {total_records} records, {file_size} bytes")
            
            self._trigger_callbacks(task_id, "completed", {
                "task_id": task_id,
                "record_count": total_records,
                "file_size": file_size
            })
            
        except Exception as e:
            logger.error(f"[ReportEngine] Task {task_id} failed: {e}", exc_info=True)
            try:
                self._update_task_status(
                    db, task_id, ReportTaskStatus.FAILED,
                    error_message=str(e)
                )
                self._trigger_callbacks(task_id, "failed", {
                    "task_id": task_id,
                    "error": str(e)
                })
            except:
                pass
        finally:
            db.close()
            with self._export_lock:
                if task_id in self._active_tasks:
                    del self._active_tasks[task_id]
    
    def start_export_task_async(
        self,
        db: Session,
        task_id: str,
        db_session_factory
    ) -> bool:
        task = db.query(ReportTask).filter(ReportTask.id == task_id).first()
        if not task:
            return False
        
        if task.status != ReportTaskStatus.PENDING:
            logger.warning(f"[ReportEngine] Task {task_id} is not in PENDING state: {task.status}")
            return False
        
        filters = task.filters or {}
        start_time = None
        if filters.get("start_time"):
            try:
                start_time = datetime.fromisoformat(filters["start_time"])
            except:
                pass
        
        end_time = None
        if filters.get("end_time"):
            try:
                end_time = datetime.fromisoformat(filters["end_time"])
            except:
                pass
        
        include_payload = filters.get("include_payload", True)
        file_path = Path(task.file_path) if task.file_path else None
        
        if not file_path:
            self._update_task_status(
                db, task_id, ReportTaskStatus.FAILED,
                error_message="Invalid file path"
            )
            return False
        
        self.ensure_reports_dir()
        
        thread = threading.Thread(
            target=self._execute_export_task,
            args=(task_id, db_session_factory, task.device_id, file_path, start_time, end_time, include_payload),
            daemon=True
        )
        
        with self._export_lock:
            self._active_tasks[task_id] = thread
        
        thread.start()
        logger.info(f"[ReportEngine] Started async export task: {task_id}")
        return True
    
    def get_task_status(self, db: Session, task_id: str) -> Optional[Dict]:
        task = db.query(ReportTask).filter(ReportTask.id == task_id).first()
        if not task:
            return None
        
        return {
            "id": task.id,
            "task_name": task.task_name,
            "device_id": task.device_id,
            "status": task.status.value,
            "progress": task.progress,
            "record_count": task.record_count,
            "file_size_bytes": task.file_size_bytes,
            "file_name": task.file_name,
            "download_url": task.download_url,
            "error_message": task.error_message,
            "start_time": task.start_time.isoformat() if task.start_time else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            "expire_at": task.expire_at.isoformat() if task.expire_at else None,
            "created_at": task.created_at.isoformat() if task.created_at else None
        }
    
    def cleanup_expired_reports(self, db: Session) -> int:
        now = datetime.utcnow()
        expired_tasks = db.query(ReportTask).filter(
            ReportTask.expire_at <= now,
            ReportTask.status.in_([ReportTaskStatus.COMPLETED, ReportTaskStatus.FAILED])
        ).all()
        
        cleaned_count = 0
        for task in expired_tasks:
            try:
                if task.file_path and os.path.exists(task.file_path):
                    os.unlink(task.file_path)
                    logger.info(f"[ReportEngine] Removed expired report file: {task.file_path}")
                db.delete(task)
                cleaned_count += 1
            except Exception as e:
                logger.warning(f"[ReportEngine] Failed to cleanup task {task.id}: {e}")
        
        if cleaned_count > 0:
            db.commit()
            logger.info(f"[ReportEngine] Cleaned up {cleaned_count} expired reports")
        
        return cleaned_count
    
    def register_callback(self, task_id: str, callback: callable, event: str = "completed"):
        with self._export_lock:
            if task_id not in self._task_callbacks:
                self._task_callbacks[task_id] = []
            self._task_callbacks[task_id].append({
                "callback": callback,
                "event": event
            })
    
    def _trigger_callbacks(self, task_id: str, event: str, data: Dict):
        with self._export_lock:
            callbacks = self._task_callbacks.get(task_id, [])
        
        for cb_info in callbacks:
            if cb_info["event"] == event or cb_info["event"] == "all":
                try:
                    cb_info["callback"](event, data)
                except Exception as e:
                    logger.warning(f"[ReportEngine] Callback error for task {task_id}: {e}")
        
        with self._export_lock:
            if task_id in self._task_callbacks:
                del self._task_callbacks[task_id]

class ScheduledReportManager:
    def __init__(self, report_engine: ReportExportEngine, scheduler=None):
        self.engine = report_engine
        self.scheduler = scheduler
        self._configs_cache: Dict[str, ScheduledReportConfig] = {}
        self._job_ids: Dict[str, str] = {}
        
    def load_active_configs(self, db: Session):
        active_configs = db.query(ScheduledReportConfig).filter(
            ScheduledReportConfig.is_active == True
        ).all()
        
        for config in active_configs:
            self._configs_cache[config.id] = config
        
        logger.info(f"[ScheduledReportManager] Loaded {len(self._configs_cache)} active scheduled report configs")
    
    def _create_default_daily_config(self, db: Session) -> ScheduledReportConfig:
        config = ScheduledReportConfig(
            id=str(uuid.uuid4()),
            name="每日运行简报",
            description="每日 8 点自动生成昨日设备运行简报",
            report_type=ScheduledReportType.DAILY_BRIEFING,
            cron_expression="0 8 * * *",
            output_directory="./reports/daily",
            retention_days=7,
            file_name_template="daily_briefing_{date}.csv",
            is_active=True
        )
        db.add(config)
        db.commit()
        db.refresh(config)
        return config
    
    def ensure_daily_briefing_config(self, db: Session) -> ScheduledReportConfig:
        existing = db.query(ScheduledReportConfig).filter(
            ScheduledReportConfig.report_type == ScheduledReportType.DAILY_BRIEFING
        ).first()
        
        if existing:
            return existing
        
        return self._create_default_daily_config(db)
    
    def generate_daily_briefing_filename(self, date: datetime, template: str = None) -> str:
        if template:
            return template.format(date=date.strftime("%Y%m%d"))
        return f"daily_briefing_{date.strftime('%Y%m%d')}.csv"
    
    def _execute_scheduled_report(self, config_id: str, db_session_factory):
        db = db_session_factory()
        try:
            config = db.query(ScheduledReportConfig).filter(
                ScheduledReportConfig.id == config_id
            ).first()
            
            if not config or not config.is_active:
                logger.warning(f"[ScheduledReport] Config {config_id} not found or inactive")
                return
            
            config.last_run_at = datetime.utcnow()
            config.last_run_status = "running"
            db.commit()
            
            logger.info(f"[ScheduledReport] Executing {config.report_type.value}: {config.name}")
            
            if config.report_type == ScheduledReportType.DAILY_BRIEFING:
                self._generate_daily_briefing(db, config, db_session_factory)
            else:
                logger.warning(f"[ScheduledReport] Unsupported report type: {config.report_type}")
            
            config.last_run_status = "success"
            db.commit()
            logger.info(f"[ScheduledReport] Completed {config.name}")
            
        except Exception as e:
            logger.error(f"[ScheduledReport] Failed to execute report {config_id}: {e}", exc_info=True)
            try:
                config = db.query(ScheduledReportConfig).filter(
                    ScheduledReportConfig.id == config_id
                ).first()
                if config:
                    config.last_run_status = "failed"
                    config.last_run_error = str(e)
                    db.commit()
            except:
                pass
        finally:
            db.close()
    
    def _generate_daily_briefing(
        self, 
        db: Session, 
        config: ScheduledReportConfig,
        db_session_factory
    ):
        now = datetime.utcnow()
        yesterday = now - timedelta(days=1)
        start_of_yesterday = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
        end_of_yesterday = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
        
        output_dir = Path(config.output_directory or "./reports/daily")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        devices = db.query(Device).all()
        
        for device in devices:
            history_count = db.query(DeviceDataHistory).filter(
                DeviceDataHistory.device_id == device.device_id,
                DeviceDataHistory.timestamp >= start_of_yesterday,
                DeviceDataHistory.timestamp <= end_of_yesterday
            ).count()
            
            if history_count == 0:
                continue
            
            file_name = f"daily_briefing_{device.device_id}_{yesterday.strftime('%Y%m%d')}.csv"
            file_path = output_dir / file_name
            
            task = self.engine.create_export_task(
                db,
                device_id=device.device_id,
                task_name=f"每日简报_{device.device_id}_{yesterday.strftime('%Y-%m-%d')}",
                start_time=start_of_yesterday,
                end_time=end_of_yesterday,
                include_payload=True,
                retention_hours=config.retention_days * 24
            )
            
            task.file_path = str(file_path)
            task.file_name = file_name
            db.commit()
            
            self.engine.start_export_task_async(db, task.id, db_session_factory)
            
            logger.info(f"[DailyBriefing] Created export task for device {device.device_id}: {task.id}")
    
    def schedule_all_active(self, db: Session):
        if not self.scheduler:
            logger.warning("[ScheduledReportManager] No scheduler available, skipping scheduling")
            return
        
        active_configs = db.query(ScheduledReportConfig).filter(
            ScheduledReportConfig.is_active == True
        ).all()
        
        for config in active_configs:
            self.schedule_config(config, db)
    
    def schedule_config(self, config: ScheduledReportConfig, db: Session):
        if not self.scheduler:
            return
        
        if config.id in self._job_ids:
            try:
                self.scheduler.remove_job(self._job_ids[config.id])
            except:
                pass
        
        cron_expr = config.cron_expression or "0 8 * * *"
        
        try:
            from apscheduler.triggers.cron import CronTrigger
            
            cron_parts = cron_expr.split()
            if len(cron_parts) == 5:
                trigger = CronTrigger(
                    minute=cron_parts[0],
                    hour=cron_parts[1],
                    day=cron_parts[2],
                    month=cron_parts[3],
                    day_of_week=cron_parts[4]
                )
            else:
                trigger = CronTrigger(hour=8, minute=0)
            
            job = self.scheduler.add_job(
                self._execute_scheduled_report,
                trigger=trigger,
                args=[config.id, lambda: db.session_factory() if hasattr(db, 'session_factory') else None],
                id=f"scheduled_report_{config.id}",
                name=f"Scheduled Report: {config.name}",
                replace_existing=True
            )
            
            self._job_ids[config.id] = job.id
            logger.info(f"[ScheduledReportManager] Scheduled {config.name} with cron: {cron_expr}")
            
        except Exception as e:
            logger.error(f"[ScheduledReportManager] Failed to schedule config {config.id}: {e}")

report_engine = ReportExportEngine()
