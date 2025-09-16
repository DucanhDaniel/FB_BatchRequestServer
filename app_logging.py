import json
import requests
from typing import List, Dict, Any

from fastapi import FastAPI, Query, HTTPException, Body
from pydantic import BaseModel, Field, field_validator
from models import BatchRequest

USAGE_HEADERS_TO_LOG = {
    "x-business-use-case-usage",
    "x-fb-ads-insights-throttle",
    "x-ad-account-usage",
    "x-app-usage"
}

import logging
import logging.config
import os
import json
import time
from typing import Dict, Any, List

# --- CẤU HÌNH LOGGER ---

LOGS_DIR = "logs"
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

def setup_logging():
    """Cấu hình logger để ghi ra file JSON."""
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": "%(asctime)s %(levelname)s %(name)s %(message)s"
            },
        },
        "handlers": {
            "json_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "INFO",
                "formatter": "json",
                "filename": f"{LOGS_DIR}/fastapi.log.json",
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "encoding": "utf8"
            },
        },
        "root": {
            "level": "INFO",
            "handlers": ["json_file"]
        },
        "watchfiles": {
            "handlers": ["json_file"], # Gửi log của nó vào cùng file (tùy chọn)
            "level": "WARNING",       # Chỉ hiển thị log WARNING trở lên
            "propagate": False
        }
    })

# Tạo một logger instance để sử dụng trong module này
logger = logging.getLogger("FacebookBatchApp")


# --- CÁC HÀM LOGGING CỤ THỂ ---

def log_batch_start(request_id: str, client_ip: str, batch_size: int):
    """Ghi log khi một batch request được nhận."""
    logger.info(
        "Received batch request",
        extra={
            "log.type": "batch_start",
            "request_id": request_id,
            "client_ip": client_ip,
            "batch_size": batch_size
        }
    )

def log_batch_summary(
    request_id: str,
    start_time: float,
    client_ip: str,
    overall_status: str,
    success_count: int,
    error_count: int,
    batch_size: int
):
    """Ghi log tóm tắt khi một batch request hoàn thành."""
    total_duration_ms = (time.time() - start_time) * 1000
    logger.info(
        "Finished processing batch request",
        extra={
            "log.type": "batch_summary",
            "request_id": request_id,
            "client_ip": client_ip,
            "total_duration_ms": round(total_duration_ms, 2),
            "overall_status": overall_status,
            "success_count": success_count,
            "error_count": error_count,
            "batch_size": batch_size
        }
    )

def log_sub_request(
    request_id: str,
    request_index: int,
    fb_response_item: Dict[str, Any],
    processed_item: Dict[str, Any]
):
    """
    Ghi log chi tiết cho một sub-request sau khi được xử lý.
    Hàm này tự trích xuất thông tin từ phản hồi của Facebook.
    """
    # Trích xuất thông tin từ headers
    headers = {h.get("name", "").lower(): h.get("value", "") for h in fb_response_item.get("headers", [])}
    buc_info = json.loads(headers.get("x-business-use-case-usage", "{}"))
    throttle_info = json.loads(headers.get("x-fb-ads-insights-throttle", "{}"))
    app_usage = json.loads(headers.get("x-app-usage", "{}"))

    # Trích xuất duration từ BUC header
    duration_ms = None
    if buc_info:
        for _, entries in buc_info.items():
            if isinstance(entries, list) and len(entries) > 0:
                duration_ms = entries[0].get("total_time", 0) * 1000  # ms
                break

    # Chuẩn bị payload để log
    log_payload = {
        "log.type": "sub_request",
        "request_id": request_id,
        "request_index": request_index,
        "requested_url": processed_item.get("requested_url"),
        "status_code": processed_item.get("status_code"),
        "duration_ms": duration_ms,
        "rate_limit": {
            "app_id_util_pct": throttle_info.get("app_id_util_pct"),
            "acc_id_util_pct": throttle_info.get("acc_id_util_pct"),
            "business_use_case": buc_info,
            "app_call_count": app_usage.get("call_count"),
        },
        "fb_trace_id": None,
        "error": None
    }

    # Trích xuất thông tin lỗi nếu có
    if processed_item.get("status_code") != 200:
        error_body = processed_item.get("error", {})
        if isinstance(error_body, dict):
            log_payload["error"] = {
                "message": error_body.get("message"),
                "type": error_body.get("type"),
                "code": error_body.get("code"),
                "error_subcode": error_body.get("error_subcode")
            }
            log_payload["fb_trace_id"] = error_body.get("fbtrace_id")
        else:
            log_payload["error"] = {"message": "Unknown error structure."}

    message = f"Processed sub-request #{request_index}"
    # [THÊM MỚI] Kiểm tra cờ 'was_retried'
    if processed_item.get("was_retried"):
        if processed_item.get("status_code") == 200:
            message += " - RETRY SUCCESS"
        else:
            message += f" - RETRY FAILED with code {processed_item.get('status_code')}"

    # Thay thế logger.info cũ bằng message mới này
    logger.info(message, extra=log_payload)
    