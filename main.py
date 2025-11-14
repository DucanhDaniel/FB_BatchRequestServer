from typing import List
import glob, json, os
import asyncio
from fastapi import FastAPI, Query, HTTPException, Body, Request
from fastapi.staticfiles import StaticFiles
from models import BatchRequest, RateLimitResponse, RateLimitRequest
from app_logging import log_batch_summary, log_batch_start, setup_logging
from facebook_logic import send_batch_to_facebook, summarize_rate_limits
from fastapi.responses import HTMLResponse
import time
import uuid
import logging


# --- CẤU HÌNH ---
REQUEST_TIMEOUT_SECONDS = 600.0
LOG_DIR = os.getenv("LOG_DIR", "logs")
setup_logging()
logger = logging.getLogger("FacebookBatchApp") # Lấy logger instance
# --- KHỞI TẠO ỨNG DỤNG FASTAPI ---S
app = FastAPI(
    title="Facebook Batch Request API",
    description="API client gửi batch requests đến Facebook Graph API (expose qua ngrok/uvicorn).",
    version="1.2.0",
)
app.mount("/static", StaticFiles(directory="static"), name="static")


# --- ENDPOINTS ---
# GET: tiện test nhanh (chú ý giới hạn độ dài URL khi nhiều params)
@app.get("/batch", summary="Gửi batch (GET) đến Facebook API")
async def process_batch_request_get(
    access_token: str = Query(..., description="Access Token Facebook", examples=["EAAB..."]),
    relative_urls: List[str] = Query(
        ...,
        description="Danh sách URL tương đối (tối đa 50). KHÔNG kèm v23.0/",
        examples=["act_123456789/ads?fields=id,name&limit=5", "act_123456789/campaigns?fields=id,name&limit=5"]
    )
):
    try:
        results = send_batch_to_facebook(relative_urls, access_token)
        return {"status": "success", "results": results}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=f"Bad Gateway: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

# POST: khuyên dùng khi gửi nhiều URL (không bị giới hạn độ dài)
@app.post("/batch", summary="Gửi batch (POST) đến Facebook API")
async def process_batch_request_post(payload: BatchRequest = Body(...),  http_request: Request = None):
    request_id = str(uuid.uuid4())
    start_time = time.time()
    client_ip = http_request.client.host if http_request else "unknown"

    log_batch_start(request_id, client_ip, len(payload.relative_urls))

    results = []
    summary = {"success_count": 0, "error_count": 0}
    status = "UNKNOWN"

    try:
        loop = asyncio.get_running_loop()
        
        task = loop.run_in_executor(
            None, 
            send_batch_to_facebook, # Tên hàm đồng bộ, không có ()
            payload.relative_urls,  # Các đối số cho hàm đó
            payload.access_token,
            request_id,
            payload.email,
            client_ip
            # Thêm các đối số khác nếu cần...
        )

        # Bây giờ task là awaitable, ta có thể dùng wait_for như bình thường
        results, summary = await asyncio.wait_for(task, timeout=REQUEST_TIMEOUT_SECONDS)
        # results, summary = send_batch_to_facebook(
        #     relative_urls=payload.relative_urls,
        #     access_token=payload.access_token,
        #     request_id=request_id
        # )
        if summary["error_count"] == 0: status = "SUCCESS"
        elif summary["success_count"] > 0: status = "PARTIAL_FAILURE"
        else: status = "TOTAL_FAILURE"
        return {"status": "success", "results": results}
    except ValueError as e:
        status = "CLIENT_ERROR"
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        status = "GATEWAY_ERROR"
        raise HTTPException(status_code=502, detail=f"Bad Gateway: {str(e)}")
    except Exception as e:
        status = "INTERNAL_ERROR"
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
    finally:
        log_batch_summary(
            request_id,
            start_time,
            client_ip,
            status,
            summary["success_count"],
            summary["error_count"],
            len(payload.relative_urls)
        )


# Health check
@app.get("/health")
def health():
    return {"ok": True}
 
@app.post("/rate_limit", response_model=RateLimitResponse, summary="Kiểm tra Rate Limit (POST)")
async def check_facebook_rate_limit(
    payload: RateLimitRequest = Body(...),
    http_request: Request = None
):
    """
    Kiểm tra nhanh giới hạn rate limit cho một danh sách tài khoản quảng cáo.
    """
    # print(payload)
    # 1. Khởi tạo các biến cho việc logging
    request_id = str(uuid.uuid4())
    start_time = time.time()
    client_ip = http_request.client.host if http_request else "unknown"
    batch_size = len(payload.ad_account_ids) # Lấy từ payload

    # Ghi log sự kiện bắt đầu
    log_batch_start(request_id, client_ip, batch_size)

    # Khởi tạo các biến trạng thái
    status = "UNKNOWN"
    summary_data = {}
    success_count = 0
    error_count = 0

    try:
        # Pydantic đã kiểm tra ad_account_ids không rỗng, không cần check lại
        # Tạo các request nhẹ để "khơi mào" API và lấy header
        relative_urls = [f"{acc_id}/insights?fields=account_id&level=account&date_preset=yesterday&limit=1" for acc_id in payload.ad_account_ids]

        # Gọi hàm send_batch và nhận kết quả
        _results, summary, all_headers = send_batch_to_facebook(
            relative_urls,
            payload.access_token, # Lấy từ payload
            request_id,
            get_header=True
        )

        # Lấy số lượng thành công/thất bại từ kết quả trả về
        success_count = summary.get("success_count", 0)
        error_count = summary.get("error_count", 0)

        # Xử lý thông tin rate limit
        summary_data = summarize_rate_limits(all_headers)

        status = "SUCCESS"
        return RateLimitResponse(
            summary=summary_data,
            message="Truy vấn thành công."
        )

    except HTTPException:
        # Re-raise HTTPException để FastAPI xử lý
        raise

    except Exception as e:
        status = "INTERNAL_ERROR"
        # Ghi log lỗi chi tiết ra file log để gỡ lỗi
        logger.error(
            f"Internal error in /rate_limit: {e}",
            exc_info=True, # Thêm traceback vào log
            extra={"request_id": request_id}
        )
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

    finally:
        # 3. Luôn luôn ghi log tóm tắt kết quả ở cuối
        log_batch_summary(
            request_id=request_id,
            start_time=start_time,
            client_ip=client_ip,
            overall_status=status,
            success_count=success_count,
            error_count=error_count,
            batch_size=batch_size
        )
 
 
        
# [ENDPOINT MỚI] Phục vụ trang dashboard
@app.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard():
    with open("static/dashboard.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read(), status_code=200)

# [ENDPOINT MỚI] Cung cấp dữ liệu log
@app.get("/logs", summary="Đọc và trả về dữ liệu từ các file log")
async def get_logs():
    all_log_entries = []
    # Tìm tất cả các file có đuôi .log trong thư mục logs
    log_files = glob.glob(f"{LOG_DIR}/*.json")
    
    for file_path in log_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        # Giả sử mỗi dòng là một JSON object
                        log_entry = json.loads(line)
                        all_log_entries.append(log_entry)
                    except json.JSONDecodeError:
                        # Bỏ qua các dòng không phải JSON
                        continue
        except Exception as e:
            print(f"Error reading log file {file_path}: {e}")

    return all_log_entries
        
import uvicorn
if __name__ == "__main__":
    uvicorn.run("main:app", host = "0.0.0.0", port = 8001)

