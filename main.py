from typing import List, Optional
import glob, json, os
from fastapi import FastAPI, Query, HTTPException, Body, Request
from fastapi.staticfiles import StaticFiles
from models import BatchRequest, RateLimitResponse, CleanupPayload
from app_logging import log_batch_summary, log_batch_start, setup_logging
from facebook_logic import send_batch_to_facebook, summarize_rate_limits
from fastapi.responses import HTMLResponse
from datetime import datetime, timedelta
import time
import uuid
import logging


# --- CẤU HÌNH ---
API_VERSION = "v23.0"
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
        results, summary = send_batch_to_facebook(
            relative_urls=payload.relative_urls,
            access_token=payload.access_token,
            request_id=request_id
        )
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

@app.get("/rate_limit", response_model=RateLimitResponse)
async def get_facebook_rate_limit(
    access_token: str = Query(..., description="Access Token Facebook."),
    ad_account_ids: List[str] = Query(..., description="Danh sách ID tài khoản quảng cáo."),
    http_request: Request = None # Thêm http_request để lấy IP client
):
    """
    Kiểm tra nhanh giới hạn rate limit cho một danh sách tài khoản quảng cáo.
    """
    # 1. Khởi tạo các biến cho việc logging
    request_id = str(uuid.uuid4())
    start_time = time.time()
    client_ip = http_request.client.host if http_request else "unknown"
    batch_size = len(ad_account_ids)

    # Ghi log sự kiện bắt đầu
    log_batch_start(request_id, client_ip, batch_size)

    # Khởi tạo các biến trạng thái
    status = "UNKNOWN"
    summary_data = {}
    success_count = 0
    error_count = 0

    try:
        if not ad_account_ids:
            status = "CLIENT_ERROR"
            raise HTTPException(status_code=400, detail="Vui lòng cung cấp ít nhất một ID tài khoản quảng cáo.")

        # Tạo các request nhẹ để "khơi mào" API và lấy header
        relative_urls = [f"{acc_id}/insights?fields=account_id&level=account&date_preset=yesterday&limit=1" for acc_id in ad_account_ids]
        print(relative_urls)
        # Gọi hàm send_batch và nhận kết quả
        # Giả sử hàm trả về (results, summary, all_headers)
        _results, summary, all_headers = send_batch_to_facebook(
            relative_urls, access_token, request_id, get_header=True
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
        # Re-raise HTTPException để FastAPI xử lý, status đã được set ở trên
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
        
# import uvicorn
# from pyngrok import ngrok
# if __name__ == "__main__":
#     # uvicorn.run("test:app", host = "0.0.0.0", port = 8001)
#     port = 8001
    
#     try:
#         # Mở một tunnel HTTP tới cổng 8001

#         public_url = ngrok.connect(port, "http")
#         print("="*50)
#         print(f" * Ngrok tunnel đang chạy tại: {public_url}")
#         print(f" * Uvicorn đang chạy trên http://127.0.0.1:{port}")
#         print("="*50)
        
#         # Chạy uvicorn. Lưu ý: truyền đối tượng 'app' trực tiếp
#         uvicorn.run(app, host="0.0.0.0", port=port)
        
#     except Exception as e:
#         print(f"Lỗi: {e}")
#     finally:
#         # Ngắt kết nối ngrok khi ứng dụng dừng
#         ngrok.disconnect(public_url.public_url)
#         print("Đã đóng kết nối ngrok.")
