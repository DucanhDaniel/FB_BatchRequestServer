import json
import requests
from typing import List, Dict, Any
import uvicorn
import sys

from fastapi import FastAPI, Query, HTTPException, Body
from pydantic import BaseModel, Field, field_validator

# --- CẤU HÌNH ---
API_VERSION = "v23.0"

# --- KHỞI TẠO ỨNG DỤNG FASTAPI ---
app = FastAPI(
    title="Facebook Batch Request API",
    description="API client gửi batch requests đến Facebook Graph API (expose qua ngrok/uvicorn).",
    version="1.2.0",
)

# --- Pydantic models ---
class BatchRequest(BaseModel):
    access_token: str = Field(..., description="Facebook Graph API Access Token", examples=["EAAB..."])
    relative_urls: List[str] = Field(
        ...,
        description="Danh sách URL tương đối (tối đa 50), KHÔNG chứa version (v23.0).",
        examples=[[
            "act_123456789/ads?fields=id,name&limit=5",
            "act_123456789/campaigns?fields=id,name,objective&limit=5",
            "act_123456789/adsets?fields=id,name&limit=5"
        ]]
    )

    @field_validator("relative_urls")
    def validate_urls(cls, v):
        if not 1 <= len(v) <= 50:
            raise ValueError(f"Số lượng URL phải từ 1 đến 50. Hiện tại là {len(v)}.")
        return v

# --- LÕI GỬI BATCH ---
def send_batch_to_facebook(
    relative_urls: List[str],
    access_token: str,
    api_version: str = API_VERSION,
    timeout_sec: int = 120
) -> List[Dict[str, Any]]:
    """
    Gửi tối đa 50 yêu cầu trong 1 batch tới Facebook Graph API và trả về kết quả đã xử lý.
    """
    if not access_token or "YOUR_ACCESS_TOKEN" in access_token:
        raise ValueError("Bạn phải cung cấp một access_token hợp lệ.")

    if not 1 <= len(relative_urls) <= 50:
        raise ValueError(f"Số lượng URL phải từ 1 đến 50. Hiện tại là {len(relative_urls)}.")

    # Chuẩn hóa relative_url: bỏ leading '/', chặn kèm version
    normalized_urls = []
    for url in relative_urls:
        u = url.lstrip("/")  # bỏ '/' đầu
        if u.startswith(f"{api_version}/") or u.startswith("v") and u.split("/", 1)[0] == api_version:
            raise ValueError(f"relative_url không được chứa version: {url}")
        normalized_urls.append(u)

    # Endpoint batch: KHÔNG có dấu '/' ở cuối
    api_url = f"https://graph.facebook.com/{api_version}"

    batch_payload = [{"method": "GET", "relative_url": u} for u in normalized_urls]

    payload = {
        "access_token": access_token,
        "batch": json.dumps(batch_payload, ensure_ascii=False),
        "include_headers": "true"  # thêm để debug giới hạn/rate nếu cần
    }

    try:
        resp = requests.post(api_url, data=payload, timeout=timeout_sec)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Lỗi khi gọi đến Facebook API: {e}") from e

    try:
        data = resp.json()
    except json.JSONDecodeError:
        raise RuntimeError(f"Không thể parse JSON từ Facebook. Phản hồi: {resp.text[:1000]}")

    if isinstance(data, dict) and "error" in data:
        err = data["error"]
        msg = err.get("message")
        code = err.get("code")
        etype = err.get("type")
        raise RuntimeError(f"Lỗi top-level từ Facebook: {msg} (type={etype}, code={code})")

    processed_results: List[Dict[str, Any]] = []
    if not isinstance(data, list):
        # Tránh crash khi Facebook trả về định dạng lạ
        raise RuntimeError(f"Phản hồi không phải list như kỳ vọng. Raw: {data}")

    for i, item in enumerate(data):
        result_item: Dict[str, Any] = {
            "request_index": i,
            "requested_url": normalized_urls[i] if i < len(normalized_urls) else None,
            "status_code": None,
            "data": None,
            "error": None
        }

        if item is None:
            result_item["error"] = "Kết quả NULL (yêu cầu có thể thất bại hoặc bị bỏ qua)."
            processed_results.append(result_item)
            continue

        result_item["status_code"] = item.get("code")
        body_text = item.get("body", "")

        try:
            body_json = json.loads(body_text) if body_text else {}
        except json.JSONDecodeError:
            result_item["error"] = f"Body không phải JSON. Raw: {body_text[:500]}"
            processed_results.append(result_item)
            continue

        if result_item["status_code"] == 200:
            result_item["data"] = body_json
        else:
            # Lỗi ở yêu cầu con
            if isinstance(body_json, dict) and "error" in body_json:
                result_item["error"] = body_json["error"]
            else:
                result_item["error"] = body_json

        processed_results.append(result_item)

    return processed_results

# --- ENDPOINTS ---

# GET: tiện test nhanh (chú ý giới hạn độ dài URL khi nhiều params)
@app.get("/batch", summary="Gửi batch (GET) đến Facebook API")
async def process_batch_request_get(
    access_token: str = Query(..., description="Access Token Facebook", example="EAAB..."),
    relative_urls: List[str] = Query(
        ...,
        description="Danh sách URL tương đối (tối đa 50). KHÔNG kèm v23.0/",
        example=["act_123456789/ads?fields=id,name&limit=5", "act_123456789/campaigns?fields=id,name&limit=5"]
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
async def process_batch_request_post(payload: BatchRequest = Body(...)):
    try:
        results = send_batch_to_facebook(payload.relative_urls, payload.access_token)
        return {"status": "success", "results": results}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=f"Bad Gateway: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")

# Health check
@app.get("/health")
def health():
    return {"ok": True}

# def run_app():
#     """Chạy ứng dụng với Uvicorn (có thể bật ngrok nếu muốn)."""
#     port = 8000
#     print("🚀 Bắt đầu khởi tạo server...")
#     # Bật ngrok nếu cần:
#     public_url = ngrok.connect(port, "http")
#     print(f"✅ Ngrok URL: {public_url}")
#     print(f"📚 Swagger: {public_url}/docs")

#     uvicorn.run(app, host="0.0.0.0", port=port)

# if __name__ == "__main__":
#     run_app()
