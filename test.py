import json
import requests
from typing import List, Dict, Any
from collections import defaultdict

from fastapi import FastAPI, Query, HTTPException, Body, Request
from pydantic import BaseModel, Field, field_validator
from models import BatchRequest, RateLimitResponse
from app_logging import _log_sub_request_headers, log_sub_request, log_batch_summary, log_batch_start, setup_logging
import time
import uuid

# --- CẤU HÌNH ---
API_VERSION = "v23.0"
setup_logging()
# --- KHỞI TẠO ỨNG DỤNG FASTAPI ---S
app = FastAPI(
    title="Facebook Batch Request API",
    description="API client gửi batch requests đến Facebook Graph API (expose qua ngrok/uvicorn).",
    version="1.2.0",
)

# --- LÕI GỬI BATCH ---
def send_batch_to_facebook(
    relative_urls: List[str],
    access_token: str,
    request_id: str,
    api_version: str = API_VERSION,
    timeout_sec: int = 120,
    get_header = False
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
    all_headers: List[Dict[str, Any]] = []
    success_count = 0
    error_count = 0
    
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
        

        _log_sub_request_headers(
            request_index=i, 
            requested_url=result_item['requested_url'], 
            headers_list=item.get("headers", [])
        )

        all_headers.append(item.get("headers", []))

        result_item["status_code"] = item.get("code")
        body_text = item.get("body", "")

        try:
            body_json = json.loads(body_text) if body_text else {}
        except json.JSONDecodeError:
            result_item["error"] = f"Body không phải JSON. Raw: {body_text[:500]}"
            processed_results.append(result_item)
            continue

        if result_item["status_code"] == 200:
            success_count += 1
            result_item["data"] = body_json
        else:
            error_count += 1
            # Lỗi ở yêu cầu con
            if isinstance(body_json, dict) and "error" in body_json:
                result_item["error"] = body_json["error"]
            else:
                result_item["error"] = body_json

        processed_results.append(result_item)
        
        log_sub_request(request_id, i, item, result_item)

    summary = {"success_count": success_count, "error_count": error_count}

    if get_header:
        return processed_results, summary, all_headers 

    return processed_results, summary

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


import json
from typing import List, Dict, Any
from collections import defaultdict

def summarize_rate_limits(all_sub_request_headers: List[List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Phân tích danh sách header từ các sub-request của một batch,
    trả về thông tin rate limit với định dạng là một danh sách các tài khoản.
    """
    account_details_dict = defaultdict(lambda: {
        "insights_usage_pct": 0.0,
        "eta_seconds": 0,
        "business_use_cases": []
    })
    max_app_usage = 0.0

    # --- PHẦN LOGIC PARSE HEADER GIỮ NGUYÊN NHƯ TRƯỚC ---
    for headers_list in all_sub_request_headers:
        if not headers_list:
            continue
        headers_dict = {h.get("name", "").lower(): h.get("value") for h in headers_list}
        try:
            throttle_str = headers_dict.get("x-fb-ads-insights-throttle", "{}")
            throttle_data = json.loads(throttle_str)
        except (json.JSONDecodeError, TypeError):
            throttle_data = {}
        try:
            buc_str = headers_dict.get("x-business-use-case-usage", "{}")
            buc_data = json.loads(buc_str)
        except (json.JSONDecodeError, TypeError):
            buc_data = {}
        
        app_usage_pct = throttle_data.get("app_id_util_pct", 0.0)
        if isinstance(app_usage_pct, (int, float)):
            max_app_usage = max(max_app_usage, float(app_usage_pct))
            
        for acc_id, entries in buc_data.items():
            account_key = f"act_{acc_id}"
            if isinstance(entries, list):
                account_details_dict[account_key]["business_use_cases"].extend(entries)
                max_eta_for_acc = max((entry.get("estimated_time_to_regain_access", 0) for entry in entries), default=0)
                account_details_dict[account_key]["eta_seconds"] = max(account_details_dict[account_key]["eta_seconds"], max_eta_for_acc)

            acc_usage_pct = throttle_data.get("acc_id_util_pct", 0.0)
            if isinstance(acc_usage_pct, (int, float)):
                 account_details_dict[account_key]["insights_usage_pct"] = max(account_details_dict[account_key]["insights_usage_pct"], float(acc_usage_pct))

    # --- THAY ĐỔI TỪ ĐÂY ---
    # Chuyển đổi dictionary đã xử lý thành một list các dictionary
    summary_list = []
    for acc_id, details in account_details_dict.items():
        summary_list.append({
            "account_id": acc_id,
            "insights_usage_pct": details.get("insights_usage_pct", 0.0),
            "eta_seconds": details.get("eta_seconds", 0),
            "business_use_cases": details.get("business_use_cases", [])
        })

    return {
        "app_usage_pct": max_app_usage,
        "account_details": summary_list # Trả về một list thay vì dict
    }
    
@app.get("/rate_limit", response_model=RateLimitResponse)
async def get_facebook_rate_limit(
    access_token: str = Query(..., description="Access Token Facebook."),
    ad_account_ids: List[str] = Query(..., description="Danh sách ID tài khoản quảng cáo.")
):
    """
    Kiểm tra nhanh giới hạn rate limit cho một danh sách tài khoản quảng cáo.
    """
    if not ad_account_ids:
        raise HTTPException(status_code=400, detail="Vui lòng cung cấp ít nhất một ID tài khoản quảng cáo.")

    # Tạo các request nhẹ để "khơi mào" API và lấy header
    relative_urls = [f"{acc_id}/insights?fields=account_id&limit=1" for acc_id in ad_account_ids]

    try:
        request_id = str(uuid.uuid4())
        results, err_counter, all_headers = send_batch_to_facebook(relative_urls, access_token, request_id, get_header=True)
        summary = summarize_rate_limits(all_headers)
        print(summary)
        return RateLimitResponse(
            summary = summary,
            message="Truy vấn thành công."
        )
    except Exception as e:
        print(f"Internal error in /rate_limit: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

import uvicorn
if __name__ == "__main__":
    uvicorn.run("test:app", host = "0.0.0.0", port = 8000)
    # print(summarize_rate_limits(
    #     all_sub_request_headers=[[{'name': 'Expires', 'value': 'Sat, 01 Jan 2000 00:00:00 GMT'}, {'name': 'Cache-Control', 'value': 'private, no-cache, no-store, must-revalidate'}, {'name': 'Pragma', 'value': 'no-cache'}, {'name': 'Strict-Transport-Security', 'value': 'max-age=15552000; preload'}, {'name': 'Facebook-API-Version', 'value': 'v23.0'}, {'name': 'Access-Control-Allow-Origin', 'value': '*'}, 
    #                               {'name': 'X-FB-Ads-Insights-Throttle', 'value': '{"app_id_util_pct":0.01,"acc_id_util_pct":0,"ads_api_access_tier":"development_access"}'}, {'name': 'X-Business-Use-Case-Usage', 'value': '{"339410370":[{"type":"ads_insights","call_count":1,"total_cputime":1,"total_time":1,"estimated_time_to_regain_access":0,"ads_api_access_tier":"development_access"}]}'}, 
    #                               {'name': 'Vary', 'value': 'Accept-Encoding'}, {'name': 'Content-Type', 'value': 'text/javascript; charset=UTF-8'}, {'name': 'ETag', 'value': '"8c29bb54dfbf0ae0edb9b9b718c680671c64e0cf"'}], [{'name': 'Expires', 'value': 'Sat, 01 Jan 2000 00:00:00 GMT'}, {'name': 'Cache-Control', 'value': 'private, no-cache, no-store, must-revalidate'}, {'name': 'Pragma', 'value': 'no-cache'}, {'name': 'Strict-Transport-Security', 'value': 'max-age=15552000; preload'}, {'name': 'Facebook-API-Version', 'value': 'v23.0'}, {'name': 'Access-Control-Allow-Origin', 'value': '*'}, 
    #                             {'name': 'X-FB-Ads-Insights-Throttle', 'value': '{"app_id_util_pct":0.01,"acc_id_util_; charset=UTF-8'}, {'name': 'ETag', 'value': '"8c29bb54dfbf0ae0edb9b9b718c680671c64e0cf"'}], [{'name': 'Expires', 'value': 'Sat, 01 Jan 2000 00:00:00 GMT'}, {'name': 'Cache-Control', 'value': 'private, no-cache, no-store, must-revalidate'}, {'name': 'Pragma', 'value': 'no-cache'}, {'name': 'Strict-Transport-Security', 'value': 'max-age=15552000; preload'}, {'name': 'Facebook-API-Version', 'value': 'v23.0'}, {'name': 'Access-Control-Allow-Origin', 'value': '*'}, {'name': 'X-FB-Ads-Insights-Throttle', 'value': '{"app_id_util_pct":0.01,"acc_id_util_pct":0,"ads_api_access_tier":"development_access"}'}, {'name': 'X-Business-Use-Case-Usage', 'value': '{"1042231271137151":[{"type":"ads_insights","call_count":1,"total_cputime":1,"total_time":1,"estimated_time_to_regain_access":0,"ads_api_access_tier":"development_access"}]}'}, {'name': 'Vary', 'value': 'Accept-Encoding'}, {'name': 'Content-Type', 'value': 'text/javascript; charset=UTF-8'}, {'name': 'ETag', 'value': '"b38370bc35b7ac97eb5aa28aad39cf15c59a89c1"'}]]
    # ))
    # RateLimitResponse(
    #     summary = {'app_usage_pct': 0.01, 'account_details': [{'account_id': 'act_339410370', 'insights_usage_pct': 0.0, 'eta_seconds': 0, 'business_use_cases': [{'type': 'ads_insights', 'call_count': 1, 'total_cputime': 1, 'total_time': 1, 'estimated_time_to_regain_access': 0, 'ads_api_access_tier': 'development_access'}]}, {'account_id': 'act_1042231271137151', 'insights_usage_pct': 0.0, 'eta_seconds': 0, 'business_use_cases': [{'type': 'ads_insights', 'call_count': 1, 'total_cputime': 1, 'total_time': 1, 'estimated_time_to_regain_access': 0, 'ads_api_access_tier': 'development_access'}]}]},
    #     message="dmm"
    # )