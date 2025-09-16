import json
import requests
from typing import List, Dict, Any
from collections import defaultdict
from app_logging import log_sub_request, setup_logging
import logging

# --- CẤU HÌNH ---
API_VERSION = "v23.0"
setup_logging()
logger = logging.getLogger("FacebookBatchApp") # Lấy logger instance


def _send_single_request(
    relative_url: str,
    access_token: str,
    api_version: str = API_VERSION,
    timeout_sec: int = 60
) -> Dict[str, Any]:
    """Gửi một yêu cầu GET duy nhất và trả về kết quả đã được chuẩn hóa."""
    # ... (Hàm này giữ nguyên như phiên bản trước)
    api_url = f"https://graph.facebook.com/{api_version}/{relative_url}"
    params = {"access_token": access_token}
    try:
        resp = requests.get(api_url, params=params, timeout=timeout_sec)
        status_code = resp.status_code
        data = resp.json()
        if status_code == 200:
            return {"status_code": 200, "data": data, "error": None}
        else:
            return {"status_code": status_code, "data": None, "error": data.get("error", data)}
    except requests.exceptions.RequestException as e:
        return {"status_code": 599, "data": None, "error": {"message": str(e), "type": "ClientRequestError"}}
    except json.JSONDecodeError:
        return {"status_code": 599, "data": None, "error": {"message": "Failed to decode JSON from retry", "type": "ClientJSONError"}}

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

import time
def _retry_failed_requests(
    processed_results: List[Dict[str, Any]],
    access_token: str,
    api_version: str,
    request_id: str
) -> tuple[int, int]:
    """
    [HÀM MỚI] Tìm và gửi lại các sub-request thất bại (lỗi 500).
    Cập nhật trực tiếp vào list processed_results.
    Trả về số lượng retry thành công và thất bại.
    """
    requests_to_retry = [item for item in processed_results if item["status_code"] == 500]
    
    if not requests_to_retry:
        return 0, 0 # Không có gì để retry

    logger.info(f"Detected {len(requests_to_retry)} sub-requests with 500 error. Retrying...", extra={"request_id": request_id})
    successful_retries = 0

    for failed_item in requests_to_retry:
        original_index = failed_item["request_index"]
        url_to_retry = failed_item["requested_url"]
        
        logger.info(f"Retrying sub-request #{original_index}: {url_to_retry}", extra={"request_id": request_id, "request_index": original_index})
        time.sleep(0.5) # Thêm delay nhỏ để tránh dồn dập server
        
        retry_result = _send_single_request(
            relative_url=url_to_retry,
            access_token=access_token,
            api_version=api_version
        )
        
        if retry_result["status_code"] == 200:
            logger.info(f"Retry for sub-request #{original_index} SUCCEEDED.", extra={"request_id": request_id, "request_index": original_index})
            
            # Cập nhật lại item gốc trong danh sách
            processed_results[original_index]["status_code"] = 200
            processed_results[original_index]["data"] = retry_result["data"]
            processed_results[original_index]["error"] = None
            processed_results[original_index]["was_retried"] = True
            successful_retries += 1
        else:
            logger.warning(
                f"Retry for sub-request #{original_index} FAILED with new status code: {retry_result['status_code']}.",
                extra={
                    "request_id": request_id,
                    "request_index": original_index,
                    "new_status_code": retry_result['status_code'],
                    "new_error": retry_result['error']
                }
            )
            
    return successful_retries, len(requests_to_retry) - successful_retries


# main.py

def send_batch_to_facebook(
    relative_urls: List[str],
    access_token: str,
    request_id: str,
    api_version: str = API_VERSION,
    timeout_sec: int = 120
) -> List[Dict[str, Any]]:
    """
    [PHIÊN BẢN CUỐI CÙNG]
    Gửi batch, thực hiện retry cho các lỗi tạm thời, và GHI LOG KẾT QUẢ CUỐI CÙNG.
    """
    if not access_token: raise ValueError("Access token không hợp lệ.")
    if not 1 <= len(relative_urls) <= 50: raise ValueError("Số lượng URL phải từ 1 đến 50.")
    
    normalized_urls = [url.lstrip("/") for url in relative_urls]
    api_url = f"https://graph.facebook.com/{api_version}"
    batch_payload = [{"method": "GET", "relative_url": u} for u in normalized_urls]
    payload = {
        "access_token": access_token,
        "batch": json.dumps(batch_payload),
        "include_headers": "true"
    }

    try:
        resp = requests.post(api_url, data=payload, timeout=timeout_sec)
        resp.raise_for_status()
        # data là danh sách các response thô từ Facebook
        data = resp.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Lỗi khi gọi đến Facebook API: {e}")
    except json.JSONDecodeError:
        raise RuntimeError(f"Không thể parse JSON từ Facebook: {resp.text[:1000]}")

    if not isinstance(data, list):
        raise RuntimeError(f"Phản hồi không phải list như kỳ vọng.")

    # --- 1. XỬ LÝ KẾT QUẢ BAN ĐẦU ---
    # Tạo ra một danh sách các item đã được xử lý sơ bộ.
    processed_results: List[Dict[str, Any]] = []
    for i, item in enumerate(data):
        result_item = {
            "request_index": i,
            "requested_url": normalized_urls[i],
            "status_code": item.get("code") if item else 500,
            "data": None,
            "error": {"message": "Kết quả NULL từ Facebook"} if not item else None,
            "was_retried": False # Cờ để theo dõi
        }
        if item:
            try:
                body_json = json.loads(item.get("body", "{}"))
                if result_item["status_code"] == 200:
                    result_item["data"] = body_json
                else:
                    result_item["error"] = body_json.get("error", body_json)
            except json.JSONDecodeError:
                result_item["error"] = {"message": "Body không phải JSON."}
        
        processed_results.append(result_item)

    # --- 2. THỰC HIỆN RETRY ---
    # Hàm này sẽ tìm các item có lỗi 5xx trong `processed_results`,
    # gửi lại request, và cập nhật trực tiếp các item đó với kết quả mới.
    successful_retries, failed_retries = _retry_failed_requests(
        processed_results=processed_results,
        access_token=access_token,
        api_version=api_version,
        request_id=request_id
    )

    # --- 3. GHI LOG KẾT QUẢ CUỐI CÙNG (SAU KHI ĐÃ RETRY) ---
    logger.info(
        f"Logging final status for {len(processed_results)} sub-requests.", 
        extra={"request_id": request_id, "log.type": "final_logging"}
    )
    for result in processed_results:
        # Lấy lại response thô ban đầu từ Facebook để truyền vào hàm log
        original_fb_item = data[result.get("request_index")]
        
        # Gọi hàm log gốc của bạn với dữ liệu cuối cùng
        log_sub_request(
            request_id=request_id,
            request_index=result.get("request_index"),
            fb_response_item=original_fb_item,
            processed_item=result 
        )

    # Cập nhật lại bộ đếm để trả về
    success_count = len([r for r in processed_results if r["status_code"] == 200])
    error_count = len(processed_results) - success_count
    summary = {"success_count": success_count, "error_count": error_count}

    return processed_results, summary

