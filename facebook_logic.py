import json
import requests
from typing import List, Dict, Any
from collections import defaultdict
from app_logging import _log_sub_request_headers, log_sub_request, setup_logging
import logging

# --- CẤU HÌNH ---
API_VERSION = "v23.0"
setup_logging()
logger = logging.getLogger("FacebookBatchApp") # Lấy logger instance

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
 