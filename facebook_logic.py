import json
import requests
from typing import List, Dict, Any, Optional
from collections import defaultdict
from app_logging import log_sub_request, setup_logging
import logging

import os
from dotenv import load_dotenv

load_dotenv()

# --- CẤU HÌNH ---
API_VERSION = "v24.0"
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
    request_id: str,
    email: Optional[str],
    client_ip: Optional[str]
) -> tuple[int, int]:
    """
    Tìm và gửi lại các sub-request thất bại (lỗi 500).
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
                    "new_error": retry_result['error'],
                    "email":email,
                    "client_ip":client_ip
                }
            )
            
    return successful_retries, len(requests_to_retry) - successful_retries


def _send_single_batch_to_facebook(
    relative_urls: List[str],
    access_token: str,
    request_id: str,
    api_version: str,
    timeout_sec: int,
    email: Optional[str] = None,
    client_ip: Optional[str] = None
):
    """
    [HÀM HELPER] Gửi một batch duy nhất (<= 50 requests).
    Hàm này là logic cốt lõi từ phiên bản gốc của bạn.
    """
    # Validation đã có ở hàm cha, nhưng giữ lại để an toàn
    if not access_token: raise ValueError("Access token không hợp lệ.")
    if not 1 <= len(relative_urls) <= 50: raise ValueError(f"Số lượng URL cho một batch phải từ 1 đến 50, nhận được: {len(relative_urls)}")

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
        data = resp.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Lỗi khi gọi đến Facebook API: {e}")
    except json.JSONDecodeError:
        raise RuntimeError(f"Không thể parse JSON từ Facebook: {resp.text[:1000]}")

    if not isinstance(data, list):
        raise RuntimeError(f"Phản hồi không phải list như kỳ vọng.")

    # --- 1. XỬ LÝ KẾT QUẢ BAN ĐẦU ---
    processed_results: List[Dict[str, Any]] = []
    all_headers = []
    for i, item in enumerate(data):
        result_item = {
            "request_index": i,
            "requested_url": normalized_urls[i],
            "status_code": item.get("code") if item else 500,
            # "status_code":500,
            "data": None,
            "error": {"message": "Kết quả NULL từ Facebook"} if not item else None,
            "was_retried": False,
            "email":email,
            "client_ip":client_ip
        }
        if item:
            try:
                body_json = json.loads(item.get("body", "{}"))
                all_headers.append(item.get('headers', []))
                
                if result_item["status_code"] == 200:
                    result_item["data"] = body_json
                    result_item["error"] = None # Xóa lỗi mặc định nếu thành công
                else:
                    result_item["error"] = body_json.get("error", body_json)
            except json.JSONDecodeError:
                result_item["error"] = {"message": "Body không phải JSON."}
        
        processed_results.append(result_item)

    # --- 2. THỰC HIỆN RETRY ---
    # Giả sử hàm _retry_failed_requests tồn tại và hoạt động đúng
    _retry_failed_requests(
        processed_results=processed_results,
        access_token=access_token,
        api_version=api_version,
        request_id=request_id,
        email=email,
        client_ip=client_ip,
    )

    # --- 3. GHI LOG ---
    logger.info(
        f"Logging final status for {len(processed_results)} sub-requests.", 
        extra={"request_id": request_id, "log.type": "final_logging"}
    )
    for result in processed_results:
        original_fb_item = data[result.get("request_index")]
        log_sub_request(
            request_id=request_id,
            request_index=result.get("request_index"),
            fb_response_item=original_fb_item,
            processed_item=result,
            email=email,
            client_ip=client_ip
        )

    # --- 4. TẠO SUMMARY CHO BATCH NÀY ---
    success_count = len([r for r in processed_results if r["status_code"] == 200])
    error_count = len(processed_results) - success_count
    summary = {"success_count": success_count, "error_count": error_count}

    return processed_results, summary, all_headers


def send_batch_to_facebook(
    relative_urls: List[str],
    access_token: str,
    request_id: str,
    email: Optional[str] = None,
    client_ip: Optional[str] = None,
    api_version: str = API_VERSION, # API_VERSION,
    timeout_sec: int = 120,
    get_header: bool = False,
) -> List[Dict[str, Any]]:
    """
    Gửi batch request đến Facebook, tự động chia thành các chunk nhỏ hơn 50 nếu cần.
    Thực hiện retry cho các lỗi tạm thời và GHI LOG KẾT QUẢ CUỐI CÙNG.
    """
    if not access_token: raise ValueError("Access token không hợp lệ.")
    if not relative_urls: return ([], {"success_count": 0, "error_count": 0}) if not get_header else ([], {"success_count": 0, "error_count": 0}, [])

    BATCH_SIZE = 50
    
    # Khởi tạo các biến để tổng hợp kết quả từ tất cả các chunk
    all_processed_results = []
    all_headers = []
    final_summary = {"success_count": 0, "error_count": 0}

    # Chia danh sách URL thành các chunk và xử lý từng chunk
    for i in range(0, len(relative_urls), BATCH_SIZE):
        chunk_urls = relative_urls[i:i + BATCH_SIZE]
        
        logger.info(f"Sending chunk {i//BATCH_SIZE + 1}/{(len(relative_urls) - 1)//BATCH_SIZE + 1} with {len(chunk_urls)} URLs.", extra={"request_id": request_id})

        try:
            # Gọi hàm helper để xử lý một batch
            chunk_results, chunk_summary, chunk_headers = _send_single_batch_to_facebook(
                relative_urls=chunk_urls,
                access_token=access_token,
                request_id=request_id,
                api_version=api_version,
                timeout_sec=timeout_sec,
                email = email,
                client_ip = client_ip
            )

            # Điều chỉnh `request_index` để nó chính xác trên toàn bộ danh sách
            for result in chunk_results:
                result["request_index"] += i
            
            # Gộp kết quả của chunk vào kết quả tổng
            all_processed_results.extend(chunk_results)
            all_headers.extend(chunk_headers)
            final_summary["success_count"] += chunk_summary["success_count"]
            final_summary["error_count"] += chunk_summary["error_count"]

        except Exception as e:
            # Nếu một chunk thất bại hoàn toàn, tạo các bản ghi lỗi cho tất cả URL trong chunk đó
            logger.error(f"Chunk starting at index {i} failed entirely: {e}", extra={"request_id": request_id})
            for url_index, url in enumerate(chunk_urls):
                error_result = {
                    "request_index": i + url_index,
                    "client_ip":client_ip,
                    "email":email,
                    "requested_url": url.lstrip("/"),
                    "status_code": 500, # Giả định lỗi hệ thống
                    "data": None,
                    "error": {"message": f"Batch request failed: {e}"},
                    "was_retried": False
                }
                all_processed_results.append(error_result)
                final_summary["error_count"] += 1
    
    # Sắp xếp lại kết quả cuối cùng theo request_index để đảm bảo thứ tự
    all_processed_results.sort(key=lambda x: x["request_index"])

    # TODO: Logic ghi log và retry có thể được thực hiện ở đây trên `all_processed_results` nếu muốn
    # Ví dụ: bạn có thể di chuyển vòng lặp log từ hàm helper ra đây để log một lần duy nhất.
    throttling_info = summarize_rate_limits(all_headers)
    final_summary["rate_limits"] = throttling_info

    if get_header:
        return all_processed_results, final_summary, all_headers
    
    return all_processed_results, final_summary

if (__name__ == "__main__"):
    all_processed_results, final_summary = send_batch_to_facebook(
        relative_urls=["act_650248897235348/insights?level=campaign&time_increment=1&action_report_time=conversion&fields=campaign_id%2Ccampaign_name%2Caccount_id%2Caccount_name%2Cdate_start%2Cdate_stop%2Cspend%2Cimpressions%2Creach%2Cclicks%2Ccpc%2Ccpm%2Cctr%2Cfrequency%2Cactions%2Ccost_per_action_type%2Caction_values%2Cpurchase_roas&time_range=%7B%22since%22%3A%222025-10-01%22%2C%22until%22%3A%222025-10-13%22%7D&limit=200", "act_948290596967304/insights?level=campaign&time_increment=1&action_report_time=conversion&fields=campaign_id%2Ccampaign_name%2Caccount_id%2Caccount_name%2Cdate_start%2Cdate_stop%2Cspend%2Cimpressions%2Creach%2Cclicks%2Ccpc%2Ccpm%2Cctr%2Cfrequency%2Cactions%2Ccost_per_action_type%2Caction_values%2Cpurchase_roas&time_range=%7B%22since%22%3A%222025-10-01%22%2C%22until%22%3A%222025-10-13%22%7D&limit=200"],
        access_token=os.getenv("FACEBOOK_ACCESS_TOKEN"),
        request_id="1",
        email="ahihi@gmail.com",
        client_ip="123123"
    )
    print(final_summary)