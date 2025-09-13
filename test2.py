# main.py
import json
import requests
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Query, HTTPException, Body
from pydantic import BaseModel, Field, field_validator

# --- CẤU HÌNH ---
API_VERSION = "v23.0"

# --- KHỞI TẠO ỨNG DỤNG FASTAPI ---
app = FastAPI(
    title="Facebook Batch Request & Rate Limit API",
    description="API client gửi batch requests và kiểm tra rate limit của Facebook Graph API.",
    version="2.0.0",
)

# =============================================================================
# Pydantic Models (Định nghĩa cấu trúc dữ liệu)
# =============================================================================

class BatchRequest(BaseModel):
    access_token: str = Field(..., description="Facebook Graph API Access Token")
    relative_urls: List[str] = Field(
        ...,
        max_length=50,
        min_length=1,
        description="Danh sách URL tương đối (tối đa 50)."
    )

class RateLimitResponse(BaseModel):
    app_id_util_pct: Optional[float] = Field(None, description="Phần trăm giới hạn Insights đã sử dụng của ứng dụng (lấy giá trị cao nhất).")
    acc_id_util_pct: Dict[str, float] = Field(..., description="Phần trăm giới hạn Insights đã sử dụng của từng tài khoản.")
    message: str = Field(..., description="Thông báo kết quả.")

# =============================================================================
# Helper Functions (Các hàm hỗ trợ)
# =============================================================================
def _log_batch_summary(results: List[Dict[str, Any]]):
    """
    [HÀM MỚI] In ra một bảng tóm tắt tổng quan về rate limit cho toàn bộ batch request.
    """
    if not results:
        return

    # --- Bước 1: Thu thập dữ liệu từ tất cả các sub-request ---
    successful_calls = 0
    failed_calls = 0
    app_usage_values = []
    # Dict để tổng hợp chi phí cho từng tài khoản
    account_costs = {} 

    for result in results:
        if result.get("status_code") == 200:
            successful_calls += 1
        else:
            failed_calls += 1

        headers_list = result.get("headers", [])
        if not headers_list:
            continue

        headers_dict = {h.get("name", "").lower(): h.get("value") for h in headers_list}

        # Lấy app usage từ throttle header
        try:
            throttle = json.loads(headers_dict.get("x-fb-ads-insights-throttle", "{}"))
            if isinstance(throttle.get("app_id_util_pct"), (int, float)):
                app_usage_values.append(float(throttle["app_id_util_pct"]))
        except (json.JSONDecodeError, TypeError):
            pass

        # Lấy BUC để tính tổng chi phí
        try:
            buc = json.loads(headers_dict.get("x-business-use-case-usage", "{}"))
            for acc_id, entries in buc.items():
                if acc_id not in account_costs:
                    account_costs[acc_id] = {"total_cputime": 0, "total_time": 0, "max_eta": 0}
                
                for entry in entries:
                    account_costs[acc_id]["total_cputime"] += entry.get("total_cputime", 0)
                    account_costs[acc_id]["total_time"] += entry.get("total_time", 0)
                    account_costs[acc_id]["max_eta"] = max(
                        account_costs[acc_id]["max_eta"], 
                        entry.get("estimated_time_to_regain_access", 0)
                    )
        except (json.JSONDecodeError, TypeError):
            pass

    # --- Bước 2: In ra bảng tóm tắt ---
    print("\n" + "#"*80)
    print("##" + " BATCH REQUEST SUMMARY ".center(76) + "##")
    print("#"*80)

    # Tóm tắt chung
    max_app_usage = max(app_usage_values) if app_usage_values else "N/A"
    print(f"  Total Requests : {len(results)}")
    print(f"  Successful     : {successful_calls}")
    print(f"  Failed         : {failed_calls}")
    print(f"  Max App Usage %: {max_app_usage}")
    
    if not account_costs:
        print("#"*80 + "\n")
        return

    # Bảng chi phí theo tài khoản
    print("\n--- Per-Account Cost & ETA Summary ---")
    
    header = ["Account ID", "Total CPU Time", "Total Time", "Max ETA (s)"]
    col_widths = {h: len(h) for h in header}
    
    table_data = []
    for acc_id, costs in account_costs.items():
        row = {
            "Account ID": acc_id,
            "Total CPU Time": str(costs["total_cputime"]),
            "Total Time": str(costs["total_time"]),
            "Max ETA (s)": str(costs["max_eta"])
        }
        table_data.append(row)
        for h in header:
            col_widths[h] = max(col_widths[h], len(row[h]))
    
    header_line = " | ".join([h.ljust(col_widths[h]) for h in header])
    print(f"| {header_line} |")
    separator_line = "-|-".join(["-" * col_widths[h] for h in header])
    print(f"|{separator_line}|")

    for row in table_data:
        data_line = " | ".join([
            row["Account ID"].ljust(col_widths["Account ID"]),
            row["Total CPU Time"].rjust(col_widths["Total CPU Time"]),
            row["Total Time"].rjust(col_widths["Total Time"]),
            row["Max ETA (s)"].rjust(col_widths["Max ETA (s)"])
        ])
        print(f"| {data_line} |")
    
    print("#"*80 + "\n")
    
def _log_sub_request_headers(
    request_index: int, 
    requested_url: str, 
    headers_list: List[Dict[str, str]]
):
    """
    [PHIÊN BẢN BẢNG]
    Lọc và in các header usage quan trọng của một sub-request ra console
    dưới dạng một bảng tóm tắt rõ ràng.
    """
    INTERESTING_HEADERS = {
        "x-business-use-case-usage",
        "x-fb-ads-insights-throttle",
    }

    # --- Bước 1: Trích xuất và parse dữ liệu header ---
    throttle_info = {}
    buc_info = {}
    for h in headers_list:
        name = h.get("name", "").lower()
        if name in INTERESTING_HEADERS:
            try:
                if name == "x-fb-ads-insights-throttle":
                    throttle_info = json.loads(h.get("value", "{}"))
                elif name == "x-business-use-case-usage":
                    buc_info = json.loads(h.get("value", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

    if not throttle_info and not buc_info:
        return # Không có thông tin gì để in

    # --- Bước 2: In tiêu đề và thông tin throttle tóm tắt ---
    print("\n" + "="*80)
    print(f"📊 [RATE LIMIT USAGE] Sub-Request #{request_index}: {requested_url[:80]}...")
    print("-" * 80)
    
    app_usage = throttle_info.get("app_id_util_pct", "N/A")
    acc_usage = throttle_info.get("acc_id_util_pct", "N/A")
    print(f"- Insights App Usage (%): {app_usage} | Insights Account Usage (%): {acc_usage}")
    
    if not buc_info:
        print("="*80 + "\n")
        return # Nếu không có BUC thì dừng ở đây

    # --- Bước 3: Chuẩn bị và in bảng cho Business Use Case ---
    for acc_id, entries in buc_info.items():
        if not isinstance(entries, list): continue

        print("-" * 80)
        print(f"[Business Use Case Usage for Account: {acc_id}]")

        # Chuẩn bị dữ liệu và tính độ rộng cột
        header = ["Type", "Calls", "CPU Time", "Total Time", "ETA (s)", "Tier"]
        col_widths = {h: len(h) for h in header}
        
        table_data = []
        for entry in entries:
            row = {
                "Type": str(entry.get("type", "")),
                "Calls": str(entry.get("call_count", "")),
                "CPU Time": str(entry.get("total_cputime", "")),
                "Total Time": str(entry.get("total_time", "")),
                "ETA (s)": str(entry.get("estimated_time_to_regain_access", "")),
                "Tier": str(entry.get("ads_api_access_tier", ""))
            }
            table_data.append(row)
            for h in header:
                col_widths[h] = max(col_widths[h], len(row[h]))

        # In header của bảng
        header_line = " | ".join([h.ljust(col_widths[h]) for h in header])
        print(f"| {header_line} |")

        # In dòng phân cách
        separator_line = "-|-".join(["-" * col_widths[h] for h in header])
        print(f"|{separator_line}|")

        # In các dòng dữ liệu
        for row in table_data:
            data_line = " | ".join([
                row["Type"].ljust(col_widths["Type"]),
                row["Calls"].rjust(col_widths["Calls"]),
                row["CPU Time"].rjust(col_widths["CPU Time"]),
                row["Total Time"].rjust(col_widths["Total Time"]),
                row["ETA (s)"].rjust(col_widths["ETA (s)"]),
                row["Tier"].ljust(col_widths["Tier"])
            ])
            print(f"| {data_line} |")

    print("="*80 + "\n")    


def _summarize_rate_limits_from_batch(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Phân tích kết quả từ một batch request và tổng hợp thông tin rate limit quan trọng.
    """
    app_usage_values = []
    account_usages = {}
    etas = {}

    for result in results:
        # Mỗi result con có một danh sách các header
        headers_list = result.get("headers", [])
        if not headers_list:
            continue
            
        # Chuyển danh sách header thành một dict để dễ truy cập
        headers_dict = {h.get("name", "").lower(): h.get("value") for h in headers_list}

        try:
            # Lấy ad_account_id từ URL để map dữ liệu
            acc_id_from_url = result.get("requested_url", "").split('/')[0]
            if not acc_id_from_url.startswith("act_"):
                acc_id_from_url = None
        except Exception:
            acc_id_from_url = None

        # 1. Lấy thông tin từ x-fb-ads-insights-throttle
        throttle_str = headers_dict.get("x-fb-ads-insights-throttle", "{}")
        try:
            throttle = json.loads(throttle_str)
            if isinstance(throttle.get("app_id_util_pct"), (int, float)):
                app_usage_values.append(float(throttle["app_id_util_pct"]))
            if acc_id_from_url and isinstance(throttle.get("acc_id_util_pct"), (int, float)):
                account_usages[acc_id_from_url] = float(throttle["acc_id_util_pct"])
        except (json.JSONDecodeError, TypeError):
            pass

        # 2. Lấy thông tin từ x-business-use-case-usage
        buc_str = headers_dict.get("x-business-use-case-usage", "{}")
        try:
            buc = json.loads(buc_str)
            for buc_acc_id, entries in buc.items():
                if isinstance(entries, list):
                    for entry in entries:
                        entry_eta = entry.get("estimated_time_to_regain_access", 0)
                        # if entry_eta > 0:
                        etas["act_" + buc_acc_id] = max(etas.get(buc_acc_id, 0), entry_eta)
        except (json.JSONDecodeError, TypeError):
            pass
            
    return {
        "insights_app_usage": max(app_usage_values) if app_usage_values else None,
        "account_details": {
            "insights_account_usages": account_usages,
            "etas": etas
        }
    }

# =============================================================================
# Core Logic (Hàm gốc của bạn, có một chút thay đổi nhỏ)
# =============================================================================

def send_batch_to_facebook(
    relative_urls: List[str], access_token: str, api_version: str = API_VERSION, timeout_sec: int = 180
) -> List[Dict[str, Any]]:
    """
    Gửi tối đa 50 yêu cầu trong 1 batch tới Facebook Graph API và trả về kết quả đã xử lý.
    """
    if not access_token or "YOUR_ACCESS_TOKEN" in access_token:
        raise ValueError("Bạn phải cung cấp một access_token hợp lệ.")
    if not 1 <= len(relative_urls) <= 50:
        raise ValueError(f"Số lượng URL phải từ 1 đến 50. Hiện tại là {len(relative_urls)}.")
    
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
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Lỗi khi gọi đến Facebook API: {e}") from e

    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Phản hồi không phải list như kỳ vọng. Raw: {data}")

    processed_results: List[Dict[str, Any]] = []
    for i, item in enumerate(data):
        result_item: Dict[str, Any] = {
            "request_index": i,
            "requested_url": normalized_urls[i],
            "status_code": item.get("code"),
            "headers": item.get("headers", []), # Giữ nguyên headers gốc
            "data": None, "error": None
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
        else:
            result_item["error"] = {"message": "Kết quả NULL."}
            
        # _log_sub_request_headers(
        #     request_index=i, 
        #     requested_url=result_item['requested_url'], 
        #     headers_list=item.get("headers", [])
        # )
            
        processed_results.append(result_item)
    
    return processed_results

# =============================================================================
# API Endpoints
# =============================================================================

@app.post("/batch", summary="Gửi batch request và nhận lại tóm tắt rate limit")
async def process_batch_request_post(payload: BatchRequest = Body(...)):
    """
    Endpoint chính để xử lý batch request. Trả về kết quả chi tiết và
    một bản tóm tắt rate limit thông minh.
    """
    try:
        results = send_batch_to_facebook(payload.relative_urls, payload.access_token)
        rate_limit_summary = _summarize_rate_limits_from_batch(results)
        _log_batch_summary(results)
        return {
            "status": "success",
            "rate_limit_summary": rate_limit_summary,
            "results": results
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=f"Bad Gateway: {str(e)}")
    except Exception as e:
        print(f"Internal error in /batch: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


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
        results = send_batch_to_facebook(relative_urls, access_token)
        summary = _summarize_rate_limits_from_batch(results)
        
        return RateLimitResponse(
            app_id_util_pct=summary.get("insights_app_usage"),
            acc_id_util_pct=summary.get("account_details", {}).get("insights_account_usages", {}),
            message="Truy vấn thành công."
        )
    except Exception as e:
        print(f"Internal error in /rate_limit: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/health")
def health():
    return {"status": "ok"}

# --- Server Runner ---
if __name__ == "__main__":
    import uvicorn
    # Chạy với --reload để tự động tải lại khi có thay đổi code
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)