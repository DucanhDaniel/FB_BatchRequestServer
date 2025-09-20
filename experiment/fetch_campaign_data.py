import requests
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from calendar import monthrange
from dotenv import load_dotenv
import os

load_dotenv()

# --- CẤU HÌNH ---
ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN") 
ADVERTISER_ID = "6967547145545105410"
STORE_ID = "7494600253418473607"
START_DATE = "2025-09-01"
END_DATE = "2025-09-18"
API_URL = "https://business-api.tiktok.com/open_api/v1.3/gmv_max/report/get/"

HEADERS = {
    "Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}

def chunk_list(data, size):
    """Chia một danh sách thành các danh sách con có kích thước `size`."""
    for i in range(0, len(data), size):
        yield data[i:i + size]

def generate_monthly_date_chunks(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    chunks = []
    cursor_date = date(start_date.year, start_date.month, 1)
    while cursor_date <= end_date:
        _, last_day_of_month = monthrange(cursor_date.year, cursor_date.month)
        month_end_date = date(cursor_date.year, cursor_date.month, last_day_of_month)
        chunk_start = max(cursor_date, start_date)
        chunk_end = min(month_end_date, end_date)
        chunks.append({
            'start': chunk_start.strftime('%Y-%m-%d'),
            'end': chunk_end.strftime('%Y-%m-%d')
        })
        next_month = cursor_date.month + 1
        next_year = cursor_date.year
        if next_month > 12:
            next_month = 1
            next_year += 1
        cursor_date = date(next_year, next_month, 1)
    return chunks

def make_api_request_with_backoff(session, params, max_retries=5, base_delay=3):
    for attempt in range(max_retries):
        try:
            response = session.get(API_URL, params=params, timeout=45)
            print(API_URL, params)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 0: return data
            if "Too many requests" in data.get("message", ""):
                print(f"  [RATE LIMIT] Gặp lỗi (lần {attempt + 1}/{max_retries})...")
            else:
                print(f"  [LỖI API] {data.get('message')}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"  [LỖI MẠNG] (lần {attempt + 1}/{max_retries}): {e}")
        if attempt < max_retries - 1:
            delay = (base_delay ** attempt) + random.uniform(0, 1)
            print(f"  Thử lại sau {delay:.2f} giây.")
            time.sleep(delay)
    print("  [THẤT BẠI] Đã thử lại tối đa.")
    return None

def fetch_all_pages(session, params):
    all_results = []
    current_page = 1
    while True:
        params['page'] = current_page
        data = make_api_request_with_backoff(session, params)
        if not data: break
        page_data = data.get("data", {})
        result_list = page_data.get("list", [])
        all_results.extend(result_list)
        page_info = page_data.get("page_info", {})
        total_pages = page_info.get("total_page", 1)
        print(f"  [PHÂN TRANG] Đã lấy trang {current_page}/{total_pages}...")
        if current_page >= total_pages: break
        current_page += 1
        time.sleep(100)
    return all_results

def get_all_campaigns(start_date, end_date):
    print(f"Bước 1: Đang lấy danh sách Campaigns từ {start_date} đến {end_date}...")
    params = {
        "advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),
        "start_date": start_date, "end_date": end_date,
        "dimensions": json.dumps(["campaign_id"]), "metrics": json.dumps(["campaign_name"]),
        "filtering": json.dumps({"gmv_max_promotion_types": ["PRODUCT"]}), "page_size": 1000,
    }
    with requests.Session() as session:
        session.headers.update(HEADERS)
        all_campaign_items = fetch_all_pages(session, params)
    if all_campaign_items:
        campaigns = {item["dimensions"]["campaign_id"]: item["metrics"]["campaign_name"] for item in all_campaign_items}
        print(f"==> Tìm thấy tổng cộng {len(campaigns)} campaigns.")
        return campaigns
    return {}

def fetch_data_for_batch(campaign_batch, campaign_name_map, start_date, end_date):
    """
    Lấy dữ liệu cho một lô các campaign_id cùng lúc.
    """
    batch_ids = [cid for cid in campaign_batch]
    print(f"  [BẮT ĐẦU BATCH] Xử lý {len(batch_ids)} campaigns...")
    
    params_perf = {
        "advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),
        "start_date": start_date, "end_date": end_date,
        "dimensions": json.dumps(["campaign_id", "item_group_id", "stat_time_day"]),
        "metrics": json.dumps(["orders", "gross_revenue", "cost", "cost_per_order", "roi"]),
        "filtering": json.dumps({"campaign_ids": batch_ids}), 
        "page_size": 1000,
    }

    with requests.Session() as session:
        session.headers.update(HEADERS)
        perf_list = fetch_all_pages(session, params_perf)
        
    # --- Phân loại lại kết quả cho từng campaign trong batch ---
    results_in_batch = {}
    for cid in batch_ids:
        results_in_batch[cid] = {
            "campaign_id": cid,
            "campaign_name": campaign_name_map.get(cid, "N/A"),
            "performance_data": [],
        }

    for record in perf_list:
        cid = record.get("dimensions", {}).get("campaign_id")
        if cid in results_in_batch:
            results_in_batch[cid]["performance_data"].append(record)
            
    print(f"  [OK BATCH] Hoàn thành xử lý {len(batch_ids)} campaigns.")
    return list(results_in_batch.values())

if __name__ == "__main__":
    start_time = time.perf_counter()
    date_chunks = generate_monthly_date_chunks(START_DATE, END_DATE)
    print(f"Đã chia khoảng thời gian thành {len(date_chunks)} chunk.")

    all_final_results = []
    
    for chunk in date_chunks:
        chunk_start = chunk['start']
        chunk_end = chunk['end']
        print(f"\n--- BẮT ĐẦU XỬ LÝ CHUNK: {chunk_start} to {chunk_end} ---")
        
        campaigns_map = get_all_campaigns(chunk_start, chunk_end)

        if campaigns_map:
            campaign_ids = list(campaigns_map.keys())
            campaign_batches = list(chunk_list(campaign_ids, 20)) # Chia thành các lô 20 campaign
            print(f"Đã chia {len(campaign_ids)} campaigns thành {len(campaign_batches)} lô.")
            
            max_workers = 4 
            print(f"Bắt đầu lấy dữ liệu chi tiết song song với {max_workers} luồng...")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(fetch_data_for_batch, batch, campaigns_map, chunk_start, chunk_end): batch 
                    for batch in campaign_batches
                }
                
                for future in as_completed(future_to_batch):
                    batch_result = future.result() 
                    for result in batch_result:
                        if result.get("performance_data"):
                            all_final_results.append(result)
                        else:
                            print(f"  [BỎ QUA] Campaign '{result.get('campaign_name')}' vì thiếu dữ liệu.")

    total_cost = 0
    for campaign_result in all_final_results:
        for perf_record in campaign_result.get("performance_data", []):
            try:
                cost_value = float(perf_record.get("metrics", {}).get("cost", 0))
                total_cost += cost_value
            except (ValueError, TypeError):
                continue
    
    print(f"\n💰 Tổng chi phí (cost) của tất cả các campaign đã xử lý: {total_cost:,.0f} VND")
    print("\n--- HOÀN THÀNH TOÀN BỘ ---")
    print(f"Đã xử lý và giữ lại kết quả từ {len(all_final_results)} lượt campaign có dữ liệu đầy đủ.")
    
    with open("tiktok_results_final_batch.json", "w", encoding="utf-8") as f:
        json.dump(all_final_results, f, ensure_ascii=False, indent=4)
    print("Kết quả đã được ghi vào file tiktok_results_final_batch.json")
    
    end_time = time.perf_counter()
    print(f"\nTổng thời gian thực thi: {end_time - start_time:.2f} giây.")