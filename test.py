import requests
import json

# --- Cấu hình yêu cầu (Request Configuration) ---

# ID tài khoản quảng cáo của bạn
ad_account_id = "act_650248897235348"

# Access token của bạn (Lưu ý: Token bạn cung cấp có thể đã hết hạn. Hãy thay bằng token hợp lệ)
# BẢO MẬT: Không bao giờ đưa access token trực tiếp vào code trong môi trường production.
# Thay vào đó, hãy sử dụng biến môi trường hoặc các phương thức quản lý secret.
access_token = "EAARclPZAoBi8BPkIsMRDitm3kFeMFxK5kVy92ybDfMiZANfwGGG9Mvjk8o4FMUqbAjZAAROtbv1IxBvHKZAWDvIeHonSNOzWZBQMSEDyhVKlvzpVQJ4ZB2FZCY3yLkyniYbBwDmGEEUhkAGJxE88DQdidIMvUAd5LLC7iGuRM2ZAmgfXLZCxDTYIEvO8CJewJQpCLJCo5T9cgvY72hCoUnGeO60hETalDU7UZBzu2ZArviao3qUPAZDZD"

# Xây dựng URL endpoint
api_version = "v24.0"
url = f"https://graph.facebook.com/{api_version}/{ad_account_id}/ads"

# --- Tham số truy vấn (Query Parameters) ---

# Các trường (fields) bạn muốn lấy thông tin
fields_to_get = [
    "id",
    "name",
    "adset{name,id}",
    "campaign{name,id}",
    "status",
    "effective_status",
    "created_time",
    # Lấy dữ liệu insights theo ngày trong một khoảng thời gian cụ thể
    "insights.time_range({'since':'2025-10-01','until':'2025-10-13'}).time_increment(1){"
    "account_id,date_start,date_stop,spend,impressions,reach,clicks,ctr,cpc,cpm,frequency,"
    "actions,cost_per_action_type,action_values,purchase_roas"
    "}"
]

# Các trạng thái hiệu quả (effective_status) bạn muốn lọc
effective_status_filter = [
    "ACTIVE", "PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED", "WITH_ISSUES",
    "PENDING_REVIEW", "DISAPPROVED", "PREAPPROVED", "IN_PROCESS",
    "PENDING_BILLING_INFO", "ARCHIVED"
]

# Con trỏ phân trang (cursor for pagination)
after_cursor = "QVFIU01LMDBiVkphMFJhQU5pZAjJ2MTYzcWlSNGthV29lQ0pmV1RPODJITkJ1Yjk3Sk10Vm5rOHZAVTWk3ZAjlkd0o1OWx4cUFEeC1uLWttdURfNkRZAczBSRXNB"

# Tạo dictionary chứa tất cả các tham số
params = {
    "fields": ",".join(fields_to_get),
    "limit": 200,
    "effective_status": json.dumps(effective_status_filter), # Trạng thái cần được chuyển thành chuỗi JSON
    "access_token": access_token,
    "after": after_cursor
}

# --- Gửi yêu cầu và xử lý kết quả ---
try:
    print(f"Đang gửi yêu cầu GET tới: {url}")
    print("Với các tham số:")
    # In các tham số trừ access_token để bảo mật
    readable_params = params.copy()
    readable_params['access_token'] = '...TOKEN...'
    print(json.dumps(readable_params, indent=2))

    response = requests.get(url, params=params)

    # Kiểm tra xem yêu cầu có thành công không (status code 200)
    response.raise_for_status()

    # Parse kết quả JSON
    data = response.json()

    print("\n--- KẾT QUẢ THÀNH CÔNG ---")
    # In kết quả ra một cách đẹp mắt
    print(json.dumps(data, indent=2, ensure_ascii=False))

    # In ra số lượng quảng cáo nhận được
    if 'data' in data:
        print(f"\n=> Nhận được {len(data['data'])} quảng cáo.")
    
    # In thông tin phân trang nếu có
    if 'paging' in data and 'next' in data['paging']:
        print(f"\n=> Có trang tiếp theo. Sử dụng URL sau để lấy dữ liệu kế tiếp:")
        print(data['paging']['next'])


except requests.exceptions.HTTPError as errh:
    print(f"\n--- LỖI HTTP ---")
    print(f"Mã trạng thái: {errh.response.status_code}")
    print("Nội dung lỗi:")
    # Cố gắng in lỗi từ Facebook nếu có
    try:
        print(json.dumps(errh.response.json(), indent=2))
    except json.JSONDecodeError:
        print(errh.response.text)
except requests.exceptions.RequestException as err:
    print(f"\n--- LỖI YÊU CẦU ---")
    print(f"Đã xảy ra lỗi: {err}")