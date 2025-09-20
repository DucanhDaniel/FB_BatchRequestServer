import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

def get_bc_ids(access_token, max_retries=3, backoff_factor=3):
    """
    Lấy danh sách BC ID với cơ chế thử lại và tạm dừng.

    Args:
        access_token (str): Access Token API của bạn.
        max_retries (int): Số lần thử lại tối đa.
        backoff_factor (int): Số giây chờ giữa các lần thử lại.

    Returns:
        list: Một danh sách các BC ID, hoặc danh sách rỗng nếu có lỗi.
    """
    url = "https://business-api.tiktok.com/open_api/v1.3/bc/get/"
    headers = {
        'Access-Token': access_token,
    }
    print("Đang lấy danh sách BC ID...")
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            if data.get("code") == 0:
                bc_list = data.get("data", {}).get("list", [])
                bc_ids = [
                    bc.get("bc_info", {}).get("bc_id") 
                    for bc in bc_list 
                    if bc.get("bc_info", {}).get("bc_id")
                ]
                print(f"Đã lấy thành công {len(bc_ids)} BC ID.")
                return bc_ids
            else:
                print(f"Lỗi từ API khi lấy BC ID: {data.get('message')}")

        except requests.exceptions.RequestException as e:
            print(f"Lỗi kết nối khi lấy BC ID: {e}")
        
        if attempt < max_retries - 1:
            print(f"Thử lại sau {backoff_factor} giây...")
            time.sleep(backoff_factor)
    
    print("Không thể lấy danh sách BC ID sau nhiều lần thử.")
    return []


def fetch_all_tiktok_products(bc_id, store_id, access_token):
    """
    Lấy tất cả sản phẩm từ tất cả các trang cho một bc_id và store_id cụ thể từ API TikTok.

    Args:
        bc_id (str): ID của business center.
        store_id (str): ID của cửa hàng.
        access_token (str): Access Token API của bạn.

    Returns:
        list: Một danh sách các sản phẩm, hoặc danh sách rỗng nếu có lỗi.
    """
    base_url = "https://business-api.tiktok.com/open_api/v1.3/store/product/get/"
    headers = {
        'Access-Token': access_token,
        'Content-Type': 'application/json',
    }
    
    all_products = []
    current_page = 1
    total_pages = 1 # Khởi tạo là 1 để bắt đầu vòng lặp

    print(f"--- Bắt đầu lấy dữ liệu sản phẩm cho BC ID: {bc_id} ---")

    while current_page <= total_pages:
        params = {
            'bc_id': bc_id,
            'store_id': store_id,
            'page': current_page,
            'page_size': 100 # Lấy 100 sản phẩm mỗi trang để giảm số lần gọi API
        }

        try:
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()  # Ném lỗi nếu status code là 4xx hoặc 5xx

            data = response.json()

            if data.get("code") != 0:
                print(f"Lỗi từ API hoặc BC ID không có quyền: {data.get('message')} (Request ID: {data.get('request_id')})")
                return [] # Trả về danh sách rỗng nếu có lỗi để vòng lặp bên ngoài biết và thử BC ID tiếp theo

            api_data = data.get("data", {})
            products = api_data.get("store_products", [])
            page_info = api_data.get("page_info", {})

            if not products and current_page == 1:
                print(f"Không tìm thấy sản phẩm nào cho BC ID: {bc_id}.")
                break
            
            all_products.extend(products)
            print(f"Đã lấy thành công {len(products)} sản phẩm từ trang {current_page} (BC ID: {bc_id}).")

            if current_page == 1:
                total_pages = page_info.get("total_page", 1)
                print(f"Tổng số trang cần lấy cho BC ID {bc_id}: {total_pages}")
            
            current_page += 1

        except requests.exceptions.RequestException as e:
            print(f"Đã xảy ra lỗi khi gọi API: {e}")
            return [] # Trả về rỗng khi có lỗi kết nối
        except json.JSONDecodeError:
            print("Không thể giải mã phản hồi JSON từ API.")
            return [] # Trả về rỗng khi lỗi JSON
    
    print(f"--- Hoàn tất lấy sản phẩm cho BC ID: {bc_id}. Tổng cộng: {len(all_products)} sản phẩm. ---")
    return all_products

if __name__ == "__main__":
    STORE_ID = "7494600253418473607" 
    ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN")

    if not ACCESS_TOKEN:
        print("Lỗi: Vui lòng thiết lập biến môi trường TIKTOK_ACCESS_TOKEN.")
    else:
        # 1. Lấy danh sách BC ID
        bc_ids_list = get_bc_ids(ACCESS_TOKEN)
        
        final_products_list = []

        if bc_ids_list:
            print(f"\nBắt đầu quá trình tìm kiếm BC ID hợp lệ cho Store ID: {STORE_ID}...")
            # 2. Lặp qua từng BC ID để tìm BC đầu tiên có dữ liệu
            for bc_id in bc_ids_list:
                products_list = fetch_all_tiktok_products(bc_id, STORE_ID, ACCESS_TOKEN)
                # Nếu hàm trả về danh sách có sản phẩm, ta đã tìm thấy BC hợp lệ
                if products_list:
                    print(f"\n=> Tìm thấy BC ID hợp lệ: {bc_id}. Dừng quá trình tìm kiếm.")
                    final_products_list = products_list
                    break # Thoát khỏi vòng lặp ngay lập tức
            
            # 3. Xử lý kết quả cuối cùng
            if final_products_list:
                print(f"\n--- HOÀN TẤT TẤT CẢ ---")
                print(f"Tổng số sản phẩm đã lấy được: {len(final_products_list)}")

                output_filename = "tiktok_products_single_bc.json"
                try:
                    with open(output_filename, "w", encoding="utf-8") as f:
                        json.dump(final_products_list, f, ensure_ascii=False, indent=4)
                    print(f"Đã lưu tất cả sản phẩm vào file '{output_filename}'")
                except IOError as e:
                    print(f"Không thể lưu file: {e}")
            else:
                print(f"\nKhông tìm thấy BC ID nào có quyền truy cập và có sản phẩm cho Store ID: {STORE_ID}.")
        else:
            print("\nKhông có BC ID nào để xử lý. Kết thúc chương trình.")

