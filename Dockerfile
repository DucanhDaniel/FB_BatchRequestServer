# Bước 1: Sử dụng một base image Python chính thức
# python:3.11-slim là một lựa chọn tốt vì nó nhẹ và hiện đại.
FROM python:3.13-slim

# Bước 2: Thiết lập thư mục làm việc bên trong container
WORKDIR /code

# Bước 3: Sao chép file requirements.txt vào container
COPY ./requirements.txt /code/requirements.txt

# Bước 4: Cài đặt các thư viện cần thiết
# --no-cache-dir: Không lưu cache, giúp giảm kích thước image.
# --upgrade: Đảm bảo pip được cập nhật.
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Bước 5: Sao chép toàn bộ mã nguồn ứng dụng vào container
COPY ./ /code/app

# Bước 6: Mở (expose) cổng mà Uvicorn sẽ chạy
# Lệnh này không thực sự publish cổng, nó hoạt động như một tài liệu
# cho người dùng biết container sẽ lắng nghe trên cổng nào.
EXPOSE 8000

# Bước 7: Thiết lập lệnh mặc định để chạy ứng dụng khi container khởi động
# Lệnh này tương tự như "Start Command" trên Render.
# Uvicorn sẽ lắng nghe trên tất cả các địa chỉ IP bên trong container.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]