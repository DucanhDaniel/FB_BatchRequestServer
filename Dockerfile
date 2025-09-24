# Dockerfile

# Bước 1: Sử dụng một base image Python chính thức
FROM python:3.13-slim

# [THÊM MỚI] Cài đặt các gói hệ thống cần thiết, bao gồm curl cho healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Bước 2: Thiết lập thư mục làm việc bên trong container
WORKDIR /app

# Bước 3: Sao chép file requirements.txt và cài đặt thư viện
COPY ./requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Bước 4: Sao chép toàn bộ mã nguồn ứng dụng vào thư mục làm việc
COPY . .

# Bước 5: Mở cổng mà Uvicorn sẽ chạy
EXPOSE 8004

# Bước 6: Thiết lập lệnh mặc định để chạy ứng dụng
# [SỬA ĐỔI] Giả định file chính của bạn là 'main.py' và app instance là 'app'
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
CMD ["gunicorn", "-w", "5", "-k", "uvicorn.workers.UvicornWorker", "main:app", "--bind", "0.0.0.0:8010"]