
document.addEventListener('DOMContentLoaded', function () {
    // === DOM Elements ===
    const retryAttemptsTbody = document.querySelector('#retryAttemptsTable tbody');
    const totalRequestsEl = document.getElementById('totalRequests');
    const successRateEl = document.getElementById('successRate');
    const avgDurationEl = document.getElementById('avgDuration');
    const maxAppUsageEl = document.getElementById('maxAppUsage');
    const failedRequestsTbody = document.querySelector('#failedRequestsTable tbody');
    const performanceTbody = document.querySelector('#performanceTable tbody');

    // === Chart Initialization (Khởi tạo MỘT LẦN) ===
    const pieChart = new Chart(document.getElementById('statusChart').getContext('2d'), {
        type: 'pie',
        data: { labels: ['Success', 'Failed'], datasets: [{ data: [0, 0], backgroundColor: ['#27ae60', '#e74c3c'] }] },
        options: { responsive: true, maintainAspectRatio: false, radius: '75%' }
    });
    
    const timeSeriesChart = new Chart(document.getElementById('timeSeriesChart').getContext('2d'), {
        type: 'line',
        data: { labels: [], datasets: [
            { label: 'Duration (ms)', data: [], borderColor: '#3498db', yAxisID: 'yDuration', fill: false },
            { label: 'App Usage (%)', data: [], borderColor: '#e67e22', yAxisID: 'yUsage', fill: false }
        ]},
        options: { responsive: true, maintainAspectRatio: false, scales: {
            x: { type: 'time', time: { unit: 'second', displayFormats: { second: 'HH:mm:ss' } }, title: { display: true, text: 'Time'}},
            yDuration: { type: 'linear', position: 'left', title: { display: true, text: 'Duration (ms)'}, beginAtZero: true },
            yUsage: { type: 'linear', position: 'right', min: 0, max: 100, title: { display: true, text: 'App Usage %'} }
        }}
    });
    
    const costByEndpointChart = new Chart(document.getElementById('costByEndpointChart').getContext('2d'), {
        type: 'bar',
        data: { 
            labels: [], 
            datasets: [
                { label: 'Success Duration (ms)', data: [], backgroundColor: '#27ae60' },
                { label: 'Failed Duration (ms)', data: [], backgroundColor: '#e74c3c' }
            ] 
        },
        options: { 
            responsive: true, 
            maintainAspectRatio: false,
            indexAxis: 'x', 
            scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true } }
        }
    });

    // === Helper Functions ===
    function formatTimestamp(dateObj) {
        if (!dateObj || isNaN(dateObj)) {
            return 'N/A';
        }

        const year = dateObj.getFullYear();
        const month = String(dateObj.getMonth() + 1).padStart(2, '0');
        const day = String(dateObj.getDate()).padStart(2, '0');
        
        const hours = String(dateObj.getHours()).padStart(2, '0');
        const minutes = String(dateObj.getMinutes()).padStart(2, '0');
        const seconds = String(dateObj.getSeconds()).padStart(2, '0');

        // Hiển thị ngày và giờ theo múi giờ local của trình duyệt (UTC+7)
        return `${year}:${month}:${day}<br>${hours}:${minutes}:${seconds}`;
    }

    // function parseLogTimestamp(asctime) {
    //     if (!asctime) return null;

    //     // Bước 1: Thay thế dấu phẩy bằng dấu chấm.
    //     let parsableString = asctime.replace(',', '.');

    //     // Bước 2: Thay thế khoảng trắng giữa ngày và giờ bằng chữ 'T'.
    //     parsableString = parsableString.replace(' ', 'T');

    //     // === SỬA LỖI MÚI GIỜ ===
    //     // Thêm 'Z' để Date.parse() biết đây là giờ UTC, không phải giờ địa phương.
    //     if (!parsableString.endsWith('Z')) {
    //         parsableString += 'Z';
    //     }
    //     // ========================

    //     // Bước 3: Tạo đối tượng Date. Nó sẽ lưu trữ chính xác thời điểm UTC.
    //     const dateObj = new Date(parsableString);
        
    //     if (isNaN(dateObj)) {
    //         return null;
    //     }

    //     // Bỏ logic .setHours() cũ đi. Hàm formatTimestamp
    //     // sẽ tự động dùng múi giờ +7 của trình duyệt để hiển thị.
    //     return dateObj;
    // }
    function parseLogTimestamp(asctime) {
        if (!asctime) return null;

        // Bước 1: Thay thế dấu phẩy bằng dấu chấm.
        let parsableString = asctime.replace(',', '.');

        // Bước 2: Thay thế khoảng trắng giữa ngày và giờ bằng chữ 'T'.
        parsableString = parsableString.replace(' ', 'T');

        // === SỬA LỖI MÚI GIỜ ===
        // Bỏ dòng thêm 'Z'. 
        // Giờ đây "2025-11-15T00:13:07.961" sẽ được Date() hiểu là GIỜ LOCAL (UTC+7),
        // chứ không phải giờ UTC.
        // if (!parsableString.endsWith('Z')) {
        //     parsableString += 'Z';
        // }
        // ========================

        // Bước 3: Tạo đối tượng Date.
        const dateObj = new Date(parsableString);
        
        if (isNaN(dateObj)) {
            return null;
        }

        return dateObj;
    }

    function categorizeUrl(url) {
        if (!url) return 'Unknown';
        if (url.includes('/insights')) {
            if (url.includes('breakdowns=')) return 'Insights Breakdown';
            if (url.includes('time_increment=1')) return 'Insights Daily';
            return 'Insights Overview';
        }
        if (url.includes('/adcreatives')) return 'Ad Creatives';
        if (url.includes('/campaigns')) return 'Campaigns';
        if (url.includes('/adsets')) return 'Ad Sets';
        if (url.includes('/ads')) return 'Ads';
        return 'Other';
    }
    
    function formatPercentage(value) {
        if (value === null || typeof value === 'undefined') return 'N/A';
        return (value * 100).toFixed(2);
    }
    
    // === Main Logic ===
    async function fetchDataAndUpdate() {
        try {
            const response = await fetch('/logs');
            if (!response.ok) {
                console.error(`HTTP error! status: ${response.status}`);
                return;
            }
            
            const logs = await response.json();
            const allSubRequests = logs.filter(log => log['log.type'] === 'sub_request');
            
            if (allSubRequests.length === 0 && logs.length === 0) {
                return;
            }
            
            updateDashboard(logs, allSubRequests);

        } catch (error) {
            console.error('Failed to fetch and render log data:', error);
        }
    }

    function updateDashboard(logs, allSubRequests) {
        // --- 1. Tính toán dữ liệu tổng hợp (dùng allSubRequests) ---
        let successCount = 0;
        let totalDuration = 0;
        let maxAppUsage = 0;
        let costByEndpoint = {};

        allSubRequests.forEach(log => {
            const duration = log.duration_ms || 0;
            totalDuration += duration;
            
            if (log.rate_limit?.app_id_util_pct > maxAppUsage) {
                maxAppUsage = log.rate_limit.app_id_util_pct;
            }
            
            const category = categorizeUrl(log.requested_url);
            if (!costByEndpoint[category]) {
                costByEndpoint[category] = { success: 0, failed: 0 };
            }

            if (log.status_code === 200) {
                successCount++;
                costByEndpoint[category].success += duration;
            } else {
                costByEndpoint[category].failed += duration;
            }
        });

        // --- 2. Cập nhật các thẻ tóm tắt ---
        totalRequestsEl.textContent = allSubRequests.length;
        successRateEl.textContent = `${(successCount / allSubRequests.length * 100).toFixed(1)}%`;
        avgDurationEl.textContent = `${(totalDuration / allSubRequests.length).toFixed(0)} ms`;
        maxAppUsageEl.textContent = `${formatPercentage(maxAppUsage)}%`;

        // --- 3. Cập nhật các biểu đồ ---
        pieChart.data.datasets[0].data = [successCount, allSubRequests.length - successCount];
        pieChart.update();

        const oneHourAgo = new Date(new Date().getTime() - 60 * 60 * 10000);
        const recentLogs = allSubRequests.filter(log => {
            const logTime = parseLogTimestamp(log.asctime);
            return logTime && logTime >= oneHourAgo;
        });
        
        timeSeriesChart.data.labels = timeSeriesChart.data.labels = []; 
        timeSeriesChart.data.datasets[0].data = recentLogs.map(log => ({
            x: parseLogTimestamp(log.asctime),
            y: log.duration_ms
        }));

        timeSeriesChart.data.datasets[1].data = recentLogs.map(log => ({
            x: parseLogTimestamp(log.asctime),
            y: (log.rate_limit?.app_id_util_pct ?? 0) * 100
        }));

        timeSeriesChart.update();
        
        const costLabels = Object.keys(costByEndpoint);
        costByEndpointChart.data.labels = costLabels;
        costByEndpointChart.data.datasets[0].data = costLabels.map(label => costByEndpoint[label].success);
        costByEndpointChart.data.datasets[1].data = costLabels.map(label => costByEndpoint[label].failed);
        costByEndpointChart.update();

        // --- 4. Cập nhật các bảng (dùng allSubRequests) ---
        const recentTableLogs = allSubRequests.slice(-100).reverse();
        
        performanceTbody.innerHTML = recentTableLogs.map(log => {
            let eta = '0';
            try {
                const accId = log.requested_url.split('/')[0];
                const buc = log.rate_limit?.business_use_case;
                if (buc && buc[accId] && buc[accId][0]) {
                    eta = buc[accId][0].estimated_time_to_regain_access;
                }
            } catch (e) {}

            const logTime = parseLogTimestamp(log.asctime);
            const formattedTime = formatTimestamp(logTime);
            return `<tr>
                <td>${formattedTime}</td>
                <td>${log.email || 'N/A'}</td>
                <td>${log.client_ip || 'N/A'}</td>
                <td style="word-break: break-all;">${log.requested_url || 'N/A'}</td>
                <td style="text-align: center;">${log.status_code || 'N/A'}</td>
                <td style="text-align: right;">${log.duration_ms?.toFixed(0) || 'N/A'}</td>
                <td style="text-align: right;">${formatPercentage(log.rate_limit?.app_id_util_pct)}</td>
                <td style="text-align: right;">${formatPercentage(log.rate_limit?.acc_id_util_pct)}</td>
                <td style="text-align: center;">${eta}</td>
            </tr>`;
        }).join('');

        failedRequestsTbody.innerHTML = recentTableLogs.filter(log => log.status_code !== 200).map(log => {
            const errorMessage = log.error?.message || JSON.stringify(log.error) || 'Unknown error';
            const logTime = parseLogTimestamp(log.asctime);
            const formattedTime = formatTimestamp(logTime);

            return `<tr>
                <td>${formattedTime}</td>
                <td>${log.email || 'N/A'}</td>
                <td>${log.client_ip || 'N/A'}</td>
                <td style="word-break: break-all;">${log.requested_url || 'N/A'}</td>
                <td style="text-align: center; color: #e74c3c; font-weight: bold;">${log.status_code || 'N/A'}</td>
                <td>${errorMessage}</td>
            </tr>`;
        }).join('');

        
        // === SỬA LỖI LOGIC BẢNG RETRY ATTEMPTS ===
        
        // 1. Lọc tất cả các log liên quan đến retry
        const allRetryLogs = logs.filter(log => 
            log.message && 
            (log.message.includes('Retrying sub-request') || log.message.includes('Retry for sub-request'))
        );

        // 2. Nhóm các log lại theo composite key (request_id + request_index)
        const groupedRetries = new Map();

        for (const log of allRetryLogs) {
            // Dùng request_index làm key (giả sử request_id luôn giống nhau trong 1 lần load)
            // Nếu có nhiều request_id, dùng key: `${log.request_id}-${log.request_index}`
            const key = log.request_index; 
            
            // Khởi tạo nếu chưa có
            if (!groupedRetries.has(key)) {
                groupedRetries.set(key, {
                    timestamp: parseLogTimestamp(log.asctime), // Dùng timestamp của log đầu tiên
                    url: 'N/A',
                    email: 'N/A',
                    client_ip: 'N/A',
                    finalStatus: 'N/A',
                    outcome: 'Unknown',
                    outcomeClass: ''
                });
            }

            const attempt = groupedRetries.get(key);

            // 3. Cập nhật thông tin dựa trên loại log
            if (log.message.includes('Retrying sub-request')) {
                // Đây là log "bắt đầu", nó có URL
                try {
                    attempt.url = log.message.split(': ')[1];
                } catch (e) {
                    attempt.url = 'Error parsing URL';
                }
                // Nếu chưa có kết quả (FAILED/SUCCESS), thì set là Retrying
                if (attempt.outcome === 'Unknown') {
                    attempt.outcome = 'Retrying';
                }
                // Ưu tiên timestamp của log này (log bắt đầu)
                attempt.timestamp = parseLogTimestamp(log.asctime);

            } else if (log.message.includes('FAILED')) {
                // Đây là log "thất bại", nó có email, ip, status
                attempt.outcome = 'Failed';
                attempt.outcomeClass = 'status-failed';
                attempt.finalStatus = log.new_status_code || 'N/A';
                attempt.email = log.email || 'N/A';
                attempt.client_ip = log.client_ip || 'N/A';
                // Ghi đè timestamp bằng log kết thúc
                attempt.timestamp = parseLogTimestamp(log.asctime);

            } else if (log.message.includes('SUCCEEDED')) {
                // Đây là log "thành công"
                attempt.outcome = 'Success';
                attempt.outcomeClass = 'status-success';
                attempt.finalStatus = 200; // Mặc định là 200
                // Log SUCCEEDED không có email/ip (theo code Python)
                // nên ta không ghi đè email/ip
                attempt.timestamp = parseLogTimestamp(log.asctime);
            }
        }

        // 4. Sắp xếp và build HTML
        // Chuyển Map values thành Array
        const sortedAttempts = Array.from(groupedRetries.values())
            .sort((a, b) => b.timestamp - a.timestamp) // Sắp xếp mới nhất lên đầu
            .slice(0, 100); // Giới hạn 100

        retryAttemptsTbody.innerHTML = sortedAttempts.map(attempt => {
            const formattedTime = formatTimestamp(attempt.timestamp);
            
            return `<tr>
                <td>${formattedTime}</td>
                <td>${attempt.email}</td>
                <td>${attempt.client_ip}</td>
                <td style="word-break: break-all;">${attempt.url}</td>
                <td style="text-align: center;">${attempt.finalStatus}</td>
                <td class="${attempt.outcomeClass}" style="text-align: center;">${attempt.outcome}</td>
            </tr>`;
        }).join('');
        // === KẾT THÚC LOGIC MỚI ===
    }

    // Chạy lần đầu và đặt lịch cập nhật
    fetchDataAndUpdate();
    setInterval(fetchDataAndUpdate, 3000);
});