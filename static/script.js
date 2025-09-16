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
        // getMonth() trả về từ 0-11, nên cần +1.
        // padStart(2, '0') để đảm bảo luôn có 2 chữ số (VD: 9 -> "09").
        const month = String(dateObj.getMonth() + 1).padStart(2, '0');
        const day = String(dateObj.getDate()).padStart(2, '0');
        
        const hours = String(dateObj.getHours()).padStart(2, '0');
        const minutes = String(dateObj.getMinutes()).padStart(2, '0');
        const seconds = String(dateObj.getSeconds()).padStart(2, '0');

        // Lưu ý: Dùng dấu ':' theo yêu cầu của bạn
        return `${year}:${month}:${day}<br>${hours}:${minutes}:${seconds}`;
    }

    function parseLogTimestamp(asctime) {
        if (!asctime) return null;

        // Bước 1: Thay thế dấu phẩy bằng dấu chấm.
        // "2025-09-15 14:37:39,384" -> "2025-09-15 14:37:39.384"
        let parsableString = asctime.replace(',', '.');

        // Bước 2: Thay thế khoảng trắng đầu tiên (giữa ngày và giờ) bằng chữ 'T'.
        // "2025-09-15 14:37:39.384" -> "2025-09-15T14:37:39.384"
        parsableString = parsableString.replace(' ', 'T');

        // Bây giờ chuỗi đã ở định dạng được hỗ trợ toàn cầu.
        const dateObj = new Date(parsableString);

        // Kiểm tra lại lần cuối xem parse có thành công không.
        return isNaN(dateObj) ? null : dateObj;
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
            
            // Nếu không có log nào thì không làm gì cả
            if (allSubRequests.length === 0) {
                return;
            }
            
            // Gọi hàm cập nhật giao diện
            updateDashboard(allSubRequests);

        } catch (error) {
            console.error('Failed to fetch and render log data:', error);
        }
    }

    function updateDashboard(allSubRequests) {
        // --- 1. Tính toán dữ liệu tổng hợp ---
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
        // const recentLogs = allSubRequests.filter(log => new Date(log.asctime) >= oneHourAgo);
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

        // --- 4. Cập nhật các bảng ---
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
            return `<tr>
                <td>${log.requested_url || 'N/A'}</td>
                <td style="text-align: center;">${log.status_code || 'N/A'}</td>
                <td style="text-align: right;">${log.duration_ms?.toFixed(0) || 'N/A'}</td>
                <td style="text-align: right;">${formatPercentage(log.rate_limit?.app_id_util_pct)}</td>
                <td style="text-align: right;">${formatPercentage(log.rate_limit?.acc_id_util_pct)}</td>
                <td style="text-align: center;">${eta}</td>
            </tr>`;
        }).join('');

        failedRequestsTbody.innerHTML = recentTableLogs.filter(log => log.status_code !== 200).map(log => {
            const errorMessage = log.error?.message || JSON.stringify(log.error) || 'Unknown error';
            // 1. Parse timestamp bằng hàm helper như cũ
            const logTime = parseLogTimestamp(log.asctime);
            
            // 2. SỬ DỤNG HÀM MỚI để định dạng đối tượng Date 'logTime'
            const formattedTime = formatTimestamp(logTime);

            return `<tr>
                <td>${formattedTime}</td>
                <td>${log.requested_url || 'N/A'}</td>
                <td style="text-align: center; color: #e74c3c; font-weight: bold;">${log.status_code || 'N/A'}</td>
                <td>${errorMessage}</td>
            </tr>`;
        }).join('');

        const retryLogs = allSubRequests
            .filter(log => log.message && log.message.includes('RETRY'))
            .slice(-100)
            .reverse();

        // cập nhật bảng recent tried 
        retryAttemptsTbody.innerHTML = retryLogs.map(log => {
            const logTime = parseLogTimestamp(log.asctime);
            const formattedTime = formatTimestamp(logTime);
            const finalStatus = log.status_code;
            
            let outcome = 'Unknown';
            let outcomeClass = '';

            if (log.message.includes('RETRY SUCCESS')) {
                outcome = 'Success';
                outcomeClass = 'status-success';
            } else if (log.message.includes('RETRY FAILED')) {
                outcome = 'Failed';
                outcomeClass = 'status-failed';
            }

            return `<tr>
                <td>${formattedTime}</td>
                <td>${log.requested_url || 'N/A'}</td>
                <td style="text-align: center;">${finalStatus}</td>
                <td class="${outcomeClass}" style="text-align: center;">${outcome}</td>
            </tr>`;
        }).join('');
    }

    // Chạy lần đầu và đặt lịch cập nhật
    fetchDataAndUpdate();
    setInterval(fetchDataAndUpdate, 3000);
});