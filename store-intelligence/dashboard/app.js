// --- Platform Configurations & Endpoint Aliases ---
const STORE_ID = "STORE_BLR_002";
const BASE_API_URL = "http://localhost:8000";
const WS_STREAM_URL = `ws://localhost:8000/ws/stores/${STORE_ID}/telemetry`;

// Global chart context instance references
let funnelChartInstance = null;
let timelineChartInstance = null;
let staffRatioChartInstance = null;
let heatmapInstance = null;

// Anomaly tracking for UI feedback
let recentAnomalies = [];
let anomalyFlashTimeout = null;

// Initialize Heatmap Canvas
function initHeatmapCanvas() {
    const container = document.getElementById('heatmapContainer');
    
    // Create canvas element for heatmap
    let canvas = container.querySelector('canvas');
    if (canvas) {
        canvas.remove();
    }
    
    canvas = document.createElement('canvas');
    canvas.style.width = '100%';
    canvas.style.height = '100%';
    canvas.style.display = 'block';
    container.innerHTML = '';
    container.appendChild(canvas);
    
    // Initialize heatmap.js instance
    heatmapInstance = h337.create({
        container: container,
        radius: 40,
        maxOpacity: 0.8,
        minOpacity: 0.1,
        blur: 85,
        gradient: {
            '.0': '#00ff00',
            '.25': '#ffff00',
            '.5': '#ff7700',
            '.75': '#ff0000',
            '1.0': '#8b0000'
        }
    });
    
    return heatmapInstance;
}

// Initialize Funnel Chart
function initFunnelChart() {
    const ctx = document.getElementById('funnelChart').getContext('2d');
    funnelChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['1. Total Traffic', '2. Browser Engagement', '3. Checkout Intent', '4. Completed Transaction'],
            datasets: [
                {
                    label: 'Visitor Count',
                    data: [0, 0, 0, 0],
                    backgroundColor: ['#3b82f6', '#8b5cf6', '#f59e0b', '#10b981'],
                    borderWidth: 0,
                    borderRadius: 4,
                    yAxisID: 'y'
                },
                {
                    label: 'Retention % (from prev)',
                    data: [100, 0, 0, 0],
                    type: 'line',
                    borderColor: '#ef4444',
                    borderWidth: 3,
                    fill: false,
                    pointRadius: 5,
                    pointBackgroundColor: '#ef4444',
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { 
                legend: { display: true, labels: { color: '#9ca3af' } },
                tooltip: {
                    callbacks: {
                        afterLabel: function(context) {
                            if (context.datasetIndex === 1) {
                                return context.parsed.y.toFixed(1) + '%';
                            }
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: '#374151' },
                    ticks: { color: '#9ca3af' },
                    title: { display: true, text: 'Count', color: '#9ca3af' }
                },
                y1: {
                    type: 'linear',
                    position: 'right',
                    beginAtZero: true,
                    max: 100,
                    grid: { drawOnChartArea: false },
                    ticks: { color: '#ef4444' },
                    title: { display: true, text: 'Retention %', color: '#ef4444' }
                },
                x: { 
                    grid: { display: false }, 
                    ticks: { color: '#9ca3af' } 
                }
            }
        }
    });
}

// Initialize Timeline Chart
function initTimelineChart() {
    const ctx = document.getElementById('timelineChart').getContext('2d');
    timelineChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Unique Visitor Entries',
                data: [],
                borderColor: '#06b6d4',
                backgroundColor: 'rgba(6, 182, 212, 0.1)',
                tension: 0.4,
                fill: true,
                pointRadius: 4,
                pointBackgroundColor: '#06b6d4',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                y: { beginAtZero: true, grid: { color: '#374151' }, ticks: { color: '#9ca3af' } },
                x: { grid: { display: false }, ticks: { color: '#9ca3af' } }
            }
        }
    });
}

// Initialize Staff Ratio Chart
function initStaffRatioChart() {
    const ctx = document.getElementById('staffRatioChart').getContext('2d');
    staffRatioChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Customers',
                    data: [],
                    backgroundColor: '#3b82f6'
                },
                {
                    label: 'Staff',
                    data: [],
                    backgroundColor: '#10b981'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: { legend: { display: true, labels: { color: '#9ca3af' } } },
            scales: {
                x: { stacked: false, grid: { color: '#374151' }, ticks: { color: '#9ca3af' } },
                y: { grid: { display: false }, ticks: { color: '#9ca3af' } }
            }
        }
    });
}

// Fetch and Update All Analytics
async function pollAnalyticalMetrics() {
    try {
        // Pull general metrics
        const metricsRes = await fetch(`${BASE_API_URL}/stores/${STORE_ID}/metrics`);
        if (metricsRes.ok) {
            const mData = await metricsRes.json();
            document.getElementById('stat-traffic').innerText = mData.total_unique_customers;
            document.getElementById('stat-sales').innerText = mData.total_transactions;
            document.getElementById('stat-conversion').innerText = `${mData.conversion_rate_percentage}%`;
            document.getElementById('metric-queue-wait').innerText = `${mData.avg_queue_wait_time_seconds} sec`;
            document.getElementById('metric-dwell').innerText = `${mData.avg_dwell_time_minutes} min`;
            
            // Update occupancy timeline
            if (mData.peak_occupancy_hours && timelineChartInstance) {
                const hours = Object.keys(mData.peak_occupancy_hours).sort();
                const counts = hours.map(h => mData.peak_occupancy_hours[h]);
                timelineChartInstance.data.labels = hours.map(h => h.substring(11, 16));
                timelineChartInstance.data.datasets[0].data = counts;
                timelineChartInstance.update();
            }
            
            // Update staff ratio chart
            if (mData.staff_to_customer_ratio && staffRatioChartInstance) {
                const hours = Object.keys(mData.staff_to_customer_ratio).sort();
                const customers = hours.map(h => mData.staff_to_customer_ratio[h].customers);
                const staff = hours.map(h => mData.staff_to_customer_ratio[h].staff);
                staffRatioChartInstance.data.labels = hours.map(h => h.substring(11, 16));
                staffRatioChartInstance.data.datasets[0].data = customers;
                staffRatioChartInstance.data.datasets[1].data = staff;
                staffRatioChartInstance.update();
            }
        }

        // Pull conversion funnel
        const funnelRes = await fetch(`${BASE_API_URL}/stores/${STORE_ID}/funnel`);
        if (funnelRes.ok) {
            const fData = await funnelRes.json();
            if (funnelChartInstance) {
                const counts = fData.funnel_stages.map(s => s.count);
                const retention = fData.funnel_stages.map(s => s.retention_rate);
                funnelChartInstance.data.datasets[0].data = counts;
                funnelChartInstance.data.datasets[1].data = retention;
                funnelChartInstance.update();
                
                document.getElementById('metric-total-conv').innerText = `${fData.total_conversion_rate}%`;
            }
        }

        // Pull heatmap data
        const heatmapRes = await fetch(`${BASE_API_URL}/stores/${STORE_ID}/heatmap?limit=500`);
        if (heatmapRes.ok) {
            const hData = await heatmapRes.json();
            updateHeatmap(hData.coordinates);
        }

        // Pull anomalies
        const anomalyRes = await fetch(`${BASE_API_URL}/stores/${STORE_ID}/anomalies`);
        if (anomalyRes.ok) {
            const aData = await anomalyRes.json();
            renderAnomalies(aData.active_anomalies);
        }
    } catch (err) {
        console.error("[-] Analytics polling error: ", err);
    }
}

// Update Heatmap Visualization
function updateHeatmap(coordinates) {
    if (!heatmapInstance || !coordinates || coordinates.length === 0) return;
    
    const container = document.getElementById('heatmapContainer');
    const rect = container.getBoundingClientRect();
    
    // Convert normalized coordinates to pixel coordinates
    const dataPoints = coordinates.map(coord => ({
        x: Math.round(coord.x * rect.width),
        y: Math.round(coord.y * rect.height),
        value: 1
    }));
    
    heatmapInstance.setData({
        max: 10,
        min: 0,
        data: dataPoints
    });
}

// Render Dynamic Anomalies Alerts
function renderAnomalies(anomalies) {
    const container = document.getElementById('anomaly-container');
    const badge = document.getElementById('anomaly-badge');
    
    if (!anomalies || anomalies.length === 0) {
        container.innerHTML = `<div class="text-sm text-gray-500 italic p-4 bg-gray-950 rounded border border-gray-800">No active operational exceptions or metric threshold breaks detected in this window.</div>`;
        badge.classList.add('hidden');
        return;
    }

    // Update recent anomalies for flashing effect
    if (anomalies.length > 0) {
        recentAnomalies = anomalies;
        badge.classList.remove('hidden');
        
        // Flash effect
        clearTimeout(anomalyFlashTimeout);
        badge.style.animation = 'none';
        setTimeout(() => {
            badge.style.animation = 'pulse 2s infinite';
        }, 10);
    }

    container.innerHTML = anomalies.map(alert => `
        <div class="border ${alert.severity === 'CRITICAL' ? 'border-red-900 bg-red-950/40 text-red-200 anomaly-pulse' : 'border-amber-900 bg-amber-950/40 text-amber-200'} p-4 rounded-md text-sm flex justify-between items-start">
            <div>
                <span class="font-bold tracking-wide text-xs px-2 py-0.5 rounded mr-2 ${alert.severity === 'CRITICAL' ? 'bg-red-800' : 'bg-amber-800'} text-white">${alert.severity}</span>
                <span class="font-mono font-semibold">${alert.anomaly_type}</span>
                <p class="mt-1 text-gray-300 text-xs">${alert.description}</p>
                ${alert.timestamp ? `<p class="mt-1 text-gray-400 text-xs">${new Date(alert.timestamp).toLocaleTimeString()}</p>` : ''}
            </div>
        </div>
    `).join('');
}

// WebSocket Stream Handler
function establishWebSocketStream() {
    const socket = new WebSocket(WS_STREAM_URL);
    const feedContainer = document.getElementById('log-feed');
    const eventLog = [];

    socket.onopen = () => {
        console.log("[+] WebSocket connection established.");
        feedContainer.innerHTML = `<div class="text-emerald-500 font-bold font-mono">=== PERSISTENT STREAM CONNECTED ===</div>`;
    };

    socket.onmessage = (messageEvent) => {
        try {
            const rawEvent = JSON.parse(messageEvent.data);
            const timestamp = new Date(rawEvent.timestamp).toLocaleTimeString();
            
            // Check for anomalies in the event
            if (rawEvent.metadata && rawEvent.metadata.anomalies && rawEvent.metadata.anomalies.length > 0) {
                const anomalyStr = rawEvent.metadata.anomalies.join(', ');
                feedContainer.innerHTML = `<div class="text-red-400 font-bold">[${timestamp}] ANOMALY: ${rawEvent.visitor_id} - ${anomalyStr}</div>` + feedContainer.innerHTML;
            } else {
                feedContainer.innerHTML = `<div>[${timestamp}] ${rawEvent.event_type} | Visitor: ${rawEvent.visitor_id} | Zone: ${rawEvent.zone_id || 'N/A'}</div>` + feedContainer.innerHTML;
            }
            
            // Keep log size manageable (max 50 entries)
            while (feedContainer.children.length > 50) {
                feedContainer.removeChild(feedContainer.lastChild);
            }
        } catch (err) {
            console.error("[-] WebSocket message parse error:", err);
        }
    };

    socket.onerror = (err) => {
        console.error("[-] WebSocket error:", err);
        feedContainer.innerHTML = `<div class="text-red-500">Connection lost. Attempting to reconnect...</div>`;
    };

    socket.onclose = () => {
        console.log("[!] WebSocket disconnected. Retrying in 3 seconds...");
        setTimeout(establishWebSocketStream, 3000);
    };
}

// Initialize on Page Load
document.addEventListener('DOMContentLoaded', () => {
    console.log("[*] Initializing Apex Eye Dashboard...");
    
    // Initialize all charts
    initFunnelChart();
    initTimelineChart();
    initStaffRatioChart();
    initHeatmapCanvas();
    
    // Start polling metrics every 5 seconds
    pollAnalyticalMetrics();
    setInterval(pollAnalyticalMetrics, 5000);
    
    // Establish WebSocket connection
    establishWebSocketStream();
    
    console.log("[+] Dashboard initialized successfully.");
});