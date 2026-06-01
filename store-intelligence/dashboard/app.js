// --- Platform Configurations & Endpoint Aliases ---
const STORE_ID = "STORE_BLR_002";
const BASE_API_URL = "http://localhost:8000";
const WS_STREAM_URL = `ws://localhost:8000/ws/stores/${STORE_ID}/telemetry`;

// Global chart context instance reference variable
let funnelChartInstance = null;

// Initialize Charting Framework Dimensions
function initFunnelChart() {
    const ctx = document.getElementById('funnelChart').getContext('2d');
    funnelChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['1. Entry Traffic', '2. Product Zone Browsing', '3. Checkout Queue Line', '4. Final POS Purchase'],
            datasets: [{
                label: 'Retention Percentage (%)',
                data: [100, 0, 0, 0],
                backgroundColor: ['#3b82f6', '#8b5cf6', '#f59e0b', '#10b981'],
                borderWidth: 0,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                y: { beginAtZero: true, max: 100, grid: { color: '#1f2937' }, ticks: { color: '#9ca3af' } },
                x: { grid: { display: false }, ticks: { color: '#9ca3af' } }
            }
        }
    });
}

// Fetch Analytical Profiles from REST layer
async function pollAnalyticalMetrics() {
    try {
        // Pull general window calculations
        const metricsRes = await fetch(`${BASE_API_URL}/stores/${STORE_ID}/metrics`);
        if (metricsRes.ok) {
            const mData = await metricsRes.json();
            document.getElementById('stat-traffic').innerText = mData.total_unique_customers;
            document.getElementById('stat-sales').innerText = mData.total_transactions;
            document.getElementById('stat-conversion').innerText = `${mData.conversion_rate_percentage}%`;
        }

        // Pull drop-off funnel distributions
        const funnelRes = await fetch(`${BASE_API_URL}/stores/${STORE_ID}/funnel`);
        if (funnelRes.ok) {
            const fData = await funnelRes.json();
            const retentionArray = fData.funnel_stages.map(stage => stage.retention_rate);
            
            // Update live graph metrics dynamically without canvas wipes
            funnelChartInstance.data.datasets[0].data = retentionArray;
            funnelChartInstance.update();
        }

        // Pull active system anomalies anomalies
        const anomalyRes = await fetch(`${BASE_API_URL}/stores/${STORE_ID}/anomalies`);
        if (anomalyRes.ok) {
            const aData = await anomalyRes.json();
            renderAnomalies(aData.active_anomalies);
        }
    } catch (err) {
        console.error("[-] Analytics long-polling update iteration block dropped: ", err);
    }
}

// Render dynamic anomalies alerts 
function renderAnomalies(anomalies) {
    const container = document.getElementById('anomaly-container');
    if (!anomalies || anomalies.length === 0) {
        container.innerHTML = `<div class="text-sm text-gray-500 italic p-4 bg-gray-950 rounded border border-gray-800">No active operational exceptions or metric threshold breaks detected in this window.</div>`;
        return;
    }

    container.innerHTML = anomalies.map(alert => `
        <div class="border ${alert.severity === 'CRITICAL' ? 'border-red-900 bg-red-950/40 text-red-200' : 'border-amber-900 bg-amber-950/40 text-amber-200'} p-4 rounded-md text-sm flex justify-between items-start">
            <div>
                <span class="font-bold tracking-wide text-xs px-2 py-0.5 rounded mr-2 ${alert.severity === 'CRITICAL' ? 'bg-red-800' : 'bg-amber-800'} text-white">${alert.severity}</span>
                <span class="font-mono font-semibold">${alert.anomaly_type}</span>
                <p class="mt-1 text-gray-300 text-xs">${alert.description}</p>
            </div>
        </div>
    `).join('');
}

// Connect the persistent WebSocket link directly to the Kafka Consumer proxy endpoint
function establishWebSocketStream() {
    const socket = new WebSocket(WS_STREAM_URL);
    const feedContainer = document.getElementById('log-feed');

    socket.onopen = () => {
        console.log("[+] Channel connection established over WebSockets. System Online.");
        feedContainer.innerHTML = `<div class="text-emerald-500 font-bold font-mono">=== PERSISTENT STREAM CONNECTED TO BACKEND ===</div>`;
    };

    socket.onmessage = (messageEvent) => {
        const rawEvent = JSON.parse(messageEvent.data);
        
        // Dynamic Quick State Overlays (e.g., Live Queue Counters)
        if (rawEvent.event_type === "BILLING_QUEUE_JOIN") {
            document.getElementById('stat-queue').innerText = rawEvent.metadata.queue_depth || 0;
        }

        // Render standard scrollable event telemetry printout rows
        const row = document.createElement('div');
        row.className = "py-1 border-b border-gray-900 flex justify-between tracking-tighter text-gray-300 font-mono";
        
        // Exclude staff rows visually or change theme metrics colors
        const staffTag = rawEvent.is_staff ? `<span class="text-blue-400">[STAFF]</span>` : `<span class="text-gray-500">[CUST]</span>`;
        
        row.innerHTML = `
            <span>${rawEvent.timestamp.split('T')[1].replace('Z','')} | ${staffTag} <strong>${rawEvent.event_type}</strong></span>
            <span class="text-gray-500 text-right">${rawEvent.visitor_id}</span>
        `;
        
        feedContainer.insertBefore(row, feedContainer.firstChild);
        
        // Enforce trim size bounds to prevent crashing browser memory heaps on long clips runs
        if (feedContainer.childNodes.length > 50) {
            feedContainer.removeChild(feedContainer.lastChild);
        }
    };

    socket.onclose = () => {
        console.warn("[-] Streaming socket channel connection broken. Attempting fallback recovery loop in 5s...");
        setTimeout(establishWebSocketStream, 5000);
    };
}

// --- Application Init Execution Orchestrator ---
document.addEventListener('DOMContentLoaded', () => {
    initFunnelChart();
    establishWebSocketStream();
    
    // Immediate baseline updates poll
    pollAnalyticalMetrics();
    // Continuously pull transactional metrics updates every 4 seconds
    setInterval(pollAnalyticalMetrics, 4000);
});