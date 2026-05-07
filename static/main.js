function updateClock() {
    document.getElementById("clock").textContent =
    new Date().toLocaleTimeString([], { hour12: false });
}
setInterval(updateClock, 1000);
updateClock();

Chart.defaults.color = "#597489";
Chart.defaults.borderColor = "#1e2d3d";
Chart.defaults.font.family = "'Share Tech Mono', monospace";
Chart.defaults.font.size = 10;

const MAX_TREND_PTS = 40;
const MAX_BAR_PTS = 20;
const ARC_LEN = 345.6;
let historyLoaded = false;
let lastSnapshotTs = null;
let currentSnapshot = null;

function chartPadding() {
    return { top: 6, right: 12, bottom: 18, left: 8 };
}

const trendChart = new Chart(document.getElementById("chart-trend"), {
    type: "line",
    data: {
    labels: [],
    datasets: [
        {
        label: "CPU %",
        data: [],
        borderColor: "#ffb830",
        backgroundColor: "rgba(255,184,48,.08)",
        borderWidth: 1.8,
        pointRadius: 0,
        tension: 0.35,
        fill: true,
        yAxisID: "y"
        },
        {
        label: "Gecikmə ms",
        data: [],
        borderColor: "#00e5c3",
        backgroundColor: "rgba(0,229,195,.06)",
        borderWidth: 1.8,
        pointRadius: 0,
        tension: 0.35,
        fill: true,
        yAxisID: "y1"
        }
    ]
    },
    options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    layout: { padding: chartPadding() },
    interaction: { mode: "index", intersect: false },
    scales: {
        x: {
        grid: { color: "#1a2535" },
        ticks: { maxTicksLimit: 6, autoSkip: true, maxRotation: 0, minRotation: 0, padding: 8 }
        },
        y: {
        min: 0,
        max: 100,
        grid: { color: "#1a2535" },
        ticks: { padding: 8 },
        title: { display: true, text: "CPU %", color: "#ffb830" }
        },
        y1: {
        position: "right",
        min: 0,
        grid: { drawOnChartArea: false },
        ticks: { padding: 8 },
        title: { display: true, text: "Gecikmə ms", color: "#00e5c3" }
        }
    },
    plugins: {
        legend: { position: "top", labels: { boxWidth: 10 } }
    }
    }
});

const mm1Chart = new Chart(document.getElementById("chart-mm1"), {
    type: "line",
    data: {
    datasets: [
        {
        label: "Teorik gözləmə",
        data: [],
        borderColor: "#00e5c3",
        backgroundColor: "rgba(0,229,195,.06)",
        borderWidth: 1.6,
        pointRadius: 0,
        tension: 0.12,
        fill: true
        },
        {
        label: "Cari yüklənmə",
        data: [],
        borderColor: "#ff4560",
        backgroundColor: "#ff4560",
        borderWidth: 0,
        pointRadius: 5,
        showLine: false
        }
    ]
    },
    options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    layout: { padding: chartPadding() },
    scales: {
        x: {
        type: "linear",
        grid: { color: "#1a2535" },
        ticks: { maxTicksLimit: 6, padding: 8 },
        title: { display: true, text: "Trafik sürəti" }
        },
        y: {
        min: 0,
        max: 2,
        grid: { color: "#1a2535" },
        ticks: { padding: 8 },
        title: { display: true, text: "Gözləmə vaxtı" }
        }
    },
    plugins: {
        legend: { position: "top", labels: { boxWidth: 10 } }
    }
    }
});

const qosBarChart = new Chart(document.getElementById("chart-qos-bar"), {
    type: "bar",
    data: {
    labels: [],
    datasets: [{
        label: "QoS",
        data: [],
        backgroundColor: [],
        borderRadius: 3,
        borderSkipped: false
    }]
    },
    options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 180 },
    layout: { padding: chartPadding() },
    scales: {
        x: {
        grid: { color: "#1a2535" },
        ticks: { maxTicksLimit: 6, autoSkip: true, maxRotation: 0, minRotation: 0, padding: 8 }
        },
        y: {
        min: 0,
        max: 100,
        grid: { color: "#1a2535" },
        ticks: { padding: 8 },
        title: { display: true, text: "QoS /100" }
        }
    },
    plugins: {
        legend: { display: false }
    }
    }
});

function statusClass(status) {
    return {
    Normal: "badge-normal",
    Warning: "badge-warning",
    Critical: "badge-critical"
    }[status] || "badge-normal";
}

function qosBgColor(score) {
    if (score > 80) return "rgba(0,230,118,.72)";
    if (score > 50) return "rgba(255,184,48,.72)";
    return "rgba(255,69,96,.72)";
}

function formatLocalTime(ts) {
    if (!ts) return "--";
    const dt = new Date(ts);
    if (Number.isNaN(dt.getTime())) return "--";
    return dt.toLocaleTimeString([], { hour12: false });
}

function safeWait(value) {
    return Number.isFinite(value) ? value.toFixed(3) : "INF";
}

function setGauge(score, status) {
    const filled = (score / 100) * ARC_LEN;
    const arc = document.getElementById("gauge-arc");
    const colorMap = { Normal: "#00e676", Warning: "#ffb830", Critical: "#ff4560" };
    arc.setAttribute("stroke-dasharray", `${filled.toFixed(1)} ${(ARC_LEN - filled).toFixed(1)}`);
    arc.setAttribute("stroke", colorMap[status] || "#00b4ff");

    document.getElementById("gauge-score-label").textContent = score.toFixed(1);
    const label = document.getElementById("gauge-status-label");
    label.textContent = status.toUpperCase();
    label.className = status === "Critical" ? "log-critical" : status === "Warning" ? "log-warning" : "log-normal";
}

function mergeSnapshot(partialSnapshot) {
    if (!currentSnapshot) {
    currentSnapshot = {
        service_sessions: { voip: 0, video: 0, web: 0 },
        action_center: null
    };
    }

    currentSnapshot = {
    ...currentSnapshot,
    ...partialSnapshot,
    service_sessions: {
        ...(currentSnapshot.service_sessions || {}),
        ...(partialSnapshot.service_sessions || {})
    },
    action_center: partialSnapshot.action_center
        ? { ...(currentSnapshot.action_center || {}), ...partialSnapshot.action_center }
        : currentSnapshot.action_center
    };
    return currentSnapshot;
}

function updateMetricCards(snapshot) {
    document.getElementById("v-cpu").textContent = snapshot.cpu.toFixed(1);
    document.getElementById("v-jitter").textContent = snapshot.jitter.toFixed(1);
    document.getElementById("v-delay").textContent = snapshot.delay.toFixed(1);
    document.getElementById("v-lambda").textContent = snapshot.lambda_.toFixed(2);
    document.getElementById("v-wq").textContent = safeWait(snapshot.wq);
    document.getElementById("v-qos").textContent = snapshot.qos.toFixed(1);
    document.getElementById("v-ims").textContent = snapshot.ims_total ?? "--";
    document.getElementById("v-voip").textContent = snapshot.service_sessions?.voip ?? "--";
    document.getElementById("v-video").textContent = snapshot.service_sessions?.video ?? "--";
    document.getElementById("v-web").textContent = snapshot.service_sessions?.web ?? "--";

    const statusBadge = document.getElementById("v-status");
    statusBadge.textContent = snapshot.status;
    statusBadge.className = `status-badge ${statusClass(snapshot.status)}`;
}

function updateActionCenter(snapshot) {
    const action = snapshot.action_center;
    if (!action) return;

    const modeEl = document.getElementById("action-mode");
    const scenarioEl = document.getElementById("action-scenario");
    const priorityEl = document.getElementById("action-priority");
    const serviceEl = document.getElementById("action-service");
    const confidenceEl = document.getElementById("action-confidence");

    modeEl.textContent = `REJİM: ${action.mode.toUpperCase()}`;
    modeEl.className = `mini-badge ${action.mode === "manual" ? "badge-warning" : "badge-normal"}`;

    scenarioEl.textContent = `SSENARİ: ${action.scenario.replace(/_/g, " ").toUpperCase()}`;
    scenarioEl.className = `mini-badge ${statusClass(snapshot.status)}`;

    priorityEl.textContent = `PRİORİTET: ${action.priority}`;
    priorityEl.className = `mini-badge ${action.priority === "YÜKSƏK" || action.priority === "KRİTİK" ? "badge-critical" : action.priority === "ORTA" ? "badge-warning" : "badge-normal"}`;

    serviceEl.textContent = `XİDMƏT: ${action.service.toUpperCase()}`;
    serviceEl.className = "mini-badge badge-normal";

    confidenceEl.textContent = `ETİBAR: ${Number(action.confidence_score ?? 0).toFixed(2)}`;
    confidenceEl.className = `mini-badge ${Number(action.confidence_score ?? 0) >= 0.75 ? "badge-critical" : Number(action.confidence_score ?? 0) >= 0.5 ? "badge-warning" : "badge-normal"}`;

    document.getElementById("action-analysis").textContent = action.diagnosis || action.analysis;
    document.getElementById("action-decision").textContent = action.rationale || action.decision;
    document.getElementById("action-optimization").textContent = action.proposed_patch || action.optimization;
    document.getElementById("action-footer").textContent =
    `${action.optimization_result || "Hələ nəticə yoxdur."} Cari vəziyyət: ${snapshot.status}.`;
}

const logEl = document.getElementById("live-log");
function addLog(snapshot) {
    const status = snapshot.status || "Normal";
    const actionName = snapshot.action_center?.scenario
    ? snapshot.action_center.scenario.toUpperCase()
    : "SNAPSHOT";
    const confidence = snapshot.action_center?.confidence_score;
    const entry = document.createElement("div");
    entry.className = "log-entry";
    entry.innerHTML =
    `<span class="log-ts">${formatLocalTime(snapshot.ts)}</span>` +
    `<span class="${status === "Critical" ? "log-critical" : status === "Warning" ? "log-warning" : "log-normal"}">[${status}]</span> ` +
    `${actionName} | QoS=${Number(snapshot.qos).toFixed(1)} CPU=${Number(snapshot.cpu).toFixed(1)}% Gecikmə=${Number(snapshot.delay).toFixed(1)}ms` +
    `${confidence !== undefined ? ` C=${Number(confidence).toFixed(2)}` : ""}`;
    logEl.prepend(entry);
    while (logEl.children.length > 60) {
    logEl.removeChild(logEl.lastChild);
    }
}

function appendTrendPoint(snapshot) {
    const label = formatLocalTime(snapshot.ts);
    trendChart.data.labels.push(label);
    trendChart.data.datasets[0].data.push(snapshot.cpu);
    trendChart.data.datasets[1].data.push(snapshot.delay);
    if (trendChart.data.labels.length > MAX_TREND_PTS) {
    trendChart.data.labels.shift();
    trendChart.data.datasets[0].data.shift();
    trendChart.data.datasets[1].data.shift();
    }
    trendChart.update("none");
}

function appendQosPoint(snapshot) {
    const label = formatLocalTime(snapshot.ts);
    qosBarChart.data.labels.push(label);
    qosBarChart.data.datasets[0].data.push(snapshot.qos);
    qosBarChart.data.datasets[0].backgroundColor.push(qosBgColor(snapshot.qos));
    if (qosBarChart.data.labels.length > MAX_BAR_PTS) {
    qosBarChart.data.labels.shift();
    qosBarChart.data.datasets[0].data.shift();
    qosBarChart.data.datasets[0].backgroundColor.shift();
    }
    qosBarChart.update("none");
}

function updateMm1Marker(snapshot) {
    const wait = Number.isFinite(snapshot.wq) ? snapshot.wq : null;
    mm1Chart.data.datasets[1].data = wait === null ? [] : [{ x: snapshot.lambda_, y: wait }];
    mm1Chart.update("none");
}

function applySnapshot(snapshot, options = {}) {
    if (!snapshot || !snapshot.ts || snapshot.ts === lastSnapshotTs) return;
    const merged = mergeSnapshot(snapshot);
    lastSnapshotTs = merged.ts;

    updateMetricCards(merged);
    updateActionCenter(merged);
    setGauge(merged.qos, merged.status);
    updateMm1Marker(merged);

    if (!options.skipCharts) {
    appendTrendPoint(merged);
    appendQosPoint(merged);
    }
    if (!options.skipLog) {
    addLog(merged);
    }
}

function applyBootstrap(payload) {
    const rows = payload?.history || [];
    if (historyLoaded) return;
    historyLoaded = true;

    rows.forEach((row) => {
    const label = formatLocalTime(row.ts);
    trendChart.data.labels.push(label);
    trendChart.data.datasets[0].data.push(row.cpu);
    trendChart.data.datasets[1].data.push(row.delay);
    qosBarChart.data.labels.push(label);
    qosBarChart.data.datasets[0].data.push(row.qos);
    qosBarChart.data.datasets[0].backgroundColor.push(qosBgColor(row.qos));
    addLog(row);
    });

    while (trendChart.data.labels.length > MAX_TREND_PTS) {
    trendChart.data.labels.shift();
    trendChart.data.datasets[0].data.shift();
    trendChart.data.datasets[1].data.shift();
    }
    while (qosBarChart.data.labels.length > MAX_BAR_PTS) {
    qosBarChart.data.labels.shift();
    qosBarChart.data.datasets[0].data.shift();
    qosBarChart.data.datasets[0].backgroundColor.shift();
    }

    trendChart.update("none");
    qosBarChart.update("none");
}

async function loadMM1Curve() {
    try {
    const response = await fetch("/api/mm1_curve");
    const payload = await response.json();
    const curve = payload.curve.filter((point) => point.W !== null && point.W < 5);
    mm1Chart.data.datasets[0].data = curve.map((point) => ({ x: point.lambda, y: point.W }));
    mm1Chart.options.scales.y.max = Math.min(5, Math.max(...curve.map((point) => point.W)) * 1.15);
    document.getElementById("mu-label").textContent = payload.mu;
    mm1Chart.update();
    } catch (error) {
    console.warn("M/M/1 əyrisi yüklənmədi", error);
    }
}


// ═════════════════════════════════════════════════════════════
// DEBUG MODAL
// ═════════════════════════════════════════════════════════════

const debugModal = document.getElementById("debug-modal");

const debugButton = document.getElementById("debug-button");

const debugCloseBtn = document.getElementById("debug-close-btn");

const debugRefreshBtn = document.getElementById("debug-refresh-btn");

const debugSaveBtn = document.getElementById("debug-save-btn");


// ─────────────────────────────────────────────────────────────
// OPEN
// ─────────────────────────────────────────────────────────────

debugButton.addEventListener("click", async () => {

  debugModal.classList.remove("hidden");

  await loadConfig();
});


// ─────────────────────────────────────────────────────────────
// CLOSE
// ─────────────────────────────────────────────────────────────

debugCloseBtn.addEventListener("click", () => {

  debugModal.classList.add("hidden");
});


// ─────────────────────────────────────────────────────────────
// LOAD CONFIG
// ─────────────────────────────────────────────────────────────

async function loadConfig() {

  const res = await fetch("/api/config");

  const data = await res.json();

  const cfg = data.config;

  setValue("MU", cfg.MU);

  setValue("PUSH_INTERVAL", cfg.PUSH_INTERVAL);

  setValue("DATA_SOURCE", cfg.DATA_SOURCE);

  setValue("CSV_PATH", cfg.CSV_PATH);

  setRange("CPU_RANGE", cfg.CPU_RANGE);

  setRange("JITTER_RANGE", cfg.JITTER_RANGE);

  setRange("DELAY_RANGE", cfg.DELAY_RANGE);

  setRange("LAMBDA_RANGE", cfg.LAMBDA_RANGE);

  setRange("IMS_SESSION_RANGE", cfg.IMS_SESSION_RANGE);

  setValue("W_DELAY", cfg.W_DELAY);

  setValue("W_JITTER", cfg.W_JITTER);

  setValue("W_CPU", cfg.W_CPU);

  setValue("SIMULATION_CHAOS", cfg.SIMULATION_CHAOS);

  setValue("SIMULATION_MOMENTUM", cfg.SIMULATION_MOMENTUM);

  setValue("VOIP_JITTER_THRESHOLD", cfg.VOIP_JITTER_THRESHOLD);

  setValue("VIDEO_DELAY_THRESHOLD", cfg.VIDEO_DELAY_THRESHOLD);

  setValue("CPU_WARNING_THRESHOLD", cfg.CPU_WARNING_THRESHOLD);

  setValue("LAMBDA_WARNING_RATIO", cfg.LAMBDA_WARNING_RATIO);

  setValue("ACTION_MODE", cfg.ACTION_MODE);

  setValue("FORCED_ACTION", cfg.FORCED_ACTION);

  setValue("FORCED_STATUS", cfg.FORCED_STATUS);
}


// ─────────────────────────────────────────────────────────────
// REFRESH
// ─────────────────────────────────────────────────────────────

debugRefreshBtn.addEventListener("click", loadConfig);


// ─────────────────────────────────────────────────────────────
// SAVE
// ─────────────────────────────────────────────────────────────

debugSaveBtn.addEventListener("click", async () => {

  const payload = {

    MU:
      parseFloat(getValue("MU")),

    PUSH_INTERVAL:
      parseInt(getValue("PUSH_INTERVAL")),

    DATA_SOURCE:
      getValue("DATA_SOURCE"),

    CSV_PATH:
      getValue("CSV_PATH"),

    CPU_RANGE:
      getRange("CPU_RANGE"),

    JITTER_RANGE:
      getRange("JITTER_RANGE"),

    DELAY_RANGE:
      getRange("DELAY_RANGE"),

    LAMBDA_RANGE:
      getRange("LAMBDA_RANGE"),

    IMS_SESSION_RANGE:
      getRange("IMS_SESSION_RANGE"),

    W_DELAY:
      parseFloat(getValue("W_DELAY")),

    W_JITTER:
      parseFloat(getValue("W_JITTER")),

    W_CPU:
      parseFloat(getValue("W_CPU")),

    SIMULATION_CHAOS:
      parseFloat(getValue("SIMULATION_CHAOS")),

    SIMULATION_MOMENTUM:
      parseFloat(getValue("SIMULATION_MOMENTUM")),

    VOIP_JITTER_THRESHOLD:
      parseFloat(getValue("VOIP_JITTER_THRESHOLD")),

    VIDEO_DELAY_THRESHOLD:
      parseFloat(getValue("VIDEO_DELAY_THRESHOLD")),

    CPU_WARNING_THRESHOLD:
      parseFloat(getValue("CPU_WARNING_THRESHOLD")),

    LAMBDA_WARNING_RATIO:
      parseFloat(getValue("LAMBDA_WARNING_RATIO")),

    ACTION_MODE:
      getValue("ACTION_MODE"),

    FORCED_ACTION:
      getValue("FORCED_ACTION"),

    FORCED_STATUS:
      getValue("FORCED_STATUS"),
  };

  const res = await fetch("/api/config", {

    method: "POST",

    headers: {
      "Content-Type": "application/json"
    },

    body: JSON.stringify(payload)
  });

  const data = await res.json();

  console.log(data);

  if (data.ok) {

    alert("CONFIG UPDATED");

  } else {

    alert(
      "CONFIG ERROR:\n"
      + JSON.stringify(data.rejected, null, 2)
    );
  }
});


// ─────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────

function setValue(key, value) {

  const el = document.getElementById(`cfg-${key}`);

  if (el) {
    el.value = value;
  }
}

function getValue(key) {

  const el = document.getElementById(`cfg-${key}`);

  return el ? el.value : null;
}

function setRange(key, value) {

  document.getElementById(`cfg-${key}-min`).value = value[0];

  document.getElementById(`cfg-${key}-max`).value = value[1];
}

function getRange(key) {

  return [

    parseFloat(
      document.getElementById(`cfg-${key}-min`).value
    ),

    parseFloat(
      document.getElementById(`cfg-${key}-max`).value
    ),
  ];
}


const badge = document.getElementById("conn-badge");

let hasEverConnected = false;

function setStatus(state) {
    if (state === "connecting") {
        badge.textContent = "QOŞULUR";
        badge.className = "header-pill conn-warn";
    }

    if (state === "online") {
        badge.textContent = "CANLI";
        badge.className = "header-pill conn-on";
    }

    if (state === "offline") {
        badge.textContent = "OFFLAYN";
        badge.className = "header-pill conn-off";
    }
}

// initial state
setStatus("connecting");

const socket = io({
    transports: ["polling", "websocket"],
    reconnection: true,
    reconnectionAttempts: Infinity,
    reconnectionDelay: 1500
});

socket.on("connect", () => {
    hasEverConnected = true;
    setStatus("online");
});

socket.on("disconnect", (reason) => {
    console.warn("[socket] disconnect:", reason);

    // critical fix: don't show OFFLINE on first startup noise
    if (!hasEverConnected) {
        setStatus("connecting");
        return;
    }

    setStatus("offline");
});

socket.on("connect_error", () => {
    setStatus("connecting");
});

socket.on("reconnect_attempt", () => {
    setStatus("connecting");
});

socket.on("reconnect", () => {
    hasEverConnected = true;
    setStatus("online");
});

socket.on("bootstrap_data", (payload) => {
    console.log("[socket] ilkin məlumat alındı");
    applyBootstrap(payload);
    if (payload?.snapshot) {
        applySnapshot(payload.snapshot, { skipCharts: true, skipLog: true });
    }
});

socket.on("metric_update", (snapshot) => applySnapshot(snapshot));

loadMM1Curve();