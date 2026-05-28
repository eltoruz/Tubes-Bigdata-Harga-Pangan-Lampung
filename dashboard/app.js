/* ═══════════════════════════════════════════════════════
   Dashboard Monitoring Harga Pangan Lampung — app.js
   Government/Academic Monitoring Style
   ═══════════════════════════════════════════════════════ */

const API = "";

function fmt(n) {
    return n == null ? "-" : Number(n).toLocaleString("id-ID");
}
function fmtPct(n) {
    return n == null ? "-" : Number(n).toFixed(1) + "%";
}

// ─── Chart palette (updated by theme) ───
const C = {
    hist: "#60A5FA",
    histFill: "rgba(96,165,250,0.1)",
    pred: "#34D399",
    predFill: "rgba(52,211,153,0.12)",
    anom: "#F87171",
    anomFill: "rgba(248,113,113,0.6)",
    grid: "rgba(55,65,81,0.5)",
    tick: "#9CA3AF",
    tooltipBg: "#1F2937",
    tooltipText: "#F3F4F6",
    tooltipBorder: "#374151",
    legendText: "#F3F4F6",
};

let chartForecast = null,
    chartAnom = null;

// ─── Chart helpers ───
function getScaleOpts() {
    return {
        x: {
            ticks: { color: C.tick, maxRotation: 35, autoSkip: true, maxTicksLimit: 10, font: { size: 10 } },
            grid: { color: C.grid, drawBorder: false },
        },
        y: {
            ticks: { color: C.tick, callback: (v) => "Rp " + fmt(v), font: { size: 10 } },
            grid: { color: C.grid, drawBorder: false },
        },
    };
}

function getLegend() {
    return {
        labels: { color: C.legendText, usePointStyle: true, pointStyleWidth: 10, padding: 16, font: { size: 11, weight: "500" } },
    };
}

function getTooltip() {
    return {
        backgroundColor: C.tooltipBg,
        titleColor: C.tooltipText,
        bodyColor: C.tooltipText,
        borderColor: C.tooltipBorder,
        borderWidth: 1,
        cornerRadius: 8,
        padding: 10,
        callbacks: { label: (ctx) => ctx.dataset.label + ": Rp " + fmt(ctx.raw) },
    };
}

// ─── Forecast Chart (area style, not sharp line) ───
function makeForecastChart(historikal, forecast) {
    const ctx = document.getElementById("chart-forecast").getContext("2d");
    if (chartForecast) chartForecast.destroy();

    const lastHist = historikal[historikal.length - 1];
    const allLabels = [...historikal.map((d) => d.tanggal), ...forecast.map((d) => d.tanggal)];
    const histData = [...historikal.map((d) => d.harga), ...new Array(forecast.length).fill(null)];
    const fcBridge = [lastHist.harga, ...forecast.map((d) => d.harga)];
    const fcData = [...new Array(historikal.length - 1).fill(null), ...fcBridge];

    chartForecast = new Chart(ctx, {
        type: "line",
        data: {
            labels: allLabels,
            datasets: [
                {
                    label: "Harga Historis (60 hari)",
                    data: histData,
                    borderColor: C.hist,
                    backgroundColor: C.histFill,
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                    tension: 0.35,
                    fill: true,
                    spanGaps: false,
                },
                {
                    label: "Prediksi 7 Hari",
                    data: fcData,
                    borderColor: C.pred,
                    backgroundColor: C.predFill,
                    borderWidth: 2.5,
                    pointRadius: 4,
                    pointBackgroundColor: C.pred,
                    pointBorderColor: C.pred,
                    pointHoverRadius: 6,
                    tension: 0.35,
                    fill: true,
                    spanGaps: true,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: "index", intersect: false },
            plugins: { legend: getLegend(), tooltip: getTooltip() },
            scales: getScaleOpts(),
        },
    });
}

// ─── Anomaly Bar Chart ───
function makeAnomChart(daily) {
    const ctx = document.getElementById("chart-anom-daily").getContext("2d");
    if (chartAnom) chartAnom.destroy();

    chartAnom = new Chart(ctx, {
        type: "bar",
        data: {
            labels: daily.map((d) => d.tanggal),
            datasets: [{
                label: "Jumlah Anomali",
                data: daily.map((d) => d.jumlah),
                backgroundColor: C.anomFill,
                borderColor: C.anom,
                borderWidth: 1,
                borderRadius: 4,
                hoverBackgroundColor: C.anom,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: getTooltip(),
            },
            scales: {
                x: {
                    ticks: { color: C.tick, maxRotation: 35, autoSkip: true, maxTicksLimit: 10, font: { size: 10 } },
                    grid: { color: C.grid, drawBorder: false },
                },
                y: {
                    ticks: { color: C.tick, stepSize: 1, font: { size: 10 } },
                    grid: { color: C.grid, drawBorder: false },
                },
            },
        },
    });
}

// ─── Tabs ───
function initTabs() {
    document.querySelectorAll(".tab-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
            document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
            document.querySelectorAll(".tab-panel").forEach((p) => p.classList.remove("active"));
            btn.classList.add("active");
            const t = document.getElementById(btn.dataset.tab);
            if (t) t.classList.add("active");
        });
    });
}

// ─── Load Dataset Info ───
async function loadDatasetInfo() {
    try {
        const res = await fetch(API + "/api/dataset-info");
        const info = await res.json();

        document.getElementById("stat-rows").textContent = fmt(info.total_rows);
        document.getElementById("stat-kabkot").textContent = info.total_kabkot;
        document.getElementById("stat-kom").textContent = info.total_komoditas;
        document.getElementById("stat-date").textContent = info.tanggal_awal + " ~ " + info.tanggal_akhir;
        document.getElementById("dataset-tagline").textContent =
            "Sumber: Disperindag Lampung · " + info.tanggal_awal + " s/d " + info.tanggal_akhir;

        const sel = document.getElementById("kom-select");
        info.komoditas_list.forEach((k) => {
            const opt = document.createElement("option");
            opt.value = k;
            opt.textContent = k;
            sel.appendChild(opt);
        });
        sel.addEventListener("change", onKomChange);
        if (sel.options.length > 0) sel.selectedIndex = 0;
        onKomChange();
    } catch (e) {
        console.error("Gagal load dataset info:", e);
        document.getElementById("dataset-tagline").textContent = "Gagal memuat data. Pastikan server aktif.";
    }
}

// ─── On Komoditas Change ───
async function onKomChange() {
    const kom = document.getElementById("kom-select").value;
    if (!kom) return;

    ["metric-mae", "metric-rmse", "metric-mape"].forEach(
        (id) => (document.getElementById(id).innerHTML = '<span class="spinner"></span>')
    );
    document.getElementById("forecast-row").innerHTML =
        '<div class="loading-text"><span class="spinner"></span> Memuat prediksi...</div>';
    document.querySelector("#tbl-forecast tbody").innerHTML = "";

    try {
        const res = await fetch(API + "/api/predict/" + encodeURIComponent(kom));
        if (!res.ok) throw new Error("Not found");
        const data = await res.json();

        const ev = data.evaluasi;
        document.getElementById("metric-mae").textContent = "Rp " + fmt(ev.metrics.mae);
        document.getElementById("metric-rmse").textContent = "Rp " + fmt(ev.metrics.rmse);
        document.getElementById("metric-mape").textContent = fmtPct(ev.metrics.mape);

        const fc = data.forecast_7_hari;
        renderForecastCards(fc.forecast);
        makeForecastChart(fc.historikal, fc.forecast);
        renderForecastTable(fc);
    } catch (e) {
        ["metric-mae", "metric-rmse", "metric-mape"].forEach(
            (id) => (document.getElementById(id).textContent = "N/A")
        );
        document.getElementById("forecast-row").innerHTML =
            '<div class="loading-text">Data tidak tersedia untuk komoditas ini</div>';
        console.error("Gagal load prediksi:", e);
    }
}

// ─── Forecast Cards ───
function renderForecastCards(forecast) {
    const row = document.getElementById("forecast-row");
    row.innerHTML = "";
    forecast.forEach((f) => {
        const card = document.createElement("div");
        card.className = "forecast-item";
        card.innerHTML =
            `<div class="fi-day">H+${f.hari_ke}</div>` +
            `<div class="fi-date">${f.tanggal}</div>` +
            `<div class="fi-price">Rp ${fmt(f.harga)}</div>`;
        row.appendChild(card);
    });
}

// ─── Forecast Table ───
function renderForecastTable(fc) {
    const tbody = document.querySelector("#tbl-forecast tbody");
    tbody.innerHTML = "";
    fc.forecast.forEach((f, i) => {
        const tr = document.createElement("tr");
        tr.innerHTML =
            `<td>H+${f.hari_ke}</td>` +
            `<td>${f.tanggal}</td>` +
            `<td>Rp ${fmt(f.harga)}</td>` +
            `<td>Rp ${fmt(fc.metrics_horizon.mae[i])}</td>` +
            `<td>${fmtPct(fc.metrics_horizon.mape[i])}</td>`;
        tbody.appendChild(tr);
    });
}

// ─── Anomaly ───
let allAnomRows = [];
let anomShowAll = false;

async function loadAnomaly() {
    try {
        const res = await fetch(API + "/api/anomaly");
        const data = await res.json();

        document.getElementById("anom-count").textContent =
            "Terdeteksi " + fmt(data.total_anomali) + " anomali dari " + fmt(data.total_data) +
            " data (" + fmtPct(data.pct_anomali) + ")";

        makeAnomChart(data.daily_counts);
        allAnomRows = data.top_anomalies;
        renderAnomTable(false);

        const btn = document.getElementById("btn-toggle-anom");
        if (allAnomRows.length > 10) {
            btn.style.display = "inline-flex";
            btn.addEventListener("click", toggleAnomTable);
        }
    } catch (e) {
        console.error("Gagal load anomali:", e);
        document.getElementById("anom-count").textContent = "Gagal memuat data anomali";
    }
}

function toggleAnomTable() {
    anomShowAll = !anomShowAll;
    renderAnomTable(anomShowAll);
    document.getElementById("btn-toggle-anom").textContent = anomShowAll
        ? "Tampilkan 10 teratas ▴"
        : `Tampilkan semua (${allAnomRows.length} data) ▾`;
}

function renderAnomTable(showAll) {
    const tbody = document.querySelector("#tbl-anom tbody");
    tbody.innerHTML = "";
    const rows = showAll ? allAnomRows : allAnomRows.slice(0, 10);
    rows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML =
            `<td>${r.tanggal}</td>` +
            `<td>${r.kabupaten_kota}</td>` +
            `<td>${r.komoditas}</td>` +
            `<td>Rp ${fmt(r.harga_rupiah)}</td>` +
            `<td>${r.deviasi_pct >= 0 ? "+" : ""}${r.deviasi_pct}%</td>` +
            `<td>${r.delta_pct >= 0 ? "+" : ""}${r.delta_pct}%</td>` +
            `<td>${r.curah_hujan_mm}</td>` +
            `<td>${r.is_libur_nasional ? "Ya" : "-"}</td>` +
            `<td class="${r.score < -0.05 ? "score-critical" : ""}">${r.score}</td>`;
        tbody.appendChild(tr);
    });
}

// ─── Theme Toggle ───
function initTheme() {
    const saved = localStorage.getItem("pangan-theme");
    if (saved === "light") {
        document.documentElement.setAttribute("data-theme", "light");
        document.getElementById("theme-toggle").textContent = "☀️";
    }
    document.getElementById("theme-toggle").addEventListener("click", toggleTheme);
}

function toggleTheme() {
    const html = document.documentElement;
    const btn = document.getElementById("theme-toggle");
    const isLight = html.getAttribute("data-theme") === "light";

    if (isLight) {
        html.removeAttribute("data-theme");
        btn.textContent = "🌙";
        localStorage.setItem("pangan-theme", "dark");
    } else {
        html.setAttribute("data-theme", "light");
        btn.textContent = "☀️";
        localStorage.setItem("pangan-theme", "light");
    }
    syncChartColors();
}

function syncChartColors() {
    const light = document.documentElement.getAttribute("data-theme") === "light";
    C.hist       = light ? "#3B82F6" : "#60A5FA";
    C.histFill   = light ? "rgba(59,130,246,0.08)" : "rgba(96,165,250,0.1)";
    C.pred       = light ? "#059669" : "#34D399";
    C.predFill   = light ? "rgba(5,150,105,0.08)" : "rgba(52,211,153,0.12)";
    C.anom       = light ? "#EF4444" : "#F87171";
    C.anomFill   = light ? "rgba(239,68,68,0.5)" : "rgba(248,113,113,0.6)";
    C.grid       = light ? "rgba(209,213,219,0.5)" : "rgba(55,65,81,0.5)";
    C.tick       = light ? "#6B7280" : "#9CA3AF";
    C.tooltipBg  = light ? "#FFFFFF" : "#1F2937";
    C.tooltipText = light ? "#1F2937" : "#F3F4F6";
    C.tooltipBorder = light ? "#D1D5DB" : "#374151";
    C.legendText = light ? "#1F2937" : "#F3F4F6";

    // Rebuild charts
    const kom = document.getElementById("kom-select").value;
    if (kom) onKomChange();
    if (allAnomRows.length > 0) loadAnomaly();
}

// ─── Init ───
initTheme();
initTabs();
loadDatasetInfo();
loadAnomaly();
