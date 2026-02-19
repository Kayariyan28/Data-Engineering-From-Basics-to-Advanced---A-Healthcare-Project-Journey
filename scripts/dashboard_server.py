#!/usr/bin/env python3
"""
scripts/dashboard_server.py
============================
Standalone web dashboard — uses ONLY Python stdlib + pandas (no Flask needed).

Run:
    cd /path/to/healthcare-platform
    python3 scripts/dashboard_server.py

Then open: http://localhost:5050
"""

import json
import math
import traceback
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
import pandas as pd


def to_json(obj) -> bytes:
    """Serialize to JSON, converting NaN/Inf floats → null (valid JSON).
    pandas .to_dict() produces float('nan') for missing values; the standard
    json module writes those as NaN which browsers reject."""
    def sanitize(o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        if isinstance(o, dict):
            return {k: sanitize(v) for k, v in o.items()}
        if isinstance(o, list):
            return [sanitize(v) for v in o]
        return o
    return json.dumps(sanitize(obj)).encode()

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD         = PROJECT_ROOT / "data" / "output" / "gold"
AUDIT_PATH   = PROJECT_ROOT / "data" / "output" / "pipeline_audit_log.json"

PATHS = {
    "kpis":    GOLD / "dept_kpis"     / "data.csv",
    "readmit": GOLD / "readmissions"  / "data.csv",
    "finance": GOLD / "financial"     / "data.csv",
    "crit":    GOLD / "critical_labs" / "data.csv",
}

def csv(key: str) -> pd.DataFrame:
    p = PATHS[key]
    if not p.exists():
        raise FileNotFoundError(
            f"File not found: {p}\n"
            "Run  python scripts/run_pipeline_local.py  first."
        )
    return pd.read_csv(p)


# ── Data functions ────────────────────────────────────────────────────────────

def data_summary():
    kpis = csv("kpis")
    crit = csv("crit")
    dept = kpis.groupby("department", as_index=False).agg(
        admissions   = ("total_admissions",    "sum"),
        revenue      = ("total_revenue_usd",   "sum"),
        readmit_rate = ("readmission_rate_pct","mean"),
    )
    quarantined, last_run = 0, "—"
    if AUDIT_PATH.exists():
        log = json.loads(AUDIT_PATH.read_text())
        for step in log:
            if "quarantined=" in step.get("notes",""):
                quarantined = int(step["notes"].split("quarantined=")[1].split()[0])
            last_run = step.get("timestamp","—")
    total_adm = int(dept["admissions"].sum())
    return {
        "total_admissions": total_adm,
        "total_revenue_m":  round(float(dept["revenue"].sum()) / 1e6, 2),
        "critical_labs":    int(len(crit)),
        "quarantined":      quarantined,
        "avg_readmission":  round(float(dept["readmit_rate"].mean()), 2),
        "last_run":         last_run,
        "departments":      int(len(dept)),
        "data_quality_pct": round(total_adm / (total_adm + quarantined) * 100, 1),
    }

def data_dept_annual():
    kpis = csv("kpis")
    dept = kpis.groupby("department", as_index=False).agg(
        total_admissions = ("total_admissions",    "sum"),
        unique_patients  = ("unique_patients",     "sum"),
        avg_los          = ("avg_los_days",        "mean"),
        avg_readmit_rate = ("readmission_rate_pct","mean"),
        total_revenue    = ("total_revenue_usd",   "sum"),
        icu_admissions   = ("icu_admissions",      "sum"),
        avg_cost         = ("avg_cost_usd",        "mean"),
    )
    dept["avg_los"]          = dept["avg_los"].round(1)
    dept["avg_readmit_rate"] = dept["avg_readmit_rate"].round(1)
    dept["total_revenue_m"]  = (dept["total_revenue"] / 1e6).round(2)
    dept["avg_cost"]         = dept["avg_cost"].round(0).astype(int)
    dept = dept.sort_values("total_admissions", ascending=False)
    return dept.drop(columns=["total_revenue"]).to_dict(orient="records")

def data_monthly_trends():
    kpis = csv("kpis")
    pivot = (
        kpis.pivot_table(
            index="year_month", columns="department",
            values="total_admissions", aggfunc="sum", fill_value=0,
        )
        .reset_index()
        .sort_values("year_month")
    )
    return pivot.to_dict(orient="records")

def data_readmission_risk():
    readmit = csv("readmit")
    risk = readmit.groupby("readmission_risk_tier", as_index=False).agg(
        cases    = ("total_admissions", "sum"),
        avg_rate = ("readmission_rate_pct", "mean"),
    )
    risk["avg_rate"] = risk["avg_rate"].round(1)
    return risk.to_dict(orient="records")

def data_critical_labs():
    crit = csv("crit")
    keep = ["lab_id","patient_id","test_name","marker","result_value","unit",
            "reference_low","reference_high","result_interpretation",
            "deviation_from_range","lab_status","department",
            "attending_doctor","collected_at"]
    keep = [c for c in keep if c in crit.columns]
    out  = crit[keep].head(100).copy()
    for col in ["result_value","reference_low","reference_high","deviation_from_range"]:
        if col in out.columns:
            out[col] = out[col].round(3)
    return out.to_dict(orient="records")

def data_audit():
    if not AUDIT_PATH.exists():
        return []
    return json.loads(AUDIT_PATH.read_text())


def data_insurer_revenue():
    """Revenue and admissions broken down by insurer — from financial Gold table."""
    fin = csv("finance")
    by_insurer = fin.groupby("insurer_name", as_index=False).agg(
        total_revenue  = ("total_revenue_usd", "sum"),
        admissions     = ("admissions",        "sum"),
        avg_rev_per_adm= ("avg_revenue_per_admission", "mean"),
    )
    by_insurer["total_revenue_m"]   = (by_insurer["total_revenue"] / 1e6).round(2)
    by_insurer["avg_rev_per_adm"]   = by_insurer["avg_rev_per_adm"].round(0).astype(int)
    by_insurer = by_insurer.sort_values("total_revenue", ascending=False)
    return by_insurer.drop(columns=["total_revenue"]).to_dict(orient="records")


def data_cost_tier():
    """Admission counts and revenue by cost tier (LOW/MEDIUM/HIGH/VERY_HIGH)."""
    fin = csv("finance")
    by_tier = fin.groupby("cost_tier", as_index=False).agg(
        admissions    = ("admissions",        "sum"),
        total_revenue = ("total_revenue_usd", "sum"),
    )
    ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "VERY_HIGH": 3}
    by_tier["_order"] = by_tier["cost_tier"].map(ORDER).fillna(99)
    by_tier = by_tier.sort_values("_order").drop(columns=["_order"])
    by_tier["total_revenue_m"] = (by_tier["total_revenue"] / 1e6).round(2)
    return by_tier.drop(columns=["total_revenue"]).to_dict(orient="records")


def data_doctor_alerts():
    """Top 15 doctors ranked by number of critical lab results."""
    crit = csv("crit")
    docs = (
        crit.groupby("attending_doctor", as_index=False)
        .agg(
            critical_labs = ("lab_id",       "count"),
            departments   = ("department",   lambda x: ", ".join(sorted(x.unique()))),
            high_count    = ("result_interpretation", lambda x: (x == "HIGH").sum()),
            low_count     = ("result_interpretation", lambda x: (x == "LOW").sum()),
        )
        .sort_values("critical_labs", ascending=False)
        .head(15)
    )
    return docs.to_dict(orient="records")


def data_quarterly():
    """Quarterly admissions and readmission rate per department."""
    kpis = csv("kpis")
    q = kpis.groupby(["admission_quarter", "department"], as_index=False).agg(
        total_admissions = ("total_admissions",    "sum"),
        readmit_rate     = ("readmission_rate_pct","mean"),
        total_revenue    = ("total_revenue_usd",   "sum"),
    )
    q["readmit_rate"]    = q["readmit_rate"].round(1)
    q["total_revenue_m"] = (q["total_revenue"] / 1e6).round(2)
    q = q.sort_values("admission_quarter")
    return q.drop(columns=["total_revenue"]).to_dict(orient="records")


# ── HTTP handler ──────────────────────────────────────────────────────────────

ROUTES = {
    "/api/summary":          data_summary,
    "/api/dept-annual":      data_dept_annual,
    "/api/monthly-trends":   data_monthly_trends,
    "/api/readmission-risk": data_readmission_risk,
    "/api/critical-labs":    data_critical_labs,
    "/api/audit":            data_audit,
    "/api/insurer-revenue":  data_insurer_revenue,
    "/api/cost-tier":        data_cost_tier,
    "/api/doctor-alerts":    data_doctor_alerts,
    "/api/quarterly":        data_quarterly,
}

class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/":
            self._send(200, DASHBOARD.encode(), "text/html; charset=utf-8")
            return

        fn = ROUTES.get(path)
        if fn:
            try:
                result = fn()
                body = to_json(result)
                self._send(200, body, "application/json")
            except FileNotFoundError as e:
                self._send(503, json.dumps({"error": str(e)}).encode(), "application/json")
            except Exception:
                err = traceback.format_exc()
                self._send(500, json.dumps({"error": err}).encode(), "application/json")
        else:
            self._send(404, b"Not found", "text/plain")

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # suppress per-request noise


# ── Dashboard HTML ────────────────────────────────────────────────────────────

DASHBOARD = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Healthcare Analytics Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    body { font-family: system-ui, -apple-system, sans-serif; background: #f1f5f9; }
    .card { background:#fff; border-radius:14px; box-shadow:0 1px 4px rgba(0,0,0,.07),0 4px 16px rgba(0,0,0,.04); }
    .chart-wrap { position:relative; height:270px; }
    .badge { display:inline-flex; align-items:center; font-size:.68rem; font-weight:700;
             padding:2px 8px; border-radius:9999px; letter-spacing:.04em; }
    .b-ok     { background:#dcfce7; color:#15803d; }
    .b-warn   { background:#fef9c3; color:#92400e; }
    .b-crit   { background:#fee2e2; color:#b91c1c; }
    .b-high   { background:#fee2e2; color:#b91c1c; }
    .b-normal { background:#f0fdf4; color:#15803d; }
    .b-low    { background:#fef9c3; color:#92400e; }
    .b-unknown{ background:#f1f5f9; color:#64748b; }
    .b-bronze { background:#fde68a; color:#78350f; }
    .b-silver { background:#e2e8f0; color:#334155; }
    .b-gold   { background:#fef3c7; color:#92400e; }
    table { width:100%; border-collapse:collapse; font-size:.83rem; }
    thead th { background:#f8fafc; color:#64748b; font-size:.68rem; font-weight:700;
               text-transform:uppercase; letter-spacing:.07em; padding:10px 14px;
               border-bottom:2px solid #e2e8f0; white-space:nowrap; text-align:left; }
    tbody td { padding:10px 14px; border-bottom:1px solid #f1f5f9; color:#334155; white-space:nowrap; }
    tbody tr:last-child td { border-bottom:none; }
    tbody tr:hover td { background:#f8fafc; }
    .scroll-tbl { max-height:360px; overflow-y:auto; }
  </style>
</head>
<body class="min-h-screen">

<!-- Header -->
<header class="bg-blue-950 text-white px-8 py-5 shadow-xl">
  <div class="max-w-7xl mx-auto flex items-center justify-between flex-wrap gap-3">
    <div>
      <h1 class="text-xl font-bold tracking-tight">🏥 Healthcare Analytics Dashboard</h1>
      <p class="text-blue-300 text-xs mt-0.5">Bronze → Silver → Gold Pipeline &nbsp;·&nbsp; 2024 Data</p>
    </div>
    <div class="flex items-center gap-4">
      <div class="text-right">
        <span class="bg-emerald-500 text-white text-xs font-bold px-3 py-1 rounded-full">● PIPELINE OK</span>
        <p id="last-run" class="text-blue-300 text-xs mt-1">Last run: …</p>
      </div>
      <button onclick="init()" class="bg-blue-800 hover:bg-blue-700 text-white text-xs font-semibold px-4 py-2 rounded-lg transition">
        ↺ Refresh
      </button>
    </div>
  </div>
</header>

<main class="max-w-7xl mx-auto px-6 py-8 space-y-6">

  <!-- Summary cards -->
  <section class="grid grid-cols-2 lg:grid-cols-4 gap-4">
    <div class="card p-5">
      <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Total Admissions</p>
      <p id="c-adm" class="text-3xl font-bold text-slate-800 mt-1">…</p>
      <p class="text-xs text-slate-400 mt-1"><span id="c-depts">…</span> departments</p>
    </div>
    <div class="card p-5">
      <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Total Revenue</p>
      <p id="c-rev" class="text-3xl font-bold text-slate-800 mt-1">…</p>
      <p class="text-xs text-slate-400 mt-1">avg readmit <span id="c-readmit">…</span>%</p>
    </div>
    <div class="card p-5">
      <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Critical Lab Alerts</p>
      <p id="c-crit" class="text-3xl font-bold text-red-600 mt-1">…</p>
      <p class="text-xs text-slate-400 mt-1">values outside reference range</p>
    </div>
    <div class="card p-5">
      <p class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Data Quality</p>
      <p id="c-dq" class="text-3xl font-bold text-emerald-600 mt-1">…</p>
      <p class="text-xs text-slate-400 mt-1"><span id="c-quar">…</span> records quarantined</p>
    </div>
  </section>

  <!-- Department KPI table -->
  <section class="card">
    <div class="px-6 py-4 border-b border-slate-100 flex items-center justify-between">
      <h2 class="font-semibold text-slate-800">Department KPIs — 2024 Annual</h2>
      <span class="text-xs text-slate-400">Source: gold/dept_kpis/data.csv</span>
    </div>
    <div class="scroll-tbl">
      <table>
        <thead><tr>
          <th>Department</th><th>Admissions</th><th>Avg LOS</th>
          <th>Readmission Rate</th><th>Revenue</th><th>ICU Stays</th><th>Avg Cost</th>
        </tr></thead>
        <tbody id="dept-tbody">
          <tr><td colspan="7" class="text-center text-slate-400 py-10">Loading…</td></tr>
        </tbody>
      </table>
    </div>
  </section>

  <!-- Charts row 1 -->
  <section class="grid grid-cols-1 lg:grid-cols-2 gap-4">
    <div class="card p-5">
      <h3 class="font-semibold text-slate-700 text-sm mb-4">Annual Admissions by Department</h3>
      <div class="chart-wrap"><canvas id="ch-adm"></canvas></div>
    </div>
    <div class="card p-5">
      <h3 class="font-semibold text-slate-700 text-sm mb-4">Monthly Admissions Trend (2024)</h3>
      <div class="chart-wrap"><canvas id="ch-monthly"></canvas></div>
    </div>
  </section>

  <!-- Charts row 2 -->
  <section class="grid grid-cols-1 lg:grid-cols-2 gap-4">
    <div class="card p-5">
      <h3 class="font-semibold text-slate-700 text-sm mb-4">Total Revenue by Department ($M)</h3>
      <div class="chart-wrap"><canvas id="ch-rev"></canvas></div>
    </div>
    <div class="card p-5">
      <h3 class="font-semibold text-slate-700 text-sm mb-4">Readmission Risk Distribution</h3>
      <div class="chart-wrap"><canvas id="ch-risk"></canvas></div>
    </div>
  </section>

  <!-- Charts row 3: Insurer revenue + Cost tier -->
  <section class="grid grid-cols-1 lg:grid-cols-2 gap-4">
    <div class="card p-5">
      <h3 class="font-semibold text-slate-700 text-sm mb-4">Revenue by Insurer ($M)</h3>
      <div class="chart-wrap"><canvas id="ch-insurer"></canvas></div>
    </div>
    <div class="card p-5">
      <h3 class="font-semibold text-slate-700 text-sm mb-4">Admissions by Cost Tier</h3>
      <div class="chart-wrap"><canvas id="ch-cost-tier"></canvas></div>
    </div>
  </section>

  <!-- Quarterly breakdown -->
  <section class="card p-5">
    <h3 class="font-semibold text-slate-700 text-sm mb-4">Quarterly Admissions by Department (2024)</h3>
    <div class="chart-wrap" style="height:240px"><canvas id="ch-quarterly"></canvas></div>
  </section>

  <!-- Doctor alerts table -->
  <section class="card">
    <div class="px-6 py-4 border-b border-slate-100 flex items-center justify-between">
      <h2 class="font-semibold text-slate-800">Top Doctors by Critical Lab Count</h2>
      <span class="text-xs text-slate-400">Source: gold/critical_labs/data.csv</span>
    </div>
    <div class="scroll-tbl">
      <table>
        <thead><tr>
          <th>Doctor</th><th>Critical Labs</th><th>HIGH Results</th>
          <th>LOW Results</th><th>Departments</th>
        </tr></thead>
        <tbody id="doctor-tbody">
          <tr><td colspan="5" class="text-center text-slate-400 py-6">Loading…</td></tr>
        </tbody>
      </table>
    </div>
  </section>

  <!-- Critical labs table -->
  <section class="card">
    <div class="px-6 py-4 border-b border-slate-100 flex items-center justify-between">
      <h2 class="font-semibold text-slate-800">
        Critical Lab Values <span id="crit-count" class="text-red-500 ml-1 text-sm font-normal"></span>
      </h2>
      <span class="text-xs text-slate-400">Source: gold/critical_labs/data.csv — top 100 shown</span>
    </div>
    <div class="scroll-tbl">
      <table>
        <thead><tr>
          <th>Lab ID</th><th>Patient</th><th>Test / Marker</th>
          <th>Result</th><th>Reference Range</th>
          <th>Interpretation</th><th>Deviation</th>
          <th>Status</th><th>Department</th><th>Doctor</th>
        </tr></thead>
        <tbody id="crit-tbody">
          <tr><td colspan="10" class="text-center text-slate-400 py-10">Loading…</td></tr>
        </tbody>
      </table>
    </div>
  </section>

  <!-- Pipeline audit -->
  <section class="card">
    <div class="px-6 py-4 border-b border-slate-100">
      <h2 class="font-semibold text-slate-800">Pipeline Audit Log — Last Run</h2>
    </div>
    <div id="audit-wrap" class="px-6 py-5 space-y-2">
      <p class="text-slate-400 text-sm">Loading…</p>
    </div>
  </section>

  <footer class="text-center text-xs text-slate-400 pb-4">
    All numbers read live from <code>data/output/gold/</code> — zero hardcoded values.
  </footer>
</main>

<script>
// Colours
const DC = {
  CARDIOLOGY:'#ef4444', EMERGENCY:'#f97316', ICU:'#8b5cf6',
  NEUROLOGY:'#3b82f6', ONCOLOGY:'#10b981', ORTHOPEDICS:'#f59e0b',
};
const RC = { HIGH_RISK:'#ef4444', MEDIUM_RISK:'#f59e0b', LOW_RISK:'#22c55e' };

// Formatters
const N = n => n != null ? Number(n).toLocaleString()       : '—';
const M = n => n != null ? `$${Number(n).toFixed(1)}M`      : '—';
const P = n => n != null ? `${Number(n).toFixed(1)}%`       : '—';
const D = n => n != null ? `${Number(n).toFixed(1)} d`      : '—';
const C = n => n != null ? `$${Number(n).toLocaleString()}` : '—';

// Chart registry (so Refresh destroys & redraws)
const CHS = {};
function mkChart(id, cfg) {
  if (CHS[id]) CHS[id].destroy();
  CHS[id] = new Chart(document.getElementById(id).getContext('2d'), cfg);
}

// Summary cards
function renderSummary(d) {
  document.getElementById('c-adm').textContent     = N(d.total_admissions);
  document.getElementById('c-depts').textContent   = d.departments;
  document.getElementById('c-rev').textContent     = M(d.total_revenue_m);
  document.getElementById('c-readmit').textContent = d.avg_readmission.toFixed(1);
  document.getElementById('c-crit').textContent    = N(d.critical_labs);
  document.getElementById('c-dq').textContent      = `${d.data_quality_pct}%`;
  document.getElementById('c-quar').textContent    = N(d.quarantined);
  document.getElementById('last-run').textContent  = `Last run: ${d.last_run}`;
}

// Dept KPI table
function renderDeptTable(rows) {
  document.getElementById('dept-tbody').innerHTML = rows.map(r => {
    const rt = r.avg_readmit_rate;
    const [cls,lbl] = rt>20?['b-crit','CRITICAL']:rt>15?['b-warn','WARNING']:['b-ok','OK'];
    const col = DC[r.department]||'#6b7280';
    return `<tr>
      <td><span style="color:${col}">●</span> <strong>${r.department}</strong></td>
      <td>${N(r.total_admissions)}</td>
      <td>${D(r.avg_los)}</td>
      <td>${P(r.avg_readmit_rate)} <span class="badge ${cls} ml-1">${lbl}</span></td>
      <td>${M(r.total_revenue_m)}</td>
      <td>${N(r.icu_admissions)}</td>
      <td>${C(r.avg_cost)}</td>
    </tr>`;
  }).join('');
}

// Bar: admissions by dept
function renderAdmChart(rows) {
  mkChart('ch-adm', { type:'bar', data:{
    labels: rows.map(r=>r.department),
    datasets:[{ label:'Admissions', data:rows.map(r=>r.total_admissions),
      backgroundColor:rows.map(r=>DC[r.department]||'#6b7280'),
      borderRadius:7, borderSkipped:false }]
  }, options:{ responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{display:false} },
    scales:{ y:{beginAtZero:true,grid:{color:'#f1f5f9'},ticks:{color:'#94a3b8',font:{size:11}}},
             x:{grid:{display:false},ticks:{color:'#475569',font:{size:11}}} }
  }});
}

// Line: monthly trend
function renderMonthlyChart(data) {
  const depts = Object.keys(data[0]).filter(k=>k!=='year_month');
  mkChart('ch-monthly', { type:'line', data:{
    labels: data.map(r=>r.year_month),
    datasets: depts.map(dept=>({ label:dept, data:data.map(r=>r[dept]),
      borderColor:DC[dept]||'#6b7280', backgroundColor:'transparent',
      borderWidth:2, pointRadius:0, tension:0.3 }))
  }, options:{ responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{position:'bottom',labels:{font:{size:10},boxWidth:12,padding:8}} },
    scales:{ y:{grid:{color:'#f1f5f9'},ticks:{color:'#94a3b8',font:{size:11}}},
             x:{grid:{display:false},ticks:{color:'#475569',font:{size:10},maxTicksLimit:6}} }
  }});
}

// Bar: revenue by dept
function renderRevChart(rows) {
  mkChart('ch-rev', { type:'bar', data:{
    labels: rows.map(r=>r.department),
    datasets:[{ label:'Revenue ($M)', data:rows.map(r=>r.total_revenue_m),
      backgroundColor:rows.map(r=>(DC[r.department]||'#6b7280')+'bb'),
      borderColor:rows.map(r=>DC[r.department]||'#6b7280'),
      borderWidth:1.5, borderRadius:7, borderSkipped:false }]
  }, options:{ responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{display:false} },
    scales:{ y:{grid:{color:'#f1f5f9'},ticks:{color:'#94a3b8',font:{size:11},callback:v=>`$${v}M`}},
             x:{grid:{display:false},ticks:{color:'#475569',font:{size:11}}} }
  }});
}

// Doughnut: readmission risk
function renderRiskChart(data) {
  const order = ['HIGH_RISK','MEDIUM_RISK','LOW_RISK'];
  const sorted = order.map(k=>data.find(d=>d.readmission_risk_tier===k)).filter(Boolean);
  mkChart('ch-risk', { type:'doughnut', data:{
    labels: sorted.map(d=>d.readmission_risk_tier.replace('_',' ')),
    datasets:[{ data:sorted.map(d=>d.cases),
      backgroundColor:sorted.map(d=>RC[d.readmission_risk_tier]),
      borderWidth:3, borderColor:'#fff', hoverOffset:10 }]
  }, options:{ responsive:true, maintainAspectRatio:false, cutout:'60%',
    plugins:{
      legend:{position:'bottom',labels:{font:{size:11},boxWidth:14,padding:12}},
      tooltip:{callbacks:{
        label:ctx=>` ${ctx.parsed.toLocaleString()} cases — avg ${sorted[ctx.dataIndex]?.avg_rate}%`
      }}
    }
  }});
}

// Critical labs table
function renderCritLabs(rows) {
  document.getElementById('crit-count').textContent = `(${rows.length} shown)`;
  const ib = v => ({HIGH:'b-high',LOW:'b-low',NORMAL:'b-normal'}[v]||'b-unknown');
  const sb = v => `<span class="badge ${v==='FINAL'?'b-ok':'b-warn'}">${v||'—'}</span>`;
  document.getElementById('crit-tbody').innerHTML = rows.map(r=>`<tr>
    <td class="font-mono text-xs text-slate-500">${r.lab_id||'—'}</td>
    <td class="font-mono text-xs text-slate-500">${r.patient_id||'—'}</td>
    <td><span class="font-medium">${r.test_name||'—'}</span>
        <span class="text-slate-400 text-xs ml-1">${r.marker||''}</span></td>
    <td class="font-bold">${r.result_value??'—'} <span class="text-slate-400 font-normal text-xs">${r.unit||''}</span></td>
    <td class="text-slate-500 text-xs">${r.reference_low??'—'} – ${r.reference_high??'—'}</td>
    <td><span class="badge ${ib(r.result_interpretation)}">${r.result_interpretation||'—'}</span></td>
    <td class="text-orange-600 font-semibold">${r.deviation_from_range??'—'}</td>
    <td>${sb(r.lab_status)}</td>
    <td class="font-medium">${r.department||'—'}</td>
    <td class="text-xs text-slate-500">${r.attending_doctor||'—'}</td>
  </tr>`).join('');
}

// Bar: revenue by insurer
function renderInsurerChart(rows) {
  const IC = ['#6366f1','#0ea5e9','#10b981','#f59e0b','#ef4444','#8b5cf6','#ec4899'];
  mkChart('ch-insurer', { type:'bar', data:{
    labels: rows.map(r=>r.insurer_name),
    datasets:[{ label:'Revenue ($M)', data:rows.map(r=>r.total_revenue_m),
      backgroundColor: rows.map((_,i)=>IC[i%IC.length]),
      borderRadius:7, borderSkipped:false }]
  }, options:{ responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{display:false} },
    scales:{ y:{grid:{color:'#f1f5f9'},ticks:{color:'#94a3b8',font:{size:11},callback:v=>`$${v}M`}},
             x:{grid:{display:false},ticks:{color:'#475569',font:{size:11}}} }
  }});
}

// Doughnut: cost tier
function renderCostTierChart(rows) {
  const TC = {LOW:'#22c55e', MEDIUM:'#3b82f6', HIGH:'#f59e0b', VERY_HIGH:'#ef4444'};
  mkChart('ch-cost-tier', { type:'doughnut', data:{
    labels: rows.map(r=>r.cost_tier),
    datasets:[{ data:rows.map(r=>r.admissions),
      backgroundColor:rows.map(r=>TC[r.cost_tier]||'#6b7280'),
      borderWidth:3, borderColor:'#fff', hoverOffset:8 }]
  }, options:{ responsive:true, maintainAspectRatio:false, cutout:'58%',
    plugins:{
      legend:{position:'bottom',labels:{font:{size:11},boxWidth:14,padding:10}},
      tooltip:{callbacks:{label:ctx=>` ${ctx.parsed.toLocaleString()} admissions`}}
    }
  }});
}

// Grouped bar: quarterly admissions by dept
function renderQuarterlyChart(rows) {
  const quarters = [...new Set(rows.map(r=>r.admission_quarter))].sort();
  const depts    = [...new Set(rows.map(r=>r.department))].sort();
  mkChart('ch-quarterly', { type:'bar', data:{
    labels: quarters,
    datasets: depts.map(dept=>({
      label: dept,
      data: quarters.map(q=>{
        const r = rows.find(x=>x.department===dept && x.admission_quarter===q);
        return r ? r.total_admissions : 0;
      }),
      backgroundColor: DC[dept]||'#6b7280',
      borderRadius:4, borderSkipped:false,
    }))
  }, options:{ responsive:true, maintainAspectRatio:false,
    plugins:{ legend:{position:'bottom',labels:{font:{size:10},boxWidth:12,padding:8}} },
    scales:{
      y:{grid:{color:'#f1f5f9'},ticks:{color:'#94a3b8',font:{size:11}},stacked:false},
      x:{grid:{display:false},ticks:{color:'#475569',font:{size:11}}}
    }
  }});
}

// Doctor alerts table
function renderDoctorAlerts(rows) {
  document.getElementById('doctor-tbody').innerHTML = rows.map((r,i)=>`<tr>
    <td><span class="font-mono font-semibold text-slate-700">${r.attending_doctor}</span></td>
    <td><span class="font-bold text-red-600">${N(r.critical_labs)}</span>
        <div class="w-24 h-1.5 bg-slate-100 rounded mt-1">
          <div class="h-1.5 bg-red-400 rounded" style="width:${Math.round(r.critical_labs/rows[0].critical_labs*100)}%"></div>
        </div></td>
    <td class="text-orange-600 font-semibold">${N(r.high_count)}</td>
    <td class="text-blue-600 font-semibold">${N(r.low_count)}</td>
    <td class="text-xs text-slate-500">${r.departments}</td>
  </tr>`).join('');
}

// Audit log timeline
function renderAudit(steps) {
  const lb = l => `<span class="badge b-${l} mr-2">${l.toUpperCase()}</span>`;
  document.getElementById('audit-wrap').innerHTML = steps.map(s=>{
    const notes = s.notes?`<span class="text-slate-400 text-xs">${s.notes}</span>`:'';
    return `<div class="flex items-center gap-3 bg-slate-50 rounded-lg px-4 py-3">
      <span class="text-emerald-500 font-bold w-4">✓</span>
      ${lb(s.layer)}
      <span class="font-medium text-slate-700 text-sm flex-1">${s.step}</span>
      <span class="text-slate-500 text-xs">${(s.records_in||0).toLocaleString()} → ${(s.records_out||0).toLocaleString()} records</span>
      ${notes}
      <span class="text-slate-400 text-xs w-16 text-right">${s.timestamp}</span>
    </div>`;
  }).join('');
}

// Fetch all, render all
async function init() {
  try {
    const [summary, deptRows, monthly, risk, critLabs, audit,
           insurerRows, costTier, doctorAlerts, quarterly] = await Promise.all([
      fetch('/api/summary').then(r=>r.json()),
      fetch('/api/dept-annual').then(r=>r.json()),
      fetch('/api/monthly-trends').then(r=>r.json()),
      fetch('/api/readmission-risk').then(r=>r.json()),
      fetch('/api/critical-labs').then(r=>r.json()),
      fetch('/api/audit').then(r=>r.json()),
      fetch('/api/insurer-revenue').then(r=>r.json()),
      fetch('/api/cost-tier').then(r=>r.json()),
      fetch('/api/doctor-alerts').then(r=>r.json()),
      fetch('/api/quarterly').then(r=>r.json()),
    ]);
    renderSummary(summary);
    renderDeptTable(deptRows);
    renderAdmChart(deptRows);
    renderMonthlyChart(monthly);
    renderRevChart(deptRows);
    renderRiskChart(risk);
    renderInsurerChart(insurerRows);
    renderCostTierChart(costTier);
    renderQuarterlyChart(quarterly);
    renderDoctorAlerts(doctorAlerts);
    renderCritLabs(critLabs);
    renderAudit(audit);
  } catch(err) {
    console.error(err);
    alert(`Dashboard failed: ${err.message}\n\nMake sure the pipeline has been run:\n  python3 scripts/run_pipeline_local.py`);
  }
}

init();
</script>
</body>
</html>"""


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = 5050
    print(f"\n{'═'*52}")
    print(f"  🏥  Healthcare Analytics Dashboard")
    print(f"  → Open in browser: http://localhost:{port}")
    print(f"  → Press Ctrl+C to stop")
    print(f"{'═'*52}\n")
    server = HTTPServer(("", port), Handler)
    server.serve_forever()
