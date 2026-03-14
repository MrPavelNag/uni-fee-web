#!/usr/bin/env python3
"""
Cloud-ready web MVP for pool analysis.

- Run existing v3/v4 agents in background jobs
- Collect JSON outputs and return merged data for UI
- Show results on-screen (no PDF required)
"""

import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from agent_common import _is_bad_fee_entry, load_chart_data_json, pairs_to_filename_suffix
from config import GOLDSKY_ENDPOINTS, TOKEN_ADDRESSES, UNISWAP_V3_SUBGRAPHS, UNISWAP_V4_SUBGRAPHS

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Uni Fee Web", version="0.1.0")

# Simple in-memory job storage (MVP)
JOBS: dict[str, dict[str, Any]] = {}
JOB_LOCK = threading.Lock()
RUN_LOCK = threading.Lock()  # prevent collisions in shared data/*.json files


def _parse_pairs_str(raw: str) -> list[tuple[str, str]]:
    out = []
    seen = set()
    for part in (raw or "").replace(" ", "").lower().split(";"):
        if "," not in part:
            continue
        a, b = part.split(",", 1)
        a, b = a.strip(), b.strip()
        if not a or not b or a == b:
            continue
        key = (a, b)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _pairs_to_string(pairs: list[tuple[str, str]]) -> str:
    return ";".join(f"{a},{b}" for a, b in pairs)


def _supported_tokens() -> list[str]:
    tokens = set()
    for per_chain in TOKEN_ADDRESSES.values():
        tokens.update(per_chain.keys())
    return sorted(tokens)


def _supported_chains() -> list[str]:
    chains = set(UNISWAP_V3_SUBGRAPHS.keys()) | set(UNISWAP_V4_SUBGRAPHS.keys()) | set(GOLDSKY_ENDPOINTS.keys())
    return sorted(chains)


def _final_income(data: dict) -> float:
    fees = data.get("fees") or []
    return float(fees[-1][1]) if fees else 0.0


def _fee_over_threshold(data: dict, threshold_pct: float) -> bool:
    """True when pool fee is above configured threshold."""
    try:
        if float(data.get("fee_pct") or 0) > threshold_pct:
            return True
    except (TypeError, ValueError):
        pass
    raw = data.get("raw_fee_tier")
    if raw is None:
        return False
    try:
        raw_int = int(raw)
    except (TypeError, ValueError):
        return False
    if (raw_int / 10000.0) > threshold_pct:
        return True
    if raw_int > 100000 and (raw_int / 1e6) > threshold_pct:
        return True
    return False


def _merge_for_web(
    token_pairs: str,
    include_chains: list[str],
    max_fee_pct: float,
) -> dict[str, Any]:
    suffix = pairs_to_filename_suffix(token_pairs)
    v3_path = DATA_DIR / f"pools_v3_{suffix}.json"
    v4_path = DATA_DIR / f"pools_v4_{suffix}.json"

    v3_data = load_chart_data_json(str(v3_path))
    v4_data = load_chart_data_json(str(v4_path))
    merged: dict[str, dict] = {}
    merged.update(v3_data)
    merged.update(v4_data)

    in_chains = {x.strip().lower() for x in include_chains if x.strip()}
    if in_chains:
        merged = {k: v for k, v in merged.items() if v.get("chain", "").lower() in in_chains}

    bad = {k: v for k, v in merged.items() if _is_bad_fee_entry(v) or _fee_over_threshold(v, max_fee_pct)}
    good = {k: v for k, v in merged.items() if k not in bad}

    sorted_good = sorted(good.items(), key=lambda kv: _final_income(kv[1]), reverse=True)
    sorted_bad = sorted(bad.items(), key=lambda kv: _final_income(kv[1]), reverse=True)

    rows = []
    chart_series = []
    for pool_id, v in sorted_good + sorted_bad:
        fees = v.get("fees") or []
        tvl = v.get("tvl") or []
        status = "error_fee_gt_3pct" if pool_id in bad else "ok"
        row = {
            "pool_id": v.get("pool_id", pool_id),
            "chain": v.get("chain", ""),
            "version": v.get("version", ""),
            "pair": v.get("pair", ""),
            "fee_pct": float(v.get("fee_pct") or 0),
            "final_income": _final_income(v),
            "last_tvl": float(tvl[-1][1]) if tvl else 0.0,
            "status": status,
        }
        rows.append(row)
        if status == "ok":
            chart_series.append(
                {
                    "label": f"{row['chain']} {row['version']} {row['pair']} ...{row['pool_id'][-4:]}",
                    "pool_id": row["pool_id"],
                    "fees": fees,
                    "tvl": tvl,
                }
            )

    return {
        "suffix": suffix,
        "total": len(merged),
        "chart_pools": len(sorted_good),
        "error_pools": len(sorted_bad),
        "rows": rows,
        "series": chart_series,
    }


def _run_subprocess(script_name: str, env: dict[str, str], min_tvl: float, logs: list[str]) -> None:
    cmd = [sys.executable, str(BASE_DIR / script_name), "--min-tvl", str(min_tvl)]
    proc = subprocess.run(
        cmd,
        cwd=str(BASE_DIR),
        env=env,
        text=True,
        capture_output=True,
    )
    logs.append(f"$ {' '.join(cmd)}")
    if proc.stdout:
        logs.append(proc.stdout[-4000:])
    if proc.stderr:
        logs.append(proc.stderr[-4000:])
    if proc.returncode != 0:
        raise RuntimeError(f"{script_name} failed with code {proc.returncode}")


def _run_pool_job(job_id: str, req: "PoolsRunRequest") -> None:
    def _set_stage(stage: str, label: str, progress: int) -> None:
        with JOB_LOCK:
            j = JOBS.get(job_id)
            if not j:
                return
            j["stage"] = stage
            j["stage_label"] = label
            j["progress"] = max(0, min(100, progress))

    with JOB_LOCK:
        job = JOBS[job_id]
        job["status"] = "running"
        job["started_at"] = time.time()
        job["stage"] = "prepare"
        job["stage_label"] = "Preparing parameters"
        job["progress"] = 5

    # Build up to 3 selected token pairs
    pairs = []
    for pair in req.pairs[:3]:
        part = (pair or "").strip().lower()
        if "," not in part:
            continue
        a, b = part.split(",", 1)
        a, b = a.strip(), b.strip()
        if a and b and a != b:
            pairs.append((a, b))
    pairs = list(dict.fromkeys(pairs))
    if not pairs:
        with JOB_LOCK:
            job["status"] = "failed"
            job["error"] = "No valid pairs provided."
            job["stage"] = "failed"
            job["stage_label"] = "Validation failed"
            job["progress"] = 100
        return
    token_pairs = _pairs_to_string(pairs)

    include_chains = [c.strip().lower() for c in req.include_chains if c.strip()]

    logs: list[str] = []
    env = os.environ.copy()
    env["TOKEN_PAIRS"] = token_pairs
    env["FEE_DAYS"] = str(req.days)
    env["INCLUDE_CHAINS"] = ",".join(include_chains)

    # v4 endpoint config uses this list at import-time
    if include_chains:
        v4_supported = {c for c in include_chains if c in UNISWAP_V4_SUBGRAPHS}
        env["V4_CHAINS"] = ",".join(sorted(v4_supported))

    try:
        with RUN_LOCK:
            _set_stage("v3", "Running v3 discovery and calculations", 25)
            _run_subprocess("agent_v3.py", env, req.min_tvl, logs)
            _set_stage("v4", "Running v4 discovery and calculations", 55)
            _run_subprocess("agent_v4.py", env, req.min_tvl, logs)
        _set_stage("merge", "Merging results for web", 80)
        result = _merge_for_web(token_pairs, include_chains=include_chains, max_fee_pct=req.max_fee_pct)
        with JOB_LOCK:
            job["status"] = "done"
            job["stage"] = "done"
            job["stage_label"] = "Completed"
            job["progress"] = 100
            job["result"] = {
                "request": {
                    "pairs": token_pairs,
                    "days": req.days,
                    "min_tvl": req.min_tvl,
                    "include_chains": include_chains,
                    "max_fee_pct": req.max_fee_pct,
                },
                **result,
                "logs": logs[-8:],
            }
    except Exception as e:
        with JOB_LOCK:
            job["status"] = "failed"
            job["error"] = str(e)
            job["stage"] = "failed"
            job["stage_label"] = "Failed"
            job["progress"] = 100
            job["result"] = {"logs": logs[-8:]}
    finally:
        with JOB_LOCK:
            job["finished_at"] = time.time()


class PoolsRunRequest(BaseModel):
    pairs: list[str] = Field(default_factory=list, description="Up to 3 pairs: tokenA,tokenB")
    include_chains: list[str] = Field(default_factory=list)
    min_tvl: float = 100.0
    days: int = 90
    max_fee_pct: float = 3.0


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return HTML_PAGE


@app.get("/stables", response_class=HTMLResponse)
def stables_page() -> str:
    return "<h2>Stables Analysis</h2><p>Coming soon.</p><p><a href='/'>Back</a></p>"


@app.get("/positions", response_class=HTMLResponse)
def positions_page() -> str:
    return "<h2>My Open Positions</h2><p>Coming soon.</p><p><a href='/'>Back</a></p>"


@app.get("/api/meta")
def meta() -> dict[str, Any]:
    return {
        "tokens": _supported_tokens(),
        "chains": _supported_chains(),
        "quote_tokens": ["usdt", "usdc", "eth"],
    }


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/pools/run")
def run_pools(req: PoolsRunRequest) -> dict[str, str]:
    if not os.environ.get("THE_GRAPH_API_KEY"):
        raise HTTPException(status_code=400, detail="Missing THE_GRAPH_API_KEY on server.")
    if req.days < 1 or req.days > 3650:
        raise HTTPException(status_code=400, detail="days must be between 1 and 3650")
    if req.min_tvl < 0:
        raise HTTPException(status_code=400, detail="min_tvl must be >= 0")
    if req.max_fee_pct <= 0 or req.max_fee_pct > 100:
        raise HTTPException(status_code=400, detail="max_fee_pct must be between 0 and 100")

    job_id = str(uuid.uuid4())
    with JOB_LOCK:
        JOBS[job_id] = {
            "id": job_id,
            "status": "queued",
            "stage": "queued",
            "stage_label": "Queued",
            "progress": 0,
            "created_at": time.time(),
            "result": None,
            "error": None,
        }
    t = threading.Thread(target=_run_pool_job, args=(job_id, req), daemon=True)
    t.start()
    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str) -> dict[str, Any]:
    with JOB_LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


HTML_PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Uni Fee - Pools</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {
      --bg: #f3f7ff;
      --card: #ffffff;
      --muted: #64748b;
      --text: #0f172a;
      --border: #d9e2f0;
      --ok: #22c55e;
      --warn: #eab308;
      --danger: #ef4444;
      --accent: #2563eb;
      --accent-2: #06b6d4;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, Arial, sans-serif;
      background: linear-gradient(180deg, #eef5ff 0%, var(--bg) 100%);
      color: var(--text);
    }
    .container {
      max-width: 1200px;
      margin: 0 auto;
      padding: 18px;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
    }
    .title {
      margin: 0;
      font-size: 30px;
      font-weight: 800;
      letter-spacing: 0.2px;
    }
    .subtitle {
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    .nav {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .nav a {
      color: #1d4ed8;
      text-decoration: none;
      border: 1px solid #bfdbfe;
      border-radius: 999px;
      padding: 6px 12px;
      font-size: 13px;
      background: #eff6ff;
    }
    .grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06);
    }
    .card h3 {
      margin: 0 0 10px;
      font-size: 18px;
    }
    .form-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }
    .row {
      display: grid;
      grid-template-columns: 180px 1fr;
      gap: 12px;
      align-items: start;
    }
    .row label {
      font-weight: 700;
      font-size: 14px;
      padding-top: 8px;
    }
    .row .hint {
      color: var(--muted);
      font-size: 12px;
      margin-top: 5px;
    }
    input, textarea, select {
      width: 100%;
      background: #ffffff;
      border: 1px solid #cbd5e1;
      color: var(--text);
      border-radius: 8px;
      padding: 10px;
      font-size: 14px;
    }
    select { appearance: auto; }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    .btn {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      font-weight: 700;
      font-size: 14px;
      cursor: pointer;
      color: #fff;
      background: linear-gradient(90deg, var(--accent), #4338ca);
    }
    .btn.secondary {
      background: #f8fafc;
      border: 1px solid #d1d5db;
      color: #334155;
    }
    .chips {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .chip {
      border-radius: 999px;
      border: 1px solid #cbd5e1;
      background: #f8fafc;
      color: #334155;
      padding: 5px 10px;
      font-size: 12px;
      cursor: pointer;
    }
    .status {
      font-size: 14px;
      color: #cbd5e1;
    }
    .status.running { color: var(--warn); }
    .status.ok { color: var(--ok); }
    .status.fail { color: var(--danger); }
    .progress-wrap { width: 100%; margin-top: 10px; }
    .progress-meta {
      display: flex;
      justify-content: space-between;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .progress-bar {
      width: 100%;
      height: 10px;
      border-radius: 999px;
      background: #1f2937;
      border: 1px solid #334155;
      overflow: hidden;
    }
    .progress-fill {
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      transition: width 0.3s ease;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
    }
    .metric {
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      padding: 10px;
    }
    .metric .k {
      color: var(--muted);
      font-size: 12px;
    }
    .metric .v {
      font-size: 22px;
      font-weight: 800;
      margin-top: 3px;
    }
    .charts-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }
    .plot {
      width: 100%;
      min-height: 420px;
      border: 1px solid #dbe3ef;
      border-radius: 10px;
      background: #ffffff;
    }
    .table-wrap {
      overflow-x: auto;
      border: 1px solid #dbe3ef;
      border-radius: 10px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      min-width: 900px;
    }
    th, td {
      border-bottom: 1px solid #e2e8f0;
      padding: 8px;
      text-align: left;
    }
    th {
      background: #eff6ff;
      color: #1e3a8a;
      position: sticky;
      top: 0;
      cursor: pointer;
      white-space: nowrap;
    }
    tr.ok-row:hover td { background: rgba(34,197,94,0.10); }
    tr.error-row td { background: rgba(239,68,68,0.08); }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
    details { margin-top: 10px; }
    pre {
      max-height: 220px;
      overflow: auto;
      background: #f8fafc;
      border: 1px solid #dbe3ef;
      border-radius: 8px;
      padding: 10px;
      color: #334155;
      font-size: 12px;
    }
    .pair-row {
      display: grid;
      grid-template-columns: repeat(3, minmax(180px, 1fr));
      gap: 8px;
    }
    .chain-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 6px 10px;
      margin-top: 4px;
    }
    .check {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 13px;
      color: #334155;
    }
    .check input {
      width: auto;
      padding: 0;
    }
    .tooltip {
      color: #475569;
      font-size: 12px;
      margin-left: 6px;
      cursor: help;
      border-bottom: 1px dotted #94a3b8;
    }
    .inline-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    @media (max-width: 980px) {
      .row { grid-template-columns: 1fr; }
      .row label { padding-top: 0; }
      .pair-row { grid-template-columns: 1fr; }
      .inline-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div>
        <h1 class="title">Pools Analysis</h1>
        <p class="subtitle">Uniswap v3/v4 screening with on-screen charts, filtering and ranked pool table.</p>
      </div>
      <div class="nav">
        <a href="/stables">Stables (later)</a>
        <a href="/positions">My positions (later)</a>
      </div>
    </div>

    <div class="grid">
      <section class="card">
        <div class="form-grid">
          <div class="row">
            <label title="Select up to 3 pairs for analysis">Pairs <span class="tooltip" title="Select up to 3 pairs from the list. Search by typing first letters.">?</span></label>
            <div>
              <div class="pair-row">
                <input id="pair1" list="pairHints" placeholder="pair 1 (e.g. wbtc,usdt)" title="Pair #1"/>
                <input id="pair2" list="pairHints" placeholder="pair 2" title="Pair #2"/>
                <input id="pair3" list="pairHints" placeholder="pair 3" title="Pair #3"/>
              </div>
              <datalist id="pairHints"></datalist>
              <div class="hint">Maximum 3 pairs.</div>
            </div>
          </div>

          <div class="row">
            <label title="Choose chains for analysis">Include chains <span class="tooltip" title="Choose 'all' or select specific chains">?</span></label>
            <div>
              <label class="check"><input type="checkbox" id="allChains" checked onchange="toggleAllChains()"> all</label>
              <div class="chain-grid" id="chainChecks"></div>
            </div>
          </div>

          <div class="row">
            <label>Filters <span class="tooltip" title="Control TVL/history and fee cutoff for chart inclusion">?</span></label>
            <div>
              <div class="inline-grid">
                <div>
                  <div class="hint" style="margin-bottom:4px">Min TVL (USD)</div>
                  <input id="minTvl" value="1000" type="number" title="Pools below this TVL are ignored"/>
                </div>
                <div>
                  <div class="hint" style="margin-bottom:4px">History days</div>
                  <input id="days" value="90" type="number" title="How many historical days to load"/>
                </div>
              </div>
            </div>
          </div>

          <div class="row">
            <label for="maxFeePct" title="Pools above this fee are excluded from chart and flagged as error">Fee cutoff <span class="tooltip" title="Exclude pools with fee higher than X%">?</span></label>
            <div>
              <input id="maxFeePct" value="3" type="number" step="0.1" min="0.1" max="100" title="Exclude pools with fee above this value"/>
              <div class="hint">Exclude pools above X% fee.</div>
            </div>
          </div>
        </div>

        <div class="actions" style="margin-top:14px">
          <button class="btn" id="runBtn" onclick="runJob()">Run analysis</button>
          <button class="btn secondary" onclick="exportCsv()">Export CSV</button>
          <span id="status" class="status">Ready</span>
        </div>
        <div class="progress-wrap">
          <div class="progress-meta">
            <span id="stageText">Stage: waiting</span>
            <span id="progressText">0%</span>
          </div>
          <div class="progress-bar"><div id="progressFill" class="progress-fill"></div></div>
        </div>
      </section>

      <section class="card">
        <h3>Summary</h3>
        <div class="metrics">
          <div class="metric"><div class="k">Pairs suffix</div><div class="v" id="mSuffix">-</div></div>
          <div class="metric"><div class="k">Total pools</div><div class="v" id="mTotal">0</div></div>
          <div class="metric"><div class="k">Pools in chart</div><div class="v" id="mChart">0</div></div>
          <div class="metric"><div class="k">Error fee > 3%</div><div class="v" id="mErr">0</div></div>
        </div>
        <details>
          <summary>Latest run logs</summary>
          <pre id="logs">No logs yet.</pre>
        </details>
      </section>

      <section class="card">
        <h3>Charts</h3>
        <div class="charts-grid">
          <div id="feesChart" class="plot"></div>
          <div id="tvlChart" class="plot"></div>
        </div>
      </section>

      <section class="card">
        <h3>Pools Table</h3>
        <div class="hint" style="margin-bottom:8px">Click a column header to sort.</div>
        <div class="table-wrap">
          <table id="resultTable"></table>
        </div>
      </section>
    </div>
  </div>

  <script>
    const SORTABLE = {
      chain: (r) => r.chain || "",
      version: (r) => r.version || "",
      pair: (r) => r.pair || "",
      fee_pct: (r) => Number(r.fee_pct || 0),
      final_income: (r) => Number(r.final_income || 0),
      last_tvl: (r) => Number(r.last_tvl || 0),
      status: (r) => r.status || ""
    };

    let lastRows = [];
    let renderedRows = [];
    let sortKey = "final_income";
    let sortDesc = true;
    const FORM_STORAGE_KEY = "uni_fee_form_v2";
    const FIELD_IDS = ["pair1", "pair2", "pair3", "minTvl", "days", "maxFeePct", "allChains"];
    let availableChains = [];

    function splitCSV(v) {
      return (v || "").split(",").map(x => x.trim().toLowerCase()).filter(Boolean);
    }

    function formatUsd(v) {
      return new Intl.NumberFormat("en-US", {maximumFractionDigits: 0}).format(Number(v || 0));
    }

    function setDays(v) {
      document.getElementById("days").value = v;
      saveFormState();
    }

    function setStatus(text, cssClass) {
      const el = document.getElementById("status");
      el.textContent = text;
      el.className = "status " + (cssClass || "");
    }

    function setBusy(flag) {
      document.getElementById("runBtn").disabled = flag;
      document.getElementById("runBtn").style.opacity = flag ? "0.7" : "1";
      document.getElementById("runBtn").textContent = flag ? "Running..." : "Run analysis";
    }

    function updateProgress(progress, stageLabel) {
      const p = Math.max(0, Math.min(100, Number(progress || 0)));
      document.getElementById("progressFill").style.width = p + "%";
      document.getElementById("progressText").textContent = p + "%";
      document.getElementById("stageText").textContent = "Stage: " + (stageLabel || "running");
    }

    function saveFormState() {
      const state = {};
      for (const id of FIELD_IDS) {
        const el = document.getElementById(id);
        if (!el) continue;
        if (el.type === "checkbox") {
          state[id] = !!el.checked;
        } else {
          state[id] = el.value;
        }
      }
      state.selectedChains = getSelectedChains();
      localStorage.setItem(FORM_STORAGE_KEY, JSON.stringify(state));
    }

    function loadFormState() {
      try {
        const raw = localStorage.getItem(FORM_STORAGE_KEY);
        if (!raw) return;
        const state = JSON.parse(raw);
        for (const id of FIELD_IDS) {
          const el = document.getElementById(id);
          if (!el || state[id] == null) continue;
          if (el.type === "checkbox") {
            el.checked = !!state[id];
          } else if (typeof state[id] === "string") {
            el.value = state[id];
          }
        }
        if (Array.isArray(state.selectedChains)) {
          for (const c of state.selectedChains) {
            const box = document.getElementById("chain_" + c);
            if (box) box.checked = true;
          }
        }
      } catch (e) {
        console.warn("load form state failed", e);
      }
    }

    function attachAutosave() {
      for (const id of FIELD_IDS) {
        const el = document.getElementById(id);
        if (el) {
          el.addEventListener("input", saveFormState);
          el.addEventListener("change", saveFormState);
        }
      }
    }

    function getSelectedPairs() {
      return [document.getElementById("pair1").value, document.getElementById("pair2").value, document.getElementById("pair3").value]
        .map(v => (v || "").trim().toLowerCase())
        .filter(Boolean);
    }

    function getSelectedChains() {
      if (document.getElementById("allChains").checked) return [];
      const out = [];
      for (const c of availableChains) {
        const el = document.getElementById("chain_" + c);
        if (el && el.checked) out.push(c);
      }
      return out;
    }

    function toggleAllChains() {
      const all = document.getElementById("allChains").checked;
      for (const c of availableChains) {
        const el = document.getElementById("chain_" + c);
        if (el) el.disabled = all;
      }
      saveFormState();
    }

    function renderTable(rows) {
      const table = document.getElementById("resultTable");
      const hdr = [
        ["chain", "Chain"], ["version", "Version"], ["pair", "Pair"], ["pool_id", "Pool ID"],
        ["fee_pct", "Fee %"], ["final_income", "Cumul $"], ["last_tvl", "TVL"], ["status", "Status"]
      ];

      let html = "<tr>";
      for (const h of hdr) {
        const marker = sortKey === h[0] ? (sortDesc ? " ▼" : " ▲") : "";
        html += `<th onclick="sortBy('${h[0]}')">${h[1]}${marker}</th>`;
      }
      html += "</tr>";

      for (const r of rows) {
        const cls = r.status === "ok" ? "ok-row" : "error-row";
        html += `<tr class="${cls}">`;
        html += `<td>${r.chain}</td>`;
        html += `<td>${r.version}</td>`;
        html += `<td>${r.pair}</td>`;
        html += `<td class="mono">${r.pool_id}</td>`;
        html += `<td>${Number(r.fee_pct).toFixed(2)}</td>`;
        html += `<td>$${formatUsd(r.final_income)}</td>`;
        html += `<td>$${formatUsd(r.last_tvl)}</td>`;
        html += `<td>${r.status === "ok" ? "ok" : "error fee>3%"}</td>`;
        html += "</tr>";
      }
      table.innerHTML = html;
    }

    function sortBy(key) {
      if (!SORTABLE[key]) return;
      if (sortKey === key) {
        sortDesc = !sortDesc;
      } else {
        sortKey = key;
        sortDesc = key === "final_income" || key === "last_tvl" || key === "fee_pct";
      }
      const fn = SORTABLE[sortKey];
      const sorted = [...lastRows].sort((a, b) => {
        const x = fn(a), y = fn(b);
        if (x < y) return sortDesc ? 1 : -1;
        if (x > y) return sortDesc ? -1 : 1;
        return 0;
      });
      renderedRows = sorted;
      renderTable(sorted);
    }

    function exportCsv() {
      const rows = renderedRows.length ? renderedRows : lastRows;
      if (!rows.length) {
        setStatus("No rows to export yet.", "fail");
        return;
      }
      const headers = ["chain", "version", "pair", "pool_id", "fee_pct", "final_income", "last_tvl", "status"];
      const lines = [headers.join(",")];
      for (const r of rows) {
        const vals = headers.map(h => {
          const val = r[h] == null ? "" : String(r[h]);
          return `"${val.replace(/"/g, '""')}"`;
        });
        lines.push(vals.join(","));
      }
      const blob = new Blob([lines.join("\\n")], {type: "text/csv;charset=utf-8;"});
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "pools_analysis.csv";
      a.click();
      URL.revokeObjectURL(url);
    }

    async function loadMeta() {
      try {
        const r = await fetch("/api/meta");
        const meta = await r.json();
        const commonQuotes = ["usdt", "usdc", "eth"];
        const pairs = [];
        for (const t of meta.tokens) {
          for (const q of commonQuotes) {
            if (t !== q) pairs.push(`${t},${q}`);
          }
        }
        const pairHints = document.getElementById("pairHints");
        pairHints.innerHTML = pairs.slice(0, 500).map(p => `<option value="${p}"></option>`).join("");

        availableChains = meta.chains || [];
        const checks = document.getElementById("chainChecks");
        checks.innerHTML = availableChains.map(c => (
          `<label class="check"><input type="checkbox" id="chain_${c}" onchange="saveFormState()"> ${c}</label>`
        )).join("");
        loadFormState();
        toggleAllChains();
      } catch (e) {
        console.warn("meta load failed", e);
      }
    }

    async function runJob() {
      const payload = {
        pairs: getSelectedPairs(),
        include_chains: getSelectedChains(),
        min_tvl: Number(document.getElementById("minTvl").value || 0),
        days: Number(document.getElementById("days").value || 90),
        max_fee_pct: Number(document.getElementById("maxFeePct").value || 3)
      };
      if (payload.pairs.length === 0) {
        setStatus("Select at least one pair.", "fail");
        return;
      }

      saveFormState();
      setBusy(true);
      setStatus("Starting...", "running");
      updateProgress(2, "Submitting job");
      const r = await fetch("/api/pools/run", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload)
      });
      const data = await r.json();
      if (!r.ok) {
        setBusy(false);
        setStatus("Error: " + (data.detail || "request failed"), "fail");
        return;
      }
      pollJob(data.job_id);
    }

    async function pollJob(jobId) {
      const timer = setInterval(async () => {
        const r = await fetch("/api/jobs/" + jobId);
        const job = await r.json();
        updateProgress(job.progress, job.stage_label || job.stage);
        if (job.status === "done") {
          clearInterval(timer);
          setBusy(false);
          setStatus("Done", "ok");
          renderResult(job.result);
        } else if (job.status === "failed") {
          clearInterval(timer);
          setBusy(false);
          setStatus("Failed: " + (job.error || "unknown"), "fail");
          if (job.result && job.result.logs) {
            document.getElementById("logs").textContent = job.result.logs.join("\\n\\n");
          }
        } else {
          setStatus("Status: " + (job.stage_label || job.status), "running");
        }
      }, 2000);
    }

    function renderResult(result) {
      document.getElementById("mSuffix").textContent = result.suffix;
      document.getElementById("mTotal").textContent = result.total;
      document.getElementById("mChart").textContent = result.chart_pools;
      document.getElementById("mErr").textContent = result.error_pools;
      document.getElementById("logs").textContent = (result.logs || []).join("\\n\\n") || "No logs.";

      const feeTraces = [];
      const tvlTraces = [];
      for (const s of result.series) {
        const feeX = s.fees.map(p => new Date(p[0] * 1000));
        const feeY = s.fees.map(p => p[1]);
        const tvlX = s.tvl.map(p => new Date(p[0] * 1000));
        const tvlY = s.tvl.map(p => p[1] / 1000.0);
        feeTraces.push({x: feeX, y: feeY, mode: "lines", name: s.label});
        tvlTraces.push({x: tvlX, y: tvlY, mode: "lines", name: s.label});
      }
      Plotly.newPlot("feesChart", feeTraces, {
        title: "Cumulative Fees",
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#ffffff",
        font: {color: "#0f172a"}
      }, {displaylogo: false, responsive: true});
      Plotly.newPlot("tvlChart", tvlTraces, {
        title: "TVL dynamics (thousands USD)",
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#ffffff",
        font: {color: "#0f172a"}
      }, {displaylogo: false, responsive: true});

      lastRows = result.rows || [];
      sortKey = "final_income";
      sortDesc = true;
      sortBy("final_income");
    }

    attachAutosave();
    loadMeta();
  </script>
</body>
</html>
"""
