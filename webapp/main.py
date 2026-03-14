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


def _merge_for_web(
    token_pairs: str,
    exclude_chains: list[str],
    exclude_suffixes: list[str],
) -> dict[str, Any]:
    suffix = pairs_to_filename_suffix(token_pairs)
    v3_path = DATA_DIR / f"pools_v3_{suffix}.json"
    v4_path = DATA_DIR / f"pools_v4_{suffix}.json"

    v3_data = load_chart_data_json(str(v3_path))
    v4_data = load_chart_data_json(str(v4_path))
    merged: dict[str, dict] = {}
    merged.update(v3_data)
    merged.update(v4_data)

    ex_chains = {x.strip().lower() for x in exclude_chains if x.strip()}
    if ex_chains:
        merged = {k: v for k, v in merged.items() if v.get("chain", "").lower() not in ex_chains}

    ex_suffixes = {x.strip().lower() for x in exclude_suffixes if x.strip()}
    if ex_suffixes:
        filtered = {}
        for k, v in merged.items():
            pid = (v.get("pool_id") or k or "").lower()
            tail4 = pid[-4:] if len(pid) >= 4 else pid
            if tail4 not in ex_suffixes:
                filtered[k] = v
        merged = filtered

    bad = {k: v for k, v in merged.items() if _is_bad_fee_entry(v)}
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

    # Build token pairs from manual pairs + quick token picks
    pairs = _parse_pairs_str(req.pairs)
    for token in [t.strip().lower() for t in req.tokens if t.strip()]:
        for quote in [q.strip().lower() for q in req.quote_tokens if q.strip()]:
            if token != quote:
                pairs.append((token, quote))
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
    exclude_chains = [c.strip().lower() for c in req.exclude_chains if c.strip()]
    exclude_suffixes = [s.strip().lower() for s in req.exclude_pool_suffix if s.strip()]

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
        result = _merge_for_web(token_pairs, exclude_chains=exclude_chains, exclude_suffixes=exclude_suffixes)
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
                    "exclude_chains": exclude_chains,
                    "exclude_pool_suffix": exclude_suffixes,
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
    pairs: str = Field(default="", description="Manual pairs: tokenA,tokenB;tokenC,tokenD")
    tokens: list[str] = Field(default_factory=list, description="Quick tokens list to pair with quote_tokens")
    quote_tokens: list[str] = Field(default_factory=lambda: ["usdt", "usdc", "eth"])
    include_chains: list[str] = Field(default_factory=list)
    exclude_chains: list[str] = Field(default_factory=list)
    exclude_pool_suffix: list[str] = Field(default_factory=list)
    min_tvl: float = 100.0
    days: int = 90


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
  <title>Uni Fee - Pools Analysis</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {
      --bg: #0f172a;
      --card: #111827;
      --muted: #94a3b8;
      --text: #e2e8f0;
      --border: #334155;
      --ok: #22c55e;
      --warn: #eab308;
      --danger: #ef4444;
      --accent: #4f46e5;
      --accent-2: #22d3ee;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, Arial, sans-serif;
      background: radial-gradient(circle at top right, #1d4ed8 0%, var(--bg) 40%);
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
      color: #c7d2fe;
      text-decoration: none;
      border: 1px solid #4338ca;
      border-radius: 999px;
      padding: 6px 12px;
      font-size: 13px;
      background: rgba(67, 56, 202, 0.18);
    }
    .grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }
    .card {
      background: linear-gradient(180deg, rgba(17,24,39,0.98), rgba(17,24,39,0.85));
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 8px 24px rgba(0, 0, 0, 0.22);
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
      grid-template-columns: 220px 1fr;
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
    input, textarea {
      width: 100%;
      background: #0b1220;
      border: 1px solid #374151;
      color: var(--text);
      border-radius: 8px;
      padding: 10px;
      font-size: 14px;
    }
    textarea { min-height: 76px; resize: vertical; }
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
      background: #1f2937;
      border: 1px solid #334155;
      color: #dbeafe;
    }
    .chips {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .chip {
      border-radius: 999px;
      border: 1px solid #334155;
      background: #0b1220;
      color: #bfdbfe;
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
      background: #0b1220;
      border: 1px solid #263445;
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
      border: 1px solid #263445;
      border-radius: 10px;
      background: #0b1220;
    }
    .table-wrap {
      overflow-x: auto;
      border: 1px solid #263445;
      border-radius: 10px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      min-width: 900px;
    }
    th, td {
      border-bottom: 1px solid #223040;
      padding: 8px;
      text-align: left;
    }
    th {
      background: #111b2d;
      color: #93c5fd;
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
      background: #0b1220;
      border: 1px solid #263445;
      border-radius: 8px;
      padding: 10px;
      color: #9fb3c8;
      font-size: 12px;
    }
    @media (max-width: 980px) {
      .row { grid-template-columns: 1fr; }
      .row label { padding-top: 0; }
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
        <h3>Run Configuration</h3>
        <div class="form-grid">
          <div class="row">
            <label for="pairs" title="Manual token pairs in format tokenA,tokenB;tokenC,tokenD">Pairs (manual)</label>
            <div>
              <textarea id="pairs" placeholder="wbtc,usdt;wbtc,usdc" title="Example: wbtc,usdt;wbtc,usdc"></textarea>
              <div class="hint">Format: tokenA,tokenB;tokenC,tokenD</div>
            </div>
          </div>

          <div class="row">
            <label for="tokens" title="List base tokens; each will be paired with quote tokens">Quick tokens list</label>
            <div>
              <input id="tokens" list="tokenHints" placeholder="paxg,fluid,wbtc" title="Comma-separated list: paxg,fluid,wbtc"/>
              <datalist id="tokenHints"></datalist>
              <div class="hint">Each token will be paired with quote tokens below.</div>
            </div>
          </div>

          <div class="row">
            <label for="quotes" title="Quote tokens for quick-token pairing">Quote tokens</label>
            <div>
              <input id="quotes" value="usdt,usdc,eth" title="Comma-separated quote tokens, e.g. usdt,usdc,eth"/>
              <div class="chips">
                <button class="chip" onclick="setQuotes('usdt,usdc,eth')">USDT/USDC/ETH</button>
                <button class="chip" onclick="setQuotes('usdc,eth')">USDC/ETH</button>
                <button class="chip" onclick="setQuotes('usdt,eth')">USDT/ETH</button>
              </div>
            </div>
          </div>

          <div class="row">
            <label for="includeChains" title="If empty, all supported chains are included">Include chains (empty = all)</label>
            <div>
              <input id="includeChains" list="chainHints" placeholder="ethereum,arbitrum-one,polygon" title="Comma-separated chain slugs"/>
              <datalist id="chainHints"></datalist>
              <div class="chips" id="chainChips"></div>
            </div>
          </div>

          <div class="row">
            <label for="minTvl" title="Minimum pool TVL in USD">Min TVL</label>
            <input id="minTvl" value="1000" type="number" title="Pools below this TVL are ignored"/>
          </div>

          <div class="row">
            <label for="days" title="History depth in days for fee/TVL dynamics">History days</label>
            <div>
              <input id="days" value="90" type="number" title="How many days of historical data to load"/>
              <div class="chips">
                <button class="chip" onclick="setDays(30)">30d</button>
                <button class="chip" onclick="setDays(90)">90d</button>
                <button class="chip" onclick="setDays(180)">180d</button>
                <button class="chip" onclick="setDays(365)">365d</button>
              </div>
            </div>
          </div>

          <div class="row">
            <label for="excludeChains" title="Chains to exclude from final chart/table">Exclude chains</label>
            <input id="excludeChains" placeholder="base,polygon" title="Comma-separated chain slugs to exclude"/>
          </div>

          <div class="row">
            <label for="excludeSuffix" title="Filter out pools by the last 4 chars of pool_id">Exclude pool suffix (last 4)</label>
            <input id="excludeSuffix" placeholder="1a2b,dead" title="Comma-separated suffixes; each compared to pool_id tail"/>
          </div>
        </div>

        <div class="actions" style="margin-top:14px">
          <button class="btn" id="runBtn" onclick="runJob()">Run analysis</button>
          <button class="btn secondary" onclick="applyPreset('gold')">Preset: Gold</button>
          <button class="btn secondary" onclick="applyPreset('btc')">Preset: BTC</button>
          <button class="btn secondary" onclick="applyPreset('majors')">Preset: Majors</button>
          <button class="btn secondary" onclick="exportCsv()">Export CSV</button>
          <span id="status" class="status">Idle</span>
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
    const FORM_STORAGE_KEY = "uni_fee_form_v1";
    const FIELD_IDS = ["pairs", "tokens", "quotes", "includeChains", "minTvl", "days", "excludeChains", "excludeSuffix"];

    function splitCSV(v) {
      return (v || "").split(",").map(x => x.trim().toLowerCase()).filter(Boolean);
    }

    function formatUsd(v) {
      return new Intl.NumberFormat("en-US", {maximumFractionDigits: 0}).format(Number(v || 0));
    }

    function setQuotes(v) {
      document.getElementById("quotes").value = v;
      saveFormState();
    }

    function setDays(v) {
      document.getElementById("days").value = v;
      saveFormState();
    }

    function applyPreset(name) {
      if (name === "gold") {
        document.getElementById("pairs").value = "paxg,usdt;paxg,usdc;xaut,usdt;xaut,usdc";
        document.getElementById("tokens").value = "paxg,xaut";
        document.getElementById("includeChains").value = "ethereum,arbitrum-one,polygon";
        document.getElementById("minTvl").value = "1000";
        document.getElementById("days").value = "90";
      } else if (name === "btc") {
        document.getElementById("pairs").value = "wbtc,usdt;wbtc,usdc;wbtc,eth";
        document.getElementById("tokens").value = "wbtc";
        document.getElementById("includeChains").value = "ethereum,arbitrum-one,polygon";
        document.getElementById("minTvl").value = "10000";
        document.getElementById("days").value = "90";
      } else if (name === "majors") {
        document.getElementById("pairs").value = "eth,usdt;eth,usdc;wbtc,usdt;wbtc,usdc";
        document.getElementById("tokens").value = "wbtc,eth";
        document.getElementById("includeChains").value = "";
        document.getElementById("minTvl").value = "5000";
        document.getElementById("days").value = "90";
      }
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
        if (el) state[id] = el.value;
      }
      localStorage.setItem(FORM_STORAGE_KEY, JSON.stringify(state));
    }

    function loadFormState() {
      try {
        const raw = localStorage.getItem(FORM_STORAGE_KEY);
        if (!raw) return;
        const state = JSON.parse(raw);
        for (const id of FIELD_IDS) {
          const el = document.getElementById(id);
          if (el && typeof state[id] === "string") {
            el.value = state[id];
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
        const tokenHints = document.getElementById("tokenHints");
        tokenHints.innerHTML = meta.tokens.map(t => `<option value="${t}"></option>`).join("");
        const chainHints = document.getElementById("chainHints");
        chainHints.innerHTML = meta.chains.map(c => `<option value="${c}"></option>`).join("");

        const chips = document.getElementById("chainChips");
        chips.innerHTML = meta.chains.map(c => `<button class="chip" onclick="toggleChain('${c}')">${c}</button>`).join("");
      } catch (e) {
        console.warn("meta load failed", e);
      }
    }

    function toggleChain(chain) {
      const input = document.getElementById("includeChains");
      const list = splitCSV(input.value);
      const idx = list.indexOf(chain);
      if (idx >= 0) list.splice(idx, 1); else list.push(chain);
      input.value = list.join(",");
      saveFormState();
    }

    async function runJob() {
      const payload = {
        pairs: document.getElementById("pairs").value,
        tokens: splitCSV(document.getElementById("tokens").value),
        quote_tokens: splitCSV(document.getElementById("quotes").value),
        include_chains: splitCSV(document.getElementById("includeChains").value),
        exclude_chains: splitCSV(document.getElementById("excludeChains").value),
        exclude_pool_suffix: splitCSV(document.getElementById("excludeSuffix").value),
        min_tvl: Number(document.getElementById("minTvl").value || 0),
        days: Number(document.getElementById("days").value || 90)
      };
      if (!payload.pairs && payload.tokens.length === 0) {
        setStatus("Add pairs or quick tokens list first.", "fail");
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
        paper_bgcolor: "#0b1220",
        plot_bgcolor: "#0b1220",
        font: {color: "#cbd5e1"}
      }, {displaylogo: false, responsive: true});
      Plotly.newPlot("tvlChart", tvlTraces, {
        title: "TVL dynamics (thousands USD)",
        paper_bgcolor: "#0b1220",
        plot_bgcolor: "#0b1220",
        font: {color: "#cbd5e1"}
      }, {displaylogo: false, responsive: true});

      lastRows = result.rows || [];
      sortKey = "final_income";
      sortDesc = true;
      sortBy("final_income");
    }

    loadFormState();
    attachAutosave();
    loadMeta();
  </script>
</body>
</html>
"""
