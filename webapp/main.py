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
    with JOB_LOCK:
        job = JOBS[job_id]
        job["status"] = "running"
        job["started_at"] = time.time()

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
            _run_subprocess("agent_v3.py", env, req.min_tvl, logs)
            _run_subprocess("agent_v4.py", env, req.min_tvl, logs)
        result = _merge_for_web(token_pairs, exclude_chains=exclude_chains, exclude_suffixes=exclude_suffixes)
        with JOB_LOCK:
            job["status"] = "done"
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
  <title>Uni Fee Cloud MVP</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; }
    .row { margin-bottom: 10px; }
    label { display:inline-block; width: 220px; font-weight: bold; vertical-align: top; }
    input, textarea, select { width: 500px; padding: 6px; }
    textarea { height: 70px; }
    button { padding: 8px 14px; cursor: pointer; }
    .muted { color: #666; font-size: 12px; }
    table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 13px; }
    th, td { border: 1px solid #ddd; padding: 6px; text-align: left; }
    th { background: #f4f4f4; }
    .error-row { background: #fff3f3; }
    .ok-row { background: #f8fff8; }
  </style>
</head>
<body>
  <h2>Pools Analysis</h2>
  <p><a href="/stables">Stables (later)</a> | <a href="/positions">My positions (later)</a></p>

  <div class="row">
    <label>Pairs (manual):</label>
    <textarea id="pairs" placeholder="wbtc,usdt;wbtc,usdc"></textarea>
    <div class="muted">Format: tokenA,tokenB;tokenC,tokenD</div>
  </div>
  <div class="row">
    <label>Quick tokens list:</label>
    <input id="tokens" placeholder="paxg,fluid,wbtc"/>
    <div class="muted">Each token will be paired with quote tokens below.</div>
  </div>
  <div class="row">
    <label>Quote tokens:</label>
    <input id="quotes" value="usdt,usdc,eth"/>
  </div>
  <div class="row">
    <label>Include chains (empty = all):</label>
    <input id="includeChains" placeholder="ethereum,arbitrum-one,polygon"/>
  </div>
  <div class="row">
    <label>Min TVL:</label>
    <input id="minTvl" value="100" type="number"/>
  </div>
  <div class="row">
    <label>History days:</label>
    <input id="days" value="90" type="number"/>
  </div>
  <div class="row">
    <label>Exclude chains:</label>
    <input id="excludeChains" placeholder="base,polygon"/>
  </div>
  <div class="row">
    <label>Exclude pool suffix (last 4):</label>
    <input id="excludeSuffix" placeholder="1a2b,dead"/>
  </div>

  <div class="row">
    <button onclick="runJob()">Run analysis</button>
    <span id="status"></span>
  </div>

  <h3>Charts</h3>
  <div id="feesChart" style="height:400px;"></div>
  <div id="tvlChart" style="height:400px;"></div>

  <h3>Pools table</h3>
  <div id="summary"></div>
  <table id="resultTable"></table>

  <script>
    function splitCSV(v) {
      return (v || "").split(",").map(x => x.trim().toLowerCase()).filter(Boolean);
    }

    async function runJob() {
      document.getElementById("status").innerText = "Starting...";
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
      const r = await fetch("/api/pools/run", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload)
      });
      const data = await r.json();
      if (!r.ok) {
        document.getElementById("status").innerText = "Error: " + (data.detail || "request failed");
        return;
      }
      pollJob(data.job_id);
    }

    async function pollJob(jobId) {
      document.getElementById("status").innerText = "Running...";
      const timer = setInterval(async () => {
        const r = await fetch("/api/jobs/" + jobId);
        const job = await r.json();
        if (job.status === "done") {
          clearInterval(timer);
          document.getElementById("status").innerText = "Done";
          renderResult(job.result);
        } else if (job.status === "failed") {
          clearInterval(timer);
          document.getElementById("status").innerText = "Failed: " + (job.error || "unknown");
          if (job.result && job.result.logs) {
            console.log(job.result.logs.join("\\n\\n"));
          }
        } else {
          document.getElementById("status").innerText = "Status: " + job.status;
        }
      }, 2000);
    }

    function renderResult(result) {
      document.getElementById("summary").innerText =
        `Pairs suffix: ${result.suffix} | Total: ${result.total} | In chart: ${result.chart_pools} | Error fee>3%: ${result.error_pools}`;

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
      Plotly.newPlot("feesChart", feeTraces, {title: "Cumulative Fees"});
      Plotly.newPlot("tvlChart", tvlTraces, {title: "TVL dynamics (thousands USD)"});

      const table = document.getElementById("resultTable");
      let html = "<tr><th>Chain</th><th>Version</th><th>Pair</th><th>Pool</th><th>Fee %</th><th>Cumul $</th><th>TVL</th><th>Status</th></tr>";
      for (const r of result.rows) {
        const cls = r.status === "ok" ? "ok-row" : "error-row";
        html += `<tr class="${cls}"><td>${r.chain}</td><td>${r.version}</td><td>${r.pair}</td><td>${r.pool_id}</td><td>${Number(r.fee_pct).toFixed(2)}</td><td>${Math.round(r.final_income)}</td><td>${Math.round(r.last_tvl)}</td><td>${r.status}</td></tr>`;
      }
      table.innerHTML = html;
    }
  </script>
</body>
</html>
"""
