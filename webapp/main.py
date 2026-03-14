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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from agent_common import load_chart_data_json, pairs_to_filename_suffix
from config import GOLDSKY_ENDPOINTS, TOKEN_ADDRESSES, UNISWAP_V3_SUBGRAPHS, UNISWAP_V4_SUBGRAPHS
from uniswap_client import get_graph_endpoint, graphql_query

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
TOKEN_CATALOG_PATH = DATA_DIR / "token_catalog.json"
CHAIN_CATALOG_PATH = DATA_DIR / "chain_catalog.json"

app = FastAPI(title="Uni Fee Web", version="0.1.0")


@app.on_event("startup")
def _on_startup() -> None:
    _start_catalog_auto_refresh()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    _stop_catalog_auto_refresh()

# Simple in-memory job storage (MVP)
JOBS: dict[str, dict[str, Any]] = {}
JOB_LOCK = threading.Lock()
RUN_LOCK = threading.Lock()  # prevent collisions in shared data/*.json files
CATALOG_REFRESH_INTERVAL_SEC = max(60, int(os.environ.get("CATALOG_REFRESH_INTERVAL_SEC", str(24 * 60 * 60))))
CATALOG_REFRESH_STOP = threading.Event()
CATALOG_REFRESH_THREAD: threading.Thread | None = None


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


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        import json

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_json(path: Path, data: dict[str, Any]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def _load_chain_catalog(refresh: bool = False) -> dict[str, Any]:
    if not refresh:
        cached = _read_json(CHAIN_CATALOG_PATH)
        if cached and isinstance(cached.get("items"), list):
            return cached
    items = _supported_chains()
    out = {"updated_at": _iso_now(), "count": len(items), "items": items}
    _write_json(CHAIN_CATALOG_PATH, out)
    return out


def _fetch_v3_tokens(chain: str, limit: int = 4000) -> list[str]:
    endpoint = get_graph_endpoint(chain, "v3")
    if not endpoint:
        return []
    symbols: list[str] = []
    seen = set()
    skip = 0
    page_size = 1000
    query = """
    query Tokens($skip: Int!) {
      tokens(first: 1000, skip: $skip, orderBy: totalValueLockedUSD, orderDirection: desc) {
        symbol
      }
    }
    """
    while skip < limit:
        data = graphql_query(endpoint, query, {"skip": skip})
        tokens = data.get("data", {}).get("tokens", [])
        if not tokens:
            break
        for t in tokens:
            sym = (t.get("symbol") or "").strip().lower()
            if not sym:
                continue
            if sym not in seen:
                seen.add(sym)
                symbols.append(sym)
        if len(tokens) < page_size:
            break
        skip += page_size
    return symbols


def _load_token_catalog(refresh: bool = False) -> dict[str, Any]:
    if not refresh:
        cached = _read_json(TOKEN_CATALOG_PATH)
        if cached and isinstance(cached.get("items"), list):
            return cached
    by_chain: dict[str, list[str]] = {}
    all_tokens: set[str] = set()
    chains = _supported_chains()
    for c in chains:
        # Token entity is guaranteed in v3 schema; for v4 we still derive from supported list.
        if c not in UNISWAP_V3_SUBGRAPHS and c not in GOLDSKY_ENDPOINTS:
            continue
        try:
            syms = _fetch_v3_tokens(c)
        except Exception:
            syms = []
        if syms:
            by_chain[c] = syms
            all_tokens.update(syms)

    # Fallback to config tokens if endpoint data is limited.
    if not all_tokens:
        all_tokens.update(_supported_tokens())
    out = {
        "updated_at": _iso_now(),
        "count": len(all_tokens),
        "items": sorted(all_tokens),
        "by_chain": by_chain,
    }
    _write_json(TOKEN_CATALOG_PATH, out)
    return out


def _refresh_catalogs_once() -> None:
    """Refresh tokens/chains catalogs and keep app responsive on partial failures."""
    try:
        _load_chain_catalog(refresh=True)
    except Exception as e:
        print(f"[catalog-refresh] chains refresh failed: {e}")
    try:
        _load_token_catalog(refresh=True)
    except Exception as e:
        print(f"[catalog-refresh] tokens refresh failed: {e}")


def _catalog_refresh_loop(interval_sec: int) -> None:
    # Immediate refresh on startup, then every `interval_sec`.
    _refresh_catalogs_once()
    while not CATALOG_REFRESH_STOP.wait(interval_sec):
        _refresh_catalogs_once()


def _start_catalog_auto_refresh() -> None:
    global CATALOG_REFRESH_THREAD
    if CATALOG_REFRESH_THREAD and CATALOG_REFRESH_THREAD.is_alive():
        return
    CATALOG_REFRESH_STOP.clear()
    CATALOG_REFRESH_THREAD = threading.Thread(
        target=_catalog_refresh_loop,
        args=(CATALOG_REFRESH_INTERVAL_SEC,),
        daemon=True,
        name="catalog-auto-refresh",
    )
    CATALOG_REFRESH_THREAD.start()


def _stop_catalog_auto_refresh() -> None:
    CATALOG_REFRESH_STOP.set()


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
    min_fee_pct: float,
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

    def in_fee_range(item: dict) -> bool:
        try:
            pct = float(item.get("fee_pct") or 0)
        except (TypeError, ValueError):
            pct = 0.0
        return min_fee_pct <= pct <= max_fee_pct

    all_items = sorted(merged.items(), key=lambda kv: _final_income(kv[1]), reverse=True)

    rows = []
    chart_series = []
    filtered_out = 0
    for pool_id, v in all_items:
        fees = v.get("fees") or []
        tvl = v.get("tvl") or []
        status = "ok" if in_fee_range(v) else "filtered_fee_range"
        if status != "ok":
            filtered_out += 1
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
        "chart_pools": len(chart_series),
        "error_pools": filtered_out,
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
        result = _merge_for_web(
            token_pairs,
            include_chains=include_chains,
            min_fee_pct=req.min_fee_pct,
            max_fee_pct=req.max_fee_pct,
        )
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
                    "min_fee_pct": req.min_fee_pct,
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
    min_fee_pct: float = 0.0
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
    token_catalog = _load_token_catalog(refresh=False)
    chain_catalog = _load_chain_catalog(refresh=False)
    return {
        "tokens": token_catalog.get("items", []),
        "chains": chain_catalog.get("items", []),
        "token_catalog": {
            "count": token_catalog.get("count", 0),
            "updated_at": token_catalog.get("updated_at"),
        },
        "chain_catalog": {
            "count": chain_catalog.get("count", 0),
            "updated_at": chain_catalog.get("updated_at"),
        },
    }


@app.post("/api/catalog/tokens/review")
def review_tokens() -> dict[str, Any]:
    data = _load_token_catalog(refresh=True)
    return {
        "ok": True,
        "count": data.get("count", 0),
        "updated_at": data.get("updated_at"),
    }


@app.post("/api/catalog/chains/review")
def review_chains() -> dict[str, Any]:
    data = _load_chain_catalog(refresh=True)
    return {
        "ok": True,
        "count": data.get("count", 0),
        "updated_at": data.get("updated_at"),
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
    if req.min_fee_pct < 0 or req.min_fee_pct >= 100:
        raise HTTPException(status_code=400, detail="min_fee_pct must be between 0 and <100")
    if req.max_fee_pct <= 0 or req.max_fee_pct > 100:
        raise HTTPException(status_code=400, detail="max_fee_pct must be between >0 and 100")
    if req.min_fee_pct >= req.max_fee_pct:
        raise HTTPException(status_code=400, detail="min_fee_pct must be lower than max_fee_pct")

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
      --bg: #e2eaf8;
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
      background: linear-gradient(180deg, #d9e3f5 0%, var(--bg) 100%);
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
      padding: 8px;
      font-size: 14px;
    }
    select { appearance: auto; }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      justify-content: flex-end;
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
      grid-template-columns: repeat(4, minmax(120px, 1fr));
      gap: 10px;
    }
    .metric {
      background: #f8fafc;
      border: 1px solid #e2e8f0;
      border-radius: 10px;
      padding: 8px;
    }
    .metric .k {
      color: var(--muted);
      font-size: 12px;
    }
    .metric .v {
      font-size: 18px;
      font-weight: 800;
      margin-top: 1px;
    }
    .charts-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }
    .plot {
      width: 100%;
      min-height: 330px;
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
      padding: 8px;
      color: #334155;
      font-size: 12px;
    }
    .pair-row {
      display: grid;
      grid-template-columns: repeat(3, minmax(180px, 1fr));
      gap: 8px;
    }
    .pair-item { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; }
    .top-line { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 6px; }
    .meta-badge { border: 1px solid #cbd5e1; border-radius: 999px; padding: 4px 10px; font-size: 12px; color: #475569; background: #f8fafc; }
    .small-btn { border: 1px solid #d1d5db; background: #f8fafc; color: #334155; border-radius: 8px; padding: 6px 10px; font-size: 12px; cursor: pointer; font-weight: 600; }
    .color-dot { display: inline-block; width: 12px; height: 12px; border-radius: 50%; border: 1px solid #64748b; }

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
    
    .inline-grid {
      display: grid;
      grid-template-columns: 120px 120px 180px 180px;
      gap: 8px;
    }
    @media (max-width: 980px) {
      .row { grid-template-columns: 1fr; }
      .row label { padding-top: 0; }
      .pair-row { grid-template-columns: 1fr; }
      .inline-grid { grid-template-columns: 1fr 1fr; }
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
            <label title="Select up to 3 pairs for analysis">Pairs</label>
            <div>
              <div class="top-line">
                <button class="small-btn" onclick="reviewTokens()">Review tokens</button>
                <span class="meta-badge" id="tokensMeta">tokens: -</span>
              </div>
              <div class="pair-row">
                <div class="pair-item">
                  <input id="pair1a" list="tokenHints" placeholder="base token"/>
                  <input id="pair1b" list="tokenHints" placeholder="quote token"/>
                </div>
                <div class="pair-item">
                  <input id="pair2a" list="tokenHints" placeholder="base token"/>
                  <input id="pair2b" list="tokenHints" placeholder="quote token"/>
                </div>
                <div class="pair-item">
                  <input id="pair3a" list="tokenHints" placeholder="base token"/>
                  <input id="pair3b" list="tokenHints" placeholder="quote token"/>
                </div>
              </div>
              <datalist id="tokenHints"></datalist>
              <div class="hint">Maximum 3 pairs.</div>
            </div>
          </div>

          <div class="row">
            <label title="Choose chains for analysis">Include chains</label>
            <div>
              <div class="top-line">
                <button class="small-btn" onclick="reviewChains()">Review chains</button>
                <span class="meta-badge" id="chainsMeta">chains: -</span>
              </div>
              <label class="check"><input type="checkbox" id="allChains" checked onchange="toggleAllChains()"> all</label>
              <div class="chain-grid" id="chainChecks"></div>
            </div>
          </div>

          <div class="row">
            <label>Filters</label>
            <div>
              <div class="inline-grid">
                <div>
                  <div class="hint" style="margin-bottom:4px">Min TVL (USD)</div>
                  <input id="minTvl" value="1000" type="number"/>
                </div>
                <div>
                  <div class="hint" style="margin-bottom:4px">History days</div>
                  <input id="days" value="90" type="number"/>
                </div>
                <div>
                  <div class="hint" style="margin-bottom:4px">Exclude pools above X% fee</div>
                  <input id="maxFeePct" value="3" type="number" step="0.1" min="0.1" max="100"/>
                </div>
                <div>
                  <div class="hint" style="margin-bottom:4px">Exclude pools below X% fee</div>
                  <input id="minFeePct" value="0" type="number" step="0.1" min="0" max="99.9"/>
                </div>
              </div>
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
          <div class="metric"><div class="k">Filtered by fee range</div><div class="v" id="mErr">0</div></div>
        </div>
        <details>
          <summary>Latest run logs</summary>
          <pre id="logs">No logs yet.</pre>
        </details>
      </section>

      <section class="card">
        <div class="charts-grid">
          <div id="feesChart" class="plot"></div>
          <div id="tvlChart" class="plot"></div>
        </div>
      </section>

      <section class="card">
        <h3>Pools Table</h3>
        <div class="hint" style="margin-bottom:8px">Sorted by cumulative fee descending by default.</div>
        <div class="table-wrap">
          <table id="resultTable"></table>
        </div>
      </section>
    </div>
  </div>

  <script>
    const SORTABLE = {
      color: (r) => r.pool_id || "",
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
    const FORM_STORAGE_KEY = "uni_fee_form_v3";
    const FIELD_IDS = ["pair1a", "pair1b", "pair2a", "pair2b", "pair3a", "pair3b", "minTvl", "days", "maxFeePct", "minFeePct", "allChains"];
    let availableChains = [];
    let colorMap = {};

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
      const out = [];
      for (let i = 1; i <= 3; i++) {
        const a = (document.getElementById(`pair${i}a`).value || "").trim().toLowerCase();
        const b = (document.getElementById(`pair${i}b`).value || "").trim().toLowerCase();
        if (a && b && a !== b) out.push(`${a},${b}`);
      }
      return out;
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
        ["color", ""], ["chain", "Chain"], ["version", "Version"], ["pair", "Pair"], ["pool_id", "Pool ID"],
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
        const color = colorMap[r.pool_id] || "#94a3b8";
        html += `<tr class="${cls}">`;
        html += `<td><span class="color-dot" style="background:${color}"></span></td>`;
        html += `<td>${r.chain}</td>`;
        html += `<td>${r.version}</td>`;
        html += `<td>${r.pair}</td>`;
        html += `<td class="mono">${r.pool_id}</td>`;
        html += `<td>${Number(r.fee_pct).toFixed(2)}</td>`;
        html += `<td>$${formatUsd(r.final_income)}</td>`;
        html += `<td>$${formatUsd(r.last_tvl)}</td>`;
        html += `<td>${r.status === "ok" ? "ok" : "filtered by fee range"}</td>`;
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
        tokenHints.innerHTML = (meta.tokens || []).slice(0, 5000).map(t => `<option value="${t}"></option>`).join("");
        document.getElementById("tokensMeta").textContent = `tokens: ${meta.token_catalog?.count || 0}, updated: ${meta.token_catalog?.updated_at || "-"}`;

        availableChains = meta.chains || [];
        document.getElementById("chainsMeta").textContent = `chains: ${meta.chain_catalog?.count || 0}, updated: ${meta.chain_catalog?.updated_at || "-"}`;
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

    async function reviewTokens() {
      setStatus("Refreshing token catalog...", "running");
      const r = await fetch("/api/catalog/tokens/review", {method: "POST"});
      if (!r.ok) {
        setStatus("Token refresh failed", "fail");
        return;
      }
      await loadMeta();
      setStatus("Token catalog updated", "ok");
    }

    async function reviewChains() {
      setStatus("Refreshing chain catalog...", "running");
      const r = await fetch("/api/catalog/chains/review", {method: "POST"});
      if (!r.ok) {
        setStatus("Chain refresh failed", "fail");
        return;
      }
      await loadMeta();
      setStatus("Chain catalog updated", "ok");
    }

    async function runJob() {
      const payload = {
        pairs: getSelectedPairs(),
        include_chains: getSelectedChains(),
        min_tvl: Number(document.getElementById("minTvl").value || 0),
        days: Number(document.getElementById("days").value || 90),
        max_fee_pct: Number(document.getElementById("maxFeePct").value || 3),
        min_fee_pct: Number(document.getElementById("minFeePct").value || 0)
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
          setStatus("Completed", "ok");
          renderResult(job.result);
        } else if (job.status === "failed") {
          clearInterval(timer);
          setBusy(false);
          setStatus("Failed: " + (job.error || "unknown"), "fail");
          if (job.result && job.result.logs) {
            document.getElementById("logs").textContent = job.result.logs.join("\\n\\n");
          }
        } else {
          setStatus(job.stage_label || job.status, "running");
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
      colorMap = {};
      const palette = ["#1d4ed8", "#0891b2", "#7c3aed", "#ea580c", "#059669", "#dc2626", "#4f46e5", "#0ea5e9", "#65a30d", "#be185d"];
      for (const s of result.series) {
        const feeX = s.fees.map(p => new Date(p[0] * 1000));
        const feeY = s.fees.map(p => p[1]);
        const tvlX = s.tvl.map(p => new Date(p[0] * 1000));
        const tvlY = s.tvl.map(p => p[1] / 1000.0);
        const c = palette[feeTraces.length % palette.length];
        colorMap[s.pool_id] = c;
        feeTraces.push({x: feeX, y: feeY, mode: "lines", name: s.label, line: {color: c, width: 2}});
        tvlTraces.push({x: tvlX, y: tvlY, mode: "lines", name: s.label, line: {color: c, width: 2}});
      }
      Plotly.newPlot("feesChart", feeTraces, {
        title: "Cumulative Fees",
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#ffffff",
        font: {color: "#0f172a"},
        showlegend: false,
        margin: {t: 34, b: 28, l: 50, r: 16}
      }, {displaylogo: false, responsive: true});
      Plotly.newPlot("tvlChart", tvlTraces, {
        title: "TVL dynamics (thousands USD)",
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#ffffff",
        font: {color: "#0f172a"},
        showlegend: false,
        margin: {t: 34, b: 28, l: 50, r: 16}
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
