#!/usr/bin/env python3
"""
Cloud-ready web MVP for pool analysis.

- Run existing v3/v4 agents in background jobs
- Collect JSON outputs and return merged data for UI
- Show results on-screen (no PDF required)
"""

import os
import re
import secrets
import smtplib
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field
from eth_account import Account
from eth_account.messages import encode_defunct

from agent_common import load_chart_data_json, pairs_to_filename_suffix
from config import GOLDSKY_ENDPOINTS, TOKEN_ADDRESSES, UNISWAP_V3_SUBGRAPHS, UNISWAP_V4_SUBGRAPHS
from uniswap_client import get_graph_endpoint, graphql_query

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)


def _resolve_catalog_dir() -> Path:
    # Highest priority: explicit path from env.
    explicit = os.environ.get("CATALOG_STORAGE_DIR", "").strip()
    if explicit:
        return Path(explicit).expanduser()

    # Render disk mount can be exposed under different env names.
    for env_name in ("RENDER_DISK_PATH", "RENDER_DISK_MOUNT_PATH", "RENDER_DISK_MOUNT"):
        mount = os.environ.get(env_name, "").strip()
        if mount:
            return Path(mount) / "uni_fee_cache"

    # Common Render persistent disk mount point.
    default_render_mount = Path("/var/data")
    if default_render_mount.exists():
        return default_render_mount / "uni_fee_cache"

    # Local/dev fallback.
    return DATA_DIR


CATALOG_DIR = _resolve_catalog_dir()
CATALOG_DIR.mkdir(parents=True, exist_ok=True)
TOKEN_CATALOG_PATH = CATALOG_DIR / "token_catalog.json"
CHAIN_CATALOG_PATH = CATALOG_DIR / "chain_catalog.json"
UNISWAP_TOKEN_LIST_URL = os.environ.get("UNISWAP_TOKEN_LIST_URL", "https://tokens.uniswap.org")
TOKENS_MIN_TVL_USD = float(os.environ.get("TOKENS_MIN_TVL_USD", "1000000"))
CHAIN_ID_TO_NAME = {
    1: "ethereum",
    10: "optimism",
    56: "bnb",
    130: "unichain",
    1301: "unichain",
    137: "polygon",
    324: "zksync",
    8453: "base",
    42161: "arbitrum-one",
    43114: "avalanche",
    81457: "blast",
}

app = FastAPI(title="Uni Fee Web", version="0.1.0")


@app.on_event("startup")
def _on_startup() -> None:
    _start_catalog_auto_refresh()
    _start_analytics()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    _stop_catalog_auto_refresh()
    _stop_analytics()

# Simple in-memory job storage (MVP)
JOBS: dict[str, dict[str, Any]] = {}
JOB_LOCK = threading.Lock()
RUN_LOCK = threading.Lock()  # prevent collisions in shared data/*.json files
RUN_HISTORY: dict[str, list[dict[str, Any]]] = {}
RUN_HISTORY_LIMIT = 10
SESSION_COOKIE_NAME = "uni_fee_sid"
SESSION_TTL_SEC = int(os.environ.get("SESSION_TTL_SEC", str(30 * 24 * 60 * 60)))
CATALOG_REFRESH_INTERVAL_SEC = max(60, int(os.environ.get("CATALOG_REFRESH_INTERVAL_SEC", str(24 * 60 * 60))))
CATALOG_REFRESH_ON_STARTUP = os.environ.get("CATALOG_REFRESH_ON_STARTUP", "0").strip().lower() in ("1", "true", "yes", "on")
CATALOG_REFRESH_STOP = threading.Event()
CATALOG_REFRESH_THREAD: threading.Thread | None = None
AUTH_NONCE_TTL_SEC = int(os.environ.get("AUTH_NONCE_TTL_SEC", "300"))
AUTH_NONCES: dict[str, dict[str, Any]] = {}
AUTH_SESSIONS: dict[str, dict[str, Any]] = {}
AUTH_LOCK = threading.Lock()
ANALYTICS_DB_PATH = Path(os.environ.get("ANALYTICS_DB_PATH", str(CATALOG_DIR / "analytics.sqlite3")))
ANALYTICS_ENABLED = os.environ.get("ANALYTICS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
ANALYTICS_DAILY_EMAIL_ENABLED = os.environ.get("ANALYTICS_DAILY_EMAIL_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
ANALYTICS_REPORT_TO = os.environ.get("ANALYTICS_REPORT_TO", "nagibin@gmail.ru").strip()
ANALYTICS_REPORT_HOUR_UTC = max(0, min(23, int(os.environ.get("ANALYTICS_REPORT_HOUR_UTC", "7"))))
ANALYTICS_SMTP_HOST = os.environ.get("ANALYTICS_SMTP_HOST", "").strip()
ANALYTICS_SMTP_PORT = int(os.environ.get("ANALYTICS_SMTP_PORT", "587"))
ANALYTICS_SMTP_USER = os.environ.get("ANALYTICS_SMTP_USER", "").strip()
ANALYTICS_SMTP_PASS = os.environ.get("ANALYTICS_SMTP_PASS", "").strip()
ANALYTICS_SMTP_FROM = os.environ.get("ANALYTICS_SMTP_FROM", ANALYTICS_SMTP_USER).strip()
ANALYTICS_SMTP_TLS = os.environ.get("ANALYTICS_SMTP_TLS", "1").strip().lower() in ("1", "true", "yes", "on")
ANALYTICS_REPORT_SUBJECT_PREFIX = os.environ.get("ANALYTICS_REPORT_SUBJECT_PREFIX", "Uni Fee analytics").strip()
ANALYTICS_STOP = threading.Event()
ANALYTICS_THREAD: threading.Thread | None = None


def _analytics_conn() -> sqlite3.Connection:
    ANALYTICS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(ANALYTICS_DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_analytics_db() -> None:
    if not ANALYTICS_ENABLED:
        return
    with _analytics_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              session_id TEXT NOT NULL,
              event_type TEXT NOT NULL,
              path TEXT NOT NULL DEFAULT '',
              payload TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_analytics_events_ts ON analytics_events(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_analytics_events_type ON analytics_events(event_type)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics_state (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )
            """
        )
        conn.commit()


def _analytics_set_state(key: str, value: str) -> None:
    if not ANALYTICS_ENABLED:
        return
    with _analytics_conn() as conn:
        conn.execute(
            "INSERT INTO analytics_state(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()


def _analytics_get_state(key: str) -> str:
    if not ANALYTICS_ENABLED:
        return ""
    with _analytics_conn() as conn:
        row = conn.execute("SELECT value FROM analytics_state WHERE key = ?", (key,)).fetchone()
    return str(row[0]) if row else ""


def _analytics_log_event(session_id: str, event_type: str, path: str = "", payload: str = "") -> None:
    if not ANALYTICS_ENABLED:
        return
    safe_sid = session_id if _is_valid_session_id(session_id) else "unknown"
    try:
        with _analytics_conn() as conn:
            conn.execute(
                "INSERT INTO analytics_events(ts, session_id, event_type, path, payload) VALUES(?, ?, ?, ?, ?)",
                (_iso_now(), safe_sid, event_type, (path or "")[:128], (payload or "")[:2000]),
            )
            conn.commit()
    except Exception:
        pass


def _analytics_report_window_utc(day_offset: int = 1) -> tuple[str, str, str]:
    now = datetime.now(timezone.utc)
    report_day = now.date().toordinal() - day_offset
    start = datetime.fromordinal(report_day).replace(tzinfo=timezone.utc)
    end = datetime.fromordinal(report_day + 1).replace(tzinfo=timezone.utc)
    return (
        start.isoformat(timespec="seconds").replace("+00:00", "Z"),
        end.isoformat(timespec="seconds").replace("+00:00", "Z"),
        start.date().isoformat(),
    )


def _analytics_build_daily_report(day_offset: int = 1) -> tuple[str, str]:
    start_ts, end_ts, day_label = _analytics_report_window_utc(day_offset=day_offset)
    if not ANALYTICS_ENABLED:
        return day_label, "Analytics is disabled."
    with _analytics_conn() as conn:
        total_events = conn.execute(
            "SELECT COUNT(*) FROM analytics_events WHERE ts >= ? AND ts < ?",
            (start_ts, end_ts),
        ).fetchone()[0]
        page_views = conn.execute(
            "SELECT COUNT(*) FROM analytics_events WHERE ts >= ? AND ts < ? AND event_type = 'page_view'",
            (start_ts, end_ts),
        ).fetchone()[0]
        visitors = conn.execute(
            "SELECT COUNT(DISTINCT session_id) FROM analytics_events WHERE ts >= ? AND ts < ? AND event_type = 'page_view'",
            (start_ts, end_ts),
        ).fetchone()[0]
        runs_started = conn.execute(
            "SELECT COUNT(*) FROM analytics_events WHERE ts >= ? AND ts < ? AND event_type = 'run_start'",
            (start_ts, end_ts),
        ).fetchone()[0]
        runs_done = conn.execute(
            "SELECT COUNT(*) FROM analytics_events WHERE ts >= ? AND ts < ? AND event_type = 'run_done'",
            (start_ts, end_ts),
        ).fetchone()[0]
        runs_failed = conn.execute(
            "SELECT COUNT(*) FROM analytics_events WHERE ts >= ? AND ts < ? AND event_type = 'run_failed'",
            (start_ts, end_ts),
        ).fetchone()[0]
        top_pages = conn.execute(
            """
            SELECT path, COUNT(*) c
            FROM analytics_events
            WHERE ts >= ? AND ts < ? AND event_type = 'page_view'
            GROUP BY path
            ORDER BY c DESC
            LIMIT 8
            """,
            (start_ts, end_ts),
        ).fetchall()
        top_pairs = conn.execute(
            """
            SELECT payload, COUNT(*) c
            FROM analytics_events
            WHERE ts >= ? AND ts < ? AND event_type = 'run_start'
            GROUP BY payload
            ORDER BY c DESC
            LIMIT 8
            """,
            (start_ts, end_ts),
        ).fetchall()

    lines = [
        f"Uni Fee daily report (UTC): {day_label}",
        "",
        f"Visitors (unique sessions): {visitors}",
        f"Page views: {page_views}",
        f"Run starts: {runs_started}",
        f"Run success: {runs_done}",
        f"Run failed: {runs_failed}",
        f"All tracked events: {total_events}",
        "",
        "Top pages:",
    ]
    if top_pages:
        lines.extend([f"- {path or '/'}: {count}" for path, count in top_pages])
    else:
        lines.append("- no page views")
    lines.append("")
    lines.append("Top requested pairs:")
    if top_pairs:
        lines.extend([f"- {pairs}: {count}" for pairs, count in top_pairs if pairs])
        if not any(pairs for pairs, _ in top_pairs):
            lines.append("- no run requests")
    else:
        lines.append("- no run requests")
    return day_label, "\n".join(lines)


def _send_email_smtp(*, to_email: str, subject: str, body: str) -> tuple[bool, str]:
    if not ANALYTICS_SMTP_HOST or not ANALYTICS_SMTP_USER or not ANALYTICS_SMTP_PASS:
        return False, "SMTP credentials are missing (ANALYTICS_SMTP_HOST/USER/PASS)."
    if not to_email:
        return False, "Recipient email is empty."
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = ANALYTICS_SMTP_FROM or ANALYTICS_SMTP_USER
    msg["To"] = to_email
    msg.set_content(body)
    try:
        with smtplib.SMTP(ANALYTICS_SMTP_HOST, ANALYTICS_SMTP_PORT, timeout=25) as smtp:
            smtp.ehlo()
            if ANALYTICS_SMTP_TLS:
                smtp.starttls()
                smtp.ehlo()
            smtp.login(ANALYTICS_SMTP_USER, ANALYTICS_SMTP_PASS)
            smtp.send_message(msg)
        return True, "sent"
    except Exception as e:
        return False, str(e)


def _send_daily_analytics_report(force: bool = False) -> tuple[bool, str]:
    if not ANALYTICS_ENABLED:
        return False, "Analytics disabled."
    if not ANALYTICS_DAILY_EMAIL_ENABLED and not force:
        return False, "Daily email disabled."
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    if not force:
        if now.hour < ANALYTICS_REPORT_HOUR_UTC:
            return False, "Too early for daily send."
        last_sent = _analytics_get_state("last_daily_email_date_utc")
        if last_sent == today:
            return False, "Already sent today."
    day_label, body = _analytics_build_daily_report(day_offset=1)
    subject = f"{ANALYTICS_REPORT_SUBJECT_PREFIX} | {day_label}"
    ok, info = _send_email_smtp(to_email=ANALYTICS_REPORT_TO, subject=subject, body=body)
    if ok:
        _analytics_set_state("last_daily_email_date_utc", today)
        return True, f"Sent: {subject}"
    return False, info


def _send_test_analytics_email() -> tuple[bool, str]:
    day_label, body = _analytics_build_daily_report(day_offset=0)
    subject = f"{ANALYTICS_REPORT_SUBJECT_PREFIX} TEST | {day_label}"
    return _send_email_smtp(to_email=ANALYTICS_REPORT_TO, subject=subject, body=body)


def _analytics_loop() -> None:
    # Lightweight scheduler: check once per minute.
    while not ANALYTICS_STOP.wait(60):
        try:
            _send_daily_analytics_report(force=False)
        except Exception:
            continue


def _start_analytics() -> None:
    global ANALYTICS_THREAD
    _init_analytics_db()
    if not ANALYTICS_ENABLED:
        return
    if ANALYTICS_THREAD and ANALYTICS_THREAD.is_alive():
        return
    ANALYTICS_STOP.clear()
    ANALYTICS_THREAD = threading.Thread(target=_analytics_loop, daemon=True, name="analytics-daily-email")
    ANALYTICS_THREAD.start()


def _stop_analytics() -> None:
    ANALYTICS_STOP.set()


def _public_base_url(request: Request) -> str:
    env_base = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")
    if env_base:
        return env_base
    return f"{request.url.scheme}://{request.url.netloc}"


def _short_addr(address: str) -> str:
    a = (address or "").strip()
    if len(a) < 12:
        return a
    return f"{a[:6]}...{a[-4:]}"


def _is_eth_address(value: str) -> bool:
    return bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", (value or "").strip()))


def _new_nonce() -> str:
    return secrets.token_urlsafe(18)


def _siwe_message(*, domain: str, uri: str, address: str, chain_id: int, nonce: str, issued_at: str) -> str:
    return (
        f"{domain} wants you to sign in with your Ethereum account:\n"
        f"{address}\n\n"
        "Sign in to Uni Fee.\n\n"
        f"URI: {uri}\n"
        "Version: 1\n"
        f"Chain ID: {chain_id}\n"
        f"Nonce: {nonce}\n"
        f"Issued At: {issued_at}"
    )


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


def _new_session_id() -> str:
    return secrets.token_urlsafe(24)


def _is_valid_session_id(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{16,128}", value or ""))


def _ensure_session_cookie(request: Request, response: Response) -> str:
    sid = request.cookies.get(SESSION_COOKIE_NAME, "")
    if _is_valid_session_id(sid):
        return sid
    sid = _new_session_id()
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=sid,
        max_age=SESSION_TTL_SEC,
        httponly=True,
        samesite="lax",
        secure=(request.url.scheme == "https"),
        path="/",
    )
    return sid


def _load_chain_catalog(refresh: bool = False) -> dict[str, Any]:
    if not refresh:
        cached = _read_json(CHAIN_CATALOG_PATH)
        if cached and isinstance(cached.get("items"), list):
            return cached
    items = _supported_chains()
    out = {"updated_at": _iso_now(), "count": len(items), "items": items}
    _write_json(CHAIN_CATALOG_PATH, out)
    return out


def _is_clean_symbol(symbol: str) -> bool:
    # keep only plain alnum symbols to avoid spam/malicious names
    return bool(re.fullmatch(r"[A-Za-z0-9]{2,20}", symbol))


def _fetch_uniswap_verified_tokens() -> tuple[set[str], dict[str, list[str]]]:
    import json

    with urlopen(UNISWAP_TOKEN_LIST_URL, timeout=20) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    supported = set(_supported_chains())
    by_chain_set: dict[str, set[str]] = {}
    all_tokens: set[str] = set()
    for item in payload.get("tokens", []):
        symbol = str(item.get("symbol") or "").strip()
        if not _is_clean_symbol(symbol):
            continue
        chain_name = CHAIN_ID_TO_NAME.get(int(item.get("chainId") or 0))
        if not chain_name or chain_name not in supported:
            continue
        sym = symbol.lower()
        by_chain_set.setdefault(chain_name, set()).add(sym)
        all_tokens.add(sym)

    by_chain = {k: sorted(v) for k, v in by_chain_set.items()}
    return all_tokens, by_chain


def _fetch_tokens_by_tvl_endpoint(endpoint: str, min_tvl_usd: float) -> set[str]:
    query = """
    query TokenListByTvl($minTvl: BigDecimal!, $skip: Int!) {
      tokens(
        first: 1000,
        skip: $skip,
        orderBy: totalValueLockedUSD,
        orderDirection: desc,
        where: { totalValueLockedUSD_gte: $minTvl }
      ) {
        symbol
      }
    }
    """
    out: set[str] = set()
    skip = 0
    while skip < 10000:
        data = graphql_query(endpoint, query, {"minTvl": str(min_tvl_usd), "skip": skip})
        rows = data.get("data", {}).get("tokens", [])
        if not rows:
            break
        for row in rows:
            sym = str(row.get("symbol") or "").strip()
            if _is_clean_symbol(sym):
                out.add(sym.lower())
        if len(rows) < 1000:
            break
        skip += 1000
    return out


def _fetch_tokens_by_chain_tvl(min_tvl_usd: float) -> tuple[set[str], dict[str, list[str]]]:
    all_tokens: set[str] = set()
    by_chain: dict[str, set[str]] = {}
    for chain in _supported_chains():
        chain_set: set[str] = set()
        for version in ("v3", "v4"):
            endpoint = get_graph_endpoint(chain, version)
            if not endpoint:
                continue
            try:
                chain_set.update(_fetch_tokens_by_tvl_endpoint(endpoint, min_tvl_usd))
            except Exception:
                continue
        if chain_set:
            by_chain[chain] = chain_set
            all_tokens.update(chain_set)
    return all_tokens, {k: sorted(v) for k, v in by_chain.items()}


def _load_token_catalog(refresh: bool = False) -> dict[str, Any]:
    cached = _read_json(TOKEN_CATALOG_PATH)
    if not refresh and cached and isinstance(cached.get("items"), list):
        try:
            cached_min_tvl = float(cached.get("min_tvl_usd") or 0)
        except (TypeError, ValueError):
            cached_min_tvl = 0.0
        # Rebuild cache automatically when threshold changed (e.g. 10k -> 1M).
        if abs(cached_min_tvl - TOKENS_MIN_TVL_USD) < 1e-9:
            return cached

    by_chain: dict[str, list[str]] = {}
    all_tokens: set[str] = set()
    source = "tokens-tvl-threshold"
    try:
        all_tokens, by_chain = _fetch_tokens_by_chain_tvl(TOKENS_MIN_TVL_USD)
        try:
            verified_all, verified_by_chain = _fetch_uniswap_verified_tokens()
            if verified_all:
                all_tokens = {t for t in all_tokens if t in verified_all}
                filtered_by_chain: dict[str, list[str]] = {}
                for chain, items in by_chain.items():
                    allowed = set(verified_by_chain.get(chain, []))
                    keep = [t for t in items if t in allowed]
                    if keep:
                        filtered_by_chain[chain] = keep
                by_chain = filtered_by_chain
                source = "tokens-tvl-threshold+verified"
        except Exception:
            pass
    except Exception as e:
        print(f"[catalog-refresh] active pools tokens fetch failed: {e}")

    # If live refresh fails, keep previously cached catalog (avoid shrinking to tiny fallback sets).
    if not all_tokens and cached and isinstance(cached.get("items"), list):
        return cached

    # Last-resort fallback.
    if not all_tokens:
        source = "local-config-fallback"
        all_tokens.update(_supported_tokens())
    out = {
        "updated_at": _iso_now(),
        "count": len(all_tokens),
        "items": sorted(all_tokens),
        "by_chain": by_chain,
        "source": source,
        "min_tvl_usd": TOKENS_MIN_TVL_USD,
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


def _catalog_stale(path: Path, max_age_sec: int) -> bool:
    cached = _read_json(path)
    if not cached:
        return True
    updated_at_raw = str(cached.get("updated_at") or "").strip()
    if not updated_at_raw:
        return True
    try:
        updated_at = datetime.fromisoformat(updated_at_raw.replace("Z", "+00:00"))
    except ValueError:
        return True
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    age_sec = (datetime.now(timezone.utc) - updated_at.astimezone(timezone.utc)).total_seconds()
    return age_sec >= max_age_sec


def _catalog_refresh_loop(interval_sec: int, run_on_startup: bool) -> None:
    # Optional refresh on startup, but only when cache is missing/stale.
    if run_on_startup and (
        _catalog_stale(TOKEN_CATALOG_PATH, interval_sec) or _catalog_stale(CHAIN_CATALOG_PATH, interval_sec)
    ):
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
        args=(CATALOG_REFRESH_INTERVAL_SEC, CATALOG_REFRESH_ON_STARTUP),
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
    exclude_suffixes: list[str],
    merged_override: dict[str, dict] | None = None,
) -> dict[str, Any]:
    suffix = pairs_to_filename_suffix(token_pairs)
    if merged_override is None:
        v3_path = DATA_DIR / f"pools_v3_{suffix}.json"
        v4_path = DATA_DIR / f"pools_v4_{suffix}.json"
        v3_data = load_chart_data_json(str(v3_path))
        v4_data = load_chart_data_json(str(v4_path))
        merged: dict[str, dict] = {}
        merged.update(v3_data)
        merged.update(v4_data)
    else:
        merged = dict(merged_override)

    in_chains = {x.strip().lower() for x in include_chains if x.strip()}
    if in_chains:
        merged = {k: v for k, v in merged.items() if v.get("chain", "").lower() in in_chains}

    suffix_set = {str(s).strip().lower().replace("0x", "")[-4:] for s in exclude_suffixes if str(s).strip()}

    def in_fee_range(item: dict) -> bool:
        try:
            pct = float(item.get("fee_pct") or 0)
        except (TypeError, ValueError):
            pct = 0.0
        return min_fee_pct <= pct <= max_fee_pct

    def excluded_by_suffix(item: dict, pid: str) -> bool:
        if not suffix_set:
            return False
        pool_id = str(item.get("pool_id") or pid or "").lower().replace("0x", "")
        return any(pool_id.endswith(suf) for suf in suffix_set)

    all_items = sorted(merged.items(), key=lambda kv: _final_income(kv[1]), reverse=True)

    rows = []
    chart_series = []
    filtered_out = 0
    default_visible = 0
    for pool_id, v in all_items:
        fees = v.get("fees") or []
        tvl = v.get("tvl") or []
        if excluded_by_suffix(v, pool_id):
            status = "filtered_suffix"
        elif in_fee_range(v):
            status = "ok"
        else:
            status = "filtered_fee_range"
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
        if fees or tvl:
            chart_series.append(
                {
                    "label": f"{row['chain']} {row['version']} {row['pair']} ...{row['pool_id'][-4:]}",
                    "chain": row["chain"],
                    "version": row["version"],
                    "pair": row["pair"],
                    "fee_pct": row["fee_pct"],
                    "status": status,
                    "pool_id": row["pool_id"],
                    "fees": fees,
                    "tvl": tvl,
                }
            )
            if status == "ok":
                default_visible += 1

    return {
        "suffix": suffix,
        "total": len(merged),
        "chart_pools": default_visible,
        "error_pools": filtered_out,
        "rows": rows,
        "series": chart_series,
    }


def _canonical_token_symbol(sym: str) -> str:
    s = (sym or "").strip().lower()
    if s in {"eth", "weth", "weth.e", "weth9"}:
        return "eth"
    return s


def _requested_pair_key(a: str, b: str) -> tuple[str, str]:
    x = _canonical_token_symbol(a)
    y = _canonical_token_symbol(b)
    return tuple(sorted((x, y)))


def _pair_label_key(pair_label: str) -> tuple[str, str] | None:
    if "/" not in (pair_label or ""):
        return None
    a, b = pair_label.split("/", 1)
    return _requested_pair_key(a, b)


def _run_subprocess(script_name: str, env: dict[str, str], min_tvl: float, logs: list[str]) -> None:
    cmd = [sys.executable, str(BASE_DIR / script_name), "--min-tvl", str(min_tvl)]
    timeout_sec = int(env.get("AGENT_TIMEOUT_SEC", "480"))
    logs.append(f"$ {' '.join(cmd)}  # timeout={timeout_sec}s")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout_sec,
        )
        if proc.stdout:
            logs.append(proc.stdout[-4000:])
        if proc.stderr:
            logs.append(proc.stderr[-4000:])
        if proc.returncode != 0:
            raise RuntimeError(f"{script_name} failed with code {proc.returncode}")
    except subprocess.TimeoutExpired as e:
        if e.stdout:
            logs.append(str(e.stdout)[-4000:])
        if e.stderr:
            logs.append(str(e.stderr)[-4000:])
        raise RuntimeError(
            f"{script_name} timed out after {timeout_sec}s. "
            "Try fewer chains or smaller history window."
        )


def _push_run_history(
    *,
    session_id: str,
    status: str,
    token_pairs: str,
    include_chains: list[str],
    min_tvl: float,
    days: int,
    include_versions: list[str],
    min_fee_pct: float,
    max_fee_pct: float,
    exclude_suffixes: list[str],
    logs: list[str],
    error: str | None = None,
) -> None:
    item = {
        "ts": _iso_now(),
        "status": status,
        "request": {
            "pairs": token_pairs,
            "include_chains": include_chains,
            "min_tvl": min_tvl,
            "days": days,
            "include_versions": include_versions,
            "min_fee_pct": min_fee_pct,
            "max_fee_pct": max_fee_pct,
            "exclude_suffixes": exclude_suffixes,
        },
        "error": error or "",
        "logs": logs[-8:],
    }
    with JOB_LOCK:
        bucket = RUN_HISTORY.setdefault(session_id, [])
        bucket.insert(0, item)
        del bucket[RUN_HISTORY_LIMIT:]
    _analytics_log_event(
        session_id=session_id,
        event_type="run_done" if status == "done" else "run_failed",
        path="/api/pools/run",
        payload=token_pairs,
    )


def _run_pool_job(job_id: str, req: "PoolsRunRequest", session_id: str) -> None:
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

    # Build up to 4 selected token pairs
    pairs = []
    for pair in req.pairs[:4]:
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
    requested_pair_keys = {_requested_pair_key(a, b) for a, b in pairs}

    include_chains = [c.strip().lower() for c in req.include_chains if c.strip()]
    include_versions = [v.strip().lower() for v in req.include_versions if v.strip()]
    run_v3 = "v3" in include_versions
    run_v4 = "v4" in include_versions

    logs: list[str] = []
    env = os.environ.copy()
    env["TOKEN_PAIRS"] = token_pairs
    env["FEE_DAYS"] = str(req.days)
    env["INCLUDE_CHAINS"] = ",".join(include_chains)
    env["DISABLE_PDF_OUTPUT"] = "1"
    env["GRAPHQL_RETRIES"] = os.environ.get("WEB_GRAPHQL_RETRIES", "1")

    # v4 endpoint config uses this list at import-time
    if run_v4 and include_chains:
        v4_supported = {c for c in include_chains if c in UNISWAP_V4_SUBGRAPHS}
        env["V4_CHAINS"] = ",".join(sorted(v4_supported))

    try:
        merged_raw: dict[str, dict] = {}
        with RUN_LOCK:
            total_pairs = max(1, len(pairs))
            for idx, (a, b) in enumerate(pairs, start=1):
                pair_str = f"{a},{b}"
                pair_suffix = pairs_to_filename_suffix(pair_str)
                env["TOKEN_PAIRS"] = pair_str

                base_progress = int(10 + (idx - 1) * (65 / total_pairs))
                if run_v3:
                    _set_stage("v3", f"Running v3 ({idx}/{total_pairs}): {pair_str}", min(70, base_progress + 10))
                    _run_subprocess("agent_v3.py", env, req.min_tvl, logs)
                    v3_path = DATA_DIR / f"pools_v3_{pair_suffix}.json"
                    merged_raw.update(load_chart_data_json(str(v3_path)))
                if run_v4:
                    _set_stage("v4", f"Running v4 ({idx}/{total_pairs}): {pair_str}", min(78, base_progress + 20))
                    _run_subprocess("agent_v4.py", env, req.min_tvl, logs)
                    v4_path = DATA_DIR / f"pools_v4_{pair_suffix}.json"
                    merged_raw.update(load_chart_data_json(str(v4_path)))

        # Keep only pools that really match requested pairs.
        # Protects against occasional cross-token resolution artifacts (e.g. POL instead of ETH).
        if requested_pair_keys:
            merged_raw = {
                k: v
                for k, v in merged_raw.items()
                if (_pair_label_key(str(v.get("pair") or "")) in requested_pair_keys)
            }

        _set_stage("merge", "Merging results for web", 85)
        result = _merge_for_web(
            token_pairs,
            include_chains=include_chains,
            min_fee_pct=req.min_fee_pct,
            max_fee_pct=req.max_fee_pct,
            exclude_suffixes=req.exclude_suffixes,
            merged_override=merged_raw,
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
                    "include_versions": include_versions,
                    "min_fee_pct": req.min_fee_pct,
                    "max_fee_pct": req.max_fee_pct,
                    "exclude_suffixes": req.exclude_suffixes,
                    "lp_allocation_usd": float(env.get("LP_ALLOCATION_USD", "1000")),
                },
                **result,
                "logs": logs[-8:],
            }
        _push_run_history(
            session_id=session_id,
            status="done",
            token_pairs=token_pairs,
            include_chains=include_chains,
            min_tvl=req.min_tvl,
            days=req.days,
            include_versions=include_versions,
            min_fee_pct=req.min_fee_pct,
            max_fee_pct=req.max_fee_pct,
            exclude_suffixes=req.exclude_suffixes,
            logs=logs,
        )
    except Exception as e:
        with JOB_LOCK:
            job["status"] = "failed"
            job["error"] = str(e)
            job["stage"] = "failed"
            job["stage_label"] = "Failed"
            job["progress"] = 100
            job["result"] = {"logs": logs[-8:]}
        _push_run_history(
            session_id=session_id,
            status="failed",
            token_pairs=token_pairs,
            include_chains=include_chains,
            min_tvl=req.min_tvl,
            days=req.days,
            include_versions=include_versions,
            min_fee_pct=req.min_fee_pct,
            max_fee_pct=req.max_fee_pct,
            exclude_suffixes=req.exclude_suffixes,
            logs=logs,
            error=str(e),
        )
    finally:
        with JOB_LOCK:
            job["finished_at"] = time.time()


class PoolsRunRequest(BaseModel):
    pairs: list[str] = Field(default_factory=list, description="Up to 4 pairs: tokenA,tokenB")
    include_chains: list[str] = Field(default_factory=list)
    include_versions: list[str] = Field(default_factory=lambda: ["v3", "v4"])
    min_tvl: float = 1000.0
    days: int = 30
    min_fee_pct: float = 0.0
    max_fee_pct: float = 2.0
    exclude_suffixes: list[str] = Field(default_factory=list, description="Exclude pool ids by last 4 chars")


class AuthNonceRequest(BaseModel):
    address: str
    chain_id: int = 1
    wallet: str = "injected"


class AuthVerifyRequest(BaseModel):
    address: str
    chain_id: int = 1
    wallet: str = "injected"
    message: str
    signature: str


INTENT_OPTIONS: list[tuple[str, str]] = [
    ("/", "Find the best pool on Uniswap"),
    ("/pancake", "Find the best pool on PancakeSwap"),
    ("/stables", "Find the best stablecoin yield"),
    ("/positions", "Analise my DeFi positions"),
    ("/help", "Get help"),
]


def _intent_options_html(selected_path: str) -> str:
    rows: list[str] = []
    for path, label in INTENT_OPTIONS:
        sel = " selected" if path == selected_path else ""
        rows.append(f'<option value="{path}"{sel}>{label}</option>')
    return "\n".join(rows)


def _render_placeholder_page(page_title: str, subtitle: str, selected_path: str) -> str:
    options_html = _intent_options_html(selected_path)
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Uni Fee - {page_title}</title>
  <style>
    body {{
      margin: 0;
      font-family: Inter, Arial, sans-serif;
      background: linear-gradient(180deg, #d9e3f5 0%, #ecf2ff 100%);
      color: #0f172a;
    }}
    .container {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 18px;
    }}
    .header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
    }}
    .title {{
      margin: 0;
      font-size: 30px;
      font-weight: 800;
      letter-spacing: 0.2px;
    }}
    .subtitle {{
      margin: 4px 0 0;
      color: #64748b;
      font-size: 14px;
    }}
    .top-controls {{
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: flex-end;
      flex-wrap: nowrap;
    }}
    .intent-prefix {{
      font-size: 14px;
      font-weight: 700;
      color: #334155;
      white-space: nowrap;
    }}
    .intent-select {{
      border: 1px solid #cbd5e1;
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 14px;
      color: #334155;
      background: #f8fbff;
      min-width: 260px;
      max-width: 300px;
    }}
    .connect-btn {{
      border: 1px solid #bfdbfe;
      border-radius: 10px;
      padding: 10px 16px;
      font-size: 14px;
      font-weight: 700;
      color: #1d4ed8;
      background: #eff6ff;
      cursor: pointer;
      white-space: nowrap;
    }}
    .card {{
      background: #f3f7ff;
      border: 1px solid #cfdcec;
      border-radius: 14px;
      padding: 18px;
      box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06);
    }}
    .card h2 {{
      margin: 0 0 8px;
      font-size: 22px;
    }}
    .hint {{
      color: #475569;
      margin: 0;
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div>
        <h1 class="title">Pools Analysis</h1>
        <p class="subtitle">Uniswap v3/v4 screening with on-screen charts, filtering and ranked pool table.</p>
      </div>
      <div class="top-controls">
        <span class="intent-prefix">I want to</span>
        <select class="intent-select" id="intentSelect" onchange="navigateIntent(this.value)">
          {options_html}
        </select>
        <button class="connect-btn" onclick="window.location.href='/connect'">Connect Wallet</button>
      </div>
    </div>
    <section class="card">
      <h2>{page_title}</h2>
      <p class="hint">{subtitle}</p>
    </section>
  </div>
  <script>
    function navigateIntent(path) {{
      if (!path) return;
      window.location.href = path;
    }}
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    resp = HTMLResponse(HTML_PAGE)
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/")
    return resp


@app.get("/stables", response_class=HTMLResponse)
def stables_page(request: Request) -> HTMLResponse:
    resp = HTMLResponse(_render_placeholder_page("Lending Stablecoin", "This page is a placeholder for the future stablecoin workflow.", "/stables"))
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/stables")
    return resp


@app.get("/positions", response_class=HTMLResponse)
def positions_page(request: Request) -> HTMLResponse:
    resp = HTMLResponse(_render_placeholder_page("DeFi Positions", "This page is a placeholder for your positions dashboard.", "/positions"))
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/positions")
    return resp


@app.get("/pancake", response_class=HTMLResponse)
def pancake_page(request: Request) -> HTMLResponse:
    resp = HTMLResponse(_render_placeholder_page("Pancake Pool Finder", "This page is a placeholder for Pancake pool analysis.", "/pancake"))
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/pancake")
    return resp


@app.get("/help", response_class=HTMLResponse)
def help_page(request: Request) -> HTMLResponse:
    resp = HTMLResponse(_render_placeholder_page("Get a Hand", "This page is a placeholder for guided help.", "/help"))
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/help")
    return resp


@app.get("/connect", response_class=HTMLResponse)
def connect_page(request: Request) -> HTMLResponse:
    resp = HTMLResponse(_render_placeholder_page("Connect", "Connect is a placeholder for wallet/account integration.", ""))
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/connect")
    return resp


@app.post("/api/analytics/send-test")
def send_test_analytics_email() -> dict[str, Any]:
    ok, info = _send_test_analytics_email()
    return {"ok": ok, "info": info, "to": ANALYTICS_REPORT_TO}


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
            "source": token_catalog.get("source", ""),
            "min_tvl_usd": token_catalog.get("min_tvl_usd", TOKENS_MIN_TVL_USD),
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


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt(request: Request) -> str:
    base = _public_base_url(request)
    return "\n".join(
        [
            "User-agent: *",
            "Allow: /",
            "Disallow: /api/",
            f"Sitemap: {base}/sitemap.xml",
            "",
        ]
    )


@app.get("/sitemap.xml")
def sitemap_xml(request: Request) -> Response:
    base = _public_base_url(request)
    urls = ["/", "/pancake", "/stables", "/positions", "/help", "/connect"]
    now = datetime.now(timezone.utc).date().isoformat()
    body = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path in urls:
        body.extend(
            [
                "  <url>",
                f"    <loc>{base}{path}</loc>",
                f"    <lastmod>{now}</lastmod>",
                "    <changefreq>daily</changefreq>",
                "  </url>",
            ]
        )
    body.append("</urlset>")
    return Response(content="\n".join(body), media_type="application/xml")


@app.post("/api/pools/run")
def run_pools(req: PoolsRunRequest, request: Request, response: Response) -> dict[str, str]:
    session_id = _ensure_session_cookie(request, response)
    if not os.environ.get("THE_GRAPH_API_KEY"):
        raise HTTPException(status_code=400, detail="Missing THE_GRAPH_API_KEY on server.")
    if req.days < 1 or req.days > 3650:
        raise HTTPException(status_code=400, detail="days must be an integer between 1 and 3650")
    if req.min_tvl < 0 or req.min_tvl > 10_000_000:
        raise HTTPException(status_code=400, detail="min_tvl must be in range 0..10000000")
    if req.min_fee_pct < 0 or req.min_fee_pct > 1:
        raise HTTPException(status_code=400, detail="min_fee_pct must be in range 0..1")
    if req.max_fee_pct < 1 or req.max_fee_pct > 3:
        raise HTTPException(status_code=400, detail="max_fee_pct must be in range 1..3")
    if req.min_fee_pct >= req.max_fee_pct:
        raise HTTPException(status_code=400, detail="min_fee_pct must be lower than max_fee_pct")
    req.include_versions = [str(v).strip().lower() for v in (req.include_versions or []) if str(v).strip()]
    req.include_versions = [v for v in req.include_versions if v in {"v3", "v4"}]
    req.include_versions = list(dict.fromkeys(req.include_versions))
    if not req.include_versions:
        raise HTTPException(status_code=400, detail="Select at least one protocol version (v3 or v4)")
    req.exclude_suffixes = [
        str(x).strip().lower().replace("0x", "")[-4:]
        for x in req.exclude_suffixes
        if str(x).strip()
    ]
    _analytics_log_event(
        session_id=session_id,
        event_type="run_start",
        path="/api/pools/run",
        payload=_pairs_to_string(_parse_pairs_str(";".join(req.pairs))),
    )

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
    t = threading.Thread(target=_run_pool_job, args=(job_id, req, session_id), daemon=True)
    t.start()
    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str) -> dict[str, Any]:
    with JOB_LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/api/runs/recent")
def recent_runs(request: Request, response: Response) -> dict[str, Any]:
    session_id = _ensure_session_cookie(request, response)
    with JOB_LOCK:
        items = list(RUN_HISTORY.get(session_id, []))
    return {"items": items}


@app.post("/api/runs/reset")
def reset_runs(request: Request, response: Response) -> dict[str, Any]:
    session_id = _ensure_session_cookie(request, response)
    with JOB_LOCK:
        RUN_HISTORY.pop(session_id, None)
    return {"ok": True}


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
      --card: #f4f7fc;
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
    .top-controls {
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: flex-end;
      flex-wrap: nowrap;
    }
    .intent-prefix {
      font-size: 14px;
      font-weight: 700;
      color: #334155;
      white-space: nowrap;
    }
    .intent-select {
      border: 1px solid #cbd5e1;
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 14px;
      color: #334155;
      background: #f8fbff;
      min-width: 260px;
      max-width: 300px;
    }
    .connect-btn {
      border: 1px solid #bfdbfe;
      border-radius: 10px;
      padding: 10px 16px;
      font-size: 14px;
      font-weight: 700;
      color: #1d4ed8;
      background: #eff6ff;
      cursor: pointer;
      white-space: nowrap;
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
    .control-card {
      background: linear-gradient(180deg, #f4f8ff 0%, #eef4ff 100%);
      border-color: #cfdcec;
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
    .control-card .form-grid { gap: 10px; }
    .row {
      display: grid;
      grid-template-columns: 180px 1fr;
      gap: 12px;
      align-items: start;
    }
    .control-card .row {
      background: #f8fbff;
      border: 1px solid #d7e1ef;
      border-radius: 12px;
      padding: 10px 12px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7);
    }
    .row label {
      font-weight: 700;
      font-size: 14px;
      padding-top: 8px;
    }
    .control-card .row label {
      color: #1f2f4a;
      letter-spacing: 0.2px;
    }
    .row .hint {
      color: var(--muted);
      font-size: 12px;
      margin-top: 5px;
    }
    input, textarea, select {
      width: 100%;
      background: #f8fbff;
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
      align-items: center;
      justify-content: space-between;
    }
    .actions-left, .actions-right {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    .control-card .actions {
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px solid #d7e1ef;
    }
    .actions .run-btn { order: 99; }
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
      display: inline-block;
      width: 260px;
      text-align: right;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .status.running { color: var(--warn); }
    .status.ok { color: var(--ok); }
    .status.fail { color: var(--danger); }
    .progress-wrap { width: 100%; margin-top: 10px; }
    .control-card .progress-wrap {
      margin-top: 8px;
      padding: 8px 10px 0;
      border-top: 1px dashed #d7e1ef;
    }
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
    .summary-strip {
      display: flex;
      gap: 8px;
      flex-wrap: nowrap;
      align-items: stretch;
      width: 100%;
      overflow-x: auto;
      padding-bottom: 2px;
    }
    .summary-box {
      flex: 1 1 0;
      min-width: 0;
      background: #f8fbff;
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      padding: 8px 10px;
      height: 56px;
      display: flex;
      flex-direction: column;
      justify-content: center;
    }
    .summary-box .k {
      color: #64748b;
      font-size: 11px;
      line-height: 1.1;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .summary-box .v {
      font-size: 17px;
      font-weight: 800;
      line-height: 1.1;
      margin-top: 2px;
      color: #0f172a;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
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
      background: #f8fbff;
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
    .pair-row { display: grid; grid-template-columns: repeat(4, minmax(160px, 1fr)); gap: 6px; }
    .pair-item {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0;
      border: 1px solid #cbd5e1;
      border-radius: 10px;
      overflow: hidden;
      background: #eef2f7;
    }
    .token-input-wrap { display: flex; align-items: center; gap: 0; }
    .token-input-wrap:first-child { border-right: 1px solid #cbd5e1; }
    .token-input-wrap input {
      border: 0;
      border-radius: 0;
      background: #f8fbff;
    }
    /* Hide native datalist indicator to avoid double arrows with custom button */
    .token-input-wrap input[list]::-webkit-calendar-picker-indicator { display: none !important; }
    .token-input-wrap .dd-btn {
      border: 0;
      background: #eef2f7;
      color: #334155;
      height: 100%;
      min-width: 32px;
      cursor: pointer;
      font-size: 12px;
      font-weight: 700;
    }
    .invalid-input {
      border: 1px solid #ef4444 !important;
      box-shadow: 0 0 0 2px rgba(239, 68, 68, 0.18);
    }
    .top-line { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }
    .meta-badge { border: 1px solid #cbd5e1; border-radius: 999px; padding: 4px 10px; font-size: 12px; color: #475569; background: #f8fafc; }
    .info-chip {
      border: 1px solid #cbd5e1;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 12px;
      color: #475569;
      background: #f1f5f9;
      white-space: nowrap;
    }
    .small-btn { border: 1px solid #d1d5db; background: #f8fafc; color: #334155; border-radius: 8px; padding: 6px 10px; font-size: 12px; cursor: pointer; font-weight: 600; }
    .line-swatch {
      display: inline-block;
      width: 28px;
      height: 0;
      border-top-width: 3px;
      border-top-style: solid;
      border-top-color: #64748b;
      vertical-align: middle;
    }

    .chain-grid {
      display: grid;
      grid-template-rows: repeat(2, minmax(22px, auto));
      grid-auto-flow: column;
      grid-auto-columns: minmax(110px, max-content);
      gap: 4px 8px;
      margin-top: 2px;
    }
    .check {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 12px;
      color: #334155;
    }
    .check input {
      width: auto;
      padding: 0;
    }
    
    .inline-grid { display: grid; grid-template-columns: 90px 90px 120px 120px 220px 130px; gap: 6px; align-items: end; }
    .filter-item .hint {
      margin-bottom: 4px !important;
      min-height: 34px;
      display: flex;
      align-items: flex-end;
      line-height: 1.15;
      white-space: normal;
    }
    .proto-checks {
      display: flex;
      gap: 10px;
      align-items: center;
      height: 34px;
      padding: 0 4px;
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      background: #f8fbff;
    }
    .proto-checks label {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      font-size: 12px;
      font-weight: 600;
      color: #334155;
      padding-top: 0;
      line-height: 1;
      white-space: nowrap;
    }
    .proto-checks input[type="checkbox"] {
      width: auto;
      min-width: 0;
      padding: 0;
      margin: 0;
      flex: 0 0 auto;
      accent-color: #2563eb;
      transform: translateY(-1px);
    }
    @media (max-width: 980px) {
      .row { grid-template-columns: 1fr; }
      .row label { padding-top: 0; }
      .pair-row { grid-template-columns: 1fr 1fr; }
      .inline-grid { grid-template-columns: 1fr 1fr; }
      .summary-strip { gap: 6px; }
      .summary-box { min-width: 150px; }
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
      <div class="top-controls">
        <span class="intent-prefix">I want to</span>
        <select class="intent-select" id="intentSelect" onchange="navigateIntent(this.value)">
          <option value="/" selected>Find the best pool on Uniswap</option>
          <option value="/pancake">Find the best pool on PancakeSwap</option>
          <option value="/stables">Find the best stablecoin yield</option>
          <option value="/positions">Analise my DeFi positions</option>
          <option value="/help">Get help</option>
        </select>
        <button class="connect-btn" onclick="window.location.href='/connect'">Connect Wallet</button>
      </div>
    </div>

    <div class="grid">
      <section class="card control-card">
        <div class="form-grid">
          <div class="row">
            <label title="Select up to 4 pairs for analysis">Pairs</label>
            <div>
              <div class="top-line">
                <button class="small-btn" onclick="addPairRow()">+ pair</button>
                <button class="small-btn" id="removePairBtn" onclick="removePairRow()">- pair</button>
                <span class="meta-badge" id="tokensMeta">popular tokens: -</span>
                <span class="info-chip">Popular list only - manual token input supported</span>
              </div>
              <div class="pair-row" id="pairRows">
                <div class="pair-item" id="pairRow1">
                  <div class="token-input-wrap"><input id="pair1a" list="tokenHints" placeholder="base token" value="usdt"/><button class="dd-btn" onclick="openTokenList('pair1a')">▼</button></div>
                  <div class="token-input-wrap"><input id="pair1b" list="tokenHints" placeholder="quote token" value="usdc"/><button class="dd-btn" onclick="openTokenList('pair1b')">▼</button></div>
                </div>
                <div class="pair-item" id="pairRow2" style="display:none">
                  <div class="token-input-wrap"><input id="pair2a" list="tokenHints" placeholder="base token"/><button class="dd-btn" onclick="openTokenList('pair2a')">▼</button></div>
                  <div class="token-input-wrap"><input id="pair2b" list="tokenHints" placeholder="quote token"/><button class="dd-btn" onclick="openTokenList('pair2b')">▼</button></div>
                </div>
                <div class="pair-item" id="pairRow3" style="display:none">
                  <div class="token-input-wrap"><input id="pair3a" list="tokenHints" placeholder="base token"/><button class="dd-btn" onclick="openTokenList('pair3a')">▼</button></div>
                  <div class="token-input-wrap"><input id="pair3b" list="tokenHints" placeholder="quote token"/><button class="dd-btn" onclick="openTokenList('pair3b')">▼</button></div>
                </div>
                <div class="pair-item" id="pairRow4" style="display:none">
                  <div class="token-input-wrap"><input id="pair4a" list="tokenHints" placeholder="base token"/><button class="dd-btn" onclick="openTokenList('pair4a')">▼</button></div>
                  <div class="token-input-wrap"><input id="pair4b" list="tokenHints" placeholder="quote token"/><button class="dd-btn" onclick="openTokenList('pair4b')">▼</button></div>
                </div>
              </div>
              <datalist id="tokenHints"></datalist>
            </div>
          </div>

          <div class="row">
            <label title="Choose chains for analysis">Include chains</label>
            <div>
              <div class="top-line">
                <span class="meta-badge" id="chainsMeta">chains: -</span>
              </div>
              <div class="chain-grid" id="chainChecks"></div>
            </div>
          </div>

          <div class="row">
            <label>Filters</label>
            <div>
              <div class="inline-grid">
                <div class="filter-item">
                  <div class="hint">Min TVL<br/>(USD)</div>
                  <input id="minTvl" value="1000" type="number" min="0" max="10000000" step="1"/>
                </div>
                <div class="filter-item">
                  <div class="hint">History<br/>days</div>
                  <input id="days" value="30" type="number" min="1" max="3650" step="1"/>
                </div>
                <div class="filter-item">
                  <div class="hint">Exclude below<br/>X% fee</div>
                  <input id="minFeePct" value="0" type="number" step="0.1" min="0" max="1"/>
                </div>
                <div class="filter-item">
                  <div class="hint">Exclude above<br/>X% fee</div>
                  <input id="maxFeePct" value="2" type="number" step="0.1" min="1" max="3"/>
                </div>
                <div class="filter-item">
                  <div class="hint">Protocol<br/>version</div>
                  <div class="proto-checks">
                    <label><input id="protoV3" type="checkbox" checked/> V3</label>
                    <label><input id="protoV4" type="checkbox" checked/> V4</label>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="actions" style="margin-top:14px">
          <div class="actions-left">
            <button class="btn secondary" onclick="toggleLogs()">Latest run logs</button>
            <button class="btn secondary" onclick="resetLogs()">Reset logs</button>
            <button class="btn secondary" onclick="exportCsv()">Export CSV</button>
          </div>
          <div class="actions-right">
            <button class="btn run-btn" id="runBtn" onclick="runJob()">Run analysis</button>
            <span id="status" class="status">Ready</span>
          </div>
        </div>
        <div class="progress-wrap">
          <div class="progress-meta">
            <span id="stageText">Stage: waiting</span>
            <span id="progressText">0%</span>
          </div>
          <div class="progress-bar"><div id="progressFill" class="progress-fill"></div></div>
        </div>
        <div id="logsWrap" style="display:none; margin-top:10px">
          <pre id="logs">No logs yet.</pre>
        </div>
      </section>

      <section class="card">
        <div class="charts-grid">
          <div id="feesChart" class="plot"></div>
          <div id="tvlChart" class="plot"></div>
        </div>
      </section>

      <section class="card">
        <h3>Pools Table</h3>
        <div class="table-wrap">
          <table id="resultTable"></table>
        </div>
      </section>
    </div>
  </div>

  <script>
    const SORTABLE = {
      color: (r) => r.pool_id || "",
      visibility: (r) => r.pool_id || "",
      chain: (r) => r.chain || "",
      version: (r) => r.version || "",
      pair: (r) => r.pair || "",
      fee_pct: (r) => Number(r.fee_pct || 0),
      final_income: (r) => Number(r.final_income || 0),
      apy_pct: (r) => Number(r.apy_pct || 0),
      last_tvl: (r) => Number(r.last_tvl || 0),
      status: (r) => r.status || ""
    };

    let lastRows = [];
    let renderedRows = [];
    let sortKey = "final_income";
    let sortDesc = true;
    const FORM_STORAGE_KEY = "uni_fee_form_v4";
    const FIELD_IDS = ["pair1a", "pair1b", "pair2a", "pair2b", "pair3a", "pair3b", "pair4a", "pair4b", "minTvl", "days", "maxFeePct", "minFeePct", "protoV3", "protoV4", "allChains"];
    let availableChains = [];
    let colorMap = {};
    let dashMap = {};
    let visibilityMap = {};
    let seriesByPool = {};
    let currentRequest = {};
    let pairRowsVisible = 1;

    function splitCSV(v) {
      return (v || "").split(",").map(x => x.trim().toLowerCase()).filter(Boolean);
    }

    function formatUsd(v) {
      return new Intl.NumberFormat("en-US", {maximumFractionDigits: 0}).format(Number(v || 0));
    }

    function formatUsdShort(v) {
      const n = Number(v || 0);
      if (n >= 1000000) return "$" + (n / 1000000).toFixed(n % 1000000 === 0 ? 0 : 1) + "M";
      if (n >= 1000) return "$" + (n / 1000).toFixed(n % 1000 === 0 ? 0 : 1) + "k";
      return "$" + String(Math.round(n));
    }

    function getDaysValue() {
      const v = Number(document.getElementById("days")?.value || 30);
      return Number.isFinite(v) && v > 0 ? v : 30;
    }

    function setDays(v) {
      document.getElementById("days").value = v;
      saveFormState();
    }

    function navigateIntent(path) {
      if (!path) return;
      window.location.href = path;
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
      state.pairRowsVisible = pairRowsVisible;
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
        // Always start with one visible pair row.
        pairRowsVisible = 1;
        updatePairRows();
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

    function updatePairRows() {
      for (let i = 1; i <= 4; i++) {
        const row = document.getElementById(`pairRow${i}`);
        if (!row) continue;
        row.style.display = i <= pairRowsVisible ? "grid" : "none";
      }
      const removeBtn = document.getElementById("removePairBtn");
      if (removeBtn) removeBtn.disabled = pairRowsVisible <= 1;
    }

    function addPairRow() {
      if (pairRowsVisible < 4) {
        pairRowsVisible += 1;
        updatePairRows();
        saveFormState();
      }
    }

    function removePairRow() {
      if (pairRowsVisible <= 1) return;
      for (const side of ["a", "b"]) {
        const el = document.getElementById(`pair${pairRowsVisible}${side}`);
        if (el) {
          el.value = "";
          el.classList.remove("invalid-input");
        }
      }
      pairRowsVisible -= 1;
      updatePairRows();
      saveFormState();
    }

    function openTokenList(inputId) {
      const el = document.getElementById(inputId);
      if (!el) return;
      el.value = "";
      try {
        if (typeof el.showPicker === "function") {
          el.showPicker();
          return;
        }
      } catch (_) {}
      el.focus();
      el.dispatchEvent(new KeyboardEvent("keydown", {key: "ArrowDown"}));
      saveFormState();
    }

    function clearPairErrors() {
      for (let i = 1; i <= 4; i++) {
        for (const side of ["a", "b"]) {
          const el = document.getElementById(`pair${i}${side}`);
          if (el) el.classList.remove("invalid-input");
        }
      }
    }

    function validatePairs() {
      clearPairErrors();
      const out = [];
      let hasError = false;
      for (let i = 1; i <= 4; i++) {
        const aEl = document.getElementById(`pair${i}a`);
        const bEl = document.getElementById(`pair${i}b`);
        const a = (aEl?.value || "").trim().toLowerCase();
        const b = (bEl?.value || "").trim().toLowerCase();
        if (!a && !b) continue;
        if (!a || !b || a === b) {
          if (aEl) aEl.classList.add("invalid-input");
          if (bEl) bEl.classList.add("invalid-input");
          hasError = true;
          continue;
        }
        out.push(`${a},${b}`);
      }
      const unique = Array.from(new Set(out));
      return {pairs: unique, valid: !hasError && unique.length > 0};
    }

    function getSelectedPairs() {
      return validatePairs().pairs;
    }

    function getSelectedProtocols() {
      const out = [];
      if (document.getElementById("protoV3")?.checked) out.push("v3");
      if (document.getElementById("protoV4")?.checked) out.push("v4");
      return out;
    }

    function getSelectedChains() {
      const allEl = document.getElementById("allChains");
      if (!allEl) return [];
      if (allEl.checked) return [];
      const out = [];
      for (const c of availableChains) {
        const el = document.getElementById("chain_" + c);
        if (el && el.checked) out.push(c);
      }
      return out;
    }

    function toggleAllChains() {
      const allEl = document.getElementById("allChains");
      if (!allEl) return;
      const all = allEl.checked;
      for (const c of availableChains) {
        const el = document.getElementById("chain_" + c);
        if (el) el.checked = all;
      }
      saveFormState();
    }

    function onChainToggle() {
      let checkedCount = 0;
      for (const c of availableChains) {
        const el = document.getElementById("chain_" + c);
        if (el && el.checked) checkedCount += 1;
      }
      const allEl = document.getElementById("allChains");
      if (!allEl) return;
      allEl.checked = checkedCount === availableChains.length && availableChains.length > 0;
      saveFormState();
    }

    function toggleLogs() {
      const wrap = document.getElementById("logsWrap");
      wrap.style.display = wrap.style.display === "none" ? "block" : "none";
      if (wrap.style.display !== "none") {
        loadRecentLogs();
      }
    }

    async function loadRecentLogs() {
      const logsEl = document.getElementById("logs");
      if (!logsEl) return;
      try {
        const r = await fetch("/api/runs/recent");
        const data = await r.json();
        const items = Array.isArray(data.items) ? data.items : [];
        if (!items.length) {
          logsEl.textContent = "No logs yet.";
          return;
        }
        const chunks = [];
        for (const it of items.slice(0, 10)) {
          const req = it.request || {};
          const minTvlTxt = req.min_tvl != null ? formatUsdShort(req.min_tvl) : "-";
          const head = `[${it.ts || "-"}] ${String(it.status || "").toUpperCase()} | pairs=${req.pairs || "-"} | days=${req.days ?? "-"} | min_tvl=${minTvlTxt} | chains=${(req.include_chains || []).join(",") || "all"}`;
          const err = it.error ? `ERROR: ${it.error}` : "";
          const body = (it.logs || []).join("\\n\\n");
          chunks.push([head, err, body].filter(Boolean).join("\\n"));
        }
        logsEl.textContent = chunks.join("\\n\\n----------------------------------------\\n\\n");
      } catch (e) {
        logsEl.textContent = "Failed to load recent logs.";
      }
    }

    async function resetLogs() {
      try {
        const r = await fetch("/api/runs/reset", {method: "POST"});
        if (!r.ok) {
          setStatus("Failed to reset logs", "fail");
          return;
        }
        const logsEl = document.getElementById("logs");
        if (logsEl) logsEl.textContent = "No logs yet.";
        setStatus("Logs reset", "ok");
      } catch (e) {
        setStatus("Failed to reset logs", "fail");
      }
    }

    function togglePoolVisibility(el) {
      const poolId = el?.dataset?.poolId;
      if (!poolId) return;
      visibilityMap[poolId] = !!el.checked;
      redrawCharts();
    }

    function redrawCharts() {
      const palette = ["#1e3a8a", "#155e75", "#14532d", "#7e22ce", "#7f1d1d", "#1d4ed8", "#0e7490", "#166534", "#6d28d9", "#be123c"];
      const dashes = ["dash", "dot", "dashdot", "longdash", "longdashdot"];
      const feeTraces = [];
      const tvlTraces = [];
      let maxTs = 0;

      const pools = Object.keys(seriesByPool);
      for (let i = 0; i < pools.length; i++) {
        const poolId = pools[i];
        const s = seriesByPool[poolId];
        if (!visibilityMap[poolId]) continue;
        const feeX = (s.fees || []).map(p => new Date(p[0] * 1000));
        const feeY = (s.fees || []).map(p => p[1]);
        const tvlX = (s.tvl || []).map(p => new Date(p[0] * 1000));
        const tvlY = (s.tvl || []).map(p => p[1] / 1000.0);
        const localMax = Math.max(
          ...((s.fees || []).map(p => Number(p[0] || 0))),
          ...((s.tvl || []).map(p => Number(p[0] || 0))),
          0
        );
        if (localMax > maxTs) maxTs = localMax;
        const c = colorMap[poolId] || palette[i % palette.length];
        const d = dashMap[poolId] || (i < palette.length ? "solid" : dashes[(i - palette.length) % dashes.length]);
        const hoverData = feeX.map(() => [s.chain || "", s.version || "", Number(s.fee_pct || 0).toFixed(2), s.pair || ""]);
        feeTraces.push({
          x: feeX, y: feeY, mode: "lines", name: s.label, customdata: hoverData,
          hovertemplate: "%{x|%b %d}<br>%{customdata[0]} %{customdata[1]} | %{customdata[2]}% | %{customdata[3]}<extra></extra>",
          line: {color: c, width: 2, dash: d}
        });
        tvlTraces.push({
          x: tvlX, y: tvlY, mode: "lines", name: s.label, customdata: hoverData,
          hovertemplate: "%{x|%b %d}<br>%{customdata[0]} %{customdata[1]} | %{customdata[2]}% | %{customdata[3]}<extra></extra>",
          line: {color: c, width: 2, dash: d}
        });
      }

      const alloc = Number(currentRequest?.lp_allocation_usd || 1000);
      const days = Number(currentRequest?.days || getDaysValue());
      const endDate = maxTs > 0 ? new Date(maxTs * 1000) : new Date();
      const startDate = new Date(endDate.getTime() - days * 24 * 3600 * 1000);
      Plotly.newPlot("feesChart", feeTraces, {
        title: `Cumulative Fees (LP allocation: $${formatUsd(alloc)})`,
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#f8fbff",
        font: {color: "#0f172a"},
        showlegend: false,
        margin: {t: 30, b: 42, l: 50, r: 14},
        xaxis: {showgrid: true, gridcolor: "#d9e2f0", nticks: 18, tickformat: "%b %d", automargin: true, range: [startDate, endDate]},
        yaxis: {showgrid: true, gridcolor: "#d9e2f0", nticks: 12, zeroline: false}
      }, {displaylogo: false, responsive: true});
      Plotly.newPlot("tvlChart", tvlTraces, {
        title: "TVL dynamics (thousands USD)",
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#f8fbff",
        font: {color: "#0f172a"},
        showlegend: false,
        margin: {t: 30, b: 42, l: 50, r: 14},
        xaxis: {showgrid: true, gridcolor: "#d9e2f0", nticks: 18, tickformat: "%b %d", automargin: true, range: [startDate, endDate]},
        yaxis: {showgrid: true, gridcolor: "#d9e2f0", nticks: 12, zeroline: false}
      }, {displaylogo: false, responsive: true});
    }

    function renderTable(rows) {
      const table = document.getElementById("resultTable");
      const hdr = [
        ["color", ""], ["visibility", "Visibility"], ["chain", "Chain"], ["version", "Version"], ["pair", "Pair"], ["pool_id", "Pool ID"],
        ["fee_pct", "Fee %"], ["final_income", "Cumul $"], ["apy_pct", "APY"], ["last_tvl", "TVL"], ["status", "Status"]
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
        const dash = dashMap[r.pool_id] || "solid";
        const cssDash = (dash === "solid") ? "solid" : (dash === "dot" ? "dotted" : "dashed");
        const visible = !!visibilityMap[r.pool_id];
        const hasSeries = !!seriesByPool[r.pool_id];
        const disabled = hasSeries ? "" : "disabled";
        const poolIdDisplay = (r.version === "v4" && (r.pool_id || "").length > 24)
          ? `${r.pool_id.slice(0, 12)}...${r.pool_id.slice(-8)}`
          : r.pool_id;
        html += `<tr class="${cls}">`;
        html += `<td><span class="line-swatch" style="border-top-color:${color};border-top-style:${cssDash};"></span></td>`;
        html += `<td><input type="checkbox" data-pool-id="${r.pool_id}" ${visible ? "checked" : ""} ${disabled} onchange="togglePoolVisibility(this)"/></td>`;
        html += `<td>${r.chain}</td>`;
        html += `<td>${r.version}</td>`;
        html += `<td>${r.pair}</td>`;
        html += `<td class="mono">${poolIdDisplay}</td>`;
        html += `<td>${Number(r.fee_pct).toFixed(2)}</td>`;
        html += `<td>$${formatUsd(r.final_income)}</td>`;
        html += `<td>${Number(r.apy_pct || 0).toFixed(1)}%</td>`;
        html += `<td>$${formatUsd(r.last_tvl)}</td>`;
        const statusLabel = r.status === "ok"
          ? "ok"
          : (r.status === "filtered_suffix" ? "excluded by suffix" : "filtered by fee range");
        html += `<td>${statusLabel}</td>`;
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
        sortDesc = key === "final_income" || key === "apy_pct" || key === "last_tvl" || key === "fee_pct";
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
      const headers = ["visibility", "chain", "version", "pair", "pool_id", "fee_pct", "final_income", "apy_pct", "last_tvl", "status"];
      const lines = [headers.join(",")];
      for (const r of rows) {
        const vals = headers.map(h => {
          const rawVal = (h === "visibility") ? (!!visibilityMap[r.pool_id]) : r[h];
          const val = rawVal == null ? "" : String(rawVal);
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
        tokenHints.innerHTML = (meta.tokens || []).map(t => `<option value="${t}"></option>`).join("");
        const minTvl = Number(meta.token_catalog?.min_tvl_usd || 10000);
        document.getElementById("tokensMeta").textContent = `popular tokens (TVL>${formatUsdShort(minTvl)}): ${meta.token_catalog?.count || 0}, updated: ${meta.token_catalog?.updated_at || "-"}`;

        availableChains = meta.chains || [];
        document.getElementById("chainsMeta").textContent = `chains: ${meta.chain_catalog?.count || 0}, updated: ${meta.chain_catalog?.updated_at || "-"}`;
        const checks = document.getElementById("chainChecks");
        checks.innerHTML = [
          `<label class="check"><input type="checkbox" id="allChains" checked onchange="toggleAllChains()"> all</label>`,
          ...availableChains.map(c => `<label class="check"><input type="checkbox" id="chain_${c}" checked onchange="onChainToggle()"> ${c}</label>`)
        ].join("");
        loadFormState();
        if (document.getElementById("allChains").checked) toggleAllChains();
        else onChainToggle();
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
      try {
        const pairCheck = validatePairs();
        const payload = {
          pairs: pairCheck.pairs,
          include_chains: getSelectedChains(),
          include_versions: getSelectedProtocols(),
          min_tvl: Number(document.getElementById("minTvl").value || 1000),
          days: Number(document.getElementById("days").value || 30),
          max_fee_pct: Number(document.getElementById("maxFeePct").value || 2),
          min_fee_pct: Number(document.getElementById("minFeePct").value || 0),
        };
        if (!payload.include_versions.length) {
          setStatus("Select at least one protocol (V3/V4).", "fail");
          return;
        }
        if (!Number.isInteger(payload.days) || payload.days < 1 || payload.days > 3650) {
          setStatus("History days must be an integer in range 1..3650.", "fail");
          return;
        }
        if (!Number.isFinite(payload.min_tvl) || payload.min_tvl < 0 || payload.min_tvl > 10000000) {
          setStatus("Min TVL must be in range 0..10000000.", "fail");
          return;
        }
        if (!Number.isFinite(payload.min_fee_pct) || payload.min_fee_pct < 0 || payload.min_fee_pct > 1) {
          setStatus("Exclude below must be in range 0..1.", "fail");
          return;
        }
        if (!Number.isFinite(payload.max_fee_pct) || payload.max_fee_pct < 1 || payload.max_fee_pct > 3) {
          setStatus("Exclude above must be in range 1..3.", "fail");
          return;
        }
        if (payload.min_fee_pct >= payload.max_fee_pct) {
          setStatus("Exclude below must be lower than Exclude above.", "fail");
          return;
        }
        if (!pairCheck.valid) {
          setStatus("Invalid pairs: fill both tokens and avoid duplicates in a pair.", "fail");
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
      } catch (e) {
        setBusy(false);
        setStatus("Frontend error: " + (e?.message || "unknown"), "fail");
      }
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
      const mSuffix = document.getElementById("mSuffix");
      const mTotal = document.getElementById("mTotal");
      const mChart = document.getElementById("mChart");
      const mErr = document.getElementById("mErr");
      if (mSuffix) mSuffix.textContent = result.suffix;
      if (mTotal) mTotal.textContent = result.total;
      if (mChart) mChart.textContent = result.chart_pools;
      if (mErr) mErr.textContent = result.error_pools;
      loadRecentLogs();

      currentRequest = result?.request || {};
      colorMap = {};
      dashMap = {};
      visibilityMap = {};
      seriesByPool = {};
      const palette = ["#1e3a8a", "#155e75", "#14532d", "#7e22ce", "#7f1d1d", "#1d4ed8", "#0e7490", "#166534", "#6d28d9", "#be123c"];
      const dashes = ["dash", "dot", "dashdot", "longdash", "longdashdot"];
      for (let i = 0; i < (result.series || []).length; i++) {
        const s = result.series[i];
        const c = palette[i % palette.length];
        const d = i < palette.length ? "solid" : dashes[(i - palette.length) % dashes.length];
        colorMap[s.pool_id] = c;
        dashMap[s.pool_id] = d;
        seriesByPool[s.pool_id] = s;
        visibilityMap[s.pool_id] = (s.status === "ok");
      }

      const daysForApy = Number(result?.request?.days || getDaysValue());
      lastRows = (result.rows || []).map((r) => {
        const income = Number(r.final_income || 0);
        const alloc = Number(currentRequest?.lp_allocation_usd || 1000);
        const apyPct = (alloc > 0 && daysForApy > 0) ? (income / alloc) * (365 / daysForApy) * 100 : 0;
        return {...r, apy_pct: apyPct};
      });
      redrawCharts();
      sortKey = "final_income";
      sortDesc = true;
      sortBy("final_income");
    }

    function renderEmptyCharts() {
      const now = new Date();
      const start = new Date(now.getTime() - getDaysValue() * 24 * 3600 * 1000);
      const baseline = [{x: [start, now], y: [0, 0], mode: "lines", line: {color: "rgba(0,0,0,0)", width: 1}, hoverinfo: "skip", showlegend: false}];
      const emptyLayout = {
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#f8fbff",
        font: {color: "#0f172a"},
        showlegend: false,
        margin: {t: 30, b: 42, l: 50, r: 14},
        xaxis: {showgrid: true, gridcolor: "#d9e2f0", nticks: 18, tickformat: "%b %d", range: [start, now], automargin: true},
        yaxis: {title: "Value", showgrid: true, gridcolor: "#d9e2f0", nticks: 12, zeroline: false, range: [0, 1]},
        annotations: [{text: "Run analysis to load data", x: 0.5, y: 0.5, xref: "paper", yref: "paper", showarrow: false, font: {color: "#64748b"}}],
      };
      Plotly.newPlot("feesChart", baseline, {title: "Cumulative Fees", ...emptyLayout, yaxis: {...emptyLayout.yaxis, title: "Cumulative fee (USD)"}}, {displaylogo: false, responsive: true});
      Plotly.newPlot("tvlChart", baseline, {title: "TVL dynamics (thousands USD)", ...emptyLayout, yaxis: {...emptyLayout.yaxis, title: "TVL (k USD)"}}, {displaylogo: false, responsive: true});
    }

    attachAutosave();
    updatePairRows();
    renderEmptyCharts();
    loadMeta();
  </script>
</body>
</html>
"""
