# NFT catalog: параллельный конвейер для v4 и тяжёлых протоколов

## Идея

1. **Фаза 1 (лёгкая)** — сразу после сборки строк из tokennfttx: `positions()` на V3 NPM / Pancake v3 NPM (как раньше, последовательно, лимит по времени и строкам).
2. **Фаза 2 (тяжёлая, параллельная)** — отдельный пул потоков для:
   - `uniswap_v4` — снимок через `getPoolAndPositionInfo` + `getPositionLiquidity` (не v3 `positions()`).
   - `pancake_infinity_cl` / `pancake_infinity_bin` — через общий `_fetch_heavy_protocol_nft_snapshot` (v4-ветка или v3 snapshot по протоколу).

Закрытые строки (`catalog_segment == closed`) в heavy-очередь не попадают.

## Переменные окружения

| Переменная | По умолчанию | Смысл |
|------------|--------------|--------|
| `POSITIONS_NFT_HEAVY_PIPELINE` | `1` | Включить фазу 2 после фазы 1 |
| `POSITIONS_NFT_HEAVY_WORKERS` | `4` | Размер пула для фазы 2 |
| `POSITIONS_NFT_HEAVY_MAX_SECONDS` | `48` | Бюджет времени на фазу 2 (ещё ограничен остатком общего окна enrich) |
| `POSITIONS_NFT_HEAVY_MAX_ROWS` | `120` | Макс. число heavy-строк за один скан |
| `POSITIONS_NFT_HEAVY_DEBUG_LOG` | `1` | Писать строки в SQLite `heavy_protocol_enrich_debug` |
| `POSITIONS_HEAVY_ENRICH_DEBUG_HTTP` | `0` | `GET /api/debug/heavy-protocol-enrich?limit=50` |

## Таблица SQLite

Файл: тот же, что и аналитика (`ANALYTICS_DB_PATH`, на Render часто `/var/data/.../analytics.sqlite3`).

Таблица: **`heavy_protocol_enrich_debug`**

Поля: `ts`, `job_id`, `session_id`, `chain_id`, `owner`, `pm_contract`, `position_id`, `internal_protocol`, `phase` (например `heavy_parallel`), `status` (`ok` / `fail`), `detail` (JSON), `duration_ms`.

Пример запроса:

```sql
SELECT * FROM heavy_protocol_enrich_debug ORDER BY id DESC LIMIT 30;
```

## Метрики в ответе скана

В `debug_timings` (при `hard_scan` / отладке): `nft_catalog_heavy_parallel_ok`, `nft_catalog_heavy_parallel_fail`, `nft_catalog_heavy_pipeline`.

В `info_notes` краткая сводка по heavy-пайплайну.

## Реестр PM

В `_nft_pm_registry_for_chain` добавлен **`infinity_bin`**; для open/closed префильтра bin-NFT попадают в `allowed` без v4-multicall (иначе PM другого типа).
