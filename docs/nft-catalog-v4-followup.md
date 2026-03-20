# NFT-каталог и Uniswap v4 — зафиксированный подход (после v0.1.4)

Краткая памятка, **что пробовали** и **куда копать**, если снова включать доработки.
Код на момент отката к **v0.1.4** этих веток в `main.py` не содержит.

## Симптомы

- Позиции **Uniswap v4** не видны или только во «Closed» / не на всех вкладках.
- **Спам** перестаёт отфильтровываться, **время скана** растёт (больше строк в основном списке, enrich, RPC).

## Корневые причины (выявленные)

1. **Пустые `tokenName`/`tokenSymbol` в tokennfttx** → строка не считалась Uniswap → unsupported / не попадала в liquidity multicall.  
   **Идея:** константа известных PM (`v3npm`, `v4pm`, …) → `supported` и liquidity-батчи даже без метаданных explorer.

2. **`_fetch_v3_position_contract_snapshot` для `uniswap_v4`** вызывал V3 `positions(uint256)` → для v4 PM ответ не парсится → **всегда `None`**, enrich не работал.  
   **Идея:** отдельный снимок **`getPoolAndPositionInfo` + `getPositionLiquidity`** (как в `_scan_uniswap_v4_positions_onchain`), тот же кеш snapshot.

3. **Префильтр open/closed:** `getPositionLiquidity` через Multicall3: при **`ok=false`/коротком `data`** нельзя считать позицию закрытой.  
   **Идея:** множество **`v4_uncertain`** `(chain_id, pm, token_id)` → в UI сегмент **`unknown`**, затем PM snapshot и финализация сегмента по факту.

4. **`POSITIONS_NFT_CATALOG_V4_SKIP_CLOSED_PREFILTER=1`** для всех v4 не в whitelist → сегмент **`unknown`** → **все «закрытые» v4 сыплются в основной список**.  
   **Идея:** по умолчанию **выкл.**; закрытые по префильтру снова `closed`; после успешного snapshot для explorer v4 — **`_finalize_v4_catalog_segment`**: пусто → `closed`, иначе `open`.

5. **Строки с `catalog_segment == "closed"`** раньше **пропускали** `_enrich_nft_catalog_rows_from_chain` → ложный closed не исправлялся.  
   **Идея:** для **explorer + uniswap_v4 + closed** (и приоритетно **unknown** v4) — не skip; сортировка enrich: сначала **closed/unknown v4**.

## Риски при включении обратно

- Больше **unknown/open** строк → дольше скан, слабее **spam-эвристики** (пара resolved / TVL), если не ограничивать `max_rows`/время enrich.
- Имеет смысл включать **поэтапно** и мерить: `POSITIONS_EXPLORER_NFT_CATALOG_MAX_ROWS`, `max_seconds` enrich, флаги spam.

## Файлы / функции (ориентиры в `webapp/main.py`)

- `_NFT_CATALOG_KNOWN_PM_KINDS`, `_explorer_nft_catalog_is_uniswap_or_pancake`, построение строк NFT-каталога (`catalog_segment`).
- `filter_uniswap_v3_v4_open_liquidity_token_ids` — ключ **`v4_uncertain`**.
- `_nft_catalog_compute_open_liquidity_allowed` → **`(allowed, v4_uncertain)`**.
- `_fetch_uniswap_v4_position_contract_snapshot`, ветка в `_fetch_v3_position_contract_snapshot` для `uniswap_v4`.
- `_enrich_nft_catalog_rows_from_chain` — приоритет сортировки, не skip closed v4 explorer, **`_enrich_nft_catalog_row_finalize_v4_catalog_segment`**.
- Env: `POSITIONS_NFT_CATALOG_V4_SKIP_CLOSED_PREFILTER`, `POSITIONS_NFT_CATALOG_OPEN_LIQUIDITY_FILTER`.

## Откат

Состояние **v0.1.4** в репозитории: коммит, где зафиксированы версия и футер (поиск: `git log -S 'APP_VERSION = "0.1.4"' -- webapp/main.py`).

Восстановление файла:  
`git checkout <тот_коммит> -- webapp/main.py`
