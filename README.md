# Uniswap Pool Fee Agent

Агент для поиска пар Uniswap v3/v4, фильтрации по TVL и расчёта накопленных комиссий LP.

## Архитектура: 4 агента

- **Agent 1 (agent_v3.py)** — базовая версия, только v3. Ищет пулы, сохраняет `data/pools_v3_{пары}.json`.
- **Agent 2 (agent_v4.py)** — только v4 (The Graph, нужен API ключ). Сохраняет `data/pools_v4_{пары}.json`.
- **Agent Smart (agent_smart.py)** — выбирает top пулов из v3+v4 по smart score, сохраняет `data/pools_smart_{пары}.json`.
- **Agent 3 (agent_merge.py)** — объединяет данные Agent 1, Agent 2 и Agent Smart на одном графике.

### Запуск одной командой

```bash
export THE_GRAPH_API_KEY="ваш_ключ"
python run_all.py uni,eth
python run_all.py fluid,eth --min-tvl 500
python run_all.py "wbtc,usdt;wbtc,usdc" --min-tvl 500000   # несколько пар — в кавычках!
```

Запускает по очереди: agent_v3 → agent_v4 → agent_smart → agent_merge.

**По каждому токену:** для каждого указанного токена запускается отдельный полный прогон (пары с usdt, usdc, eth) — свои PDF и графики на токен:

```bash
python run_all.py --tokens "paxg,fluid,wbtc"
# То же: paxg+usdt, paxg+usdc, paxg+eth → затем fluid → затем wbtc
```

Или по отдельности:

```bash
TOKEN_PAIRS="uni,eth" python agent_v3.py
TOKEN_PAIRS="uni,eth" python agent_v4.py
TOKEN_PAIRS="uni,eth" python agent_merge.py
```

## Установка

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### Проектные env-переменные (рекомендуется)

В репозитории есть шаблон `\.env.example` и локальный файл `\.env` (игнорируется git).

```bash
cp .env.example .env
# заполните THE_GRAPH_API_KEY в .env
set -a
source .env
set +a
```

Перед запуском можно также установить API ключ вручную:

```bash
export THE_GRAPH_API_KEY="ваш_ключ"
```

## Web (Cloud MVP)

Теперь проект можно запускать как веб-сервис (без PDF в UI, все результаты на экране).

### Локально (как облачный сервер)

```bash
export THE_GRAPH_API_KEY="ваш_ключ"
python -m uvicorn webapp.main:app --host 0.0.0.0 --port 8000
```

Откройте: `http://localhost:8000`

### Docker (для облака)

```bash
docker build -t uni-fee-web .
docker run -p 8000:8000 -e THE_GRAPH_API_KEY="ваш_ключ" uni-fee-web
```

### Что есть в веб-форме

- ручной ввод пар (`tokenA,tokenB;tokenC,tokenD`);
- быстрый ввод токенов списком (каждый токен автоматически парится с `usdt/usdc/eth`);
- include chains (или пусто = все поддерживаемые);
- `min TVL`, `days`, `exclude chains`, `exclude pool suffix`;
- отдельная страница `GET /riko` для интерфейса RIKO Vault (deposit/mint/redeem + admin whitelist);
- результат: таблица + 2 интерактивных графика (fees/tvl), без PDF.

## Deploy на Render

В репозитории уже есть `render.yaml` для автоконфигурации.

### Быстрый деплой одной командой

```bash
chmod +x scripts/deploy_render.sh
./scripts/deploy_render.sh "your commit message"
```

Что делает скрипт:
- проверяет, что вы на ветке `milestone/web-mvp-stable`;
- коммитит изменения (исключая `data/*.sqlite3*`);
- пушит в `origin/milestone/web-mvp-stable`.

Если хотите запускать деплой и проверку healthz сразу из терминала:

```bash
export RENDER_DEPLOY_HOOK_URL="https://api.render.com/deploy/srv-...?..."
export RENDER_HEALTHCHECK_URL="https://uni-fee-web.onrender.com/healthz"
./scripts/deploy_render.sh "your commit message"
```

После этого скрипт сам:
- триггерит Deploy Hook;
- ждет успешный `/healthz`.

Если `RENDER_DEPLOY_HOOK_URL` не задан, скрипт только коммитит+пушит.

### Локальные git hooks (защита от JS-падения UI)

В проекте есть pre-commit проверка inline JavaScript в `webapp/main.py`,
которая ловит частые фатальные ошибки (например, дубли `const/let` в одном скоупе).

Включить один раз:

```bash
git config core.hooksPath .githooks
```

Проверка вручную:

```bash
python3 scripts/check_inline_js_guardrails.py webapp/main.py
```

### Шаги

1. Запушьте проект в GitHub (если еще не там).
2. В Render: **New +** → **Blueprint**.
3. Подключите ваш GitHub-репозиторий.
4. Render прочитает `render.yaml` и создаст web service `uni-fee-web`.
5. В переменных окружения задайте секрет и проектные флаги:
   - `THE_GRAPH_API_KEY=...`
   - `ENABLE_BASE_CHAIN=1`
   - `BASE_V3_ISOLATED_PIPELINE=0`
   - `V3_BASE_LIGHT_SCAN_PAGES=3`
   - `WEB_GRAPHQL_RETRIES=2`
   - `WEB_GRAPHQL_READ_TIMEOUT_SEC_NORMAL=30`
   - `WEB_DISABLE_V4_SYMBOL_FALLBACK_NORMAL=1`
   - `WEB_V4_SKIP_CHAIN_AFTER_TIMEOUT=1`
6. Нажмите Deploy.

После деплоя сайт будет доступен по URL Render, healthcheck:
- `/healthz`

### Примечания по стоимости

- Для фоновых расчетов и стабильности лучше `Starter` (в `render.yaml` уже указан).
- Если нужно дешевле на старте, можно переключить план в Render UI, но при засыпании инстанса запуск задач будет медленнее.

## Использование

```bash
# Вариант 1: три отдельных агента (рекомендуется)
TOKEN_PAIRS="uni,eth" python agent_v3.py
TOKEN_PAIRS="uni,eth" python agent_v4.py
TOKEN_PAIRS="uni,eth" python agent_merge.py

# Вариант 2: единый agent.py (v3+v4 в одном запуске)
python agent.py
TOKEN_PAIRS="uni,eth" python agent.py --min-tvl 1000
```

### Аргументы командной строки

- **`pairs`** — пары токенов (один аргумент): `uni,eth` или `"wbtc,usdt;wbtc,usdc"`.
- **`--tokens LIST`** — список токенов через запятую: для каждого токена запускается полный цикл (v3→v4→merge) с парами токен+usdt, токен+usdc, токен+eth. Пример: `--tokens "paxg,fluid,wbtc"`.
- **`--min-tvl USD`** — минимальный TVL пула в USD. Переопределяет `MIN_TVL` и config.

### Переменные окружения

- **`THE_GRAPH_API_KEY`** — **обязателен** для Ethereum, Arbitrum, Optimism, Polygon и др. Без ключа The Graph возвращает auth error. Получить бесплатно: [The Graph Studio](https://thegraph.com/studio/apikeys/). Для Base можно использовать Goldsky (без ключа).
- **`TOKEN_PAIRS`** — пары токенов. Пары разделяются `;`, в паре токены через `,`. Примеры:
  - `cvx,crv` — одна пара
  - `cvx,crv;eth,usdt;eth,usdc` — три пары
- **`MIN_TVL`** — минимальный TVL пула в USD (по умолчанию: 100). `MIN_TVL=0` — все пулы. Аргумент `--min-tvl` имеет приоритет.

## Что делает агент

0. **Поиск пулов** — ищет пары fluid/eth (или заданные в `TOKEN_PAIRS`) на Uniswap v3 во всех чейнах: Ethereum, Arbitrum, Base, Optimism, Polygon, BSC, Avalanche, Celo, Unichain.
1. **Фильтрация** — оставляет только пулы с TVL ≥ MIN_TVL.
2. **Сохранение** — записывает результат в `data/available_pairs_{пары}.pdf`.
3. **Комиссии** — загружает PoolDayData за последние 3 месяца и считает накопленные комиссии для LP-позиции на $10 000.
4. **График** — строит общий график накопленных комиссий по всем пулам и сохраняет в `data/fee_chart_{пары}.pdf`.

## Выходные файлы

Имена файлов зависят от заданных пар (например, `fluid,eth;fluid,usdt` → `fluid_eth_fluid_usdt`):

- `data/available_pairs_v3_{пары}.pdf` — таблица v3 пулов (Agent 1)
- `data/pools_v3_{пары}.json` — данные v3 для графика (Agent 1)
- `data/pools_v4_{пары}.json` — данные v4 для графика (Agent 2)
- `data/fee_chart_{пары}.pdf` — объединённый график v3+v4 (Agent 3)
- `data/dynamic_tokens.json` — кеш адресов токенов, найденных автоматически по символу (создаётся при первом запросе новых пар)

### v4: ошибка "bad indexers"

The Graph иногда возвращает `bad indexers` — это сбой их инфраструктуры. Обойти можно только альтернативным источником:

1. Зарегистрируйтесь на [Ormi 0xGraph](https://app.ormilabs.com/) (бесплатно)
2. Найдите Uniswap v4 Base в каталоге subgraphs
3. Скопируйте GraphQL URL
4. Запустите:
```bash
export V4_OVERRIDE_BASE="https://ваш-url-от-ormi"
TOKEN_PAIRS="uni,eth" python agent_v4.py
```

Агент делает retry при ошибках; при постоянном сбое Base используйте `V4_OVERRIDE_BASE`.

### Ограничения

Для части пулов The Graph возвращает feesUSD=0 и volumeUSD=0 в PoolDayData. В таких случаях график комиссий будет 0. Это известное ограничение subgraph; для расчёта используется fallback: fees = volumeUSD × feeTier при feesUSD=0.
