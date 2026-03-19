# Production Smoke Checklist

Короткая проверка после каждого deploy на Render (1-2 минуты).

## Быстрый прогон одной командой

```bash
chmod +x scripts/smoke_render.sh
./scripts/smoke_render.sh
```

По умолчанию скрипт проверяет `https://uni-fee-web.onrender.com`.

Если нужен другой URL:

```bash
SMOKE_BASE_URL="https://your-service.onrender.com" ./scripts/smoke_render.sh
```

## Ручной чеклист

1. Deploy в Render завершился со статусом **Live**.
2. `GET /healthz` возвращает `200`.
3. `GET /api/meta` возвращает `200` и JSON.
4. `GET /api/positions/chains` возвращает `200` и JSON.
5. Страницы `/`, `/positions`, `/help`, `/connect` открываются без `5xx`.
6. На `/positions` запускается короткий scan без падения backend.

## Шаблон фиксации результата

- Deploy: `OK/FAIL`
- `healthz`: `OK/FAIL`
- API (`/api/meta`, `/api/positions/chains`): `OK/FAIL`
- UI (`/`, `/positions`, `/help`, `/connect`): `OK/FAIL`
- Scan smoke: `OK/FAIL`
- Комментарий: `...`
