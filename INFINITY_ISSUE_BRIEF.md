# Pancake Infinity: краткий бриф по проблеме

## Что нужно получить
- Найти и отобразить реальные позиции пользователя в `Pancake Infinity` (прежде всего `CL`, также `BIN`) в разделе сканирования пулов.
- Источник данных: on-chain (RPC/контракты менеджеров позиций), при необходимости индексаторы (Rabby/DeBank-подход).

## Где именно не получается
- На BSC для проблемного адреса видно `balanceOf = 1` на `CLPositionManager`, но итоговый список `tokenId` остается пустым.
- Из-за этого `count=0` по `pancake_infinity_cl`, хотя NFT-позиция фактически есть.
- Ранее тяжелые fallback-ветки приводили к деградации по времени и таймаутам без результата по Infinity.

## Почему сейчас не работает (по факту логов)
- Контракт/индексация Infinity не дает стабильный путь извлечения `tokenId` владельца стандартными методами.
- Часть стратегий дает нули, часть нестабильна на RPC (ошибки/пустые ответы), часть слишком тяжелая.
- В результате даже при ненулевом `balanceOf` не удается reliably получить идентификаторы позиций.

## Что уже пробовали
- Сканы через стандартные ERC721 пути (`tokenOfOwnerByIndex`, `ownerOf`, `nextTokenId`-скан).
- Сканы через `Transfer`-логи (узкие и широкие окна), включая глубокий lookback.
- Сканы через `MintPosition`-события и проверку владельца.
- Explorer fallback (BscScan/BaseScan: `tokennfttx`, `txlist`, receipts + проверка `ownerOf`).
- Chain-priority и параллелизм/перестройка пайплайна скана.
- Удаление локальных тайминговых ограничителей и затем ограничение тяжелых owner-сканов.
- Rabby/DeBank-подход: добавлен индексаторный fallback (опционально, через AccessKey), но в текущих прогонах это не дало итогового `tokenId`.
- Временная стабилизация: Infinity отключен по умолчанию, чтобы вернуть надежный результат по остальным протоколам.

## Что нужно от следующего специалиста
- Дать надежный метод извлечения `tokenId` владельца именно для `Pancake Infinity CL/BIN` (BSC/Base), который не ломает общий скан.
- Предложить минимальный, легкий и воспроизводимый путь (без тяжелых brute-force веток).

## Ссылки, которые использовались в обсуждении
- Pancake Infinity overview: <https://docs.pancakeswap.finance/trade/pancakeswap-infinity>
- Pancake Infinity accounting layer: <https://developer.pancakeswap.finance/contracts/infinity/overview/accounting-layer-vault>
- Pancake Infinity manage liquidity: <https://developer.pancakeswap.finance/contracts/infinity/guides/manage-liquidity>
- Pancake Infinity addresses: <https://developer.pancakeswap.finance/contracts/infinity/resources/addresses>
- Infinity periphery repo: <https://github.com/pancakeswap/infinity-periphery>
- Infinity core repo: <https://github.com/pancakeswap/infinity-core>
- Pancake info API (исторический/архивный): <https://github.com/pancakeswap/pancake-info-api>
- Graph explorer (Infinity CL subgraph reference): <https://thegraph.com/explorer/subgraphs/xRvRArtBhXaz6RgB5xKgWYVhMJjErGCusyHd9LCs5S?chain=arbitrum-one&view=Indexers>
- Rabby repo: <https://github.com/RabbyHub/Rabby>
- Rabby desktop repo: <https://github.com/RabbyHub/RabbyDesktop>
- Rabby supported chains/assets: <https://support.rabby.io/hc/en-us/sections/11150465941391-Supported-Chains-and-Assets>
- DeBank main site: <https://www.debank.com/>
- DeBank API reference: <https://docs.cloud.debank.com/en/readme/api-pro-reference>
- DeBank User API: <https://docs.cloud.debank.com/en/readme/api-pro-reference/user>
- DeBank Protocol API: <https://docs.cloud.debank.com/en/readme/api-pro-reference/protocol>
- DeBank PortfolioItem model: <https://docs.cloud.debank.com/en/readme/api-models/portfolioitemobject>
- DeBank Pro endpoint (complex protocol list): <https://pro-openapi.debank.com/v1/user/complex_protocol_list>
- DeBank Pro endpoint (all complex protocol list): <https://pro-openapi.debank.com/v1/user/all_complex_protocol_list>
