#!/usr/bin/env python3
"""
Agent 3: Объединение данных Агента 1 (v3) и Агента 2 (v4) на одном графике.

- Читает data/pools_v3_{suffix}.json
- Читает data/pools_v4_{suffix}.json
- Объединяет и сохраняет график в data/fee_chart_{suffix}.pdf
"""

import argparse
import os

from agent_common import (
    load_chart_data_json,
    pairs_to_filename_suffix,
    parse_token_pairs,
    save_chart,
    save_merged_list_pdf,
    _is_bad_fee_entry,
)
from config import DEFAULT_TOKEN_PAIRS


def main() -> None:
    parser = argparse.ArgumentParser(description="Agent 3: объединение v3 + v4 на одном графике")
    parser.add_argument(
        "--exclude-chains",
        type=str,
        default="",
        help="Список чейнов через запятую, которые НЕ рисовать (пример: base,polygon)",
    )
    parser.add_argument(
        "--exclude-pools-suffix",
        type=str,
        default="",
        help="Список суффиксов (последние 4 символа pool_id) через запятую, которые НЕ рисовать",
    )
    args = parser.parse_args()

    token_pairs = os.environ.get("TOKEN_PAIRS", DEFAULT_TOKEN_PAIRS)
    suffix = pairs_to_filename_suffix(token_pairs)

    v3_path = f"data/pools_v3_{suffix}.json"
    v4_path = f"data/pools_v4_{suffix}.json"
    chart_path = f"data/fee_chart_{suffix}.pdf"
    list_pdf_path = f"data/available_pairs_merged_{suffix}.pdf"

    print("Agent 3: объединение v3 + v4")
    print("Token pairs:", token_pairs, "→ suffix:", suffix)
    if not os.path.isfile(v3_path):
        print("  Missing:", v3_path, "→ run: TOKEN_PAIRS=\"%s\" python agent_v3.py" % token_pairs)
    if not os.path.isfile(v4_path):
        print("  Missing:", v4_path, "→ run: TOKEN_PAIRS=\"%s\" python agent_v4.py" % token_pairs)

    v3_data = load_chart_data_json(v3_path)
    v4_data = load_chart_data_json(v4_path)

    merged: dict[str, dict] = {}
    for k, v in v3_data.items():
        merged[k] = v
    for k, v in v4_data.items():
        merged[k] = v

    n_v3 = len(v3_data)
    n_v4 = len(v4_data)
    print(f"Merged: {n_v3} v3 pools + {n_v4} v4 pools = {len(merged)} total")

    # Фильтрация только на этапе построения графика (данные в JSON остаются полными)
    exclude = {c.strip().lower() for c in (args.exclude_chains or "").split(",") if c.strip()}
    if exclude:
        before = len(merged)
        merged = {
            k: v
            for k, v in merged.items()
            if v.get("chain", "").lower() not in exclude
        }
        removed = before - len(merged)
        print(f"Excluded chains in chart: {', '.join(sorted(exclude))} (removed {removed} pools)")

    # Исключение пулов по последним 4 символам pool_id
    suffixes = {s.strip().lower() for s in (args.exclude_pools_suffix or "").split(",") if s.strip()}
    if suffixes:
        before = len(merged)
        filtered: dict[str, dict] = {}
        for k, v in merged.items():
            pid = (v.get("pool_id") or k or "").lower()
            tail4 = pid[-4:] if len(pid) >= 4 else pid
            if tail4 not in suffixes:
                filtered[k] = v
        merged = filtered
        removed = before - len(merged)
        print(f"Excluded pools by suffix ({', '.join(sorted(suffixes))}): removed {removed} pools")

    if not merged:
        print("No data to plot. Run agent_v3.py and/or agent_v4.py first.")
        return

    # Исключаем из графика пулы с fee > 3% (ошибка данных subgraph),
    # но оставляем их в PDF в конце списка с пометкой.
    bad_fee = {k: v for k, v in merged.items() if _is_bad_fee_entry(v)}
    if bad_fee:
        print(f"Excluded {len(bad_fee)} pools with fee > 3% from chart (they are listed at the end of PDF).")
    good = {k: v for k, v in merged.items() if k not in bad_fee}

    if not good:
        print("No data to plot after excluding bad-fee pools.")
        save_merged_list_pdf(merged, list_pdf_path)
        print("Done. Only list:", list_pdf_path)
        return

    save_chart(good, chart_path)
    save_merged_list_pdf(merged, list_pdf_path)
    print("Done. Chart:", chart_path, "| List:", list_pdf_path)


if __name__ == "__main__":
    main()
