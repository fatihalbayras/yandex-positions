#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Параллельный collect для опроса готовых async-операций (опрос не тарифицируется).

Опрашивает операции из state/<дата>.json пулом потоков, догружает их в
results/<дата>.json (пропуская уже обработанные ключи). Формат выхода — тот же,
что у positions.py collect, поэтому latest.json и дальнейший пайплайн не меняются.

Переменные окружения: как у positions.py (YANDEX_SEARCH_API_KEY, YANDEX_FOLDER_ID,
YP_REGION, YP_SITE).
"""
import base64
import json
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import positions

ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "state"
RESULTS_DIR = ROOT / "results"
WORKERS = 12


def fetch_one(op):
    """Один ключ: опрос операции -> запись результата. Возвращает (keyword, record, is_pending)."""
    kw = op["keyword"]
    try:
        st = positions.api_op(op["operationId"], retries=3)
    except RuntimeError as e:
        return kw, {"tag": op["tag"], "source": op.get("source", "base"),
                    "error": f"operation check: {e}"}, False
    if not st.get("done"):
        return kw, None, True
    if "response" not in st:
        return kw, {"tag": op["tag"], "source": op.get("source", "base"),
                    "error": f"operation done without response: {st.get('error', st)}"}, False
    raw = base64.b64decode(st["response"]["rawData"]).decode("utf-8", "replace")
    try:
        docs, our = positions.parse_serp(raw)
        return kw, {
            "tag": op["tag"],
            "source": op.get("source", "base"),
            "position": our["position"] if our else None,
            "url": our["url"] if our else None,
            "inTop100": our is not None,
            "foundDocs": len(docs),
            "top10": [d["domain"] for d in docs[:10]],
        }, False
    except ET.ParseError as e:
        return kw, {"tag": op["tag"], "source": op.get("source", "base"),
                    "error": f"xml parse: {e}"}, False


def main():
    date = positions.msk_today()
    state_file = STATE_DIR / f"{date}.json"
    if not state_file.exists():
        candidates = sorted(STATE_DIR.glob("*.json"))
        if not candidates:
            raise SystemExit("нет state-файлов: сначала запустите submit")
        state_file = candidates[-1]
    state = json.loads(state_file.read_text(encoding="utf-8"))

    out_file = RESULTS_DIR / f"{state['date']}.json"
    out = (json.loads(out_file.read_text(encoding="utf-8"))
           if out_file.exists()
           else {"date": state["date"], "region": state.get("region", positions.REGION),
                 "site": positions.SITE_MARKER, "total": 0, "done": 0, "pending": [],
                 "positions": {}})
    results = out["positions"]
    positions.carry_forward(state, results)
    pending = []

    todo = [op for op in state["operations"] if op["keyword"] not in results]
    print(f"к опросу: {len(todo)} операций (уже в файле: {len(results)})", flush=True)

    lock = threading.Lock()
    processed = 0
    t0 = time.time()

    def save():
        with lock:
            snapshot = dict(out)
            snapshot.update({"total": positions.state_total(state),
                             "done": len(results), "pending": pending})
            out_file.write_text(json.dumps(snapshot, ensure_ascii=False, indent=1),
                                encoding="utf-8")

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(fetch_one, op) for op in todo]
        for fut in as_completed(futures):
            kw, record, is_pending = fut.result()
            with lock:
                if is_pending:
                    pending.append(kw)
                else:
                    results[kw] = record
                processed += 1
                n = processed
            if n % 100 == 0:
                rate = n / (time.time() - t0) * 60
                print(f"  обработано {n}/{len(todo)} ({rate:.0f}/мин)", flush=True)
                save()

    save()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out.update({"total": positions.state_total(state), "done": len(results), "pending": pending})
    (RESULTS_DIR / "latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    in_top = sum(1 for r in results.values() if r.get("inTop100"))
    errs = sum(1 for r in results.values() if "error" in r)
    print(f"collect parallel: {len(results)} готово, {len(pending)} в ожидании, "
          f"{in_top} в ТОП-100, ошибок {errs} -> {out_file}", flush=True)


if __name__ == "__main__":
    main()
