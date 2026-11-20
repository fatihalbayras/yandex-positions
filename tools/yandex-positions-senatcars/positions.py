#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ночной сбор позиций Яндекса для onlineastrolog.ru через Yandex Search API.

ЭКОНОМИКА (важно, не менять без перепроверки прайса):
- Отложенный режим (searchAsync) работает ТОЛЬКО 00:00-06:00 МСК, цена 25.41-30.5 руб/1000.
  Днём async гарантированно отвечает 429 "search rate limit exceeded" — это регламент.
- Синхронный дневной режим — 488 руб/1000 (0.49 руб за ТОП-100) — ДОРОЖЕ Rush Lite
  (0.1 лимита/ключ за ТОП-30). Днём позиции смотрим в Rush Analytics / Topvisor.
- Один async-запрос с groupsOnPage=100 отдаёт ТОП-100 целиком -> ~0.025 руб/ключ/день.

ИСТОЧНИКИ КЛЮЧЕЙ (задачи можно готовить днём — в работу они пойдут ночью):
- keywords.txt        — постоянный список мониторинга, собирается КАЖДУЮ ночь.
- queue/*.txt         — разовые задачи (тот же формат). Ночной submit забирает их
                        в работу и переносит в queue/processed/. См. Poziccii_Search_API.md.

Режимы:
  submit            отправить async-запросы по keywords.txt + queue/*.txt -> state/<дата>.json
  collect           забрать результаты -> results/<дата>.json + results/latest.json
  sync-one "запрос" разовая точечная проверка (ДОРОГО, 0.49 руб, только для отладки)

Переменные окружения: YANDEX_SEARCH_API_KEY (обязателен), YANDEX_FOLDER_ID,
YP_REGION (код региона Яндекса, по умолчанию 213 = Москва),
YP_SITE (домен отслеживаемого сайта, по умолчанию onlineastrolog.ru).
"""
import base64
import datetime
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

BASE = "https://searchapi.api.cloud.yandex.net"
BASE_OP = "https://operation.api.cloud.yandex.net"
ROOT = Path(__file__).resolve().parent
KEYWORDS_FILE = ROOT / "keywords.txt"
QUEUE_DIR = ROOT / "queue"
PROCESSED_DIR = QUEUE_DIR / "processed"
STATE_DIR = ROOT / "state"
RESULTS_DIR = ROOT / "results"
SITE_MARKER = os.environ.get("YP_SITE", "onlineastrolog.ru")
GROUPS_ON_PAGE = 100  # ТОП-100 одним запросом (лимит API — 250)

KEY = os.environ.get("YANDEX_SEARCH_API_KEY", "")
FOLDER_ID = os.environ.get("YANDEX_FOLDER_ID", "b1g4gqlmtd9mncfea3tk")
REGION = os.environ.get("YP_REGION", "213")


def msk_now() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=3)


def msk_today() -> str:
    return msk_now().date().isoformat()


def api(path: str, body=None, retries: int = 8):
    """POST (body is not None) или GET к searchapi; повторы при 429/5xx с backoff."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data,
        headers={"Authorization": "Api-Key " + KEY, "Content-Type": "application/json"},
    )
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            payload = e.read().decode("utf-8", "replace")[:300]
            if e.code == 429 and attempt < retries - 1:
                wait = 20 * (attempt + 1)
                print(f"  429, жду {wait}с (попытка {attempt + 1}/{retries})", file=sys.stderr)
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {e.code} for {path}: {payload}")
        except (urllib.error.URLError, OSError) as e:  # сетевые обрывы (RemoteDisconnected и т.п.) — тоже ретраим
            if attempt < retries - 1:
                time.sleep(10 * (attempt + 1))
                continue
            raise RuntimeError(f"network error: {e!r}")
    raise RuntimeError("не удалось выполнить запрос после ретраев")


def api_op(opid: str, retries: int = 8):
    """Опрос async-операции на шлюзе операций (НЕ searchapi — тот отдаёт 404/501)."""
    req = urllib.request.Request(f"{BASE_OP}/operations/{opid}",
                                 headers={"Authorization": "Api-Key " + KEY})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            payload = e.read().decode("utf-8", "replace")[:300]
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(10 * (attempt + 1))
                continue
            raise RuntimeError(f"HTTP {e.code} for operation {opid}: {payload}")
        except (urllib.error.URLError, OSError) as e:  # сетевые обрывы (RemoteDisconnected и т.п.) — тоже ретраим
            if attempt < retries - 1:
                time.sleep(10 * (attempt + 1))
                continue
            raise RuntimeError(f"network error: {e!r}")


def read_keyword_file(path: Path, source: str):
    kws = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split("|")]
        tag = (parts[1] if len(parts) > 1 and parts[1] else "misc")
        if parts[0]:
            kws.append({"keyword": parts[0], "tag": tag, "source": source})
    return kws


def collect_sources():
    """[(path, source, is_queue)] — постоянный список + необработанные queue-файлы."""
    sources = [(KEYWORDS_FILE, "base", False)]
    if QUEUE_DIR.exists():
        for f in sorted(QUEUE_DIR.glob("*.txt")):
            sources.append((f, f"queue:{f.stem}", True))
    return sources


def submit():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_file = STATE_DIR / f"{msk_today()}.json"
    # досбавляем к существующему state (безопасно при повторном запуске submit за ночь)
    if state_file.exists():
        state = json.loads(state_file.read_text(encoding="utf-8"))
        state.setdefault("operations", [])
    else:
        state = {"date": msk_today(), "region": REGION, "operations": [],
                 "submittedAtMsk": msk_now().isoformat()}
    already = {(o["keyword"], o.get("source", "base")) for o in state["operations"]}

    for path, source, is_queue in collect_sources():
        kws = [k for k in read_keyword_file(path, source)
               if (k["keyword"].lower(), source) not in already]
        if not kws:
            print(f"{source}: новых ключей нет, пропуск")
            if is_queue:
                PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
                shutil.move(str(path), str(PROCESSED_DIR / path.name))
            continue
        print(f"{source}: отправляю {len(kws)} ключей (регион {REGION})")
        for i, kw in enumerate(kws, 1):
            body = {
                "query": {
                    "searchType": "SEARCH_TYPE_RU",
                    "queryText": kw["keyword"],
                    "page": 0,
                },
                "groupSpec": {
                    "groupMode": "GROUP_MODE_FLAT",
                    "groupsOnPage": GROUPS_ON_PAGE,
                    "docsInGroup": 1,
                },
                "region": REGION,
                "folderId": FOLDER_ID,
                "responseFormat": "FORMAT_XML",
            }
            resp = api("/v2/web/searchAsync", body, retries=4)
            state["operations"].append({**kw, "operationId": resp["id"]})
            already.add((kw["keyword"].lower(), source))
            if i % 10 == 0:
                print(f"  {source}: отправлено {i}/{len(kws)}")
            time.sleep(1.1)  # квота 1 запрос/сек
        if is_queue:
            PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(PROCESSED_DIR / path.name))
            print(f"{source}: файл перенесён в queue/processed/")
        state_file.write_text(json.dumps(state, ensure_ascii=False, indent=1),
                              encoding="utf-8")
    print(f"state saved: {state_file} ({len(state['operations'])} операций всего)")


def parse_serp(xml_text: str):
    """Позиции по документам (flat): индекс группы + 1. Возвращает (docs, our_best)."""
    root = ET.fromstring(xml_text)
    docs = []
    for group in root.iter("group"):
        doc = group.find("doc")
        if doc is None:
            continue
        url = (doc.findtext("url") or "").strip()
        domain = (doc.findtext("domain") or "").strip()
        docs.append({"position": len(docs) + 1, "domain": domain, "url": url})
    our = next((d for d in docs if SITE_MARKER in d["domain"] or SITE_MARKER in d["url"]), None)
    return docs, our


def collect(date: str | None = None):
    date = date or msk_today()
    state_file = STATE_DIR / f"{date}.json"
    if not state_file.exists():
        # ночь перекатилась: собираем по самому свежему state-файлу
        candidates = sorted(STATE_DIR.glob("*.json"))
        if not candidates:
            sys.exit("нет state-файлов: сначала запустите submit")
        state_file = candidates[-1]
        print(f"state за сегодня не найден, беру последний: {state_file.name}")
    state = json.loads(state_file.read_text(encoding="utf-8"))
    # догружаем результаты из прежних выгрузок, чтобы не терять их при повторном collect
    out_file = RESULTS_DIR / f"{state['date']}.json"
    out = (json.loads(out_file.read_text(encoding="utf-8"))
           if out_file.exists()
           else {"date": state["date"], "region": state.get("region", REGION),
                 "site": SITE_MARKER, "total": 0, "done": 0, "pending": [],
                 "positions": {}})
    results = out["positions"]
    pending = []
    for i, op in enumerate(state["operations"], 1):
        if op["keyword"] in results:
            continue
        try:
            st = api_op(op['operationId'], retries=3)
        except RuntimeError as e:
            # истёкшие/пропавшие операции (404 и др.) не роняют весь сбор
            results[op["keyword"]] = {"tag": op["tag"],
                                      "source": op.get("source", "base"),
                                      "error": f"operation check: {e}"}
            if i % 25 == 0:
                out.update({"total": len(state["operations"]), "done": len(results),
                            "pending": pending})
                (RESULTS_DIR / f"{state['date']}.json").write_text(
                    json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
            continue
        if not st.get("done"):
            pending.append(op["keyword"])
            time.sleep(0.2)
            continue
        raw = base64.b64decode(st["response"]["rawData"]).decode("utf-8", "replace")
        try:
            docs, our = parse_serp(raw)
            results[op["keyword"]] = {
                "tag": op["tag"],
                "source": op.get("source", "base"),
                "position": our["position"] if our else None,
                "url": our["url"] if our else None,
                "inTop100": our is not None,
                "foundDocs": len(docs),
                "top10": [d["domain"] for d in docs[:10]],
            }
        except ET.ParseError as e:
            results[op["keyword"]] = {"tag": op["tag"],
                                      "source": op.get("source", "base"),
                                      "error": f"xml parse: {e}"}
        if i % 25 == 0:
            print(f"  обработано {i}/{len(state['operations'])}")
            out.update({"total": len(state["operations"]), "done": len(results),
                        "pending": pending})
            (RESULTS_DIR / f"{state['date']}.json").write_text(
                json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
        time.sleep(0.2)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out.update({
        "total": len(state["operations"]),
        "done": len(results),
        "pending": pending,
    })
    (RESULTS_DIR / f"{state['date']}.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    (RESULTS_DIR / "latest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    in_top = sum(1 for r in results.values() if r.get("inTop100"))
    print(f"collect: {len(results)} готово, {len(pending)} в ожидании, "
          f"{in_top} ключей в ТОП-100 -> {RESULTS_DIR}/{state['date']}.json")


def sync_one(query: str):
    """ДОРОГАЯ разовая проверка (0.49 руб) — только для отладки парсинга."""
    body = {
        "query": {"searchType": "SEARCH_TYPE_RU", "queryText": query, "page": 0},
        "groupSpec": {"groupMode": "GROUP_MODE_FLAT", "groupsOnPage": GROUPS_ON_PAGE,
                      "docsInGroup": 1},
        "region": REGION, "folderId": FOLDER_ID, "responseFormat": "FORMAT_XML",
    }
    resp = api("/v2/web/search", body, retries=2)
    raw = base64.b64decode(resp["rawData"]).decode("utf-8", "replace")
    docs, our = parse_serp(raw)
    print(f"запрос: {query} | регион {REGION} | документов: {len(docs)}")
    for d in docs[:20]:
        mark = " <== НАШ САЙТ" if SITE_MARKER in d["domain"] or SITE_MARKER in d["url"] else ""
        print(f"{d['position']:3}. {d['domain']}{mark}")
    if our:
        print(f"\nНАША ПОЗИЦИЯ: {our['position']} ({our['url']})")
    else:
        print(f"\n{SITE_MARKER} не найден в ТОП-{len(docs)}")


if __name__ == "__main__":
    if not KEY:
        sys.exit("переменная окружения YANDEX_SEARCH_API_KEY не задана")
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "submit":
        submit()
    elif mode == "collect":
        collect(sys.argv[2] if len(sys.argv) > 2 else None)
    elif mode == "sync-one" and len(sys.argv) > 2:
        sync_one(sys.argv[2])
    else:
        sys.exit(__doc__)
