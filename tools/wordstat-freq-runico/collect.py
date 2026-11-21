# -*- coding: utf-8 -*-
"""Точная частотность Wordstat (GetTop, точное соответствие фразе) по keywords.txt.
Регион: Россия (225) — отдельно от региона сбора позиций (Москва).
Кэш — freq_cache.json: {"фраза": частота|-1}. -1 = не получено, перезапросится.
Инкрементальное сохранение каждые 25 фраз + при любой ошибке/завершении.
"""
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
KEYWORDS_FILE = ROOT / "keywords.txt"
CACHE_FILE = ROOT / "freq_cache.json"

API = "https://searchapi.api.cloud.yandex.net/v2/wordstat/topRequests"
FOLDER_ID = os.environ.get("YANDEX_FOLDER_ID", "b1g4gqlmtd9mncfea3tk")
REGION = os.environ.get("WORDSTAT_REGION", "225")  # 225 = Россия (вся)
KEY = os.environ.get("YANDEX_SEARCH_API_KEY", "")
DELAY = float(os.environ.get("WORDSTAT_DELAY", "2.0"))


def load_phrases():
    out = []
    for line in KEYWORDS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(line.split("|")[0].strip())
    return out


def exact_freq(phrase):
    """ТОЧНОЕ соответствие (как в Wordstat при вводе фразы в кавычках) —
    фраза оборачивается в кавычки в самом запросе; ответ.results[].count
    ВСЕГДА про широкое соответствие (не меняется от кавычек!) — это баг,
    найденный 21.11.2026: нужно брать totalCount именно ПРИ запросе в
    кавычках, а не искать совпадение в results[]."""
    quoted = f'"{phrase}"'
    body = json.dumps({
        "phrase": quoted, "numPhrases": 1, "regions": [REGION],
        "devices": ["DEVICE_ALL"], "folderId": FOLDER_ID,
    }).encode("utf-8")
    req = urllib.request.Request(
        API, data=body,
        headers={"Authorization": f"Api-Key {KEY}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        d = json.loads(r.read().decode("utf-8"))
    return int(d.get("totalCount", 0))


def save(cache):
    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding="utf-8")


def main():
    if not KEY:
        sys.exit("YANDEX_SEARCH_API_KEY не задан")
    phrases = load_phrases()
    cache = json.loads(CACHE_FILE.read_text(encoding="utf-8")) if CACHE_FILE.exists() else {}
    todo = [p for p in phrases if cache.get(p, -1) == -1]
    print(f"фраз: {len(phrases)}, в кэше готово: {len(phrases) - len(todo)}, добираю: {len(todo)}", flush=True)

    fail_streak = 0
    done = 0
    for phrase in todo:
        ok = False
        for attempt in range(4):
            try:
                cache[phrase] = exact_freq(phrase)
                ok = True
                break
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < 3:
                    wait = 60 * (attempt + 1)
                    print(f"429 '{phrase}': жду {wait}с", flush=True)
                    time.sleep(wait)
                else:
                    cache[phrase] = -1
                    print(f"HTTP {e.code} '{phrase}'", flush=True)
                    break
            except (urllib.error.URLError, OSError) as e:
                if attempt < 3:
                    time.sleep(10 * (attempt + 1))
                    continue
                cache[phrase] = -1
                print(f"NET-ERR '{phrase}': {e!r}", flush=True)
                break
            except Exception as e:
                cache[phrase] = -1
                print(f"ERR '{phrase}': {e}", flush=True)
                break
        if not ok and cache.get(phrase) == -1:
            fail_streak += 1
            save(cache)
            if fail_streak >= 20:
                print("20 подряд неудач — пауза 30 мин", flush=True)
                time.sleep(1800)
                fail_streak = 0
        else:
            fail_streak = 0
            done += 1
            if done % 25 == 0:
                save(cache)
                print(f"{done}/{len(todo)}", flush=True)
        time.sleep(DELAY)

    save(cache)
    minus = sum(1 for v in cache.values() if v == -1)
    print(f"ГОТОВО: кэш {len(cache)}, без частоты: {minus}", flush=True)


if __name__ == "__main__":
    main()
