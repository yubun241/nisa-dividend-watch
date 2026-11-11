# -*- coding: utf-8 -*-
"""NISA 配当ウォッチ — 長期保有向け 高配当株 一覧ツール

起動:  source /Users/luna/python/env1/bin/activate && python app.py
データ: Yahoo Finance (yfinance)。銘柄リストは watchlist.json に保存される。
"""
import json
import math
import pickle
import re
import socket
import sys
import threading
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from flask import Flask, jsonify, request, send_from_directory

BASE = Path(__file__).resolve().parent
WATCHLIST_FILE = BASE / "watchlist.json"
NIKKEI_FILE = BASE / "nikkei225.json"   # 日経225 構成銘柄のスナップショット
CACHE_FILE = BASE / "cache.pkl"         # 取得済みデータ(再起動してもすぐ表示できるように保存)
HOST, PORT = "127.0.0.1", 5057
CACHE_TTL = 30 * 60          # 秒。これより古ければ自動で再取得
NISA_GROWTH_LIMIT = 2_400_000  # 成長投資枠の年間上限

EXTRA_DEFAULTS = [{"code": "1489", "name": "NF・日経高配当株50 ETF", "sector": "ETF"}]

SECTOR_JA = {
    "Financial Services": "金融",
    "Industrials": "資本財",
    "Consumer Cyclical": "一般消費財",
    "Consumer Defensive": "生活必需品",
    "Healthcare": "ヘルスケア",
    "Technology": "情報技術",
    "Communication Services": "通信",
    "Energy": "エネルギー",
    "Basic Materials": "素材",
    "Real Estate": "不動産",
    "Utilities": "公益",
}

app = Flask(__name__, static_folder=str(BASE / "static"))
_lock = threading.Lock()
_cache = {"rows": {}, "detail": {}, "fetched_at": 0.0, "failed": []}
_state = {"refreshing": False, "error": None}


# ───────────────────────── 日経225 構成銘柄 ─────────────────────────
def parse_nikkei225(wikitext):
    """Wikipedia「日経平均株価」の『構成銘柄一覧』節(業種ごとの表)を解析する。"""
    a = wikitext.find("構成銘柄一覧")
    if a < 0:
        return []
    m = re.search(r"\n==[^=]", wikitext[a + 10:])
    body = wikitext[a: a + 10 + m.start()] if m else wikitext[a:]
    out, seen, sector = [], set(), None
    for block in re.split(r"\n(?====)", body):
        h = re.match(r"=+\s*(.+?)\s*[（(]\d+銘柄[）)]\s*=+", block)
        if not h:
            continue
        sector = h.group(1)
        for row in block.split("\n|-")[1:]:
            cells = [c.strip() for c in re.split(r"\|\||\n\|", "\n" + row.strip().lstrip("|")) if c.strip() != "}"]
            cells = [c for c in cells if c or len(cells) > 2]
            if len(cells) < 2 or not re.fullmatch(r"\d{3}[0-9A-Z]", cells[0]):
                continue
            name = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]*)\]\]", r"\1", cells[1])
            name = re.sub(r"<[^>]*>|\{\{[^}]*\}\}", "", name).strip()
            if name and cells[0] not in seen:
                seen.add(cells[0])
                out.append({"code": cells[0], "name": name, "sector": sector})
    return out


def fetch_nikkei225():
    """最新の構成銘柄を取得してスナップショットを更新。失敗したら手元のスナップショットを返す。"""
    try:
        r = requests.get("https://ja.wikipedia.org/w/api.php",
                         params={"action": "parse", "page": "日経平均株価", "prop": "wikitext", "format": "json"},
                         headers={"User-Agent": "dividend-watch/1.0"}, timeout=20)
        items = parse_nikkei225(r.json()["parse"]["wikitext"]["*"])
        if 215 <= len(items) <= 235:   # 形式が変わって壊れた結果は採用しない
            NIKKEI_FILE.write_text(json.dumps(items, ensure_ascii=False, indent=1), encoding="utf-8")
            return items, None
        err = "構成銘柄の取得結果が %d 件で不自然なため、保存済みの一覧を使いました" % len(items)
    except Exception as e:
        err = "構成銘柄の取得に失敗したため、保存済みの一覧を使いました (%s)" % e
    if NIKKEI_FILE.exists():
        return json.loads(NIKKEI_FILE.read_text(encoding="utf-8")), err
    return [], err


def sync_nikkei225():
    """ウォッチリストを 日経225 全銘柄 + 自分で追加した銘柄 にそろえる。"""
    items, err = fetch_nikkei225()
    if not items:
        return err or "構成銘柄を取得できませんでした"
    codes = {e["code"] for e in items}
    old = json.loads(WATCHLIST_FILE.read_text(encoding="utf-8")) if WATCHLIST_FILE.exists() else EXTRA_DEFAULTS
    save_watchlist(items + [e for e in old if e["code"] not in codes])
    return err


# ───────────────────────── ウォッチリスト ─────────────────────────
def load_watchlist():
    if WATCHLIST_FILE.exists():
        try:
            return json.loads(WATCHLIST_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    sync_nikkei225()
    return json.loads(WATCHLIST_FILE.read_text(encoding="utf-8")) if WATCHLIST_FILE.exists() else []


def save_watchlist(wl):
    WATCHLIST_FILE.write_text(json.dumps(wl, ensure_ascii=False, indent=1), encoding="utf-8")


def normalize_code(raw):
    code = (raw or "").strip().upper().replace("．", ".")
    code = code.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    if code.endswith(".T"):
        code = code[:-2]
    return code if re.fullmatch(r"[0-9A-Z.\-]{1,10}", code) else ""


def to_symbol(code):
    """4桁コード(285A のような新形式含む)は東証、それ以外は米国株などとしてそのまま。"""
    return code + ".T" if re.fullmatch(r"\d{3}[0-9A-Z]", code) else code


def lookup_japanese_name(code):
    """Yahoo!ファイナンス(日本)のページタイトルから日本語名を拾う。失敗したら None。"""
    try:
        r = requests.get("https://finance.yahoo.co.jp/quote/%s" % to_symbol(code),
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        m = re.search(r"<title>(.+?)【", r.text)
        if r.ok and m:
            return m.group(1).replace("(株)", "").strip()
    except Exception:
        pass
    return None


# ───────────────────────── 指標計算 ─────────────────────────
def num(v):
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def rnd(v, n=2):
    v = num(v)
    return None if v is None else round(v, n)


def pct_change_since(close, days):
    """days 日前(暦日)の直近終値からの騰落率(%)。履歴が足りなければ None。"""
    target = close.index[-1] - timedelta(days=days)
    if close.index[0] > target + timedelta(days=10):
        return None
    past = close[close.index <= target]
    if past.empty:
        return None
    return (close.iloc[-1] / past.iloc[-1] - 1) * 100


def rsi14(close):
    d = close.diff().dropna()
    if len(d) < 30:
        return None
    up = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    if dn.iloc[-1] == 0:
        return 100.0
    return 100 - 100 / (1 + up.iloc[-1] / dn.iloc[-1])


def judge_entry(r):
    """機械的なエントリー目安。スコアと根拠を返す(投資助言ではない)。"""
    score, why = 0, []

    y, y5 = r.get("yield"), r.get("yield_5y_avg")
    if y and y5 and not r.get("yield_suspect"):
        ratio = y / y5
        if ratio >= 1.15:
            score += 2; why.append(("+", "利回り %.2f%% が5年平均 %.2f%% を大きく上回る(割安寄り)" % (y, y5)))
        elif ratio >= 1.0:
            score += 1; why.append(("+", "利回りが5年平均 %.2f%% 以上" % y5))
        elif ratio <= 0.8:
            score -= 1; why.append(("-", "利回りが5年平均 %.2f%% を大きく下回る(割高寄り)" % y5))

    p = r.get("pos52")
    if p is not None:
        if p <= 25:
            score += 2; why.append(("+", "52週レンジの下位 %d%% 付近(安値圏)" % p))
        elif p <= 45:
            score += 1; why.append(("+", "52週レンジの中央より下(%d%%)" % p))
        elif p >= 90:
            score -= 2; why.append(("-", "52週高値のすぐ近く(%d%%)" % p))
        elif p >= 75:
            score -= 1; why.append(("-", "52週レンジの上位(%d%%)" % p))

    rsi = r.get("rsi")
    if rsi is not None:
        if rsi <= 30:
            score += 2; why.append(("+", "RSI %.0f:売られすぎ水準" % rsi))
        elif rsi <= 40:
            score += 1; why.append(("+", "RSI %.0f:やや売られ気味" % rsi))
        elif rsi >= 70:
            score -= 2; why.append(("-", "RSI %.0f:買われすぎ水準" % rsi))

    d = r.get("dev200")
    if d is not None:
        if d <= 0:
            score += 1; why.append(("+", "200日移動平均を %.1f%% 下回る(押し目)" % abs(d)))
        elif d >= 20:
            score -= 1; why.append(("-", "200日移動平均から +%.1f%% 上方乖離" % d))

    po = r.get("payout")
    if po is not None and po > 100:
        score -= 2; why.append(("!", "配当性向 %.0f%%:利益以上に配当しており減配リスク" % po))
    elif po is not None and po > 80:
        why.append(("!", "配当性向 %.0f%%:やや高め" % po))
    if r.get("div_warn"):
        why.append(("!", r["div_warn"]))
    if r.get("div_cut_recent"):
        score -= 1; why.append(("!", "直近数年に減配あり"))

    if score >= 4:
        label, level = "買い場候補", 2
    elif score >= 2:
        label, level = "押し目気味", 1
    elif score <= -2:
        label, level = "過熱気味・待ち", -1
    else:
        label, level = "中立", 0
    return {"score": score, "label": label, "level": level, "why": why}


def fetch_info(symbol):
    for attempt in range(2):
        try:
            info = yf.Ticker(symbol).info
            if info:
                return info
        except Exception:
            time.sleep(0.8)
    return {}


def build_row(code, name, hist, info, sector=None):
    close = hist["Close"].dropna()
    if len(close) < 2:
        return None, None
    divs = hist["Dividends"].fillna(0)
    divs = divs[divs > 0]
    adj = hist["Adj Close"].dropna()
    last_date = close.index[-1]

    price = num(info.get("regularMarketPrice")) or num(info.get("currentPrice")) or float(close.iloc[-1])
    prev = num(info.get("regularMarketPreviousClose")) or num(info.get("previousClose")) or float(close.iloc[-2])

    y1 = close[close.index >= last_date - timedelta(days=365)]
    hi52, lo52 = float(max(y1.max(), price)), float(min(y1.min(), price))
    y3 = close[close.index >= last_date - timedelta(days=365 * 3)]

    # 配当: 会社予想(年間)があればそれ、無ければ過去12ヶ月の実績
    ttm = float(divs[divs.index >= last_date - timedelta(days=365)].sum())
    fwd = num(info.get("dividendRate"))
    div_warn = None
    if fwd and fwd > 0 and ttm > 0 and not (0.5 <= fwd / ttm <= 2.0):
        # 株式分割直後などで予想配当が分割前の値のまま残っていることがある → 実績を採用
        div_rate, div_kind = ttm, "実績"
        div_warn = "予想配当(%.1f)が直近実績(%.1f)と大きく食い違うため実績値を表示。株式分割・特別配当・大幅な増減配の可能性があるので公式情報を確認してください" % (fwd, ttm)
    elif fwd and fwd > 0:
        div_rate, div_kind = fwd, "予想"
    elif ttm > 0:
        div_rate, div_kind = ttm, "実績"
    else:
        div_rate, div_kind = None, ""
    dy = div_rate / price * 100 if div_rate and price else None
    y5avg = num(info.get("fiveYearAvgDividendYield"))
    yield_suspect = bool(dy and (dy > 12 or (y5avg and not (0.4 <= dy / y5avg <= 2.5))))
    if yield_suspect and not div_warn:
        div_warn = "利回り %.1f%% は過去水準と比べて不自然です。株式分割などで元データの配当額がずれている可能性があるので公式情報を確認してください" % dy

    # 暦年ごとの配当(権利落ち日ベース)
    this_year = last_date.year
    by_year = divs.groupby(divs.index.year).sum()
    first_year = hist.index[0].year + 1   # 取得期間の最初の年は欠けている可能性があるので除外
    years = [{"year": int(y), "div": round(float(v), 2), "partial": int(y) == this_year}
             for y, v in by_year.items() if y >= first_year]
    full = [d["div"] for d in years if not d["partial"]]
    no_cut = 0
    for a, b in zip(reversed(full[:-1]), reversed(full[1:])):
        if b >= a * 0.995:
            no_cut += 1
        else:
            break
    recent = full[-4:]
    div_cut_recent = any(b < a * 0.97 for a, b in zip(recent[:-1], recent[1:]))
    div_cagr = None
    if len(full) >= 4 and full[-4] > 0 and full[-1] > 0:
        div_cagr = ((full[-1] / full[-4]) ** (1 / 3) - 1) * 100

    # 権利確定月と配当の受取月。
    # 権利確定月 = 直近およそ1年の権利落ち日の月(権利落ち日は権利確定日の前営業日なので通常は同じ月)。
    # 受取月は Yahoo に日本株の支払日が無いため目安: 日本株は権利確定の約3か月後、ETF は約1か月後、
    # 海外株は権利落ち日の約2週間後として計算する。
    is_jp = to_symbol(code).endswith(".T")
    is_etf = info.get("quoteType") == "ETF"
    by_month = {}
    for d, v in divs[divs.index >= last_date - timedelta(days=400)].items():
        if not is_jp:
            pay = (d + timedelta(days=16)).month
        else:
            pay = (d.month - 1 + (1 if is_etf else 3)) % 12 + 1
        by_month[d.month] = {"m": int(d.month), "pay": int(pay), "amount": round(float(v), 2), "ex": d.strftime("%Y-%m-%d")}
    div_months = [by_month[m] for m in sorted(by_month)]

    ma = {n: close.rolling(n).mean() for n in (25, 75, 200)}

    def dev(n):
        m = num(ma[n].iloc[-1])
        return (price / m - 1) * 100 if m else None

    payout = num(info.get("payoutRatio"))
    ex_ts = info.get("exDividendDate")
    ex_date = None
    if ex_ts:
        try:
            ex_date = datetime.utcfromtimestamp(int(ex_ts)).strftime("%Y-%m-%d")
        except Exception:
            pass

    lot = 100 if is_jp and not is_etf else 1
    weekly = y3.resample("W").last().dropna()

    row = {
        "code": code,
        "name": name or info.get("longName") or info.get("shortName") or code,
        "sector": sector or SECTOR_JA.get(info.get("sector"), "ETF" if info.get("quoteType") == "ETF" else (info.get("sector") or "—")),
        "currency": info.get("currency") or ("JPY" if is_jp else "USD"),
        "price": rnd(price, 1 if is_jp else 2),
        "chg_1d": rnd((price / prev - 1) * 100 if prev else None),
        "chg_1d_abs": rnd(price - prev if prev else None, 1 if is_jp else 2),
        "chg_1y": rnd(pct_change_since(close, 365), 1),
        "chg_3y": rnd(pct_change_since(close, 365 * 3), 1),
        "total_3y": rnd(pct_change_since(adj, 365 * 3), 1),
        "div_rate": rnd(div_rate),
        "div_kind": div_kind,
        "div_warn": div_warn,
        "yield_suspect": yield_suspect,
        "yield": rnd(dy),
        "yield_5y_avg": rnd(info.get("fiveYearAvgDividendYield")),
        "payout": rnd(payout * 100 if payout is not None else None, 1),
        "per": rnd(info.get("trailingPE"), 1),
        "pbr": rnd(info.get("priceToBook")),
        "mcap": num(info.get("marketCap")),
        "hi52": rnd(hi52, 1),
        "lo52": rnd(lo52, 1),
        "pos52": round((price - lo52) / (hi52 - lo52) * 100) if hi52 > lo52 else None,
        "from_hi3y": rnd((price / float(max(y3.max(), price)) - 1) * 100, 1),
        "rsi": rnd(rsi14(close), 1),
        "dev25": rnd(dev(25), 1),
        "dev75": rnd(dev(75), 1),
        "dev200": rnd(dev(200), 1),
        "ex_date": ex_date,
        "div_months": div_months,
        "record_months": [d["m"] for d in div_months],
        "pay_months": sorted({d["pay"] for d in div_months}),
        "lot": lot,
        "lot_cost": rnd(price * lot, 0),
        "lot_div": rnd(div_rate * lot if div_rate else None, 0),
        "div_no_cut_years": no_cut,
        "div_cut_recent": div_cut_recent,
        "div_cagr3": rnd(div_cagr, 1),
        "spark": [rnd(v, 1) for v in weekly.tolist()],
        "last_date": last_date.strftime("%Y-%m-%d"),
    }
    row["signal"] = judge_entry(row)

    detail = {
        "dates": [d.strftime("%Y-%m-%d") for d in close.index],
        "close": [rnd(v, 2) for v in close.tolist()],
        "ma75": [rnd(v, 2) for v in ma[75].tolist()],
        "ma200": [rnd(v, 2) for v in ma[200].tolist()],
        "div_years": years,
    }
    return row, detail


def fetch(entries):
    """entries: [{"code","name"}] → (rows dict, detail dict, 取得できなかったコード)"""
    symbols = {e["code"]: to_symbol(e["code"]) for e in entries}
    names = {e["code"]: e.get("name") for e in entries}
    sectors = {e["code"]: e.get("sector") for e in entries}
    data = yf.download(list(set(symbols.values())), period="10y", auto_adjust=False, actions=True,
                       group_by="ticker", progress=False, threads=True)
    with ThreadPoolExecutor(max_workers=8) as ex:
        infos = dict(zip(symbols, ex.map(fetch_info, symbols.values())))

    rows, details, failed = {}, {}, []
    for code, sym in symbols.items():
        try:
            hist = data[sym].dropna(how="all")
            row, detail = build_row(code, names[code], hist, infos.get(code) or {}, sectors[code])
        except Exception as e:  # 1銘柄の失敗で全体を止めない
            print("[warn] %s: %s" % (code, e), file=sys.stderr)
            row = None
        if row:
            rows[code], details[code] = row, detail
        else:
            failed.append(code)
    return rows, details, failed


def refresh_all():
    try:
        wl = load_watchlist()
        rows, details, failed = fetch(wl)
        if not rows:
            raise RuntimeError("1銘柄も取得できませんでした(ネット接続を確認してください)")
        with _lock:
            _cache.update(rows=rows, detail=details, fetched_at=time.time(), failed=failed)
            snapshot = dict(_cache)
        _state["error"] = None
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(snapshot, f)
    except Exception as e:
        print("[error] refresh: %s" % e, file=sys.stderr)
        _state["error"] = "データ取得に失敗しました: %s" % e
    finally:
        _state["refreshing"] = False


def start_refresh():
    """バックグラウンドで全銘柄を再取得(多重起動はしない)。"""
    with _lock:
        if _state["refreshing"]:
            return
        _state["refreshing"] = True
    threading.Thread(target=refresh_all, daemon=True).start()


def load_cache():
    try:
        with open(CACHE_FILE, "rb") as f:
            _cache.update(pickle.load(f))
    except Exception:
        pass


def payload():
    wl = load_watchlist()
    with _lock:
        rows = [_cache["rows"][e["code"]] for e in wl if e["code"] in _cache["rows"]]
        ts = _cache["fetched_at"]
        return {
            "rows": rows,
            "total": len(wl),
            "failed": _cache.get("failed", []),
            "fetched_at": datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else "",
            "refreshing": _state["refreshing"],
            "error": _state["error"],
            "nisa_limit": NISA_GROWTH_LIMIT,
        }


# ───────────────────────── ルーティング ─────────────────────────
@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/data")
def api_data():
    stale = time.time() - _cache["fetched_at"] > CACHE_TTL
    if request.args.get("refresh") or stale:
        start_refresh()          # 取得は裏で実行。画面は手元のデータをすぐ表示し、完了を待って差し替える
    return jsonify(payload())


@app.route("/api/sync225", methods=["POST"])
def api_sync225():
    err = sync_nikkei225()
    start_refresh()
    d = payload()
    d["sync_note"] = err
    return jsonify(d)


@app.route("/api/detail/<code>")
def api_detail(code):
    with _lock:
        d = _cache["detail"].get(code)
    return (jsonify(d), 200) if d else (jsonify({"error": "not found"}), 404)


@app.route("/api/watchlist", methods=["POST"])
def api_add():
    code = normalize_code((request.get_json(silent=True) or {}).get("code"))
    if not code:
        return jsonify({"error": "銘柄コードを入力してください(例: 8306)"}), 400
    wl = load_watchlist()
    if any(e["code"] == code for e in wl):
        return jsonify({"error": "%s は登録済みです" % code}), 400
    entry = {"code": code, "name": lookup_japanese_name(code), "sector": None}
    try:
        rows, details, failed = fetch([entry])
    except Exception as e:
        return jsonify({"error": "取得に失敗しました: %s" % e}), 502
    if failed:
        return jsonify({"error": "%s のデータが見つかりませんでした" % code}), 404
    entry["name"] = rows[code]["name"]
    wl.append(entry)
    save_watchlist(wl)
    with _lock:
        _cache["rows"].update(rows)
        _cache["detail"].update(details)
    return jsonify(payload())


@app.route("/api/watchlist/<code>", methods=["DELETE"])
def api_remove(code):
    save_watchlist([e for e in load_watchlist() if e["code"] != code])
    with _lock:
        _cache["rows"].pop(code, None)
        _cache["detail"].pop(code, None)
    return jsonify(payload())


def port_in_use():
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex((HOST, PORT)) == 0


if __name__ == "__main__":
    url = "http://%s:%d/" % (HOST, PORT)
    load_cache()
    if port_in_use():
        print("すでに起動しています。ブラウザを開きます: " + url)
        webbrowser.open(url)
        sys.exit(0)
    if "--no-browser" not in sys.argv:
        threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    print("NISA 配当ウォッチを起動しました → " + url)
    print("終了するにはこのウィンドウで Ctrl+C を押すか、ウィンドウを閉じてください。")
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
