#!/usr/bin/env python3
"""
משיכת נתונים לסורק מניות ת"א-125 (תחליף לסקריפט ה-PowerShell המקומי).

מה הסקריפט עושה:
  1. קורא את tickers.txt (שם = ISIN או סימול.TA).
  2. טוען את data.js הקודם (כדי למשוך רק נתונים חדשים - טעינה אינקרמנטלית).
  3. מושך מ-Yahoo Finance מחירי סגירה ונפחים יומיים (מכל הצדדים בבת אחת).
  4. פעם ביממה מרענן גם מכפיל רווח (P/E) ודוחות כספיים.
  5. כותב data.js באותו פורמט שהסורק מכיר, ואת tickers_report.txt.

אם Yahoo חוסם את הריצה (מסומן ככשל), data.js הקודם נשאר כמו שהוא והסקריפט
יוצא עם קוד שגיאה - כך שהאתר לא ייפגע.

שימוש:  python fetch_data.py [--force-full] [--dir PATH]
"""
import argparse
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import yfinance as yf

IL = ZoneInfo("Asia/Jerusalem")
HISTORY_YEARS = 5            # היסטוריה מינימלית בפעם הראשונה
OVERLAP_DAYS = 7             # חפיפה בכל משיכה אינקרמנטלית (לתיקון נתונים שהתעדכנו)
SPLIT_TOLERANCE = 0.02       # פער מחיר בחפיפה שמעבר לו נטען מחדש את כל ההיסטוריה של המניה
FUND_EVERY_HOURS = 20        # כל כמה שעות מרעננים P/E ודוחות
MAX_FAIL_RATIO = 0.5         # אם יותר מחצי המניות נכשלו - הריצה נחשבת כושלת
UA = {"User-Agent": "Mozilla/5.0 (compatible; tase-scanner/1.0)"}


def log(*a):
    print(*a, flush=True)


def now_il():
    return datetime.now(IL).strftime("%Y-%m-%dT%H:%M:%S")


def retry(fn, tries=3, base=3.0):
    """מריץ פונקציה עם ניסיונות חוזרים והמתנה גדלה (נגד חסימות זמניות של Yahoo)."""
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(base * (2 ** i))
    raise last


# ---------- קריאת tickers.txt ו-data.js ----------

def read_tickers(path):
    """מחזיר רשימת (שם, ערך) כש-ערך הוא ISIN או סימול."""
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if "=" not in line:
                continue
            name, val = (p.strip() for p in line.split("=", 1))
            if name and val:
                out.append((name, val))
    return out


def load_data_js(path):
    """טוען את data.js הקיים. אם אין או שהוא פגום - מחזיר מבנה ריק."""
    try:
        raw = open(path, encoding="utf-8").read()
        return json.loads(raw[raw.index("=") + 1:].strip().rstrip(";"))
    except Exception:  # noqa: BLE001
        return {"updated": None, "full": None, "fx": None, "stocks": {}}


def write_data_js(path, data):
    txt = "window.TASE_DATA=" + json.dumps(data, separators=(",", ":"), ensure_ascii=False) + ";"
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(txt)
    os.replace(tmp, path)  # כתיבה אטומית: אף פעם לא נשאר קובץ חצי-כתוב


# ---------- סימולים ----------

ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")


def resolve_isin(isin):
    """מחפש ב-Yahoo סימול של הבורסה בתל אביב (.TA) לפי ISIN. None אם לא נמצא."""
    def go():
        r = requests.get(
            "https://query2.finance.yahoo.com/v1/finance/search",
            params={"q": isin, "quotesCount": 8, "newsCount": 0},
            headers=UA, timeout=20,
        )
        r.raise_for_status()
        return r.json().get("quotes", [])
    try:
        quotes = retry(go, tries=2, base=2)
    except Exception:  # noqa: BLE001
        return None
    for q in quotes:
        s = q.get("symbol", "")
        if s.endswith(".TA"):
            return s
    return None


def pick_symbol(name, val, prev):
    """סימול ידני > סימול מהריצה הקודמת > חיפוש לפי ISIN."""
    if not ISIN_RE.match(val):
        return val, "manual"
    p = prev.get(name)
    if p and p.get("sym"):
        return p["sym"], "known"
    s = resolve_isin(val)
    return (s, "resolved") if s else (None, "not-found")


# ---------- מחירים ----------

def clean_rows(df, factor):
    """DataFrame של Yahoo -> [[YYYY-MM-DD, close, volume], ...]."""
    rows = []
    if df is None or len(df) == 0:
        return rows
    for idx, r in df.iterrows():
        c = r.get("Close")
        if c is None or (isinstance(c, float) and math.isnan(c)):
            continue
        v = r.get("Volume")
        v = 0.0 if v is None or (isinstance(v, float) and math.isnan(v)) else float(v)
        d = idx.strftime("%Y-%m-%d")
        rows.append([d, round(float(c) / factor, 4), int(v)])
    return rows


def download_batch(symbols, start):
    """מושך את כל הסימולים יחד. מחזיר {סימול: DataFrame}."""
    import pandas as pd
    out = {}
    if not symbols:
        return out
    df = retry(lambda: yf.download(
        symbols, start=start, interval="1d", auto_adjust=False, actions=False,
        group_by="ticker", progress=False, threads=False, timeout=30))
    if df is None or len(df) == 0:
        return out
    if not isinstance(df.columns, pd.MultiIndex):  # סימול יחיד
        out[symbols[0]] = df
        return out
    top = set(df.columns.get_level_values(0))
    for s in symbols:
        if s in top:
            d = df[s].dropna(subset=["Close"])
            if len(d):
                out[s] = d
    return out


def full_history(sym, years=HISTORY_YEARS):
    """היסטוריה מלאה למניה אחת + זיהוי מטבע (אגורות ILA -> שקלים)."""
    t = yf.Ticker(sym)
    start = (datetime.now(IL) - timedelta(days=365 * years + 10)).strftime("%Y-%m-%d")
    df = retry(lambda: t.history(start=start, interval="1d", auto_adjust=False, actions=False))
    ccy = ""
    try:
        ccy = (t.history_metadata or {}).get("currency", "") or ""
    except Exception:  # noqa: BLE001
        pass
    factor = 100.0 if ccy.upper() == "ILA" or (not ccy and sym.endswith(".TA")) else 1.0
    return clean_rows(df, factor)


def merge_rows(old, new):
    """ממזג שורות חדשות לישנות לפי תאריך (החדש גובר), ממוין."""
    m = {r[0]: r for r in old}
    for r in new:
        m[r[0]] = r
    return [m[k] for k in sorted(m)]


def overlap_mismatch(old, new):
    """בודק אם המחירים בחפיפה שונים בהרבה (פיצול מניה / שינוי יחידות)."""
    om = {r[0]: r[1] for r in old}
    for r in new:
        o = om.get(r[0])
        if o and o > 0 and abs(r[1] / o - 1) > SPLIT_TOLERANCE:
            return True
    return False


# ---------- מכפיל רווח ודוחות ----------

def _num(x):
    try:
        if x is None:
            return None
        f = float(x)
        return None if math.isnan(f) else f
    except Exception:  # noqa: BLE001
        return None


def _stmt(df, limit):
    """דוח רווח והפסד של yfinance -> [[תאריך, הכנסות, רווח נקי, רווח למניה], ...]."""
    if df is None or getattr(df, "empty", True):
        return []

    def row(*names):
        for n in names:
            if n in df.index:
                return df.loc[n]
        return None

    rev = row("Total Revenue", "Operating Revenue")
    ni = row("Net Income", "Net Income Common Stockholders")
    eps = row("Diluted EPS", "Basic EPS")
    out = []
    for col in list(df.columns)[:limit]:
        r, n, e = (_num(s[col]) if s is not None else None for s in (rev, ni, eps))
        out.append([
            col.strftime("%Y-%m-%d"),
            None if r is None else int(round(r)),
            None if n is None else int(round(n)),
            None if e is None else round(e, 4),
        ])
    return out


def fetch_fundamentals(sym):
    """מחזיר (pe, fin) או None אם Yahoo נכשל לגמרי."""
    t = yf.Ticker(sym)
    info = retry(lambda: t.info, tries=2, base=3)
    pe = _num((info or {}).get("trailingPE"))
    pe = None if pe is None else round(pe, 2)
    fin = {"q": [], "a": []}
    try:
        q = getattr(t, "quarterly_income_stmt", None)
        a = getattr(t, "income_stmt", None)
        fin = {"q": _stmt(q, 4), "a": _stmt(a, 3)}
    except Exception:  # noqa: BLE001
        pass
    return pe, fin


def fetch_fx():
    try:
        df = retry(lambda: yf.Ticker("USDILS=X").history(period="5d"), tries=2, base=2)
        v = _num(df["Close"].dropna().iloc[-1])
        return None if v is None else round(v, 4)
    except Exception:  # noqa: BLE001
        return None


# ---------- הרצה ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=".", help="תיקייה עם tickers.txt ו-data.js")
    ap.add_argument("--force-full", action="store_true", help="לרענן גם P/E ודוחות עכשיו")
    args = ap.parse_args()
    base = args.dir
    tickers = read_tickers(os.path.join(base, "tickers.txt"))
    data_path = os.path.join(base, "data.js")
    old = load_data_js(data_path)
    prev = old.get("stocks", {})
    log(f"{len(tickers)} מניות ב-tickers.txt, {len(prev)} בנתונים הקודמים")

    # א. סימולים
    plan, report = [], []
    for name, val in tickers:
        sym, how = pick_symbol(name, val, prev)
        if not sym:
            report.append((name, val, "לא נמצא סימול ב-Yahoo - הוסף ידנית ב-tickers.txt"))
            continue
        plan.append((name, sym))
        report.append((name, sym, "נמצא" if how != "resolved" else "נמצא (חיפוש לפי ISIN)"))
    log(f"{len(plan)} עם סימול, {len(tickers) - len(plan)} בלי")

    # ב. מחירים: אינקרמנטלי לכולם בבת אחת
    last_dates = []
    for name, sym in plan:
        p = prev.get(name)
        if p and p.get("sym") == sym and p.get("rows"):
            last_dates.append(p["rows"][-1][0])
    if last_dates:
        start = (datetime.strptime(min(last_dates), "%Y-%m-%d") - timedelta(days=OVERLAP_DAYS)).strftime("%Y-%m-%d")
    else:
        start = (datetime.now(IL) - timedelta(days=OVERLAP_DAYS + 3)).strftime("%Y-%m-%d")
    batch_syms = [s for n, s in plan if prev.get(n, {}).get("sym") == s and prev.get(n, {}).get("rows")]
    log(f"מושך מחירים מ-{start} עבור {len(batch_syms)} מניות…")
    try:
        got = download_batch(batch_syms, start)
    except Exception as e:  # noqa: BLE001
        log(f"שגיאה: משיכת המחירים נכשלה ({str(e)[:120]}). הנתונים הקודמים נשארים.")
        sys.exit(1)

    stocks, failed, refetched = {}, [], []
    for name, sym in plan:
        p = prev.get(name) or {}
        rows = p.get("rows") or []
        need_full = not rows or p.get("sym") != sym
        if not need_full:
            df = got.get(sym)
            if df is None:
                failed.append(name)
                stocks[name] = p  # נשארים עם הנתונים הקודמים
                continue
            cut = (datetime.strptime(rows[-1][0], "%Y-%m-%d") - timedelta(days=OVERLAP_DAYS)).strftime("%Y-%m-%d")
            new = [r for r in clean_rows(df, 100.0 if sym.endswith(".TA") else 1.0) if r[0] >= cut]
            if overlap_mismatch(rows, new):
                need_full = True
            else:
                rows = merge_rows(rows, new)
        if need_full:
            refetched.append(name)
            try:
                rows = full_history(sym)
                time.sleep(0.4)
            except Exception as e:  # noqa: BLE001
                log(f"  כשל בהיסטוריה מלאה: {name} ({sym}): {e}")
                failed.append(name)
                stocks[name] = p if p else {"sym": sym, "pe": None, "fin": {"q": [], "a": []}, "rows": []}
                continue
        stocks[name] = {"sym": sym, "pe": p.get("pe"), "fin": p.get("fin") or {"q": [], "a": []}, "rows": rows}

    if refetched:
        log(f"נטענה היסטוריה מלאה ל-{len(refetched)} מניות: {', '.join(refetched[:10])}")
    if plan and len(failed) / len(plan) > MAX_FAIL_RATIO:
        log(f"שגיאה: {len(failed)} מתוך {len(plan)} מניות נכשלו (כנראה חסימה של Yahoo). הנתונים הקודמים נשארים.")
        sys.exit(1)

    # ג. מכפיל רווח ודוחות - פעם ביממה (או לפי דרישה, או למניות חדשות)
    full_stamp = old.get("full")
    due = args.force_full or not full_stamp
    if not due:
        try:
            age = datetime.now(IL).replace(tzinfo=None) - datetime.strptime(full_stamp, "%Y-%m-%dT%H:%M:%S")
            due = age > timedelta(hours=FUND_EVERY_HOURS)
        except Exception:  # noqa: BLE001
            due = True
    todo = [n for n, _ in plan if due or not prev.get(n)]
    fund_ok = 0
    if todo:
        log(f"מרענן P/E ודוחות ל-{len(todo)} מניות (בהפסקות קצרות)…")
        for name in todo:
            try:
                pe, fin = fetch_fundamentals(stocks[name]["sym"])
                stocks[name]["pe"] = pe
                if fin["q"] or not (stocks[name].get("fin") or {}).get("q"):
                    stocks[name]["fin"] = fin  # לא מוחקים דוחות קיימים אם Yahoo החזיר ריק
                fund_ok += 1
            except Exception as e:  # noqa: BLE001
                log(f"  כשל בדוחות: {name}: {str(e)[:80]}")
            time.sleep(0.7)
        log(f"P/E ודוחות עודכנו ל-{fund_ok} מתוך {len(todo)}")
    if todo and fund_ok >= 0.8 * len(todo):
        full_stamp = now_il()

    # ד. כתיבה (שומרים על הסדר הקודם; חדשות בסוף)
    ordered = {k: stocks[k] for k in prev if k in stocks}
    ordered.update({k: v for k, v in stocks.items() if k not in ordered})
    fx = fetch_fx() or old.get("fx")
    out = {"updated": now_il(), "full": full_stamp or now_il(), "fx": fx, "stocks": ordered}
    write_data_js(data_path, out)

    with open(os.path.join(base, "tickers_report.txt"), "w", encoding="utf-8") as f:
        f.write(f"עודכן: {out['updated']}\n")
        f.write(f"נטענו {len(ordered)} מניות; נכשלו בריצה זו: {len(failed)}\n\n")
        for name, sym, st in report:
            f.write(f"{name}\t{sym}\t{st}\n")
    log(f"נכתב data.js: {len(ordered)} מניות, נכשלו {len(failed)} ({', '.join(failed[:8])})")


if __name__ == "__main__":
    main()
