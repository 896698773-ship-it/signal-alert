"""5-minute signal alert for the C and E models (no AI needed), pushed to Telegram.

Rules (identical to scans/d_model/e_model.py and the C-R5 file; nothing added):
  E long : BTC 5m close < 48 bars ago; coin 24h quote vol >= 10M; 24h change <= 0;
           close > prior-48-bar high * 1.003; first such close in 1 h; stop = 12-bar low - tick.
  E short: BTC 5m close > 48 bars ago; vol >= 10M; 24h change >= 0;
           close < prior-48-bar low * 0.997; first such close in 1 h; stop = 12-bar high + tick.
  C-R5   : same as E long but vol >= 5M (exit: 15m pivot trailing instead of the 4 h exit).
Entry for all: market at the next 5m open; a missed bar is not chased.

Each cycle checks only the last CLOSED 5m bar, so a signal is sent once.

Environment variables:
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID  (required to send; without them the message is printed)
  EQUITY (default 100), RISK_LONG (0.02), RISK_SHORT (0.01)
  DURATION seconds to keep looping (default 0 = one pass), HEARTBEAT (1 = send a start message)
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np
import requests

BASE = "https://www.binance.com/fapi/v1"
BRACKETS = "https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets"
BJT = timezone(timedelta(hours=8))
FEE = 0.0005
N, EXT, LB = 48, 0.003, 12
S = requests.Session()


def bj(ms):
    return datetime.fromtimestamp(ms / 1000, BJT).strftime("%m-%d %H:%M")


def send(text):
    tok, chat = os.environ.get("TELEGRAM_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        print(text)
        return
    for _ in range(3):
        try:
            r = S.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                       json={"chat_id": chat, "text": text, "disable_web_page_preview": True}, timeout=20)
            if r.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    print("telegram send failed:", text[:200])


def get(path, **params):
    r = S.get(BASE + path, params=params, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"binance {path} http {r.status_code}")
    return r.json()


def universe():
    ex = get("/exchangeInfo")
    meta = {}
    for s in ex["symbols"]:
        if (s["contractType"] == "PERPETUAL" and s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
                and s.get("underlyingType") == "COIN"):
            tick = float(next(f["tickSize"] for f in s["filters"] if f["filterType"] == "PRICE_FILTER"))
            meta[s["symbol"]] = {"tick": tick}
    try:
        br = S.post(BRACKETS, json={}, timeout=20).json()["data"]["brackets"]
        for b in br:
            if b["symbol"] in meta:
                t = b["riskBrackets"][0]
                meta[b["symbol"]].update(L=t["maxOpenPosLeverage"], mmr=t["bracketMaintenanceMarginRate"])
    except Exception:  # noqa: BLE001 - leverage is informational only
        pass
    return meta


def closed_klines(sym, now_ms):
    k = get("/klines", symbol=sym, interval="5m", limit=310)
    k = [x for x in k if x[6] < now_ms]
    a = np.array([[float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[7])] for x in k])
    return [int(x[0]) for x in k], a


def safe_lev(entry, stop, side, L, mmr):
    best = 1
    for k in range(1, int(L or 1) + 1):
        liq = entry * (1 - 1 / k) / (1 - mmr) if side == "long" else entry * (1 + 1 / k) / (1 + mmr)
        if (side == "long" and liq < stop) or (side == "short" and liq > stop):
            best = k
    return best


def check(sym, t, a, btc_up, btc_down, tick):
    o, h, l, c, qv = a.T
    if len(c) < 300:
        return []
    qv24, ch24 = qv[-288:].sum(), c[-1] / c[-289] - 1
    out = []
    if c[-1] > h[-1 - N:-1].max() * (1 + EXT) and ch24 <= 0 and btc_down \
            and not any(c[-1 - k] > h[-1 - k - N:-1 - k].max() for k in range(1, LB + 1)):
        stop = l[-LB:].min() - tick
        models = (["E做多"] if qv24 >= 10e6 else []) + (["C"] if qv24 >= 5e6 else [])
        if models:
            out.append(("long", models, stop, qv24, ch24))
    if c[-1] < l[-1 - N:-1].min() * (1 - EXT) and ch24 >= 0 and btc_up and qv24 >= 10e6 \
            and not any(c[-1 - k] < l[-1 - k - N:-1 - k].min() for k in range(1, LB + 1)):
        out.append(("short", ["E做空"], h[-LB:].max() + tick, qv24, ch24))
    return out


def one_pass(meta, last_done):
    now_ms = int(time.time() * 1000)
    tick24 = {x["symbol"]: float(x["quoteVolume"]) for x in get("/ticker/24hr")}
    tb, ab = closed_klines("BTCUSDT", now_ms)
    bar = tb[-1]
    if bar == last_done:
        return bar
    btc_down, btc_up = ab[-1, 3] < ab[-1 - N, 3], ab[-1, 3] > ab[-1 - N, 3]
    eq = float(os.environ.get("EQUITY", "100"))
    rl, rs = float(os.environ.get("RISK_LONG", "0.02")), float(os.environ.get("RISK_SHORT", "0.01"))
    lines = []
    syms = [s for s in meta if tick24.get(s, 0) >= 4e6]

    def fetch(sym):
        try:
            return sym, closed_klines(sym, now_ms)
        except Exception:  # noqa: BLE001
            return sym, ([], None)

    with ThreadPoolExecutor(max_workers=8) as pool:
        data = list(pool.map(fetch, syms))
    for sym, (t, a) in data:
        m = meta[sym]
        if not t or t[-1] != bar:
            continue
        for side, models, stop, qv24, ch24 in check(sym, t, a, btc_up, btc_down, m["tick"]):
            ref = a[-1, 3]
            dist = abs(ref - stop) / ref
            risk = eq * (rl if side == "long" else rs)
            notional = risk / (dist + 2 * FEE)
            lev = safe_lev(ref, stop, side, m.get("L", 20), m.get("mmr", 0.01))
            note = "（建议暂停实盘，仅记录）" if side == "short" else ""
            lines.append(
                f"{'🟢做多' if side == 'long' else '🔴做空'} {sym} [{'+'.join(models)}]{note}\n"
                f"📌 入场价 {ref:.6g}（限价单，挂到 {bj(bar + 600_000)} 没成交就撤，不追）\n"
                f"🛑 止损价 {stop:.6g}（距离 {dist * 100:.2f}%，下单同时挂好）\n"
                f"⏱ 平仓：E 在 {bj(bar + 300_000 + 4 * 3_600_000)} 手动市价平仓（不设固定止盈）；C 按 15m 保护点抬止损\n"
                f"{eq:.0f}U 风险 {risk:.1f}U → 仓位约 {notional:.0f}U，杠杆 ≤{lev}x，保证金约 {notional / lev:.1f}U\n"
                f"24h 额 {qv24 / 1e6:.0f}M，24h {ch24 * 100:+.1f}%")
    if lines:
        send(f"⏰ {bj(bar)} 收盘信号（BTC 4h {'跌' if btc_down else '涨'}）\n\n" + "\n\n".join(lines))
    else:
        print(bj(bar), "no signal")
    return bar


def main():
    duration = int(os.environ.get("DURATION", "0"))
    end = time.time() + duration
    try:
        meta = universe()
    except Exception as e:  # noqa: BLE001
        send(f"⚠️ 提醒脚本连不上币安：{e}")
        raise
    if os.environ.get("HEARTBEAT", "1") == "1":
        send(f"✅ C/E 信号监控已启动，持续约 {duration // 3600} 小时，每 5 分钟检查一次（{len(meta)} 个币）")
    last, errors = None, 0
    while True:
        try:
            last = one_pass(meta, last)
            errors = 0
        except Exception as e:  # noqa: BLE001
            errors += 1
            if errors == 3:
                send(f"⚠️ 提醒脚本连续出错：{e}")
        if time.time() >= end:
            break
        nxt = (int(time.time()) // 300 + 1) * 300 + 20  # 20 s after the next 5m close
        time.sleep(max(5, nxt - time.time()))


if __name__ == "__main__":
    main()
