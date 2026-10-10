"""5-minute signal alert for the C + D + E models (no AI needed), pushed to Telegram.

Rules (unchanged from the backtests; nothing added):
  E long : BTC 5m close < 48 bars ago; coin 24h quote vol >= 10M; 24h change <= 0;
           close > prior-48-bar high * 1.003; first such close in 1 h; stop = 12-bar low - tick; exit after 4 h.
  E short: BTC 5m close > 48 bars ago; vol >= 10M; 24h change >= 0;
           close < prior-48-bar low * 0.997; first such close in 1 h; stop = 12-bar high + tick (record only).
  C      : ETHUSDT short only; 5m close below the prior 72-bar low; entry next open; stop entry + 2 ATR14;
           trailing stop = close + 3 ATR14 after each close (lower only); at most 24 h; one position at a time.
  D      : any coin with 12-bar median quote volume >= 5000U; short when RSI14 crosses back below 90 on a
           red bar closing <= the previous close, long when it crosses back above 10 on a green bar closing
           >= the previous close; the best score (|previous RSI - 50| + volume ratio capped at 10) is taken;
           entry next open; stop 2 ATR14; trailing 5 ATR14 from closes; at most 4 h; one position at a time;
           skipped when size, margin or liquidation limits fail.
C and D positions are replayed from recent bars on every cycle (the replay keeps their one-position rule
and trailing stops), so an exit notice is also sent when their stop is hit or their time is up.

Each cycle checks only the last CLOSED 5m bar, so a signal is sent once.

Environment variables:
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID  (required to send; without them the message is printed)
  EQUITY (default 100), RISK_LONG (0.02), RISK_SHORT (0.01), RISK_C (0.02), RISK_D (0.02), CAP_D (0.01)
  DURATION seconds to keep looping (default 0 = one pass), HEARTBEAT (1 = send a start message)
"""
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np
import requests

BASE = "https://www.binance.com/fapi/v1"
BRACKETS = "https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets"
BJT = timezone(timedelta(hours=8))
FEE, SLIP = 0.0005, 0.0002
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
            f = {x["filterType"]: x for x in s["filters"]}
            lot = f.get("MARKET_LOT_SIZE") if float(f.get("MARKET_LOT_SIZE", {}).get("stepSize", 0)) else f.get("LOT_SIZE", {})
            meta[s["symbol"]] = {"tick": float(f["PRICE_FILTER"]["tickSize"]), "step": float(lot.get("stepSize", 0)),
                                 "minq": float(lot.get("minQty", 0)), "maxq": float(lot.get("maxQty", 1e18)),
                                 "minn": float(f.get("MIN_NOTIONAL", {}).get("notional", 5))}
    try:
        br = S.post(BRACKETS, json={}, timeout=20).json()["data"]["brackets"]
        for b in br:
            if b["symbol"] in meta:
                t = b["riskBrackets"][0]
                meta[b["symbol"]].update(L=t["maxOpenPosLeverage"], mmr=t["bracketMaintenanceMarginRate"])
    except Exception:  # noqa: BLE001 - leverage falls back to defaults
        pass
    return meta


def closed_klines(sym, now_ms, limit=310):
    k = get("/klines", symbol=sym, interval="5m", limit=limit)
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
        if qv24 >= 10e6:
            out.append(("long", ["E做多"], l[-LB:].min() - tick, qv24, ch24))
    if c[-1] < l[-1 - N:-1].min() * (1 - EXT) and ch24 >= 0 and btc_up and qv24 >= 10e6 \
            and not any(c[-1 - k] < l[-1 - k - N:-1 - k].min() for k in range(1, LB + 1)):
        out.append(("short", ["E做空"], h[-LB:].max() + tick, qv24, ch24))
    return out


# ---------------------------------------------------------------- C and D
def rsi_atr(a):
    """RSI14 (exponential average, alpha 1/14) and ATR14 (simple average of 14 true ranges)."""
    h, l, c = a[:, 1], a[:, 2], a[:, 3]
    n = len(c)
    rsi = np.full(n, np.nan)
    if n > 1:
        d = np.diff(c)
        up, dn = np.maximum(d, 0), np.maximum(-d, 0)
        x, y = up[0], dn[0]
        for k in range(1, n):
            if k > 1:
                x += (up[k - 1] - x) / 14
                y += (dn[k - 1] - y) / 14
            rsi[k] = (100.0 if x > 0 else np.nan) if y == 0 else 100 - 100 / (1 + x / y)
    pc = np.concatenate([[np.nan], c[:-1]])
    tr = np.fmax(h - l, np.fmax(np.abs(h - pc), np.abs(l - pc)))
    cs = np.concatenate([[0.0], np.cumsum(tr)])
    atr = np.full(n, np.nan)
    if n >= 14:
        atr[13:] = (cs[14:] - cs[:-14]) / 14
    return rsi, atr


def step_exit(pos, a, atr, j, k_trail, max_bars, slip):
    """Advance an open C/D position through bar j; returns (reason, price) when it exits."""
    o, h, l, c = a[j, 0], a[j, 1], a[j, 2], a[j, 3]
    sg = pos["sg"]
    if j > pos["e"]:
        if pos["liq"] is not None and sg * (o - pos["liq"]) <= 0:
            return "liquidated", o
        if sg * (o - pos["trail"]) <= 0:
            return "gap", o * (1 - sg * slip)
        if j == pos["e"] + max_bars:
            return "time", o * (1 - sg * slip)
    if (sg == 1 and l <= pos["trail"]) or (sg == -1 and h >= pos["trail"]):
        return ("stop" if pos["trail"] == pos["stop"] else "trail"), pos["trail"] * (1 - sg * slip)
    if atr[j] > 0:
        tc = c - sg * k_trail * atr[j]
        pos["trail"] = max(pos["trail"], tc) if sg == 1 else min(pos["trail"], tc)
    return None


def d_candidates(data, start_t):
    cands = {}
    for s, (t, a, rsi, atr) in data.items():
        o, c, qv = a[:, 0], a[:, 3], a[:, 4]
        for i in range(14, len(c)):
            if t[i] < start_t:
                continue
            p, r = rsi[i - 1], rsi[i]
            if np.isnan(p) or np.isnan(r) or not atr[i] > 0:
                continue
            if p > 90 and r <= 90 and c[i] < o[i] and c[i] <= c[i - 1]:
                side = "short"
            elif p < 10 and r >= 10 and c[i] > o[i] and c[i] >= c[i - 1]:
                side = "long"
            else:
                continue
            med12 = float(np.median(qv[i - 11:i + 1])) if i >= 11 else 0.0
            if med12 < 5000:
                continue
            prev = qv[max(0, i - 24):i]
            m = float(np.median(prev)) if len(prev) else 0.0
            ratio = 10.0 if m <= 0 else min(10.0, qv[i] / m)
            cands.setdefault(t[i], []).append((-(abs(p - 50) + ratio), s, side, i, med12))
    return cands


def d_open(m, side, atr_i, med12, px, eq, risk, capp):
    """Size a D entry at price px with the size, margin and liquidation limits; None when it must be skipped."""
    sg = 1 if side == "long" else -1
    fill = px * (1 + sg * SLIP)
    raw = fill - sg * 2 * atr_i
    stop = (math.floor(raw / m["tick"] + 1e-9) if sg == 1 else math.ceil(raw / m["tick"] - 1e-9)) * m["tick"]
    if stop <= 0 or sg * (px - stop) <= 0:
        return None
    unit = abs(fill - stop) + FEE * fill + FEE * stop + SLIP * stop
    L, mmr = m.get("L") or 20, m.get("mmr") or 0.01
    qty = min(eq * risk / unit, min(capp * med12, 10000.0) / fill, 0.95 * eq / (1.0 / L + FEE) / fill, m.get("maxq", 1e18))
    if m.get("step"):
        qty = math.floor(qty / m["step"] + 1e-9) * m["step"]
    if qty <= 0 or qty < m.get("minq", 0) or qty * fill < m.get("minn", 5.0):
        return None
    e_ = eq - FEE * qty * fill
    liq = (fill - e_ / qty) / (1 - mmr) if sg == 1 else (fill + e_ / qty) / (1 + mmr)
    if sg * (stop - liq) <= 0:
        return None
    return dict(side=side, sg=sg, fill=fill, stop=stop, trail=stop, liq=liq, qty=qty, unit=unit)


def d_run(data, meta, eq, risk, capp, times, last_t, log=None):
    """Replay D (one position across all coins) over `times`; report what happens on the last bar."""
    cands = d_candidates(data, times[0])
    pos, events, pending = None, [], None
    for T in times:
        if pos is not None:
            t, a, _, atr = data[pos["sym"]]
            j = pos["idx"].get(T)
            if j is not None and j >= pos["e"]:
                ex = step_exit(pos, a, atr, j, 5, 48, SLIP)
                if ex:
                    if log is not None:
                        log.append(dict(pos, exit_t=T, why=ex[0], px=ex[1]))
                    if T == last_t:
                        events.append((pos, ex))
                    pos = None
                elif T == last_t and j == pos["e"] + 47:
                    events.append((pos, None))
        if pos is None and T in cands:
            for _, s, side, i, med12 in sorted(cands[T]):
                t, a, _, atr = data[s]
                if i + 1 < len(t):
                    p = d_open(meta[s], side, atr[i], med12, a[i + 1, 0], eq, risk, capp)
                    if p:
                        p.update(sym=s, e=i + 1, entry_t=t[i + 1], idx={v: k for k, v in enumerate(t)})
                        pos = p
                        break
                elif T == last_t:
                    p = d_open(meta[s], side, atr[i], med12, a[i, 3], eq, risk, capp)
                    if p:
                        p.update(sym=s, ref=a[i, 3], atr=atr[i])
                        pending = p
                        break
    return pos, events, pending


def c_run(t, a, atr, last_t, log=None):
    """Replay C on ETHUSDT (one position at a time); report what happens on the last bar."""
    o, l, c = a[:, 0], a[:, 2], a[:, 3]
    pos, events, pending = None, [], None
    for i in range(72, len(c)):
        if pos is not None:
            ex = step_exit(pos, a, atr, i, 3, 288, 0.0)
            if ex:
                if log is not None:
                    log.append(dict(pos, exit_t=t[i], why=ex[0], px=ex[1]))
                if t[i] == last_t:
                    events.append((pos, ex))
                pos = None
            elif t[i] == last_t and i == pos["e"] + 287:
                events.append((pos, None))
        if pos is None and atr[i] > 0 and c[i] < l[i - 72:i].min():
            if i + 1 < len(c):
                fill = o[i + 1]
                pos = dict(sym="ETHUSDT", side="short", sg=-1, fill=fill, stop=fill + 2 * atr[i],
                           trail=fill + 2 * atr[i], liq=None, e=i + 1, entry_t=t[i + 1])
            elif t[i] == last_t:
                pending = dict(sym="ETHUSDT", side="short", sg=-1, ref=c[i], atr=atr[i])
    return pos, events, pending


WHY = {"stop": "碰到初始止损", "trail": "碰到跟踪止损", "gap": "开盘跳空穿过止损", "time": "到最长持仓时间",
       "liquidated": "开盘跳空到强平价"}


def fmt_exit(model, pos, ex, bar):
    head = f"📤 {model} 平仓提醒 {pos['sym']} {'做多' if pos['sg'] == 1 else '做空'}（{bj(pos['entry_t'])} 入场约 {pos['fill']:.6g}）："
    if ex is None:
        return head + f"到最长持仓时间，{bj(bar + 300_000)} 开盘市价平仓"
    return head + f"{WHY[ex[0]]}，约 {ex[1]:.6g} 已平仓"


def fmt_entry(model, p, bar, eq, risk, meta, k_trail, hours, label):
    s, sg, ref, atr = p["sym"], p["sg"], p["ref"], p["atr"]
    sign = "-" if sg == 1 else "+"
    stop = p.get("stop", ref - sg * 2 * atr)
    dist = abs(ref - stop) / ref
    notional = p["qty"] * p["fill"] if "qty" in p else eq * risk / (dist + 2 * FEE)
    m = meta.get(s, {})
    lev = safe_lev(ref, stop, p["side"], m.get("L", 20), m.get("mmr", 0.01))
    return (f"{'🟢' if sg == 1 else '🔴'}{model}{'做多' if sg == 1 else '做空'} {s}（{label}）\n"
            f"📌 入场：下一根开盘市价（{bj(bar + 300_000)}），参考价 {ref:.6g}\n"
            f"🛑 初始止损：成交价{sign}2×ATR，按参考价约 {stop:.6g}（{dist * 100:.2f}%）\n"
            f"📈 跟踪止损：每根 5 分钟收盘后移到 收盘价{sign}{k_trail}×ATR（现在约 {k_trail * atr / ref * 100:.2f}%），"
            f"只往有利方向移；碰到时会提醒\n"
            f"⏱ 最长 {hours} 小时：{bj(bar + 300_000 + hours * 3_600_000)} 开盘平仓\n"
            f"{eq:.0f}U 风险 {eq * risk:.1f}U → 仓位约 {notional:.0f}U，杠杆 ≤{lev}x，保证金约 {notional / lev:.1f}U")


def one_pass(meta, last_done):
    now_ms = int(time.time() * 1000)
    tb, ab = closed_klines("BTCUSDT", now_ms)
    bar = tb[-1]
    if bar == last_done:
        return bar
    btc_down, btc_up = ab[-1, 3] < ab[-1 - N, 3], ab[-1, 3] > ab[-1 - N, 3]
    eq = float(os.environ.get("EQUITY", "100"))
    rl, rs = float(os.environ.get("RISK_LONG", "0.02")), float(os.environ.get("RISK_SHORT", "0.01"))
    rc, rd = float(os.environ.get("RISK_C", "0.02")), float(os.environ.get("RISK_D", "0.02"))
    capd = float(os.environ.get("CAP_D", "0.01"))
    lines = []

    def fetch(sym):
        try:
            return sym, closed_klines(sym, now_ms)
        except Exception:  # noqa: BLE001
            return sym, ([], None)

    with ThreadPoolExecutor(max_workers=8) as pool:
        data = list(pool.map(fetch, list(meta)))
    fresh = {}
    for sym, (t, a) in data:
        m = meta[sym]
        if not t or t[-1] != bar:
            continue
        fresh[sym] = (t, a)
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
                f"⏱ 平仓：E 在 {bj(bar + 300_000 + 4 * 3_600_000)} 手动市价平仓（不设固定止盈）\n"
                f"{eq:.0f}U 风险 {risk:.1f}U → 仓位约 {notional:.0f}U，杠杆 ≤{lev}x，保证金约 {notional / lev:.1f}U\n"
                f"24h 额 {qv24 / 1e6:.0f}M，24h {ch24 * 100:+.1f}%")
    # D: replay the last ~17.5 h (RSI warmed up on the 100 bars before) across all fresh coins
    dd = {s: (t, a, *rsi_atr(a)) for s, (t, a) in fresh.items() if len(t) >= 120}
    _, d_events, d_pending = d_run(dd, meta, eq, rd, capd, tb[max(0, len(tb) - 211):], bar)
    lines += [fmt_exit("D", p, ex, bar) for p, ex in d_events if not (ex and ex[0] == "time")]
    if d_pending:
        lines.append(fmt_entry("D", d_pending, bar, eq, rd, meta, 5, 4, "RSI 极值回头"))
    # C: replay ETHUSDT over ~3.5 days
    tc, ac = closed_klines("ETHUSDT", now_ms, limit=1000)
    if tc and tc[-1] == bar:
        _, c_events, c_pending = c_run(tc, ac, rsi_atr(ac)[1], bar)
        lines += [fmt_exit("C", p, ex, bar) for p, ex in c_events if not (ex and ex[0] == "time")]
        if c_pending:
            lines.append(fmt_entry("C", c_pending, bar, eq, rc, meta, 3, 24, "跌破前 6 小时最低价"))
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
        send(f"✅ C+D+E 信号监控已启动，持续约 {duration // 3600} 小时，每 5 分钟检查一次（{len(meta)} 个币）")
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
