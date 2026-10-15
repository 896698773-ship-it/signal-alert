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
  F      : on each closed 1h bar: any coin whose close is >= 25% above the close 24 h earlier for the
           first time (the previous hour was below) with 24h quote volume >= 10M -> short at the next 1h open;
           stop entry + 2 ATR14(1h); exit 24 h after entry; one trade per coin at a time. Adds: when an hourly close
           is at least 0.5 x the stop distance below the last entry, add one more unit at the next open (at most 5);
           after each add the stop of the whole position moves so the loss at the stop is at most the first unit's risk.
           Pushed: only entries on weekdays 08:00-11:59 BJT, without adds, at F_WIN_RISK (10%) per trade; the
           full F (all hours, adds, 1%) stays in the daily record for comparison.
C and D positions are replayed from recent bars on every cycle (the replay keeps their one-position rule
and trailing stops), so an exit notice is also sent when their stop is hit or their time is up. F is replayed
from the last 99 hourly bars at every hour close.

Each cycle checks only the last CLOSED 5m bar, so a signal is sent once.

Daily record: after 08:00 Beijing time the previous day is replayed with the same rules (every trade that
closed that day, as pushed), sent as "昨日战绩" and appended to records/daily.csv / daily.md in the repo.

Environment variables:
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID  (required to send; without them the message is printed)
  EQUITY (default 100), RISK_LONG (0.02), RISK_SHORT (0.01), RISK_C (0.02), RISK_D (0.02), CAP_D (0.01)
  DURATION seconds to keep looping (default 0 = one pass), HEARTBEAT (1 = send a start message)
"""
import math
import os
import subprocess
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
                print("telegram sent:", text[:60].replace("\n", " "), flush=True)
                return
            print("telegram error", r.status_code, r.text[:300], flush=True)
        except requests.RequestException as e:
            print("telegram request failed:", e, flush=True)
        time.sleep(3)
    print("telegram send failed:", text[:200], flush=True)


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
    """D entry: stop 2 ATR from the fill, rounded outward to the tick."""
    sg = 1 if side == "long" else -1
    raw = px * (1 + sg * SLIP) - sg * 2 * atr_i
    stop = (math.floor(raw / m["tick"] + 1e-9) if sg == 1 else math.ceil(raw / m["tick"] - 1e-9)) * m["tick"]
    return sized(m, side, px, stop, med12, eq, risk, capp)


def sized(m, side, px, stop, med12, eq, risk, capp):
    """Size an entry at price px with a given stop under the size, margin and liquidation limits; None = skip."""
    sg = 1 if side == "long" else -1
    fill = px * (1 + sg * SLIP)
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
                        p.update(sym=s, e=i + 1, entry_t=t[i + 1], idx={v: k for k, v in enumerate(t)},
                                 atr_i=atr[i], med12=med12, px_open=a[i + 1, 0])
                        pos = p
                        break
                elif T == last_t:
                    p = d_open(meta[s], side, atr[i], med12, a[i, 3], eq, risk, capp)
                    if p:
                        p.update(sym=s, ref=a[i, 3], atr=atr[i], med12=med12)
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


R_AGGR, CAP_AGGR = float(os.environ.get("RISK_D_AGGR", "0.60")), float(os.environ.get("CAP_D_AGGR", "0.02"))


def last_value(col, default=None):
    """Latest value of a column in records/daily.csv (the D aggressive account is carried there)."""
    p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "records", "daily.csv")
    lines = [x for x in open(p).read().splitlines() if x] if os.path.exists(p) else []
    if len(lines) < 2 or col not in lines[0].split(","):
        return default
    row, k = lines[-1].split(","), lines[0].split(",").index(col)
    return float(row[k]) if k < len(row) and row[k] else default


def e_aggr_line(sym, ref, stop, med12, meta, eq_a):
    """E long aggressive sizing (60% of its own account per stop, one position at a time), shown apart."""
    head = f"🔥 E激进版（独立账户 {eq_a:.0f}U，目标每笔止损亏 {R_AGGR * 100:.0f}%，一次只做一笔，和 30% 方案分开）："
    if eq_a < 5:
        return head + "账户已亏光，不再下单"
    q = sized(meta[sym], "long", ref, stop, med12, eq_a, R_AGGR, CAP_AGGR)
    if not q:
        return head + "这单超出仓位或强平限制，跳过"
    n, lev = q["qty"] * q["fill"], meta[sym].get("L") or 20
    return head + (f"仓位约 {n:.0f}U，最大杠杆 {lev}x 保证金约 {n / lev:.1f}U，碰止损约亏 {q['qty'] * q['unit']:.1f}U；"
                   f"上一笔激进单还没平仓就跳过这单")


def aggr_line(p, meta, eq_a):
    """GPT's aggressive D sizing (60% of its own account per stop, notional <= 2% of 12-bar volume), shown apart."""
    head = f"🔥 GPT激进版（独立账户 {eq_a:.0f}U，目标每笔止损亏 {R_AGGR * 100:.0f}%，和 30% 方案分开）："
    if eq_a < 5:
        return head + "账户已亏光，不再下单"
    q = d_open(meta[p["sym"]], p["side"], p["atr"], p["med12"], p["ref"], eq_a, R_AGGR, CAP_AGGR)
    if not q:
        return head + "这单超出仓位或强平限制，跳过"
    n, lev = q["qty"] * q["fill"], meta[p["sym"]].get("L") or 20
    return head + f"仓位约 {n:.0f}U，最大杠杆 {lev}x 保证金约 {n / lev:.1f}U，碰止损约亏 {q['qty'] * q['unit']:.1f}U"


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
                f"24h 额 {qv24 / 1e6:.0f}M，24h {ch24 * 100:+.1f}%"
                + ("\n" + e_aggr_line(sym, ref, stop, float(np.median(a[-12:, 4])), meta, last_value("E_aggr_eq", 100.0))
                   if side == "long" else ""))
    # D: replay the last ~17.5 h (RSI warmed up on the 100 bars before) across all fresh coins
    dd = {s: (t, a, *rsi_atr(a)) for s, (t, a) in fresh.items() if len(t) >= 120}
    _, d_events, d_pending = d_run(dd, meta, eq, rd, capd, tb[max(0, len(tb) - 211):], bar)
    lines += [fmt_exit("D", p, ex, bar) for p, ex in d_events if not (ex and ex[0] == "time")]
    if d_pending:
        lines.append(fmt_entry("D", d_pending, bar, eq, rd, meta, 5, 4, "RSI 极值回头") + "\n"
                     + aggr_line(d_pending, meta, last_value("D_aggr_eq", 100.0)))
    # C: replay ETHUSDT over ~3.5 days
    tc, ac = closed_klines("ETHUSDT", now_ms, limit=1000)
    if tc and tc[-1] == bar:
        _, c_events, c_pending = c_run(tc, ac, rsi_atr(ac)[1], bar)
        lines += [fmt_exit("C", p, ex, bar) for p, ex in c_events if not (ex and ex[0] == "time")]
        if c_pending:
            lines.append(fmt_entry("C", c_pending, bar, eq, rc, meta, 3, 24, "跌破前 6 小时最低价"))
    if (bar + 300_000) % HOUR == 0:  # the last 5m bar of an hour: that 1h bar has closed too
        lines += f_lines(meta, now_ms, bar + 300_000 - HOUR, eq)
    if lines:
        send(f"⏰ {bj(bar)} 收盘信号（BTC 4h {'跌' if btc_down else '涨'}）\n\n" + "\n\n".join(lines))
    else:
        print(bj(bar), "no signal")
    return bar


# ---------------------------------------------------------------- F (record only)
F_X, F_ADDS, F_STEP, F_RISK = 0.25, 5, 0.5, 0.01
F_WIN, F_WIN_RISK = (8, 12), 0.10  # pushed F: weekday entries 08:00-11:59 BJT, no adds, 10% risk per trade


def f_in_win(ms):
    d = datetime.fromtimestamp(ms / 1000, BJT)
    return F_WIN[0] <= d.hour < F_WIN[1] and d.weekday() < 5
HOUR = 3_600_000


def hourly_klines(sym, now_ms, limit=99):
    k = get("/klines", symbol=sym, interval="1h", limit=limit)
    k = [x for x in k if x[6] < now_ms]
    a = np.array([[float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[7])] for x in k])
    return [int(x[0]) for x in k], a


def f_trades(a, listed=False, adds=None):
    """F on one coin's closed 1h bars (rows open, high, low, close, quote volume). Each trade: signal bar i, entry bar
    e (None = signal on the last bar, entry at the next open), first-unit stop distance D, units [(price, bar)],
    current stop, exit (bar, price, why) or None while open, pend = an add is due at the next open, R when closed.
    listed = the first row is the coin's first hour, so a signal 24 h after listing counts as a first crossing.
    adds = the most adds per trade (default F_ADDS)."""
    max_adds = F_ADDS if adds is None else adds
    out, busy = [], -1
    if len(a) < 25:
        return out
    o, h, l, c, qv = a.T
    n = len(c)
    for i in range(24 if listed else 25, n):
        before = i >= 25 and c[i - 1] / c[i - 25] - 1 >= F_X
        if not (c[i] / c[i - 24] - 1 >= F_X and not before and qv[i - 23:i + 1].sum() >= 10e6):
            continue
        if i + 1 <= busy:
            continue
        hi, lo, pc = h[i - 13:i + 1], l[i - 13:i + 1], c[i - 14:i]
        D = 2 * float(np.mean(np.maximum(hi - lo, np.maximum(np.abs(hi - pc), np.abs(lo - pc)))))
        if not D > 0:
            continue
        if i + 1 >= n:
            out.append(dict(i=i, e=None, D=D, ref=c[i], r24=c[i] / c[i - 24] - 1))
            break
        e = i + 1
        bx = n  # the coin stays busy until the first unit alone would have exited (stop, gap or 24 h), as tested
        for j in range(e, min(n, e + 24)):
            if (j > e and o[j] >= o[e] + D) or h[j] >= o[e] + D or j == e + 23:
                bx = j
                break
        x = dict(i=i, e=e, D=D, units=[(o[e], e)], stop=o[e] + D, exit=None, pend=False)
        for j in range(e, min(n, e + 24)):
            if x["pend"] and o[j] < x["stop"]:
                x["units"].append((o[j], j))
                x["stop"] = min(x["stop"], (sum(p for p, _ in x["units"]) + D) / len(x["units"]))
            x["pend"] = False
            if j > e and o[j] >= x["stop"]:
                x["exit"] = (j, o[j], "gap")
            elif h[j] >= x["stop"]:
                x["exit"] = (j, x["stop"], "stop")
            elif j == e + 23:
                x["exit"] = (j, c[j], "time")
            elif len(x["units"]) - 1 < max_adds and c[j] <= x["units"][-1][0] - F_STEP * D:
                x["pend"] = True
            if x["exit"]:
                break
        if x["exit"]:
            ent, px = o[e], x["exit"][1]
            x["R"] = sum((p - px) / ent - FEE * (p + px) / ent for p, _ in x["units"]) / (D / ent + 2 * FEE)
        busy = bx
        out.append(x)
    return out


def f_lines(meta, now_ms, last_open, eq):
    """Telegram lines for F on the 1h bar that opened at last_open: entries due at the next open on a weekday
    08:00-11:59 BJT (no adds, F_WIN_RISK per trade), and the exits of such trades."""
    def fetch(sym):
        try:
            return sym, hourly_klines(sym, now_ms)
        except Exception:  # noqa: BLE001
            return sym, ([], None)

    out, nxt = [], last_open + HOUR
    if not f_in_win(nxt) and not any(f_in_win(nxt - k * HOUR) for k in range(1, 25)):
        return out
    with ThreadPoolExecutor(max_workers=8) as pool:
        data = list(pool.map(fetch, list(meta)))
    for sym, (t, a) in data:
        if not t or t[-1] != last_open:
            continue
        n = len(t)
        for x in f_trades(a, listed=n < 90, adds=0):
            if x["e"] is None:
                if not f_in_win(nxt):
                    continue
                ref, D = x["ref"], x["D"]
                m = meta.get(sym, {})
                notional = eq * F_WIN_RISK / (D / ref + 2 * FEE)
                lev = safe_lev(ref, ref + D, "short", m.get("L", 20), m.get("mmr", 0.01))
                out.append(f"🟣F做空 {sym}（工作日 08-12 点，24h 涨 {x['r24'] * 100:.0f}% 后做空，不加仓）\n"
                           f"📌 入场：下一根 1 小时开盘市价（{bj(nxt)}），参考价 {ref:.6g}\n"
                           f"🛑 止损：成交价 + 2×ATR(1h)，约 {ref + D:.6g}（{D / ref * 100:.1f}%）\n"
                           f"⏱ {bj(nxt + 24 * HOUR)} 市价平仓（没碰止损的话）\n"
                           f"{eq:.0f}U 风险 {eq * F_WIN_RISK:.0f}U（每笔 {F_WIN_RISK * 100:.0f}%）→ 仓位约 {notional:.0f}U，"
                           f"杠杆 ≤{lev}x，保证金约 {notional / lev:.1f}U")
            elif x["exit"] and x["exit"][0] == n - 1 and f_in_win(t[x["e"]]):
                j, px, why = x["exit"]
                what = {"stop": "碰到止损", "gap": "开盘跳空穿过止损", "time": f"到 24 小时，{bj(nxt)} 开盘市价平仓"}[why]
                out.append(f"📤 F 平仓提醒 {sym}（{bj(t[x['e']])} 入场）：{what}，约 {px:.6g}，结果 {x['R']:+.2f}R"
                           f"（{eq:.0f}U 账户约 {x['R'] * eq * F_WIN_RISK:+.1f}U）")
    return out


# ---------------------------------------------------------------- daily record
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REC = os.path.join(ROOT, "records")
DAY_MS = 86_400_000


def git(*args):
    try:
        return subprocess.run(["git", "-C", ROOT, *args], capture_output=True, text=True, timeout=120).returncode == 0
    except Exception:  # noqa: BLE001
        return False


def e_closed(fdata, tb, ab, meta, d0, d1):
    """E trades as pushed (next open, stop, close 4 h after entry) that exit inside [d0, d1)."""
    out, bpos = [], {v: j for j, v in enumerate(tb)}
    for s, (t, a) in fdata.items():
        o, h, l, c = a[:, 0], a[:, 1], a[:, 2], a[:, 3]
        for i in range(300, len(t) - 1):
            if t[i] < d0 - 49 * 300_000 or t[i] >= d1 or t[i] not in bpos:
                continue
            j = bpos[t[i]]
            bd, bu = ab[j, 3] < ab[j - N, 3], ab[j, 3] > ab[j - N, 3]
            for side, models, stop, _, _ in check(s, t[:i + 1], a[:i + 1], bu, bd, meta[s]["tick"]):
                sg, e, px, k = (1 if side == "long" else -1), i + 1, None, i + 1
                while k < len(t) and k <= i + 48:
                    if k > e and sg * (stop - o[k]) >= 0:
                        px = o[k]
                    elif (l[k] <= stop) if sg == 1 else (h[k] >= stop):
                        px = stop
                    elif k == i + 48:
                        px = c[k]
                    if px is not None:
                        break
                    k += 1
                if px is not None and d0 <= t[k] < d1:
                    r0 = abs(o[e] - stop) / o[e] + 2 * FEE
                    out.append(dict(model=models[0], sym=s, t=t[e], R=(sg * (px / o[e] - 1) - 2 * FEE) / r0, exit_t=t[k],
                                    fill=o[e], stop=stop, px=px, med12=float(np.median(a[i - 11:i + 1, 4]))))
    return out


def daily_summary(meta, d0):
    """Replay the day starting at d0 (Beijing midnight, ms): every C / D / E trade that closed that day."""
    d1, now = d0 + DAY_MS, int(time.time() * 1000)
    eq = float(os.environ.get("EQUITY", "100"))
    rl, rs = float(os.environ.get("RISK_LONG", "0.02")), float(os.environ.get("RISK_SHORT", "0.01"))
    rc, rd = float(os.environ.get("RISK_C", "0.02")), float(os.environ.get("RISK_D", "0.02"))
    capd = float(os.environ.get("CAP_D", "0.01"))
    need = min(1000, int((now - d0) / 300_000) + 360)
    time.sleep(60)  # let the 5-minute pass's request weight leave Binance's one-minute window first

    def fetch(sym):
        time.sleep(0.8)  # about 5 requests per second over 4 threads keeps the request weight low
        try:
            return sym, closed_klines(sym, now, limit=need)
        except Exception:  # noqa: BLE001
            return sym, ([], None)

    with ThreadPoolExecutor(max_workers=4) as pool:
        fdata = {s: (t, a) for s, (t, a) in pool.map(fetch, list(meta)) if t and len(t) >= 120}
    tb, ab = fdata["BTCUSDT"]
    E = e_closed(fdata, tb, ab, meta, d0, d1)
    dd = {s: (t, a, *rsi_atr(a)) for s, (t, a) in fdata.items()}
    dlog, clog = [], []
    dpos, _, _ = d_run(dd, meta, eq, rd, capd, [v for v in tb if v >= d0 - 72 * 300_000], tb[-1], dlog)
    D = [dict(model="D", sym=x["sym"], t=x["entry_t"], R=(x["sg"] * (x["px"] - x["fill"]) - FEE * (x["fill"] + x["px"])) / x["unit"])
         for x in dlog if d0 <= x["exit_t"] < d1]
    tc, ac = closed_klines("ETHUSDT", now, limit=1000)
    cpos, _, _ = c_run(tc, ac, rsi_atr(ac)[1], tc[-1], clog)
    C = [dict(model="C", sym="ETHUSDT", t=x["entry_t"],
              R=((x["fill"] - x["px"]) / x["fill"] - 2 * FEE) / ((x["stop"] - x["fill"]) / x["fill"] + 2 * FEE))
         for x in clog if d0 <= x["exit_t"] < d1]
    el, es = [x for x in E if x["model"] == "E做多"], [x for x in E if x["model"] == "E做空"]
    e0 = eq_e = last_value("E_aggr_eq", 100.0)  # E long aggressive: its own account, one position at a time
    busy = 0
    for x in sorted(el, key=lambda x: x["t"]):
        q = sized(meta[x["sym"]], "long", x["fill"], x["stop"], x["med12"], eq_e, R_AGGR, CAP_AGGR) \
            if x["t"] >= busy and eq_e >= 5 else None
        if q:
            eq_e += q["qty"] * (x["px"] - q["fill"]) - FEE * q["qty"] * (q["fill"] + x["px"])
            busy = x["exit_t"]
    a0 = eq_a = last_value("D_aggr_eq", 100.0)  # GPT's aggressive D: its own compounding account
    for x in sorted((x for x in dlog if d0 <= x["exit_t"] < d1), key=lambda x: x["entry_t"]):
        q = d_open(meta[x["sym"]], x["side"], x["atr_i"], x["med12"], x["px_open"], eq_a, R_AGGR, CAP_AGGR) if eq_a >= 5 else None
        if q:
            eq_a += q["qty"] * (x["sg"] * (x["px"] - q["fill"]) - FEE * (q["fill"] + x["px"]))
    hlimit = min(499, int((now - d0) / HOUR) + 70)  # 24 h of trade + 24 h change + ATR before the day

    def fetch_h(sym):
        time.sleep(0.8)
        try:
            return sym, hourly_klines(sym, now, limit=hlimit)
        except Exception:  # noqa: BLE001
            return sym, ([], None)

    F, F0 = [], []  # with adds (as pushed) / the same signals without adds
    with ThreadPoolExecutor(max_workers=4) as pool:
        for sym, (t, a) in pool.map(fetch_h, list(meta)):
            for L_, adds in ((F, None), (F0, 0)):
                for x in f_trades(a, listed=len(t) < hlimit - 5, adds=adds) if t else []:
                    if x["e"] is not None and x["exit"] and d0 <= t[x["exit"][0]] < d1:
                        L_.append(dict(sym=sym, t=t[x["e"]], R=x["R"], adds=len(x["units"]) - 1, exit_t=t[x["exit"][0]],
                                       win=F_WIN[0] <= datetime.fromtimestamp(t[x["e"]] / 1000, BJT).hour < F_WIN[1],
                                       wd=datetime.fromtimestamp(t[x["e"]] / 1000, BJT).weekday() < 5))
    f0 = eq_f = last_value("F_eq", 100.0)  # F: its own 100U account, 1% risk per trade, compounding
    for x in sorted(F, key=lambda x: x["exit_t"]):
        eq_f *= 1 + F_RISK * x["R"]
    W0 = [x for x in F0 if x["win"] and x["wd"]]  # the pushed F: weekday 08-12 entries, no adds
    WE = [x for x in F0 if x["win"] and not x["wd"]]  # weekend 08-12, for comparison only
    w0 = eq_w = last_value("FW_eq", 100.0)
    for x in sorted(W0, key=lambda x: x["exit_t"]):
        eq_w = max(0.0, eq_w * (1 + F_WIN_RISK * x["R"]))

    def block(name, L, unit):
        if not L:
            return f"{name}：0 笔"
        r = sum(x["R"] for x in L)
        return "\n".join([f"{name}：{len(L)} 笔（赚 {sum(x['R'] > 0 for x in L)} 亏 {sum(x['R'] <= 0 for x in L)}），合计 {r:+.2f}R（{r * unit:+.1f}U）"]
                         + [f"  {bj(x['t']) if x['t'] < d0 else bj(x['t'])[6:]} {x['sym']} {x['R']:+.2f}R"
                            for x in sorted(L, key=lambda x: x["t"])])

    u = sum(x["R"] for x in el) * eq * rl + sum(x["R"] for x in C) * eq * rc + sum(x["R"] for x in D) * eq * rd
    prev = 0.0
    p = os.path.join(REC, "daily.csv")
    if os.path.exists(p):
        rows = [x.split(",") for x in open(p).read().splitlines()[1:] if x]
        prev = float(rows[-1][8]) if rows else 0.0
    day = bj(d0)[:5]
    opened = [f"{m} {x['sym']}（{bj(x['entry_t'])} 入场）" for m, x in (("D", dpos), ("C", cpos)) if x]
    text = (f"📊 昨日战绩 {day}（当天平仓的单，按推送的仓位：{eq:.0f}U，每笔风险 {eq * rl:.0f}U）\n\n"
            + "\n".join([block("E做多", el, eq * rl), block("C做空ETH", C, eq * rc), block("D", D, eq * rd)])
            + f"\n\n合计 {u:+.1f}U；累计 {prev + u:+.1f}U（{eq:.0f}U → {eq + prev + u:.1f}U）\n"
            + f"{block('E做空（只记录）', es, eq * rs).splitlines()[0]}\n"
            + f"🔥 D 的 GPT 激进版（目标每笔止损亏 {R_AGGR * 100:.0f}%，独立 100U 账户，和 30% 方案分开）："
            + f"当天 {eq_a - a0:+.1f}U，{a0:.1f}U → {eq_a:.1f}U\n"
            + f"🔥 E 做多激进版（目标每笔止损亏 {R_AGGR * 100:.0f}%，一次一笔，独立 100U 账户）：当天 {eq_e - e0:+.1f}U，{e0:.1f}U → {eq_e:.1f}U"
            + (f"\n持仓中：{'；'.join(opened)}" if opened else "")
            + f"\n\n🟣 F 工作日 08-12 点（推送版，不加仓，每笔风险 {F_WIN_RISK * 100:.0f}%，独立 100U 账户）：{w0:.1f}U → {eq_w:.1f}U\n"
            + block("F", W0, w0 * F_WIN_RISK)
            + f"\n对照（只记录）：周末 08-12 点 {len(WE)} 笔 {sum(x['R'] for x in WE):+.2f}R；"
            + f"F 全天含加仓 {len(F)} 笔 {sum(x['R'] for x in F):+.2f}R（1% 账户 {f0:.1f}U → {eq_f:.1f}U）")
    row = ",".join([day, str(len(el)), f"{sum(x['R'] for x in el):.3f}", str(len(C)), f"{sum(x['R'] for x in C):.3f}",
                    str(len(D)), f"{sum(x['R'] for x in D):.3f}", f"{u:.2f}", f"{prev + u:.2f}", str(len(es)),
                    f"{sum(x['R'] for x in es):.3f}", f"{eq_a:.2f}", f"{eq_e:.2f}", str(len(F)),
                    f"{sum(x['R'] for x in F):.3f}", f"{eq_f:.2f}", str(len(W0)), f"{sum(x['R'] for x in W0):.3f}",
                    f"{eq_w:.2f}", f"{sum(x['R'] for x in WE):.3f}"])
    return day, text, row


def maybe_daily(meta, state):
    now = datetime.now(BJT)
    if now.hour < 8 or time.time() < state.get("next_try", 0):
        return
    d0 = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000) - DAY_MS
    day = bj(d0)[:5]
    if state.get("done") == day:
        return
    git("pull", "-q", "--rebase")
    p = os.path.join(REC, "daily.csv")
    if os.path.exists(p) and any(x.startswith(day + ",") for x in open(p)):
        state["done"] = day
        return
    day, text, row = daily_summary(meta, d0)
    send(text)
    state["done"] = day
    os.makedirs(REC, exist_ok=True)
    head = "date,E_long_n,E_long_R,C_n,C_R,D_n,D_R,day_U,cum_U,E_short_n,E_short_R,D_aggr_eq,E_aggr_eq,F_n,F_R,F_eq,FW_n,FW_R,FW_eq,FW_weekend_R"
    old = [x for x in open(p).read().splitlines() if x] if os.path.exists(p) else [head]
    width = len(head.split(","))
    rows = [head] + [x + "," * (width - len(x.split(","))) for x in old[1:]] + [row]
    with open(p, "w") as f:
        f.write("\n".join(rows) + "\n")
    with open(os.path.join(REC, "daily.md"), "a") as f:
        f.write(text + "\n\n---\n\n")
    if not (git("add", "records") and git("-c", "user.name=signal-alert", "-c", "user.email=signal-alert@users.noreply.github.com",
                                           "commit", "-q", "-m", f"Daily record {day}") and git("push", "-q")):
        print("daily record not pushed")


def main():
    duration = int(os.environ.get("DURATION", "0"))
    end = time.time() + duration
    try:
        meta = universe()
    except Exception as e:  # noqa: BLE001
        send(f"⚠️ 提醒脚本连不上币安：{e}")
        raise
    if os.environ.get("HEARTBEAT", "1") == "1":
        send(f"✅ C+D+E 信号监控已启动（F 只记录），持续约 {duration // 3600} 小时，每 5 分钟检查一次（{len(meta)} 个币）")
    last, errors, daily = None, 0, {}
    while True:
        try:
            last = one_pass(meta, last)
            errors = 0
        except Exception as e:  # noqa: BLE001
            errors += 1
            if errors == 3:
                send(f"⚠️ 提醒脚本连续出错：{e}")
        try:
            maybe_daily(meta, daily)
        except Exception as e:  # noqa: BLE001 - the daily record must never stop the alerts
            print("daily record failed:", e)
            daily["next_try"] = time.time() + 3600
        if time.time() >= end:
            break
        nxt = (int(time.time()) // 300 + 1) * 300 + 20  # 20 s after the next 5m close
        time.sleep(max(5, nxt - time.time()))


if __name__ == "__main__":
    main()
