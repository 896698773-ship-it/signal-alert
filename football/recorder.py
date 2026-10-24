"""Football in-play recorder (data only, no betting signals). Every minute it snapshots the live Crown and bet365 prices
(first-half / full-time over-under and Asian handicap) of every in-play match that had an in-play quote last time;
every 5 minutes it also snapshots all other matches in play. The one-file live score list is read each minute and a
match whose score changed is snapshotted at once (trigger 'goal'). If more than 20% of requests fail in a round it
falls back to 5-minute rounds for 30 minutes, to go easy on the site. Only robots.txt-allowed paths, no query strings.
Output: <out>/<UTC run start>.csv.gz, one row per match per book per snapshot.

Record-only alert (no betting): the frozen rule xa_b365gen is checked on every snapshot and sent to Telegram once per
match, also written to <out>/<UTC run start>_alerts.csv.gz with the prices at the moment of the alert. From 10:00
Beijing time the previous day's alerts (this job's and earlier jobs' on the football-data branch) are settled on the
final scores and a daily summary is sent (xa_daily_<date>.csv.gz / xa_bets_<date>.csv.gz record it).

Watch-only pushes (not yet tested): the frozen first-half rules LIVE_HT_UNDER and LIVE_HT_UNDER_EARLY (Crown's first-half
O/U falling faster than the clock) are pushed once per match each, labelled apart, and written to
<out>/<UTC run start>_hu.csv.gz; they are scored on these files later.

Jobs hand over: once a later run of the workflow is waiting (or DURATION is up, when the job starts the next run
itself) the job ends after a round, so recording continues with about a minute's gap."""
import csv, glob, gzip, io, math, os, re, signal, subprocess, sys, time, json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import requests

BJT = timezone(timedelta(hours=8))
VNT = timezone(timedelta(hours=7))  # the user's clock: pushes, daily summary and its day are in Vietnam time
BASE = "https://bf.titan007.com"
OUT = sys.argv[1] if len(sys.argv) > 1 else "out"
DURATION = int(os.environ.get("DURATION", "19800"))
WORKFLOW = "football-recorder.yml"
STEP = 300
GOAL_STEP = 30  # the live score list is read every 30 s; second-half matches are snapshotted every 30 s, others every 60 s
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0"
COLS = ["snap_utc", "match_id", "book", "label", "home_goals", "away_goals", "state",
        "half_ou_over", "half_ou_line", "half_ou_under", "full_ou_over", "full_ou_line", "full_ou_under",
        "half_ah_home", "half_ah_line", "half_ah_away", "full_ah_home", "full_ah_line", "full_ah_away", "trigger"]
FIX_COLS = ["match_id", "kickoff_bjt", "league", "home", "away"]
ALERT_COLS = ["snap_utc", "match_id", "league", "home", "away", "home_goals", "away_goals", "goal_min",
              "crown_first_over", "crown_first_line", "crown_first_under", "b365_first_over", "b365_first_line",
              "b365_first_under", "lam_crown", "lam_b365", "crown_now_over", "crown_now_line", "crown_now_under",
              "b365_now_over", "b365_now_line", "b365_now_under", "rule"]


FAILS = [0, 0]  # failed requests, all requests in the current round


def get(path, enc=None):
    FAILS[1] += 1
    for k in range(3):
        try:
            r = S.get(BASE + path, timeout=25)
            if r.status_code == 200:
                return r.content.decode(enc, "replace") if enc else r.content.decode("utf-8-sig", "replace")
            if r.status_code == 404:
                return None
        except requests.RequestException:
            pass
        time.sleep(3 * (k + 1))
    FAILS[0] += 1
    return None


def strip(x):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", x)).strip()


def fixtures(now):
    """Matches listed for yesterday and today (Beijing dates) with kickoff times."""
    out = {}
    for d in (now - timedelta(days=1), now):
        t = get(f"/football/Next_{d:%Y%m%d}.htm", "gbk")
        if not t:
            continue
        for m in re.finditer(r"<tr[^>]*sId='(\d+)'[^>]*>(.*?)</tr>", t, re.S):
            tds = re.findall(r"<td[^>]*>(.*?)</td>", m.group(2), re.S)
            if len(tds) < 6:
                continue
            ko = re.search(r"(\d+)-(\d+) (\d+):(\d+)", strip(tds[1])) or re.search(r"(\d+)日(\d+):(\d+)", strip(tds[1]))
            if not ko:
                continue
            g = list(map(int, ko.groups()))
            if len(g) == 4:
                mon, day, hh, mm = g
                year = d.year + (1 if mon < d.month - 6 else 0)
            else:
                day, hh, mm = g
                mon, year = d.month, d.year
                if day < d.day - 20:
                    mon, year = (1, year + 1) if mon == 12 else (mon + 1, year)
            try:
                kt = datetime(year, mon, day, hh, mm, tzinfo=BJT)
            except ValueError:
                continue
            out[m.group(1)] = (kt, strip(tds[0]), strip(tds[3]), strip(tds[5]))
    return out


def live_scores():
    """{match_id: (state, home goals, away goals)} for every match in the live list (state 1/2/3 = in play)."""
    t = get("/vbsxml/bfdata.js", "gb18030")
    out = {}
    for mid, body in re.findall(r'A\[\d+\]="(\d+)\^([^"]*)"', t or ""):
        f = body.split("^")
        try:
            st, h, a = int(f[12]), int(f[13] or 0), int(f[14] or 0)
        except (IndexError, ValueError):
            continue
        out[mid] = (st, h, a)
    return out


def parse_js(t):
    res = {}
    for part in t.split("var ")[1:]:
        name, _, arr = part.partition("=")
        name, arr = name.strip(), arr.strip().rstrip(";").strip()
        if name not in ("sOdds", "sOdds8") or not arr.startswith("["):
            continue
        s = re.sub(r"(?<=[\[,])\s*(?=[,\]])", "null", arr.replace("'", '"'))
        try:
            res[name] = [x for x in json.loads(s) if isinstance(x, list) and x]
        except ValueError:
            pass
    return res


def snap(mid):
    """Snapshot rows (the last row of each book that carries a price; bet365 often ends with an empty placeholder
    row around half-time) and the parsed arrays."""
    t = get(f"/jsData/{mid[:2]}/{mid[2:4]}/{mid}.js")
    time.sleep(0.1)
    if not t:
        return [], {}
    js, rows = parse_js(t), []
    for key, book in (("sOdds", "crown"), ("sOdds8", "b365")):
        arr = js.get(key) or []
        if not arr:
            continue
        priced = [x for x in arr if any(v not in (None, "") for v in (x + [None] * 46)[3:27])]
        r = (priced or arr)[-1] + [None] * 46
        rows.append([mid, book, r[0], r[1], r[2], r[39], *r[18:21], *r[24:27], *r[6:9], *r[12:15]])
    return rows, js


# ---------------------------------------------------------------- frozen rule xa_b365gen (record only)
# After a second-half goal at minute >= 61 (minute = Crown's last quote before the goal), compare the two books' FIRST
# full-time O/U quote at the new score. If bet365's implies >= 0.15 more remaining goals than Crown's (equal-EV Poisson
# de-vig on the remaining line) and bet365's line is 0 to 0.5 above Crown's -> full-time under at bet365.
# Same definitions as search_kanpan/goal/frozen_xa_b365gen.py; first qualifying goal per match only.
KMAX, FOU = 16, 21
# xa: frozen 2026-10-21 (validated). xa2: relaxed variant frozen 2026-10-21 before any new data was looked at
# (2nd half from minute 46, gap >= 0.10, line gap up to 1 goal). Both pushed and settled in the daily summary.
RULES = {
    "xa": dict(min_minute=61, gap=0.15, lmax=0.5, push=True, name="xa",
               tag="🟢 xa：回测 1,633 注每注 +14.7%（6 段历史全赚）→ 可小注试：每注 1% 本金，只在 bet365 下"),
    # xa3 frozen 2026-10-22 before Mar-Aug 2025 was downloaded; scored once there: +18.0% on 439 bets
    "xa3": dict(min_minute=46, gap=0.20, lmax=1.0, push=True, name="xa3",
                tag="🟢 xa3（价差 ≥0.20）：新数据 439 注每注 +18.0%（6 个月全赚）→ 可小注试：每注 1% 本金，只在 bet365 下"),
    "xa2": dict(min_minute=46, gap=0.10, lmax=1.0, push=True, name="xa2 放宽版",
                tag="⚪ xa2 放宽版：回测 8,804 注每注 +5.7%，太薄，价格一变差就不赚 → 只记录，不建议下"),
    # the same rules on youth / women's matches (tested on their own 2026-10-22: xa +10.5% on 507, xa2 +5.5% on 2035): settled apart
    "xa_y": dict(min_minute=61, gap=0.15, lmax=0.5, push=True, name="xa 青年/女足", youth=True,
                 tag="🟡 xa 青年/女足：回测 507 注每注 +10.5%（女足 +17%，青年 +5.5%）→ 女足可小注，青年只记录"),
    "xa2_y": dict(min_minute=46, gap=0.10, lmax=1.0, push=True, name="xa2 青年/女足", youth=True,
                  tag="⚪ xa2 青年/女足：回测 2,035 注每注 +5.5% → 只记录，不建议下"),
}
YOUTH = ("女", "U1", "U2", "青")  # youth and women's matches (reserve teams and friendlies stay out)
BAD = ("女", "U1", "U2", "青", "后备", "预备", "友谊", "U23", "U20", "U19", "U18", "U17")  # left out of the tested data too


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def _gn(x):
    try:
        return int(x) if x is not None else 0
    except (TypeError, ValueError):
        return 0


def _settle(L, k, price, under):
    """Asian under (or over) pnl per unit at HK odds; quarter lines split in half."""
    q = round(L * 4)
    parts = (L - 0.25, L + 0.25) if q % 2 else (L, L)
    out = 0.0
    for x in parts:
        win = k < x - 1e-9 if under else k > x + 1e-9
        out += 0.5 * (price if win else (0.0 if abs(k - x) < 1e-9 else -1.0))
    return out


def _pmf(lam):
    p = [math.exp(k * math.log(max(lam, 1e-12)) - lam - math.lgamma(k + 1)) for k in range(KMAX)]
    p[-1] += max(0.0, 1 - sum(p))
    return p


def lam_rem(o, L, u, g):
    """Remaining-goals Poisson mean at which under and over have the same expected pnl (None if invalid)."""
    L = L - g
    if not (o > 0 and u > 0 and o < 30 and u < 30 and L > 0 and abs(L * 4 - round(L * 4)) < 1e-6):
        return None
    d = [_settle(L, k, u, True) - _settle(L, k, o, False) for k in range(KMAX)]
    a, b = 1e-3, 9.0
    for _ in range(40):
        mid = 0.5 * (a + b)
        if sum(p * x for p, x in zip(_pmf(mid), d)) > 0:
            a = mid
        else:
            b = mid
    return 0.5 * (a + b)


def _valid(o, L, u):
    return (o > 0 and u > 0 and abs(L * 4 - round(L * 4)) < 1e-6 and 1 / (1 + o) + 1 / (1 + u) >= 1)


def xa_check(js, min_minute=61, gap=0.15, lmax=0.5):
    """The alert for the match's latest goal if it is the match's first qualifying goal, else None."""
    cr, bb = js.get("sOdds") or [], js.get("sOdds8") or []
    pad = lambda r: r + [None] * 46
    cr, bb = [pad(r) for r in cr], [pad(r) for r in bb]
    isnum = lambda x: isinstance(x, str) and x.isdigit()
    b365_first = {}
    for r in bb:
        if isnum(r[0]) and r[39] in (1, 3):
            b365_first.setdefault((_gn(r[1]), _gn(r[2])), r)
    first = None
    for j in range(1, len(cr)):
        r, p = cr[j], cr[j - 1]
        if not isnum(r[0]) or r[39] not in (1, 3) or _gn(r[1]) + _gn(r[2]) <= _gn(p[1]) + _gn(p[2]):
            continue
        gm = (min(int(p[0]), 45) if p[39] == 1 else max(int(p[0]), 46) if p[39] == 3 else int(p[0])) if isnum(p[0]) \
            else (46 if p[0] == "中场" else None)
        br = b365_first.get((_gn(r[1]), _gn(r[2])))
        if r[39] != 3 or gm is None or gm < min_minute or br is None:
            continue
        g = _gn(r[1]) + _gn(r[2])
        oc, Lc, uc = (_num(x) for x in r[FOU:FOU + 3])
        ob, Lb, ub = (_num(x) for x in br[FOU:FOU + 3])
        if not (_valid(oc, Lc, uc) and _valid(ob, Lb, ub)) or not (-1e-9 <= Lb - Lc <= lmax + 1e-9):
            continue
        lc, lb = lam_rem(oc, Lc, uc, g), lam_rem(ob, Lb, ub, g)
        if lc is None or lb is None or lb - lc < gap:
            continue
        first = dict(row=j, h=_gn(r[1]), a=_gn(r[2]), gm=gm, c=(oc, Lc, uc), b=(ob, Lb, ub), lc=lc, lb=lb)
        break
    if not first or (first["h"], first["a"]) != (_gn(cr[-1][1]), _gn(cr[-1][2])):
        return None  # no qualifying goal, or it was an earlier goal (already alerted then)
    now = lambda arr: next(((_num(x[24]), _num(x[25]), _num(x[26])) for x in reversed(arr)
                            if (_gn(x[1]), _gn(x[2])) == (first["h"], first["a"]) and x[25] not in (None, "")), (None,) * 3)
    first.update(c_now=now(cr), b_now=now(bb))
    return first


# ---------------------------------------------------------------- first-half under watch rules (watch only, not yet tested)
# LIVE_HT_UNDER: frozen 2026-10-21 (my-first-project scans/2026-10-21/kanpan/live/PREREG_live.md, live_patterns.py).
# Crown's first-half O/U implies at least 10% fewer remaining first-half goals (log) than time decay alone would give,
# over a 4-7 minute window at an unchanged score, at minute 10-35 -> first-half under at Crown. One per match.
# LIVE_HT_UNDER_EARLY: frozen 2026-10-24 (PREREG_live_early.md, run_live_test_early.py): the same over a 2-4 minute
# window, at least 6% fewer, from minute 5. Both are scored once on these recorder files; same definitions as there.
HALF_TAU = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45]
HALF_F = [0.0, -0.11478, -0.21928, -0.3476, -0.48897, -0.66022, -0.84396, -1.09617, -1.20632, -1.31647]
HU_RULES = {
    "LIVE_HT_UNDER": dict(lo=4.0, hi=7.0, target=5.0, dev=-0.10, tau_lo=10.0, tau_hi=35.0, title="👀 半场小观察（未验证）",
                          tag="⚪ 半场小观察：规则 10-21 已写死，正在用实盘数据考试（约 11 月 3 日出结果）→ 只观察，不建议下"),
    "LIVE_HT_UNDER_EARLY": dict(lo=2.0, hi=4.0, target=3.0, dev=-0.06, tau_lo=5.0, tau_hi=35.0, title="👀 半场小观察·早版（未验证）",
                                tag="⚪ 早版（看 3 分钟的降幅，比原版早）：10-24 写死，跟原版一起考（约 11 月 3 日出结果）→ 只观察，不建议下"),
}
HU_COLS = ["snap_utc", "match_id", "league", "home", "away", "home_goals", "away_goals", "tau1", "tau2", "L1", "o1", "u1",
           "L2", "o2", "u2", "lam1", "lam2", "dev", "b365_o", "b365_L", "b365_u", "rule"]  # bet365's half O/U as quoted
hu_hist = {}  # match -> this run's Crown snapshots in play: dict(t, tau, g, ok, o, L, u, lam, key)


def _fh(tau):
    x = min(tau, 45.0)
    for i in range(1, len(HALF_TAU)):
        if x <= HALF_TAU[i]:
            a, b = HALF_TAU[i - 1], HALF_TAU[i]
            return HALF_F[i - 1] + (HALF_F[i] - HALF_F[i - 1]) * (x - a) / (b - a)
    return HALF_F[-1]


def _fnum(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def hu_check(mid, rows, t, sent=()):
    """[(rule, signal)] for the watch rules that fire on this snapshot of the match and are not in sent as (rule, match)
    (rows: snap() rows of one snapshot, t: its UTC time). Every snapshot in play is kept for the windows."""
    cr = next((r for r in rows if r[1] == "crown"), None)
    if cr is None or cr[5] not in (1, 2, 3):
        return []
    st, label, g = cr[5], str(cr[2]), _gn(cr[3]) + _gn(cr[4])
    m = 45.5 if label == "中场" else _fnum(label)
    tau = None if m is None else (45.5 if label == "中场" and st == 2 else None if label == "中场" or st not in (1, 3)
                                  else min(m, 45.0) if st == 1 else max(m, 46.0))
    # the two books' rows disagree on the score (one lags a goal): unusable
    conflict = len({(_gn(r[3]), _gn(r[4])) for r in rows if r[5] in (1, 2, 3)}) > 1
    o, L, u = (_fnum(x) for x in cr[6:9])
    lam = None
    if st == 1 and tau is not None and None not in (o, L, u) and 0.40 <= o <= 2.00 and 0.40 <= u <= 2.00 and not conflict:
        lam = lam_rem(o, L, u, g)
        lam = lam if lam is not None and 0.006 <= lam <= 8.9 else None
    cur = dict(t=t, tau=tau, g=g, ok=lam is not None, o=o, L=L, u=u, lam=lam,
               key=(label, g, st, tuple(_fnum(x) for x in cr[6:18])))
    hist = hu_hist.setdefault(mid, [])
    out = []
    for rule, c in HU_RULES.items():
        if (rule, mid) in sent or not cur["ok"]:
            continue
        best = None
        for s0 in reversed(hist):
            dt = (t - s0["t"]).total_seconds() / 60
            if dt > c["hi"]:
                break
            if dt < c["lo"] or s0["g"] != g or not s0["ok"]:
                continue
            if best is None or abs(dt - c["target"]) < abs((t - best["t"]).total_seconds() / 60 - c["target"]):
                best = s0
        if best is None or best["key"] == cur["key"] or (best["tau"] == 45.0 and tau == 45.0) \
                or tau < best["tau"] or abs(L - best["L"]) >= 0.75 - 1e-9:
            continue
        dev = round(math.log(lam / (best["lam"] * math.exp(_fh(tau) - _fh(best["tau"])))), 4)
        if dev > c["dev"] or not c["tau_lo"] <= tau <= c["tau_hi"]:
            continue
        b = next((r for r in rows if r[1] == "b365"), None)
        bq = tuple(_fnum(x) for x in b[6:9]) if b is not None and (_gn(b[3]), _gn(b[4])) == (_gn(cr[3]), _gn(cr[4])) else (None,) * 3
        out.append((rule, dict(h=_gn(cr[3]), a=_gn(cr[4]), s1=best, s2=cur, dev=dev, b=bq)))
    hist.append(cur)
    hu_hist[mid] = [x for x in hist if (t - x["t"]).total_seconds() <= 600]
    return out


def tg(text):
    tok, chat = os.environ.get("TELEGRAM_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        print(text, flush=True)
        return
    for _ in range(3):
        try:
            if requests.post(f"https://api.telegram.org/bot{tok}/sendMessage", timeout=(5, 15),
                             json={"chat_id": chat, "text": text, "disable_web_page_preview": True}).status_code == 200:
                print(f"{datetime.now(VNT):%H:%M:%S} VN telegram sent:", text[:50].replace("\n", " "), flush=True)
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    print("telegram send failed:", text[:100], flush=True)


# ---------------------------------------------------------------- daily summary of the record-only alerts
DAILY_HOUR = 10  # Vietnam time; every match alerted the day before has finished by then


def git(*a):
    try:
        return subprocess.run(["git", *a], capture_output=True, timeout=120)
    except (OSError, subprocess.SubprocessError):
        return None


def gz_rows(data):
    """CSV rows (dicts) of a gzip file's bytes; a file without its end marker is read up to where it stops."""
    out = []
    try:
        for r in csv.DictReader(io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(data)), encoding="utf-8")):
            out.append(r)
    except (EOFError, OSError):
        pass
    return out


def branch_files():
    """{name: bytes getter} of data/ files on the football-data branch (fetched first)."""
    git("fetch", "-q", "origin", "football-data")
    r = git("ls-tree", "--name-only", "origin/football-data", "data/")
    names = r.stdout.decode().split() if r and r.returncode == 0 else []
    return {n.split("/")[-1]: (lambda n=n: (git("show", f"origin/football-data:{n}") or subprocess.CompletedProcess([], 1, b"")).stdout)
            for n in names}


def final_scores(days):
    """{match_id: (home, away)} of finished matches on the finished-matches pages of these Beijing dates and the
    live list (state -1 = finished)."""
    out = {}
    for d in days:
        t = get(f"/football/Over_{d:%Y%m%d}.htm", "gbk")
        for m in re.finditer(r"<tr[^>]*sId='(\d+)'[^>]*>(.*?)</tr>", t or "", re.S):
            tds = re.findall(r"<td[^>]*>(.*?)</td>", m.group(2), re.S)
            ft = re.findall(r">(\d+)<", tds[4]) if len(tds) >= 9 else []
            if len(tds) >= 9 and strip(tds[2]) == "完" and len(ft) == 2:
                out[m.group(1)] = (int(ft[0]), int(ft[1]))
    for mid, (st, h, a) in live_scores().items():
        if st == -1:
            out.setdefault(mid, (h, a))
    return out


def xa_daily(out_dir):
    """Settle yesterday's (Vietnam date) alerts of every rule and send the summary once. True when done (or already)."""
    day = (datetime.now(VNT) - timedelta(days=1)).date()
    tag = f"{day:%Y%m%d}"
    if os.path.exists(os.path.join(out_dir, f"xa_daily_{tag}.csv.gz")):
        return True
    files = branch_files()
    if f"xa_daily_{tag}.csv.gz" in files:
        return True
    alerts = {}
    sources = [f() for n, f in sorted(files.items()) if n.endswith("_alerts.csv.gz")]
    sources += [open(p, "rb").read() for p in sorted(glob.glob(os.path.join(out_dir, "*_alerts.csv.gz")))]
    for data in sources:
        for r in gz_rows(data):
            if r.get("match_id"):
                alerts.setdefault((r.get("rule") or "xa", r["match_id"]), r)
    bjt = lambda r: datetime.strptime(r["snap_utc"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).astimezone(VNT)
    todays = sorted(((k[0], r) for k, r in alerts.items() if bjt(r).date() == day), key=lambda x: bjt(x[1]))
    vsrc = [f() for n, f in sorted(files.items()) if n.endswith("_voids.csv.gz")]
    vsrc += [open(p, "rb").read() for p in sorted(glob.glob(os.path.join(out_dir, "*_voids.csv.gz")))]
    voids = {r["match_id"] for data in vsrc for r in gz_rows(data)}
    sc = final_scores([day - timedelta(days=1), day, day + timedelta(days=1)]) if todays else {}
    cum = {}
    for n, f in files.items():
        if n.startswith("xa_daily_") and n < f"xa_daily_{tag}":
            for r in gz_rows(f()):
                c = cum.setdefault(r.get("rule") or "xa", [0, 0.0, 0.0])
                c[0] += int(r["settled"]); c[1] += float(r["pnl_now"]); c[2] += float(r["pnl_first"])
    bets, daily, parts = [], [], []
    for rule in RULES:
        lines, mine = [], []
        for rl, r in todays:
            if rl != rule:
                continue
            first = (_num(r["b365_first_line"]), _num(r["b365_first_under"]))
            nowp = (_num(r["b365_now_line"]), _num(r["b365_now_under"]))
            if not (nowp[0] == nowp[0] and nowp[1] == nowp[1]):  # no current price recorded -> the first quote
                nowp = first
            if r["match_id"] in voids:
                lines.append(f"· {bjt(r):%H:%M} {r['league']} {r['home']} vs {r['away']} {r['home_goals']}:{r['away_goals']}，"
                             f"{r['goal_min']} 分钟后 → 进球被取消，作废（不算）")
                continue
            ft = sc.get(r["match_id"])
            pn = _settle(nowp[0], sum(ft), nowp[1], True) if ft else None
            pf = _settle(first[0], sum(ft), first[1], True) if ft else None
            mine.append([tag, rule, r["match_id"], r["league"], r["home"], r["away"], r["goal_min"], nowp[0], nowp[1], first[0],
                         first[1], *(ft or ("", "")), "" if pn is None else round(pn, 4), "" if pf is None else round(pf, 4)])
            res = "未找到完场比分" if ft is None else f"完场 {ft[0]}:{ft[1]} " + ("赢" if pn > 0.01 else "走水" if abs(pn) <= 0.01 else "输") + f" {pn:+.2f}"
            lines.append(f"· {bjt(r):%H:%M} {r['league']} {r['home']} vs {r['away']} {r['home_goals']}:{r['away_goals']}，"
                         f"{r['goal_min']} 分钟后 小 {nowp[0]:g}@{nowp[1]:.2f} → {res}")
        done = [b for b in mine if b[-2] != ""]
        sn, sf = sum(b[-2] for b in done), sum(b[-1] for b in done)
        c = cum.setdefault(rule, [0, 0.0, 0.0]); c[0] += len(done); c[1] += sn; c[2] += sf
        bets += mine
        daily.append([tag, rule, len(mine), len(done), round(sn, 4), round(sf, 4)])
        win = sum(1 for b in done if b[-2] > 0.01); push = sum(1 for b in done if abs(b[-2]) <= 0.01)
        txt = f"【{RULES[rule]['name']}】" + (("只有作废的信号。" if lines else "昨天没有信号。") if not mine else
                               f"{len(mine)} 注：赢 {win} 走水 {push} 输 {len(done) - win - push}" +
                               (f"，{len(mine) - len(done)} 注没找到比分" if len(done) < len(mine) else "") +
                               f"，合计 {sn:+.2f}U（按进球后第一口价 {sf:+.2f}U）")
        if RULES[rule]["push"] and lines:
            txt += "\n" + "\n".join(lines)
        if c[0]:
            txt += f"\n上线以来 {c[0]} 注，合计 {c[1]:+.2f}U，每注 {c[1] / c[0]:+.1%}（第一口价 {c[2] / c[0]:+.1%}）"
        parts.append(txt)
    with gzip.open(os.path.join(out_dir, f"xa_bets_{tag}.csv.gz"), "wt", newline="", encoding="utf-8") as fb:
        wb = csv.writer(fb)
        wb.writerow(["date", "rule", "match_id", "league", "home", "away", "goal_min", "line_now", "under_now", "line_first",
                     "under_first", "ft_home", "ft_away", "pnl_now", "pnl_first"])
        wb.writerows(bets)
    with gzip.open(os.path.join(out_dir, f"xa_daily_{tag}.csv.gz"), "wt", newline="", encoding="utf-8") as fd:
        wd = csv.writer(fd)
        wd.writerow(["date", "rule", "alerts", "settled", "pnl_now", "pnl_first"])
        wd.writerows(daily)
    tg(f"📊 小球模型昨日记录 {day:%m-%d}（越南时间；每注 1U，按推送时 bet365 的价结算；🟢 xa / xa3 可小注，⚪ xa2 只记录；同一场几条都中，各自算一注）\n\n"
       + "\n\n".join(parts))
    return True


def newer_run_waiting():
    """True when a later run of this workflow waits for this one (its concurrency group runs one at a time)."""
    tok, repo, rid = os.environ.get("GH_TOKEN"), os.environ.get("GITHUB_REPOSITORY"), int(os.environ.get("GITHUB_RUN_ID") or 0)
    if not (tok and repo and rid):
        return False
    try:
        r = requests.get(f"https://api.github.com/repos/{repo}/actions/workflows/{WORKFLOW}/runs", params={"per_page": 10},
                         headers={"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json"}, timeout=15)
        return r.status_code == 200 and any(x["id"] > rid and x["status"] != "completed" for x in r.json()["workflow_runs"])
    except (requests.RequestException, ValueError, KeyError):
        return False


def start_next_run():
    tok, repo = os.environ.get("GH_TOKEN"), os.environ.get("GITHUB_REPOSITORY")
    if tok and repo:
        try:
            r = requests.post(f"https://api.github.com/repos/{repo}/actions/workflows/{WORKFLOW}/dispatches",
                              json={"ref": os.environ.get("GITHUB_REF_NAME") or "main"}, timeout=15,
                              headers={"Authorization": f"Bearer {tok}", "Accept": "application/vnd.github+json"})
            print("next run requested:", r.status_code, flush=True)
        except requests.RequestException as e:
            print("next run not requested:", e, flush=True)


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    t0 = time.time()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # a cancelled job still closes its gzip files
    if os.environ.get("GITHUB_EVENT_NAME", "schedule") != "schedule":  # manual start: say it is running
        tg("⚽ 足球记录器已启动：下半场每 30 秒、其他每分钟盯皇冠和 bet365 的滚球盘。xa / xa3 / xa2（含青年、女足）有信号就推，"
           f"每条标明能不能下；半场小观察和早版（都未验证）也推，只看不下；进球被取消会再推一条作废；每天越南时间 {DAILY_HOUR} 点推昨日总结。")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    path, fpath = os.path.join(OUT, f"{stamp}.csv.gz"), os.path.join(OUT, f"{stamp}_fixtures.csv.gz")
    apath = os.path.join(OUT, f"{stamp}_alerts.csv.gz")
    vpath = os.path.join(OUT, f"{stamp}_voids.csv.gz")
    hpath = os.path.join(OUT, f"{stamp}_hu.csv.gz")
    fix, fix_t, written, seen_fix, alerted, daily_next = {}, 0.0, 0, set(), set(), 0.0
    hu_sent = set()
    alert_goals = {}  # match -> total goals when its alert went out (to spot a goal that is taken back)
    with gzip.open(path, "wt", newline="", encoding="utf-8") as f, gzip.open(fpath, "wt", newline="", encoding="utf-8") as ff, \
            gzip.open(apath, "wt", newline="", encoding="utf-8") as fa, gzip.open(vpath, "wt", newline="", encoding="utf-8") as fv, \
            gzip.open(hpath, "wt", newline="", encoding="utf-8") as fh:
        w, wf, wa, wv, wh = csv.writer(f), csv.writer(ff), csv.writer(fa), csv.writer(fv), csv.writer(fh)
        w.writerow(COLS); wf.writerow(FIX_COLS); wa.writerow(ALERT_COLS); wv.writerow(["snap_utc", "match_id", "alert_goals", "now_goals"])
        wh.writerow(HU_COLS)
        last_full, prev, active, slow_until, playing, sig, half2, rnd = 0.0, {}, set(), 0.0, set(), {}, set(), 0
        while time.time() - t0 < DURATION:
            round_t = time.time()
            now = datetime.now(BJT)
            if time.time() - fix_t > 3600 or not fix:
                fix, fix_t = fixtures(now) or fix, time.time()
            FAILS[0] = FAILS[1] = 0
            ls = live_scores()
            in_play = {mid for mid, (st, *_r) in ls.items() if st in (1, 2, 3)}
            goal = [mid for mid in in_play if mid in prev and ls[mid][1:] != prev[mid]]
            prev = {mid: v[1:] for mid, v in ls.items()}
            full_round = time.time() - last_full >= STEP - 5
            if full_round:
                last_full = time.time()
                live = {mid for mid, (kt, *_r) in fix.items() if kt - timedelta(minutes=2) <= now <= kt + timedelta(minutes=130)} | in_play
                todo = [(mid, "goal" if mid in goal else "5min") for mid in sorted(live)]
            elif time.time() >= slow_until:
                # in play per the live list, or per the match's own last snapshot (some leagues are not in the list)
                rnd += 1  # second-half matches (where xa / xa2 can fire) every round, the rest every other round
                todo = [(mid, "goal" if mid in goal else "1min") for mid in sorted(active & (in_play | playing))
                        if mid in half2 or rnd % 2 == 0]
            else:
                todo = []
            done_ids = {m for m, _ in todo}
            todo += [(mid, "goal") for mid in goal if mid not in done_ids]
            snap_dt = datetime.now(timezone.utc).replace(microsecond=0)
            snap_utc = snap_dt.strftime("%Y-%m-%d %H:%M:%S")
            with ThreadPoolExecutor(5) as ex:
                res = list(ex.map(lambda x: snap(x[0]), todo))
            n = 0
            for (mid, trig), (rows, js) in zip(todo, res):
                for row in rows:
                    w.writerow([snap_utc, *row, trig]); n += 1
                if rows and mid in alert_goals:  # both books now show fewer goals than at the alert: the goal was taken back
                    now_g = [_gn(r[3]) + _gn(r[4]) for r in rows if r[3] not in (None, "")]
                    if now_g and max(now_g) < alert_goals[mid]:
                        wv.writerow([snap_utc, mid, alert_goals[mid], max(now_g)]); fv.flush()
                        kt, lg, hn, an = fix.get(mid, (None, "", "", ""))
                        tg(f"⚠️ 进球被取消：{lg} {hn} vs {an} 比分退回 {rows[0][3]}:{rows[0][4]}。之前那条小球信号作废，"
                           f"没下就别下了；下了的话 bet365 一般会把这段时间的注单作废退款，以平台规则为准（越南时间 {datetime.now(VNT):%H:%M}）")
                        del alert_goals[mid]
                if any(r[10] not in (None, "") and str(r[2]) not in ("早餐", "未开场") for r in rows):
                    active.add(mid)  # has an in-play full-time line -> worth a look every minute
                if rows:  # still playing: an in-play row whose prices changed within the last 10 minutes
                    key = tuple(map(str, sum(rows, [])))
                    if sig.get(mid, (None,))[0] != key:
                        sig[mid] = (key, time.time())
                    live_now = any(r[5] in (1, 2, 3) for r in rows) and time.time() - sig[mid][1] < 600
                    (playing.add if live_now else playing.discard)(mid)
                    (half2.add if live_now and any(r[5] == 3 for r in rows) else half2.discard)(mid)
                for rule, hu in (hu_check(mid, rows, snap_dt, hu_sent) if rows else []):
                    hu_sent.add((rule, mid))
                    kt, lg, hn, an = fix.get(mid, (None, "", "", ""))
                    s1, s2, bq = hu["s1"], hu["s2"], hu["b"]
                    wh.writerow([snap_utc, mid, lg, hn, an, hu["h"], hu["a"], s1["tau"], s2["tau"], s1["L"], s1["o"], s1["u"],
                                 s2["L"], s2["o"], s2["u"], round(s1["lam"], 4), round(s2["lam"], 4), hu["dev"], *bq, rule]); fh.flush()
                    exp_lam = s2["lam"] / math.exp(hu["dev"])
                    tg(f"{HU_RULES[rule]['title']}\n{lg} {hn} vs {an}  {hu['h']}:{hu['a']}，{s2['tau']:.0f} 分钟\n"
                       f"皇冠半场大小：{s1['tau']:.0f} 分钟 {s1['L']:g} 小 {s1['u']:.2f} → 现在 {s2['L']:g} 小 {s2['u']:.2f}\n"
                       f"只算时间应该还剩 {exp_lam:.2f} 球，皇冠现在只给 {s2['lam']:.2f} 球（多降 {1 - math.exp(hu['dev']):.0%}）\n"
                       f"→ 皇冠半场小 {s2['L']:g} 水 {s2['u']:.2f}" + (f"｜bet365 现在 {bq[1]:g} 小 {bq[2]:.2f}" if None not in bq else "") +
                       # bet365 a goal or more away from Crown at the same score: it is pricing a goal not (yet) given
                       ("（bet365 跟皇冠差一个球以上，像是在等进球判定，先别看 bet365）" if None not in bq and abs(bq[1] - s2["L"]) >= 0.75 else "") +
                       f"\n{HU_RULES[rule]['tag']}（越南时间 {datetime.now(VNT):%H:%M}）")
                names = "".join(fix.get(mid, (None, "", "", ""))[1:])
                fired = []
                youth = any(b in names for b in YOUTH)
                other_bad = any(b in names for b in BAD) and not youth
                for rule, cfg in RULES.items():
                    if (rule, mid) in alerted or not js or other_bad or cfg.get("youth", False) != youth:
                        continue
                    x = xa_check(js, cfg["min_minute"], cfg["gap"], cfg["lmax"])
                    if not x:
                        continue
                    alerted.add((rule, mid))
                    alert_goals[mid] = x["h"] + x["a"]
                    kt, lg, hn, an = fix.get(mid, (None, "", "", ""))
                    wa.writerow([snap_utc, mid, lg, hn, an, x["h"], x["a"], x["gm"], *x["c"], *x["b"], round(x["lc"], 3),
                                 round(x["lb"], 3), *x["c_now"], *x["b_now"], rule]); fa.flush()
                    if cfg["push"]:
                        fired.append((cfg["name"], x, lg, hn, an))
                if fired:  # one message per match and goal, naming every rule that fired on it
                    by_goal = {}
                    for nm, x, lg, hn, an in fired:
                        by_goal.setdefault((x["h"], x["a"]), []).append((nm, x, lg, hn, an))
                    for group in by_goal.values():
                        _, x, lg, hn, an = group[0]
                        bn = x["b_now"]
                        tags = [RULES[k]["tag"] for k in RULES if RULES[k]["name"] in {g[0] for g in group}]
                        need = min(RULES[k]["gap"] for k in RULES if RULES[k]["name"] in {g[0] for g in group})
                        cn, gl = x["c_now"], x["h"] + x["a"]  # the gap again at the prices showing now (not tested; a guide)
                        lc2 = lam_rem(*cn, gl) if None not in cn and all(v == v for v in cn) else None
                        lb2 = lam_rem(*bn, gl) if None not in bn and all(v == v for v in bn) else None
                        if lc2 is not None and lb2 is not None:
                            tags.append(f"现在两家价差 {lb2 - lc2:+.2f}（模型要求 ≥{need:.2f}）"
                                        + ("" if lb2 - lc2 >= need else "：⚠️ 已经不够，这注别追"))
                        tg(f"⚽ 小球信号：{' + '.join(g[0] for g in group)}\n{lg} {hn} vs {an}  {x['h']}:{x['a']}，"
                           f"{x['gm']} 分钟进球后\nbet365 第一口：全场 {x['b'][1]:g} 小 {x['b'][2]:.2f}｜皇冠第一口：{x['c'][1]:g} 小 {x['c'][2]:.2f}\n"
                           f"→ bet365 全场小" + (f"，现在 {bn[1]:g} 小 {bn[2]:.2f}" if bn[1] is not None else "") +
                           "\n" + "\n".join(tags) +
                           f"\n下了的话记下实际成交的盘口和水位（越南时间 {datetime.now(VNT):%H:%M}）")
                if rows and mid not in seen_fix and mid in fix:
                    kt, lg, h, a = fix[mid]
                    wf.writerow([mid, kt.strftime("%Y-%m-%d %H:%M"), lg, h, a]); seen_fix.add(mid)
            f.flush(); ff.flush()
            written += n
            if FAILS[1] and FAILS[0] / FAILS[1] > 0.2:
                slow_until = time.time() + 1800
                print(f"{now:%m-%d %H:%M} BJT  失败 {FAILS[0]}/{FAILS[1]}，改成 5 分钟一轮，30 分钟后再恢复", flush=True)
            if todo and (full_round or goal):
                print(f"{now:%m-%d %H:%M} BJT  本轮 {len(todo)} 场（进球后补扫 {len(goal)}），记录 {n} 行（累计 {written}），"
                      f"用时 {time.time() - round_t:.0f} 秒", flush=True)
            if datetime.now(VNT).hour >= DAILY_HOUR and time.time() >= daily_next:
                try:
                    if xa_daily(OUT):  # done for yesterday: look again after the next midnight
                        nxt = (datetime.now(VNT) + timedelta(days=1)).replace(hour=DAILY_HOUR, minute=0, second=0)
                        daily_next = nxt.timestamp()
                except Exception as e:  # noqa: BLE001 - the summary must never stop the recording
                    print("daily summary failed:", e, flush=True)
                    daily_next = time.time() + 3600
            left = DURATION - (time.time() - t0)
            if left <= 0:
                start_next_run()  # time is up and no run was waiting (a skipped schedule)
                break
            if full_round and time.time() - t0 > 600 and newer_run_waiting():
                print("handing over to the next run", flush=True)
                break
            time.sleep(max(1, min(left, GOAL_STEP - (time.time() - round_t))))
    print("done", written, flush=True)
