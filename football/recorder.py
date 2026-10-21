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

Jobs hand over: once a later run of the workflow is waiting (or DURATION is up, when the job starts the next run
itself) the job ends after a round, so recording continues with about a minute's gap."""
import csv, glob, gzip, io, math, os, re, signal, subprocess, sys, time, json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import requests

BJT = timezone(timedelta(hours=8))
BASE = "https://bf.titan007.com"
OUT = sys.argv[1] if len(sys.argv) > 1 else "out"
DURATION = int(os.environ.get("DURATION", "19800"))
WORKFLOW = "football-recorder.yml"
STEP = 300
GOAL_STEP = 60  # the live score list is read once a minute to catch goals
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0"
COLS = ["snap_utc", "match_id", "book", "label", "home_goals", "away_goals", "state",
        "half_ou_over", "half_ou_line", "half_ou_under", "full_ou_over", "full_ou_line", "full_ou_under",
        "half_ah_home", "half_ah_line", "half_ah_away", "full_ah_home", "full_ah_line", "full_ah_away", "trigger"]
FIX_COLS = ["match_id", "kickoff_bjt", "league", "home", "away"]
ALERT_COLS = ["snap_utc", "match_id", "league", "home", "away", "home_goals", "away_goals", "goal_min",
              "crown_first_over", "crown_first_line", "crown_first_under", "b365_first_over", "b365_first_line",
              "b365_first_under", "lam_crown", "lam_b365", "crown_now_over", "crown_now_line", "crown_now_under",
              "b365_now_over", "b365_now_line", "b365_now_under"]


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


def xa_check(js):
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
        if r[39] != 3 or gm is None or gm < 61 or br is None:
            continue
        g = _gn(r[1]) + _gn(r[2])
        oc, Lc, uc = (_num(x) for x in r[FOU:FOU + 3])
        ob, Lb, ub = (_num(x) for x in br[FOU:FOU + 3])
        if not (_valid(oc, Lc, uc) and _valid(ob, Lb, ub)) or not (-1e-9 <= Lb - Lc <= 0.5 + 1e-9):
            continue
        lc, lb = lam_rem(oc, Lc, uc, g), lam_rem(ob, Lb, ub, g)
        if lc is None or lb is None or lb - lc < 0.15:
            continue
        first = dict(row=j, h=_gn(r[1]), a=_gn(r[2]), gm=gm, c=(oc, Lc, uc), b=(ob, Lb, ub), lc=lc, lb=lb)
        break
    if not first or (first["h"], first["a"]) != (_gn(cr[-1][1]), _gn(cr[-1][2])):
        return None  # no qualifying goal, or it was an earlier goal (already alerted then)
    now = lambda arr: next(((_num(x[24]), _num(x[25]), _num(x[26])) for x in reversed(arr)
                            if (_gn(x[1]), _gn(x[2])) == (first["h"], first["a"]) and x[25] not in (None, "")), (None,) * 3)
    first.update(c_now=now(cr), b_now=now(bb))
    return first


def tg(text):
    tok, chat = os.environ.get("TELEGRAM_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        print(text, flush=True)
        return
    for _ in range(3):
        try:
            if requests.post(f"https://api.telegram.org/bot{tok}/sendMessage", timeout=(5, 15),
                             json={"chat_id": chat, "text": text, "disable_web_page_preview": True}).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    print("telegram send failed:", text[:100], flush=True)


# ---------------------------------------------------------------- daily summary of the record-only alerts
DAILY_HOUR = 10  # Beijing time; every match alerted the day before has finished by then


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
    """Settle yesterday's (Beijing date) xa alerts and send the summary once. True when done (or already done)."""
    day = (datetime.now(BJT) - timedelta(days=1)).date()
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
            if r.get("match_id") and r["match_id"] not in alerts:
                alerts[r["match_id"]] = r
    bjt = lambda r: datetime.strptime(r["snap_utc"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).astimezone(BJT)
    todays = sorted((r for r in alerts.values() if bjt(r).date() == day), key=bjt)
    sc = final_scores([day - timedelta(days=1), day, day + timedelta(days=1)]) if todays else {}
    bets, lines = [], []
    for r in todays:
        first = (_num(r["b365_first_line"]), _num(r["b365_first_under"]))
        nowp = (_num(r["b365_now_line"]), _num(r["b365_now_under"]))
        if not (nowp[0] == nowp[0] and nowp[1] == nowp[1]):  # no current price recorded -> the first quote
            nowp = first
        ft = sc.get(r["match_id"])
        pn = _settle(nowp[0], sum(ft), nowp[1], True) if ft else None
        pf = _settle(first[0], sum(ft), first[1], True) if ft else None
        bets.append([tag, r["match_id"], r["league"], r["home"], r["away"], r["goal_min"], nowp[0], nowp[1], first[0], first[1],
                     *(ft or ("", "")), "" if pn is None else round(pn, 4), "" if pf is None else round(pf, 4)])
        res = "未找到完场比分" if ft is None else f"完场 {ft[0]}:{ft[1]} " + ("赢" if pn > 0.01 else "走水" if abs(pn) <= 0.01 else "输") + f" {pn:+.2f}"
        lines.append(f"· {bjt(r):%H:%M} {r['league']} {r['home']} vs {r['away']} {r['home_goals']}:{r['away_goals']}，"
                     f"{r['goal_min']} 分钟后 小 {nowp[0]:g}@{nowp[1]:.2f} → {res}")
    done = [b for b in bets if b[-2] != ""]
    sn, sf = sum(b[-2] for b in done), sum(b[-1] for b in done)
    cum = [0, 0.0, 0.0]
    for n, f in files.items():
        if n.startswith("xa_daily_") and n < f"xa_daily_{tag}":
            for r in gz_rows(f()):
                cum[0] += int(r["settled"]); cum[1] += float(r["pnl_now"]); cum[2] += float(r["pnl_first"])
    cum = [cum[0] + len(done), cum[1] + sn, cum[2] + sf]
    with gzip.open(os.path.join(out_dir, f"xa_bets_{tag}.csv.gz"), "wt", newline="", encoding="utf-8") as fb:
        wb = csv.writer(fb)
        wb.writerow(["date", "match_id", "league", "home", "away", "goal_min", "line_now", "under_now", "line_first",
                     "under_first", "ft_home", "ft_away", "pnl_now", "pnl_first"])
        wb.writerows(bets)
    with gzip.open(os.path.join(out_dir, f"xa_daily_{tag}.csv.gz"), "wt", newline="", encoding="utf-8") as fd:
        wd = csv.writer(fd)
        wd.writerow(["date", "alerts", "settled", "pnl_now", "pnl_first"])
        wd.writerow([tag, len(bets), len(done), round(sn, 4), round(sf, 4)])
    win = sum(1 for b in done if b[-2] > 0.01); push = sum(1 for b in done if abs(b[-2]) <= 0.01)
    head = f"📊 小球模型 xa 昨日记录 {day:%m-%d}（仅记录，每注 1U，按推送时 bet365 的价）\n"
    if not bets:
        body = "昨天没有信号。"
    else:
        body = (f"{len(bets)} 注：赢 {win} 走水 {push} 输 {len(done) - win - push}" + (f"，{len(bets) - len(done)} 注没找到比分" if len(done) < len(bets) else "") +
                f"，合计 {sn:+.2f}U（按进球后第一口价 {sf:+.2f}U）\n" + "\n".join(lines))
    tail = (f"\n\n上线以来：{cum[0]} 注，合计 {cum[1]:+.2f}U，每注 {cum[1] / cum[0]:+.1%}（第一口价 {cum[2] / cum[0]:+.1%}）"
            if cum[0] else "")
    tg(head + body + tail)
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
        tg("⚽ 足球记录器已启动：每分钟盯皇冠和 bet365 的滚球盘。模型 xa 有信号才推送（仅记录，不要下注），"
           f"每天 {DAILY_HOUR} 点推昨日总结。")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    path, fpath = os.path.join(OUT, f"{stamp}.csv.gz"), os.path.join(OUT, f"{stamp}_fixtures.csv.gz")
    apath = os.path.join(OUT, f"{stamp}_alerts.csv.gz")
    fix, fix_t, written, seen_fix, alerted, daily_next = {}, 0.0, 0, set(), set(), 0.0
    with gzip.open(path, "wt", newline="", encoding="utf-8") as f, gzip.open(fpath, "wt", newline="", encoding="utf-8") as ff, \
            gzip.open(apath, "wt", newline="", encoding="utf-8") as fa:
        w, wf, wa = csv.writer(f), csv.writer(ff), csv.writer(fa)
        w.writerow(COLS); wf.writerow(FIX_COLS); wa.writerow(ALERT_COLS)
        last_full, prev, active, slow_until = 0.0, {}, set(), 0.0
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
                todo = [(mid, "goal" if mid in goal else "1min") for mid in sorted(active & in_play)]
            else:
                todo = []
            done_ids = {m for m, _ in todo}
            todo += [(mid, "goal") for mid in goal if mid not in done_ids]
            snap_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            with ThreadPoolExecutor(5) as ex:
                res = list(ex.map(lambda x: snap(x[0]), todo))
            n = 0
            for (mid, trig), (rows, js) in zip(todo, res):
                for row in rows:
                    w.writerow([snap_utc, *row, trig]); n += 1
                if any(r[10] not in (None, "") and str(r[2]) not in ("早餐", "未开场") for r in rows):
                    active.add(mid)  # has an in-play full-time line -> worth a look every minute
                x = xa_check(js) if mid not in alerted and js else None
                if x:
                    alerted.add(mid)
                    kt, lg, hn, an = fix.get(mid, (None, "", "", ""))
                    wa.writerow([snap_utc, mid, lg, hn, an, x["h"], x["a"], x["gm"], *x["c"], *x["b"], round(x["lc"], 3),
                                 round(x["lb"], 3), *x["c_now"], *x["b_now"]]); fa.flush()
                    bn = x["b_now"]
                    tg(f"🧪 小球模型 xa（仅记录，不要下注）\n{lg} {hn} vs {an}  {x['h']}:{x['a']}，{x['gm']} 分钟进球后\n"
                       f"bet365 第一口：全场 {x['b'][1]:g} 小 {x['b'][2]:.2f}｜皇冠第一口：{x['c'][1]:g} 小 {x['c'][2]:.2f}\n"
                       f"→ bet365 全场小" + (f"，现在 {bn[1]:g} 小 {bn[2]:.2f}" if bn[1] is not None else "") +
                       f"\n（北京时间 {datetime.now(BJT):%H:%M}）")
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
            if datetime.now(BJT).hour >= DAILY_HOUR and time.time() >= daily_next:
                try:
                    if xa_daily(OUT):  # done for yesterday: look again after the next midnight
                        nxt = (datetime.now(BJT) + timedelta(days=1)).replace(hour=DAILY_HOUR, minute=0, second=0)
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
