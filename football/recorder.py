"""Football in-play recorder (data only, no betting signals). Every 5 minutes it takes a snapshot of the live Crown and
bet365 prices of every match in play on titan007 (first-half / full-time over-under and Asian handicap). In between,
once a minute, it reads the one-file live score list; when a score changes it snapshots that match at once, so the
first quote after a goal is caught too (trigger = 'goal'). Only robots.txt-allowed paths, no query strings.
Output: <out>/<UTC run start>.csv.gz, one row per match per book per snapshot."""
import csv, gzip, os, re, sys, time, json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import requests

BJT = timezone(timedelta(hours=8))
BASE = "https://bf.titan007.com"
OUT = sys.argv[1] if len(sys.argv) > 1 else "out"
DURATION = int(os.environ.get("DURATION", "20400"))
STEP = 300
GOAL_STEP = 60  # the live score list is read once a minute to catch goals
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0"
COLS = ["snap_utc", "match_id", "book", "label", "home_goals", "away_goals", "state",
        "half_ou_over", "half_ou_line", "half_ou_under", "full_ou_over", "full_ou_line", "full_ou_under",
        "half_ah_home", "half_ah_line", "half_ah_away", "full_ah_home", "full_ah_line", "full_ah_away", "trigger"]
FIX_COLS = ["match_id", "kickoff_bjt", "league", "home", "away"]


def get(path, enc=None):
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
    t = get(f"/jsData/{mid[:2]}/{mid[2:4]}/{mid}.js")
    time.sleep(0.3)
    if not t:
        return []
    rows = []
    for key, book in (("sOdds", "crown"), ("sOdds8", "b365")):
        arr = parse_js(t).get(key) or []
        if not arr:
            continue
        r = arr[-1] + [None] * 46
        rows.append([mid, book, r[0], r[1], r[2], r[39], *r[18:21], *r[24:27], *r[6:9], *r[12:15]])
    return rows


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    t0 = time.time()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    path, fpath = os.path.join(OUT, f"{stamp}.csv.gz"), os.path.join(OUT, f"{stamp}_fixtures.csv.gz")
    fix, fix_t, written, seen_fix = {}, 0.0, 0, set()
    with gzip.open(path, "wt", newline="", encoding="utf-8") as f, gzip.open(fpath, "wt", newline="", encoding="utf-8") as ff:
        w, wf = csv.writer(f), csv.writer(ff)
        w.writerow(COLS); wf.writerow(FIX_COLS)
        last_full, prev = 0.0, {}
        while time.time() - t0 < DURATION:
            now = datetime.now(BJT)
            if time.time() - fix_t > 3600 or not fix:
                fix, fix_t = fixtures(now) or fix, time.time()
            ls = live_scores()
            goal = [mid for mid, (st, h, a) in ls.items() if st in (1, 2, 3) and mid in prev and (h, a) != prev[mid]]
            prev = {mid: (h, a) for mid, (st, h, a) in ls.items()}
            todo = []
            if time.time() - last_full >= STEP - 5:
                last_full = time.time()
                live = [mid for mid, (kt, *_r) in fix.items() if kt - timedelta(minutes=2) <= now <= kt + timedelta(minutes=130)]
                live += [mid for mid, (st, *_r) in ls.items() if st in (1, 2, 3) and mid not in live]
                todo = [(mid, "5min") for mid in live]
            todo += [(mid, "goal") for mid in goal if mid not in {m for m, _ in todo}]
            snap_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            with ThreadPoolExecutor(3) as ex:
                res = list(ex.map(lambda x: snap(x[0]), todo))
            n = 0
            for (mid, trig), rows in zip(todo, res):
                for row in rows:
                    w.writerow([snap_utc, *row, trig]); n += 1
                if rows and mid not in seen_fix and mid in fix:
                    kt, lg, h, a = fix[mid]
                    wf.writerow([mid, kt.strftime("%Y-%m-%d %H:%M"), lg, h, a]); seen_fix.add(mid)
            f.flush(); ff.flush()
            written += n
            if todo:
                print(f"{now:%m-%d %H:%M} BJT  快照 {len(todo)} 场（其中进球后补扫 {len(goal)}），记录 {n} 行（累计 {written}）", flush=True)
            left = DURATION - (time.time() - t0)
            if left <= 0:
                break
            time.sleep(max(1, min(left, GOAL_STEP - (time.time() - t0) % GOAL_STEP)))
    print("done", written, flush=True)
