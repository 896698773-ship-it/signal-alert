"""Load the packed klines: one gzip JSON file per symbol under data/k_<interval>/."""
import gzip
import json
import os

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def symbols(interval="5m"):
    return sorted(fn[:-8] for fn in os.listdir(os.path.join(ROOT, f"k_{interval}")) if fn.endswith(".json.gz"))


def load(symbol, interval="5m"):
    """Rows of [open_ms, open, high, low, close, volume, close_ms, quote_volume] with numbers converted."""
    rows = json.load(gzip.open(os.path.join(ROOT, f"k_{interval}", symbol + ".json.gz")))
    return [[int(r[0]), *map(float, r[1:6]), int(r[6]), float(r[7])] for r in rows]
