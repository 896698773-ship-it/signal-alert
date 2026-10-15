# 幣安 USDT 永續合約 K 線（2026-07-01 ~ 10-15）

路徑都相對於這個 `kline_data` 文件夾。

這裡只有行情數據。

| 文件夾 | 內容 |
|---|---|
| `data/k_5m/<幣>.json.gz` | 523 個幣的 5 分鐘 K 線，北京時間 09-29 15:35 ~ 10-10 13:45 |
| `data/k_15m/<幣>.json.gz` | 同樣 523 個幣的 15 分鐘 K 線，09-29 15:45 ~ 10-10 13:30 |
| `data/k_5m_long/<幣>.json.gz` | 同樣 523 個幣的 5 分鐘 K 線，北京時間 07-01 15:35 ~ 10-15 06:45（較晚上架的幣從上架開始）；用 `load(幣, "5m_long")` 讀取 |
| `data/raw/` | 幣安的合約信息（`exchangeInfo`）和槓桿檔位（`brackets`） |

- 來源：`www.binance.com/fapi/v1/klines`，只有已收盤的 K 線。
- 每根 K 線是 `[開盤時間, 開, 高, 低, 收, 成交量, 收盤時間, 成交額(USDT)]`。時間是 UTC 毫秒時間戳，北京時間要加 8 小時。原始文件裡價格和量是字串。

## 讀法

```python
from load import load, symbols

rows = load("BTCUSDT", "5m")   # 數字已轉成 int / float
print(len(symbols("5m")), rows[-1])
```

不用 `load.py` 也可以直接讀：

```python
import gzip, json
rows = json.load(gzip.open("data/k_5m/BTCUSDT.json.gz"))
```
