# signal-alert

每 5 分钟检查一次币安 USDT 永续最新收盘的 5m K线。出现 E 模型信号时，推送到 Telegram。不用 AI，不耗 Claude 额度。

- 脚本：`alert/signal_alert.py`（规则与研究仓库里的 E 模型相同）
- 定时任务：`.github/workflows/signal-alert.yml`。每 6 小时启动一次，每次连续检查约 5 小时 50 分钟。
- 唯一需要的密钥：`TELEGRAM_TOKEN`（在 Settings → Secrets and variables → Actions 里添加）。Token 不写进任何文件。
- 推送内容：币种、方向、入场时间（下一根 5m 开盘，过了不追）、止损、按 100U 本金算好的仓位、杠杆上限和保证金。做空信号会标注"建议暂停实盘，仅记录"。

注意：公开仓库如果 60 天没有任何改动，GitHub 会自动停用定时任务。到时手动运行一次即可恢复。
