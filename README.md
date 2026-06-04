# A股异动股票筛选器

> "好股票会一直压着异动，沿着异动走"

基于同向异动+压线趋势+PreSurge预判的A股短线筛选工具，每日8:25自动扫描，集合竞价前完成分析。

## 策略核心

| 维度 | 规则 |
|---|---|
| 异动定义 | 3日≥20% / 10日≥100% / 30日≥200% |
| 同向异动 | 4~6次（<4次样本不够，≥7次鱼尾剔除） |
| 趋势确认 | 回调≤15% + 均线多头 + 斜率向上 |
| PreSurge | 5维评分（量能预热/韵律/收口/底部/MACD），≥3分预判候选 |
| 涨停过滤 | 涨幅≥9.5%自动标记为买不进 |
| 买点 | 当前价入场，止损=异动起点×0.97 |

## 数据来源

- **K线**：mootdx TCP
- **实时行情**：腾讯财经 API
- **强势股池**：同花顺

## 快速开始

```bash
# 每日推荐：组合模式（异动确认 + PreSurge预判）
python3 yidong_scanner.py --mode hot --both

# 仅异动确认
python3 yidong_scanner.py --mode hot

# 仅PreSurge预判
python3 yidong_scanner.py --mode hot --presurge

# 指定股票分析
python3 yidong_scanner.py --codes 600367 001896

# 重新生成历史回顾页
python3 yidong_scanner.py --history
```

## 输出

| 文件 | 说明 |
|---|---|
| `yidong_result.html` | 可视化报告（可买入/涨停/鱼尾/预判四区） |
| `yidong_result.json` | 结构化结果数据 |
| `yidong_history.html` | 历史回顾（每日摘要+跨日追踪） |
| `history/YYYY-MM-DD/` | 每日归档 |

## 依赖

```
pandas numpy mootdx requests
```

## 许可

仅供研究参考，不构成投资建议。
