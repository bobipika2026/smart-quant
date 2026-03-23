# 动态因子权重系统

## 一、设计理念

因子权重**不是一成不变**的，需要根据市场环境动态调整：

```
基准权重 → 市场环境调整 → 经济周期调整 → 风险偏好调整 → 最终权重
```

---

## 二、影响因子权重的因素

### 2.1 市场环境

| 环境 | 特征 | 权重调整 |
|------|------|----------|
| **牛市** | 收益>10%，波动<25% | 动量↑ 成长↑ 价值↓ |
| **熊市** | 收益<-10% | 质量↑ 价值↑ 动量↓ |
| **震荡市** | 收益平稳，波动高 | 价值↑ 情绪↑ 动量↓ |
| **反转期** | 短期走强 | 成长↑ 动量↑ |

### 2.2 经济周期（美林时钟）

| 周期 | 特征 | 权重调整 |
|------|------|----------|
| **复苏期** | GDP↑ CPI↓ | 成长↑ 动量↑ |
| **繁荣期** | GDP↑ CPI↑ | 动量↑ 最高 |
| **衰退期** | GDP↓ CPI↑ | 质量↑ 价值↑ |
| **萧条期** | GDP↓ CPI↓ | 价值↑ 最高 |

### 2.3 风险偏好

| 指数范围 | 偏好类型 | 权重调整 |
|----------|----------|----------|
| **>70** | 激进 | 成长↑ 动量↑ 价值↓ |
| **30-70** | 中等 | 维持基准 |
| **<30** | 保守 | 价值↑ 质量↑ 动量↓ |

---

## 三、权重矩阵

### 3.1 市场环境权重

```python
MARKET_REGIME_WEIGHTS = {
    'bull':     {value: 0.15, growth: 0.22, quality: 0.18, momentum: 0.28, sentiment: 0.17},
    'bear':     {value: 0.28, growth: 0.12, quality: 0.32, momentum: 0.10, sentiment: 0.18},
    'sideways': {value: 0.28, growth: 0.18, quality: 0.22, momentum: 0.12, sentiment: 0.20},
    'recovery': {value: 0.18, growth: 0.28, quality: 0.20, momentum: 0.22, sentiment: 0.12},
}
```

### 3.2 经济周期权重

```python
ECONOMIC_CYCLE_WEIGHTS = {
    'recovery':  {value: 0.18, growth: 0.30, quality: 0.20, momentum: 0.22, sentiment: 0.10},
    'expansion': {value: 0.15, growth: 0.22, quality: 0.18, momentum: 0.30, sentiment: 0.15},
    'slowdown':  {value: 0.22, growth: 0.15, quality: 0.30, momentum: 0.15, sentiment: 0.18},
    'recession': {value: 0.32, growth: 0.12, quality: 0.28, momentum: 0.08, sentiment: 0.20},
}
```

### 3.3 权重融合公式

```python
final_weight = (
    market_weight * 0.4 +      # 市场环境权重 40%
    cycle_weight * 0.3 +       # 经济周期权重 30%
    baseline_weight * 0.3 +    # 基准权重 30%
    risk_appetite_adjustment   # 风险偏好调整
)
```

---

## 四、市场环境识别

### 4.1 牛市识别

```python
if total_return > 10% and volatility < 25%:
    regime = 'bull'
```

### 4.2 熊市识别

```python
if total_return < -10%:
    regime = 'bear'
```

### 4.3 震荡市识别

```python
if abs(total_return) < 5% and volatility > 20%:
    regime = 'sideways'
```

### 4.4 反转期识别

```python
if trend_strength > 5% and recent_returns > 0:
    regime = 'recovery'
```

---

## 五、风险偏好计算

```python
risk_appetite = (
    volatility_score * 0.4 +    # 波动率评分
    trend_score * 0.3 +         # 趋势评分
    volume_score * 0.3          # 成交量评分
)
```

| 指标 | 计算方式 | 影响 |
|------|----------|------|
| 波动率评分 | 100 - volatility * 2 | 低波动 = 高风险偏好 |
| 趋势评分 | 50 + trend * 5 | 上涨趋势 = 高风险偏好 |
| 成交量评分 | vol_ratio * 50 | 放量 = 高风险偏好 |

---

## 六、API接口

### 6.1 获取动态权重

```
GET /api/stock-scoring/v3/dynamic-weights

Response:
{
  "weights": {value: 0.24, growth: 0.17, ...},
  "market_regime": "sideways",
  "economic_cycle": "slowdown",
  "risk_appetite": 50.0,
  "generated_at": "2026-03-15 09:18:54"
}
```

### 6.2 使用动态权重生成股票池

```
GET /api/stock-scoring/v3/pool-with-dynamic-weights?use_dynamic=true

Response:
{
  "stocks": [...],
  "weight_config": {
    "weights": {...},
    "market_regime": "sideways",
    ...
  }
}
```

---

## 七、行业轮动调整

不同行业对不同因子的敏感度不同：

| 行业 | 敏感因子 | 调整系数 |
|------|----------|----------|
| 银行 | 价值、质量 | ×1.2, ×1.3 |
| 电子 | 成长、动量 | ×1.3, ×1.2 |
| 医药 | 成长、质量 | ×1.2, ×1.2 |
| 传媒 | 情绪、动量 | ×1.4, ×1.2 |
| 房地产 | 价值、情绪 | ×1.2, ×1.2 |

---

## 八、实际应用

### 8.1 当前市场状态

```
市场环境: 震荡市 (sideways)
经济周期: 衰退期 (slowdown)
风险偏好: 50.0 (中等)

动态权重:
- 价值: 24.4% (基准22%)
- 成长: 17.1% (基准18%)
- 质量: 26.2% (基准28%)
- 动量: 14.4% (基准17%)
- 情绪: 17.9% (基准15%)
```

### 8.2 调整建议

震荡市+衰退期组合下：
- ✅ 增加价值因子权重
- ✅ 增加情绪因子权重
- ❌ 降低动量因子权重
- ❌ 适度降低成长因子权重

---

*文档版本: v1.0*
*更新时间: 2026-03-15*