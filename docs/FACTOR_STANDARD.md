# 专业量化因子体系设计规范

## 一、因子体系架构

参考 Barra CNE5、MSCI China Equity Model、中金量化因子库等专业框架。

### 1.1 三层级因子结构

```
                    ┌─────────────────────────────────────┐
                    │         综合得分 (Alpha)            │
                    │           目标收益预测               │
                    └─────────────────────────────────────┘
                                    │
        ┌───────────┬───────────┬───┴───┬───────────┬───────────┐
        ▼           ▼           ▼       ▼           ▼           ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
   │  价值   │ │  成长   │ │  质量   │ │  动量   │ │  情绪   │
   │ Value   │ │ Growth  │ │ Quality │ │ Momentum│ │Sentiment│
   │ Risk风格│ │ Risk风格│ │ Risk风格│ │ Risk风格│ │ Risk风格│
   └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘
        │           │           │           │           │
   ┌────┴────┐ ┌────┴────┐ ┌────┴────┐ ┌────┴────┐ ┌────┴────┐
   │         │ │         │ │         │ │         │ │         │
   ▼         ▼ ▼         ▼ ▼         ▼ ▼         ▼ ▼         ▼
  因子层    因子层      因子层      因子层      因子层
 (5-7个)   (5-7个)     (7-10个)    (5-7个)     (5-7个)
```

---

## 二、风格因子定义

### 2.1 价值因子 (Value)

**经济逻辑**：买入被低估的股票，等待价值回归

| 因子名称 | 因子代码 | 公式 | IC预期 | 权重 |
|----------|----------|------|--------|------|
| 盈利收益率 | EP | E_ttm / MarketCap | 0.04 | 25% |
| 账面市值比 | BP | B / MarketCap | 0.03 | 20% |
| 销售市值比 | SP | Revenue_ttm / MarketCap | 0.02 | 15% |
| 现金流市值比 | NCFP | OCFA_ttm / MarketCap | 0.04 | 20% |
| 股息率 | DIV | DPS / Price | 0.03 | 10% |
| 企业价值比 | EBITDA_EV | EBITDA / EV | 0.03 | 10% |

**打分方法**：
```python
# Step 1: 行业内百分位
EP_ind_percentile = rank_in_industry(EP)

# Step 2: 极值处理（去极值）
EP_winsorized = winsorize(EP_ind_percentile, (2.5, 97.5))

# Step 3: Z-Score标准化
EP_zscore = (EP_winsorized - mean) / std

# Step 4: 合成价值因子
Value = w1*EP + w2*BP + w3*SP + w4*NCFP + w5*DIV + w6*EBITDA_EV

# Step 5: 转换为0-100分
Value_Score = (Value - min) / (max - min) * 100
```

---

### 2.2 成长因子 (Growth)

**经济逻辑**：捕捉公司增长带来的超额收益

| 因子名称 | 因子代码 | 公式 | IC预期 | 权重 |
|----------|----------|------|--------|------|
| 营收增长率 | REV_G | (Rev_t - Rev_t-1) / |Rev_t-1| | 0.03 | 20% |
| 净利润增长率 | NP_G | (NP_t - NP_t-1) / |NP_t-1| | 0.04 | 25% |
| EPS增长率 | EPS_G | (EPS_t - EPS_t-1) / |EPS_t-1| | 0.03 | 15% |
| ROE变化 | ROE_D | ROE_t - ROE_t-1 | 0.03 | 20% |
| 营收增长加速度 | REV_ACC | ΔREV_G | 0.02 | 10% |
| 净利率变化 | NPM_D | NPM_t - NPM_t-1 | 0.02 | 10% |

**打分方法**：
```python
# 同比增长率（考虑正负）
def calc_growth(current, previous):
    if previous > 0:
        return (current - previous) / previous
    elif previous < 0:
        return (current - previous) / abs(previous)  # 扭亏为盈
    else:
        return 0

# 使用3年平均增长率平滑
REV_G_avg = (REV_G_t + REV_G_t-1 + REV_G_t-2) / 3

# 行业内百分位 + 合成
Growth_Score = weighted_sum(percentile_scores)
```

---

### 2.3 质量因子 (Quality)

**经济逻辑**：筛选财务稳健、盈利质量高的公司

| 因子名称 | 因子代码 | 公式 | IC预期 | 权重 |
|----------|----------|------|--------|------|
| ROE | ROE | NetIncome / Equity | 0.06 | 20% |
| ROA | ROA | NetIncome / Assets | 0.04 | 10% |
| 毛利率 | GPM | (Rev - COGS) / Rev | 0.03 | 10% |
| 净利率 | NPM | NetIncome / Revenue | 0.03 | 10% |
| 资产周转率 | AT | Revenue / Assets | 0.02 | 10% |
| 财务杠杆 | LEV | Assets / Equity (反向) | 0.02 | 10% |
| 流动比率 | CR | CA / CL | 0.01 | 5% |
| 应计项目 | ACCR | (NI - OCF) / Assets (反向) | 0.04 | 15% |
| 经营现金流占比 | OCF_NI | OCF / NI | 0.03 | 10% |

**打分方法**：
```python
# ROE评分（考虑稳定性）
ROE_stability = 1 - std(ROE_3y) / mean(ROE_3y)  # 3年ROE稳定性
ROE_score = ROE_current * ROE_stability

# 应计项目（越小越好，反向）
ACCR_score = -ACCR  # 反向

# 财务杠杆（适度为佳）
LEV_score = min(LEV, 3) / 3  # 杠杆率<3为健康

# 综合质量分
Quality_Score = weighted_sum(all_scores)
```

---

### 2.4 动量因子 (Momentum)

**经济逻辑**：强者恒强，趋势延续

| 因子名称 | 因子代码 | 公式 | IC预期 | 权重 |
|----------|----------|------|--------|------|
| 12月动量 | MOM12 | Ret_t-1m / Ret_t-12m - 1 | 0.05 | 30% |
| 6月动量 | MOM6 | Ret_t / Ret_t-6m - 1 | 0.04 | 20% |
| 相对强度 | RS | Stock / Industry_Index | 0.03 | 15% |
| 成交量动量 | VOL_M | Vol_5d / Vol_20d | 0.02 | 10% |
| 均线偏离 | MA_DEV | (P - MA20) / MA20 | 0.02 | 10% |
| 盈利修正 | ER | EPS_revise_up / total | 0.04 | 15% |

**打分方法**：
```python
# 经典动量（剔除最近一月，避免短期反转）
MOM12M = Price_t-1m / Price_t-12m - 1

# 行业内动量排名
MOM_rank = rank_in_industry(MOM12M)

# 相对强度
RS = Stock_Price / Industry_Index_Price

# 综合动量分
Momentum_Score = weighted_sum(MOM_rank, RS, VOL_M, ...)
```

---

### 2.5 情绪因子 (Sentiment)

**经济逻辑**：捕捉市场情绪和资金流向

| 因子名称 | 因子代码 | 公式 | IC预期 | 权重 |
|----------|----------|------|--------|------|
| 北向持股变化 | NORTH_C | ΔNorth_Holding / Shares | 0.05 | 25% |
| 机构持股比例 | INST | Inst_Holding / Shares | 0.04 | 20% |
| 分析师评级 | ANALYST | Rating_mean | 0.03 | 15% |
| 换手率 | TURN | Vol / Float_Shares | 0.02 | 10% |
| 融资余额变化 | MTGN_C | ΔMargin_Balance / Cap | 0.03 | 15% |
| 波动率 | VOL | Std(Ret_20d) * √252 (反向) | 0.02 | 15% |

**打分方法**：
```python
# 北向资金变化（近5日）
NORTH_C = (North_t - North_t-5) / Float_Shares

# 换手率（适度为佳，过热风险）
TURN_score = 1 - abs(TURN - industry_mean) / industry_std

# 波动率（反向，低波动更优）
VOL_score = -VOL

# 综合情绪分
Sentiment_Score = weighted_sum(all_scores)
```

---

## 三、标准化处理流程

### 3.1 因子处理标准流程

```python
def process_factor(raw_values: pd.Series, industry_codes: pd.Series) -> pd.Series:
    """
    因子处理标准流程
    
    Step 1: 去极值 (Winsorize)
    Step 2: 行业中性化 (Industry Neutralize)
    Step 3: 标准化 (Z-Score)
    Step 4: 缺失值处理 (Impute)
    """
    
    # Step 1: MAD法去极值
    median = raw_values.median()
    mad = (raw_values - median).abs().median()
    upper = median + 5 * mad
    lower = median - 5 * mad
    winsorized = raw_values.clip(lower, upper)
    
    # Step 2: 行业中性化
    neutralized = pd.Series(index=raw_values.index)
    for industry in industry_codes.unique():
        mask = (industry_codes == industry)
        industry_values = winsorized[mask]
        neutralized[mask] = industry_values - industry_values.mean()
    
    # Step 3: Z-Score标准化
    mean = neutralized.mean()
    std = neutralized.std()
    zscore = (neutralized - mean) / std
    
    # Step 4: 缺失值填充（行业中位数）
    zscore = zscore.fillna(zscore.median())
    
    return zscore
```

### 3.2 行业分类标准

采用**申万一级行业分类**（31个行业）：

```python
INDUSTRIES = [
    '银行', '非银金融', '房地产', '建筑装饰', '建筑材料',
    '钢铁', '煤炭', '石油石化', '有色金属', '基础化工',
    '电力设备', '机械设备', '汽车', '家用电器', '轻工制造',
    '电子', '计算机', '通信', '传媒', '电力', '环保',
    '医药生物', '食品饮料', '农林牧渔', '商贸零售',
    '社会服务', '美容护理', '纺织服饰', '国防军工', '交通运输'
]
```

---

## 四、权重配置

### 4.1 基于历史IC的权重优化

```python
# 因子IC检验（过去36个月）
IC_series = calc_IC(factor_values, forward_returns, 36)

# IR = IC_mean / IC_std
IR = IC_series.mean() / IC_series.std()

# 权重优化
weights = optimize_weights(
    IC_matrix, 
    method='max_IR',  # 最大化IR
    constraints={'sum': 1, 'min': 0.05, 'max': 0.35}
)
```

### 4.2 推荐权重配置

| 风格因子 | 权重 | 依据 |
|----------|------|------|
| 价值 Value | 22% | IC ~ 0.04, IR ~ 0.35 |
| 成长 Growth | 18% | IC ~ 0.035, IR ~ 0.30 |
| 质量 Quality | 28% | IC ~ 0.05, IR ~ 0.45 |
| 动量 Momentum | 17% | IC ~ 0.045, IR ~ 0.40 |
| 情绪 Sentiment | 15% | IC ~ 0.035, IR ~ 0.28 |

---

## 五、评级体系

### 5.1 综合评分计算

```python
# 加权综合
Composite_Score = (
    Value_Score * 0.22 +
    Growth_Score * 0.18 +
    Quality_Score * 0.28 +
    Momentum_Score * 0.17 +
    Sentiment_Score * 0.15
)

# 转换为0-100分
Final_Score = percentile_rank(Composite_Score) * 100
```

### 5.2 评级标准

| 评级 | 百分位范围 | 预期占比 | 含义 |
|------|------------|----------|------|
| **A** | 90-100 | 10% | 极强买入 |
| **B+** | 75-90 | 15% | 买入 |
| **B** | 50-75 | 25% | 增持 |
| **B-** | 35-50 | 15% | 中性 |
| **C** | 15-35 | 20% | 减持 |
| **D** | 0-15 | 15% | 卖出 |

---

## 六、风险控制

### 6.1 行业偏离控制

```python
# 股票池行业权重 vs 基准行业权重
max_industry_deviation = 0.05  # 单行业偏离不超过5%

for industry in industries:
    pool_weight = pool_stocks[industry].sum()
    benchmark_weight = benchmark[industry]
    deviation = abs(pool_weight - benchmark_weight)
    assert deviation <= max_industry_deviation
```

### 6.2 因子暴露控制

```python
# 单因子Z-Score暴露不超过±2
max_factor_exposure = 2.0

for factor in factors:
    exposure = pool_stocks[factor].mean()
    assert abs(exposure) <= max_factor_exposure
```

---

## 七、回测验证

### 7.1 IC检验标准

| 指标 | 标准 | 说明 |
|------|------|------|
| IC均值 | > 0.03 | 因子有效 |
| IC标准差 | < 0.15 | 因子稳定 |
| IR | > 0.3 | 信息比率良好 |
| IC>0占比 | > 55% | 胜率达标 |

### 7.2 分组回测

```python
# 十分组回测
groups = pd.qcut(factor_values, 10, labels=['G1', ..., 'G10'])

# 计算各组收益
group_returns = forward_returns.groupby(groups).mean()

# 多空收益
long_short = group_returns['G10'] - group_returns['G1']

# 检验标准
assert long_short > 0.02  # 多空收益>2%
assert group_returns['G10'] > group_returns['G5'] > group_returns['G1']  # 单调性
```

---

## 八、实施规范

### 8.1 数据频率

| 数据类型 | 更新频率 | 说明 |
|----------|----------|------|
| 财务数据 | 季度 | 跟随财报披露 |
| 价格数据 | 日度 | 实时更新 |
| 另类数据 | 日度/周度 | 北向、融资等 |

### 8.2 调仓频率

| 因子类型 | 推荐频率 | 说明 |
|----------|----------|------|
| 价值、成长、质量 | 月度 | 跟随基本面变化 |
| 动量 | 周度 | 捕捉趋势变化 |
| 情绪 | 周度 | 跟踪资金流向 |

### 8.3 文档规范

- 因子定义文档
- 因子检验报告
- 权重配置说明
- 调仓记录

---

*规范版本: v2.0*
*更新时间: 2026-03-15*
*参考框架: Barra CNE5, MSCI China, 中金量化*