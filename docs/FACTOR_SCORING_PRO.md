# 专业因子评分系统设计

## 一、因子体系架构

参考Barra、MSCI、中金量化等专业机构的因子框架，采用**多层级因子体系**。

```
                    ┌─────────────────────────────────────┐
                    │         综合得分 (Composite)         │
                    │            100分制                   │
                    └─────────────────────────────────────┘
                                    │
        ┌───────────┬───────────┬───┴───┬───────────┬───────────┐
        ▼           ▼           ▼       ▼           ▼           ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
   │  价值   │ │  成长   │ │  质量   │ │  动量   │ │  情绪   │
   │ Value   │ │ Growth  │ │ Quality │ │ Momentum│ │Sentiment│
   │  25%    │ │  20%    │ │  25%    │ │  15%    │ │  15%    │
   └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘
        │           │           │           │           │
    细分因子    细分因子     细分因子     细分因子     细分因子
```

---

## 二、大类因子定义

### 2.1 价值因子 (Value, 25%)

**核心逻辑**：寻找被低估的股票，买入便宜的好公司

| 因子名称 | 公式/定义 | 权重 | IC均值 | 说明 |
|----------|----------|------|--------|------|
| **EP** | E/P (盈利收益率) | 8% | 0.05 | 越高越便宜 |
| **BP** | B/P (账面市值比) | 6% | 0.04 | 价值投资核心 |
| **SP** | S/P (销售市值比) | 4% | 0.03 | 避免盈余操纵 |
| **NCFP** | 经营现金流/市值 | 4% | 0.04 | 真实盈利能力 |
| **股息率** | DPS/Price | 3% | 0.03 | 稳定回报 |

**评分方法**：百分位法 + 行业中性化

```python
# 行业内百分位
EP_percentile = rank_industry(EP)  # 行业内排名
BP_percentile = rank_industry(BP)

# 合成价值因子
Value_Score = 0.32*EP_percentile + 0.24*BP_percentile + 
              0.16*SP_percentile + 0.16*NCFP_percentile + 0.12*DivYield_percentile
```

---

### 2.2 成长因子 (Growth, 20%)

**核心逻辑**：寻找高增长公司，享受成长红利

| 因子名称 | 公式/定义 | 权重 | IC均值 | 说明 |
|----------|----------|------|--------|------|
| **营收增长** | (Rev_t - Rev_t-1) / Rev_t-1 | 5% | 0.04 | 顶层增长 |
| **利润增长** | (NI_t - NI_t-1) / NI_t-1 | 5% | 0.05 | 核心增长 |
| **ROE变化** | ROE_t - ROE_t-1 | 4% | 0.04 | 盈利能力改善 |
| **营收增长加速度** | Δ(营收增长) | 3% | 0.03 | 增长动能 |
| **EPS增长** | (EPS_t - EPS_t-1) / EPS_t-1 | 3% | 0.04 | 每股改善 |

**评分方法**：同比/环比 + 趋势判断

```python
# YoY增长率
Revenue_Growth_YoY = (Revenue_ttm - Revenue_ttm_1y_ago) / abs(Revenue_ttm_1y_ago)
Profit_Growth_YoY = (NetProfit_ttm - NetProfit_ttm_1y_ago) / abs(NetProfit_ttm_1y_ago)

# 环比加速度
Growth_Acceleration = (Growth_q - Growth_q_1) / abs(Growth_q_1)

# 综合成长分
Growth_Score = 0.25*Rev_Growth + 0.25*Profit_Growth + 
               0.20*ROE_Change + 0.15*Acceleration + 0.15*EPS_Growth
```

---

### 2.3 质量因子 (Quality, 25%)

**核心逻辑**：筛选财务健康、盈利稳定的优质公司

| 因子名称 | 公式/定义 | 权重 | IC均值 | 说明 |
|----------|----------|------|--------|------|
| **ROE** | 净利润/净资产 | 7% | 0.06 | 盈利能力核心 |
| **ROA** | 净利润/总资产 | 4% | 0.05 | 资产效率 |
| **毛利率** | (营收-成本)/营收 | 4% | 0.04 | 定价能力 |
| **净利率** | 净利润/营收 | 3% | 0.04 | 成本控制 |
| **财务杠杆** | 资产/权益 (反向) | 3% | 0.03 | 风险控制 |
| **流动比率** | 流动资产/流动负债 | 2% | 0.02 | 短期偿债 |
| **应计项目** | (净利润-经营现金流)/资产 (反向) | 2% | 0.04 | 盈利质量 |

**评分方法**：绝对阈值 + 相对排名

```python
# ROE评分（绝对+相对）
def score_roe(roe):
    if roe > 20: return 100
    elif roe > 15: return 90
    elif roe > 12: return 80
    elif roe > 10: return 70
    elif roe > 8: return 60
    elif roe > 5: return 50
    else: return 30

# 应计项目（越小越好，反向）
Accruals = (NetProfit - OperatingCashFlow) / TotalAssets
Accruals_Score = rank_descending(Accruals)  # 越小分数越高

# 综合质量分
Quality_Score = 0.28*ROE + 0.16*ROA + 0.16*GrossMargin + 
                0.12*NetMargin + 0.12*Leverage + 0.08*CurrentRatio + 0.08*Accruals
```

---

### 2.4 动量因子 (Momentum, 15%)

**核心逻辑**：强者恒强，跟随趋势

| 因子名称 | 公式/定义 | 权重 | IC均值 | 说明 |
|----------|----------|------|--------|------|
| **价格动量** | 过去12个月收益率（剔除最近1月） | 5% | 0.05 | 经典动量 |
| **相对强度** | 股价/行业指数 | 3% | 0.03 | 相对表现 |
| **收益动量** | 盈利预测上调比例 | 3% | 0.04 | 分析师预期 |
| **成交量动量** | 近5日均量/近20日均量 | 2% | 0.02 | 资金关注 |
| **均线偏离** | (Price - MA20)/MA20 | 2% | 0.03 | 短期趋势 |

**评分方法**：时间序列 + 截面排名

```python
# 经典动量（剔除最近一月，避免反转）
Price_Momentum_12M = Price_t / Price_t-12m - 1  # 12月收益
Price_Momentum_11M = Price_t-1m / Price_t-12m - 1  # 剔除最近一月
Momentum = Price_Momentum_11M  # 使用剔除版本

# 相对强度
Relative_Strength = Stock_Price / Industry_Index_Price

# 综合动量分
Momentum_Score = 0.33*Price_Momentum + 0.20*Relative_Strength + 
                 0.20*Earnings_Momentum + 0.13*Volume_Momentum + 0.13*MA_Deviation
```

---

### 2.5 情绪因子 (Sentiment, 15%)

**核心逻辑**：捕捉市场情绪和资金流向

| 因子名称 | 公式/定义 | 权重 | IC均值 | 说明 |
|----------|----------|------|--------|------|
| **北向资金** | 北向持股比例变化 | 5% | 0.05 | 聪明钱 |
| **机构持仓** | 机构持股比例 | 3% | 0.04 | 机构认可 |
| **分析师覆盖** | 分析师评级均值 | 2% | 0.03 | 专业预期 |
| **换手率** | 成交量/流通股本 (反向) | 2% | 0.03 | 过热风险 |
| **融资余额** | 融资余额变化 | 2% | 0.03 | 杠杆情绪 |
| **融券余额** | 融券余额/流通市值 (反向) | 1% | 0.02 | 做空压力 |

**评分方法**：变化率 + 持仓比例

```python
# 北向资金变化（近5日增持比例）
North_Change_5D = (North_Holding_t - North_Holding_t-5d) / Circulation_Shares

# 机构持仓
Institutional_Ratio = Institutional_Holdings / Total_Shares

# 综合情绪分
Sentiment_Score = 0.33*North_Fund + 0.20*Institutional + 
                  0.13*Analyst + 0.13*Turnover + 0.13*Margin + 0.08*Short
```

---

## 三、因子处理流程

### 3.1 数据预处理

```python
def preprocess_factor_data(df):
    """因子数据预处理"""
    
    # 1. 剔除异常值（MAD法）
    median = df['factor'].median()
    mad = (df['factor'] - median).abs().median()
    upper = median + 5 * mad
    lower = median - 5 * mad
    df['factor'] = df['factor'].clip(lower, upper)
    
    # 2. 标准化（Z-Score）
    df['factor_z'] = (df['factor'] - df['factor'].mean()) / df['factor'].std()
    
    # 3. 行业市值中性化
    df['factor_neutral'] = neutralize(df['factor'], 
                                       industry_dummies, 
                                       log_market_cap)
    
    return df
```

### 3.2 行业中性化

```python
def neutralize_by_industry(factor_values, industry_codes):
    """行业内标准化"""
    result = pd.Series(index=factor_values.index)
    
    for industry in industry_codes.unique():
        mask = (industry_codes == industry)
        industry_values = factor_values[mask]
        # 行业内Z-Score
        result[mask] = (industry_values - industry_values.mean()) / industry_values.std()
    
    return result.fillna(0)
```

### 3.3 极值处理

```python
def winsorize(series, limits=(0.025, 0.025)):
    """缩尾处理"""
    lower = series.quantile(limits[0])
    upper = series.quantile(1 - limits[1])
    return series.clip(lower, upper)
```

---

## 四、综合评分计算

### 4.1 评分公式

```python
def calculate_composite_score(stock_data):
    """
    计算综合评分
    
    Returns:
        composite_score: 综合评分 (0-100)
        style_scores: 风格因子得分
        detail_scores: 细分因子得分
    """
    
    # 1. 计算各风格因子得分
    value_score = calc_value_score(stock_data)
    growth_score = calc_growth_score(stock_data)
    quality_score = calc_quality_score(stock_data)
    momentum_score = calc_momentum_score(stock_data)
    sentiment_score = calc_sentiment_score(stock_data)
    
    # 2. 加权合成（权重根据历史IC优化）
    weights = {
        'value': 0.25,
        'growth': 0.20,
        'quality': 0.25,
        'momentum': 0.15,
        'sentiment': 0.15
    }
    
    composite_score = (
        value_score * weights['value'] +
        growth_score * weights['growth'] +
        quality_score * weights['quality'] +
        momentum_score * weights['momentum'] +
        sentiment_score * weights['sentiment']
    )
    
    # 3. 转换为0-100分制
    composite_score = composite_score * 100
    
    return {
        'composite_score': round(composite_score, 1),
        'style_scores': {
            'value': round(value_score * 100, 1),
            'growth': round(growth_score * 100, 1),
            'quality': round(quality_score * 100, 1),
            'momentum': round(momentum_score * 100, 1),
            'sentiment': round(sentiment_score * 100, 1)
        }
    }
```

### 4.2 评级标准

| 评级 | 分数范围 | 占比预期 | 说明 |
|------|----------|----------|------|
| **A+** | ≥85 | 5% | 极优 |
| **A** | ≥75 | 10% | 优秀 |
| **B+** | ≥65 | 20% | 良好 |
| **B** | ≥55 | 25% | 中等偏上 |
| **B-** | ≥45 | 20% | 中等 |
| **C** | ≥35 | 15% | 较差 |
| **D** | <35 | 5% | 不推荐 |

---

## 五、因子有效性检验

### 5.1 IC (Information Coefficient)

```python
def calc_ic(factor_values, forward_returns):
    """
    计算因子IC值
    
    IC = corr(因子值, 未来收益率)
    
    评价标准:
    - IC > 0.05: 优秀
    - IC > 0.03: 良好
    - IC > 0.01: 有效
    - IC < 0.01: 无效
    """
    return factor_values.corr(forward_returns, method='spearman')
```

### 5.2 IR (Information Ratio)

```python
def calc_ir(ic_series):
    """
    计算信息比率
    
    IR = IC均值 / IC标准差
    
    评价标准:
    - IR > 0.5: 优秀
    - IR > 0.3: 良好
    - IR > 0.1: 有效
    """
    return ic_series.mean() / ic_series.std()
```

### 5.3 分组测试

```python
def group_test(factor_values, forward_returns, n_groups=10):
    """
    分组回测
    
    将股票按因子值分成10组，检验各组收益差异
    """
    labels = ['G' + str(i+1) for i in range(n_groups)]
    factor_values['group'] = pd.qcut(factor_values, n_groups, labels=labels)
    
    group_returns = forward_returns.groupby(factor_values['group']).mean()
    
    # 多空收益
    long_short = group_returns['G10'] - group_returns['G1']
    
    return {
        'group_returns': group_returns,
        'long_short': long_short,
        'spread': group_returns['G10'] / group_returns['G1'] - 1
    }
```

---

## 六、权重优化

### 6.1 均值-方差优化

```python
def optimize_weights(ic_matrix, ir_target=0.5):
    """
    因子权重优化
    
    目标：最大化组合IR
    约束：权重和=1，权重>=0
    """
    from scipy.optimize import minimize
    
    def objective(w):
        # 组合IC = sum(w_i * IC_i)
        portfolio_ic = (w * ic_matrix.mean()).sum()
        # 组合波动 = sqrt(w.T @ Cov @ w)
        portfolio_std = np.sqrt(w @ ic_matrix.cov() @ w)
        # IR = IC / Std
        return -portfolio_ic / portfolio_std  # 最大化IR
    
    n_factors = ic_matrix.shape[1]
    initial_weights = np.ones(n_factors) / n_factors
    
    constraints = [
        {'type': 'eq', 'fun': lambda w: w.sum() - 1}  # 权重和=1
    ]
    bounds = [(0, 0.4) for _ in range(n_factors)]  # 单因子权重不超过40%
    
    result = minimize(objective, initial_weights, 
                     method='SLSQP',
                     bounds=bounds,
                     constraints=constraints)
    
    return result.x
```

---

## 七、实际应用

### 7.1 股票池生成流程

```
1. 数据获取
   ├─ 财务数据（季报/年报）
   ├─ 市场数据（日线/分钟线）
   └─ 另类数据（北向/机构/分析师）

2. 因子计算
   ├─ 价值因子 (5个)
   ├─ 成长因子 (5个)
   ├─ 质量因子 (7个)
   ├─ 动量因子 (5个)
   └─ 情绪因子 (6个)

3. 因子处理
   ├─ 极值处理 (MAD/Winsorize)
   ├─ 标准化 (Z-Score)
   └─ 行业中性化

4. 综合评分
   ├─ 风格因子合成
   ├─ 加权计算综合分
   └─ 评级划分

5. 筛选过滤
   ├─ 剔除ST/停牌/新股
   ├─ 剔除财务异常
   └─ 取Top N入池
```

### 7.2 调仓频率

| 因子类型 | 调仓频率 | 说明 |
|----------|----------|------|
| 价值因子 | 季度 | 跟随财报披露 |
| 成长因子 | 季度 | 跟随财报披露 |
| 质量因子 | 季度 | 跟随财报披露 |
| 动量因子 | 月度 | 月度再平衡 |
| 情绪因子 | 周度 | 资金流动变化 |

---

## 八、风险控制

### 8.1 行业偏离控制

```python
def control_industry_exposure(portfolio, benchmark, max_deviation=0.05):
    """
    控制行业偏离度
    单行业偏离不超过5%
    """
    industry_weights = portfolio.groupby('industry')['weight'].sum()
    benchmark_weights = benchmark.groupby('industry')['weight'].sum()
    
    deviation = (industry_weights - benchmark_weights).abs()
    
    if (deviation > max_deviation).any():
        # 超限，需要调整
        return adjust_weights(portfolio, benchmark, max_deviation)
    
    return portfolio
```

### 8.2 因子暴露控制

```python
def control_factor_exposure(portfolio, max_exposure=2.0):
    """
    控制单因子暴露
    Z-Score不超过±2
    """
    factor_exposures = calc_factor_exposures(portfolio)
    
    for factor, exposure in factor_exposures.items():
        if abs(exposure) > max_exposure:
            # 超限预警
            log.warning(f"{factor} exposure {exposure:.2f} exceeds limit {max_exposure}")
    
    return factor_exposures
```

---

*文档版本: v1.0*
*更新时间: 2026-03-15*