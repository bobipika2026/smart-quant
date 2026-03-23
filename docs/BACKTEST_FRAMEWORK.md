# 专业量化回测框架设计

## 一、框架架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    专业量化回测框架                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│  │  数据层     │  │  因子层     │  │  策略层     │            │
│  │ Data Layer  │  │Factor Layer │  │Strategy Layer│            │
│  └─────────────┘  └─────────────┘  └─────────────┘            │
│         │               │               │                      │
│         └───────────────┼───────────────┘                      │
│                         ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                    回测引擎                              │  │
│  │                 Backtest Engine                         │  │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐       │  │
│  │  │IC检验   │ │分组回测 │ │策略回测 │ │组合优化 │       │  │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘       │  │
│  └─────────────────────────────────────────────────────────┘  │
│                         │                                      │
│                         ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                    分析层                                │  │
│  │                 Analysis Layer                          │  │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐       │  │
│  │  │绩效分析 │ │风险分析 │ │归因分析 │ │报告生成 │       │  │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘       │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 二、核心模块设计

### 2.1 数据管理器

```python
class DataManager:
    """
    数据管理器
    
    职责：
    1. 数据加载与缓存
    2. 数据对齐与清洗
    3. 数据快照管理
    """
    
    def __init__(self, config):
        self.cache = DataCache()
        self.adjustments = AdjustmentProcessor()
        
    def get_bar_data(self, codes, start_date, end_date, fields):
        """获取K线数据"""
        
    def get_factor_data(self, factor_names, dates):
        """获取因子数据"""
        
    def get_forward_returns(self, horizon=20):
        """获取未来收益率"""
        
    def align_data(self, factor_data, return_data):
        """数据对齐"""
```

### 2.2 因子引擎

```python
class FactorEngine:
    """
    因子引擎
    
    职责：
    1. 因子计算
    2. 因子处理（去极值、标准化、正交化）
    3. 因子检验
    """
    
    def __init__(self, config):
        self.factor_library = FactorLibrary()
        self.processor = FactorProcessor()
        self.tester = FactorTester()
    
    def calc_factor(self, factor_name, date, data):
        """计算因子"""
        
    def process_factor(self, factor_values, method='industry_neutral'):
        """因子处理"""
        # 1. 去极值
        winsorized = self.processor.winsorize(factor_values, method='mad')
        # 2. 标准化
        normalized = self.processor.normalize(winsorized, method='zscore')
        # 3. 行业中性化
        neutralized = self.processor.neutralize(normalized, industry_codes)
        return neutralized
    
    def test_factor(self, factor_name, start_date, end_date):
        """因子检验"""
        return self.tester.run_tests(factor_name, start_date, end_date)
```

### 2.3 回测引擎

```python
class BacktestEngine:
    """
    回测引擎
    
    支持三种回测模式：
    1. 因子回测 - IC检验、分组回测
    2. 策略回测 - 策略信号、交易模拟
    3. 组合回测 - 组合优化、风险控制
    """
    
    def __init__(self, config):
        self.event_engine = EventEngine()
        self.portfolio = PortfolioManager()
        self.execution_engine = ExecutionEngine()
        self.risk_manager = RiskManager()
        
        # 配置
        self.commission_rate = config.get('commission', 0.0003)
        self.slippage_rate = config.get('slippage', 0.001)
        self.min_trade_value = config.get('min_trade_value', 10000)
    
    def run_factor_backtest(self, factor_name, config):
        """因子回测"""
        
    def run_strategy_backtest(self, strategy, config):
        """策略回测"""
        
    def run_portfolio_backtest(self, signals, config):
        """组合回测"""
```

---

## 三、因子检验模块

### 3.1 IC检验器

```python
class ICTester:
    """IC检验器"""
    
    def calc_ic(self, factor_values, forward_returns, method='spearman'):
        """计算单期IC"""
        from scipy.stats import spearmanr
        ic, pvalue = spearmanr(factor_values, forward_returns)
        return ic, pvalue
    
    def calc_ic_series(self, factor_name, start_date, end_date, horizon=20):
        """计算IC时间序列"""
        ic_series = []
        
        for date in self.date_range(start_date, end_date):
            factors = self.get_factor_values(factor_name, date)
            returns = self.get_forward_returns(date, horizon)
            
            ic, pvalue = self.calc_ic(factors, returns)
            ic_series.append({
                'date': date,
                'ic': ic,
                'pvalue': pvalue
            })
        
        return pd.DataFrame(ic_series)
    
    def calc_ic_stats(self, ic_series):
        """计算IC统计指标"""
        return {
            'ic_mean': ic_series['ic'].mean(),
            'ic_std': ic_series['ic'].std(),
            'ir': ic_series['ic'].mean() / ic_series['ic'].std(),
            'ic_positive_ratio': (ic_series['ic'] > 0).mean(),
            'ic_significant_ratio': (ic_series['pvalue'] < 0.05).mean(),
            't_stat': ic_series['ic'].mean() / (ic_series['ic'].std() / len(ic_series) ** 0.5)
        }
```

### 3.2 分组回测器

```python
class GroupTester:
    """分组回测器"""
    
    def __init__(self, n_groups=10):
        self.n_groups = n_groups
    
    def run_group_test(self, factor_values, forward_returns):
        """运行分组回测"""
        # 分组
        labels = pd.qcut(factor_values, self.n_groups, labels=False, duplicates='drop')
        
        # 计算各组收益
        group_returns = {}
        for g in range(self.n_groups):
            mask = (labels == g)
            group_returns[f'G{g+1}'] = forward_returns[mask].mean()
        
        # 多空收益
        long_short = group_returns[f'G{self.n_groups}'] - group_returns['G1']
        
        # 单调性检验
        returns_list = [group_returns[f'G{i+1}'] for i in range(self.n_groups)]
        monotonic = self._check_monotonic(returns_list)
        
        return {
            'group_returns': group_returns,
            'long_short': long_short,
            'monotonic': monotonic,
            'spread': group_returns[f'G{self.n_groups}'] / group_returns['G1'] - 1
        }
    
    def run_group_test_series(self, factor_name, start_date, end_date):
        """分组回测时间序列"""
        results = []
        
        for date in self.date_range(start_date, end_date):
            factors = self.get_factor_values(factor_name, date)
            returns = self.get_forward_returns(date)
            
            result = self.run_group_test(factors, returns)
            result['date'] = date
            results.append(result)
        
        return pd.DataFrame(results)
```

---

## 四、策略回测模块

### 4.1 事件驱动引擎

```python
class EventEngine:
    """事件驱动引擎"""
    
    def __init__(self):
        self.event_queue = Queue()
        self.handlers = defaultdict(list)
    
    def register_handler(self, event_type, handler):
        """注册事件处理器"""
        self.handlers[event_type].append(handler)
    
    def put_event(self, event):
        """放入事件"""
        self.event_queue.put(event)
    
    def process_events(self):
        """处理事件"""
        while not self.event_queue.empty():
            event = self.event_queue.get()
            for handler in self.handlers[event.type]:
                handler(event)


class Event:
    """事件基类"""
    MARKET_DATA = 'market_data'
    SIGNAL = 'signal'
    ORDER = 'order'
    FILL = 'fill'
    
    def __init__(self, event_type, data):
        self.type = event_type
        self.data = data
        self.timestamp = datetime.now()
```

### 4.2 组合管理器

```python
class PortfolioManager:
    """组合管理器"""
    
    def __init__(self, initial_capital=1e6):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {}  # {code: {'shares': n, 'avg_cost': price}}
        self.trades = []
        self.nav_history = []
    
    def update_position(self, code, shares, price, timestamp):
        """更新持仓"""
        if shares > 0:  # 买入
            if code in self.positions:
                old_shares = self.positions[code]['shares']
                old_cost = self.positions[code]['avg_cost']
                new_shares = old_shares + shares
                new_cost = (old_shares * old_cost + shares * price) / new_shares
                self.positions[code] = {'shares': new_shares, 'avg_cost': new_cost}
            else:
                self.positions[code] = {'shares': shares, 'avg_cost': price}
            self.cash -= shares * price
            
        else:  # 卖出
            shares = abs(shares)
            if code in self.positions:
                self.cash += shares * price
                self.positions[code]['shares'] -= shares
                if self.positions[code]['shares'] <= 0:
                    del self.positions[code]
        
        # 记录交易
        self.trades.append({
            'timestamp': timestamp,
            'code': code,
            'shares': shares,
            'price': price
        })
    
    def calc_nav(self, prices, timestamp):
        """计算净值"""
        position_value = sum(
            pos['shares'] * prices.get(code, pos['avg_cost'])
            for code, pos in self.positions.items()
        )
        nav = self.cash + position_value
        
        self.nav_history.append({
            'timestamp': timestamp,
            'nav': nav,
            'cash': self.cash,
            'position_value': position_value
        })
        
        return nav
    
    def get_returns(self):
        """获取收益率序列"""
        nav_series = pd.DataFrame(self.nav_history).set_index('timestamp')['nav']
        returns = nav_series.pct_change()
        return returns
```

### 4.3 执行引擎

```python
class ExecutionEngine:
    """执行引擎"""
    
    def __init__(self, config):
        self.commission_rate = config.get('commission', 0.0003)
        self.slippage_rate = config.get('slippage', 0.001)
        self.min_trade_value = config.get('min_trade_value', 10000)
    
    def execute_order(self, order, market_data):
        """执行订单"""
        # 滑点处理
        if order.direction == 'buy':
            fill_price = order.price * (1 + self.slippage_rate)
        else:
            fill_price = order.price * (1 - self.slippage_rate)
        
        # 佣金计算
        trade_value = order.shares * fill_price
        commission = max(trade_value * self.commission_rate, 5)  # 最小5元
        
        # 成交量检查
        avg_volume = market_data.get('avg_volume_20d', 1e6)
        max_shares = avg_volume * 0.1  # 不超过日均量10%
        actual_shares = min(order.shares, max_shares)
        
        # 最小交易金额检查
        if actual_shares * fill_price < self.min_trade_value:
            return None  # 不执行
        
        return Fill(
            order_id=order.id,
            code=order.code,
            direction=order.direction,
            shares=actual_shares,
            price=fill_price,
            commission=commission,
            timestamp=datetime.now()
        )
```

---

## 五、风险控制模块

### 5.1 风险管理器

```python
class RiskManager:
    """风险管理器"""
    
    def __init__(self, config):
        self.max_position_weight = config.get('max_position_weight', 0.1)
        self.max_sector_weight = config.get('max_sector_weight', 0.3)
        self.max_turnover = config.get('max_turnover', 0.5)
        self.max_drawdown = config.get('max_drawdown', 0.2)
        
    def check_position_limit(self, portfolio, code, target_weight):
        """检查持仓限制"""
        current_weight = portfolio.get_weight(code)
        if current_weight + target_weight > self.max_position_weight:
            return False, f"超过单只股票权重限制 {self.max_position_weight}"
        return True, "OK"
    
    def check_sector_limit(self, portfolio, code, target_weight, sector_map):
        """检查行业限制"""
        sector = sector_map.get(code, '其他')
        sector_weight = portfolio.get_sector_weight(sector, sector_map)
        if sector_weight + target_weight > self.max_sector_weight:
            return False, f"超过行业权重限制 {self.max_sector_weight}"
        return True, "OK"
    
    def check_drawdown(self, portfolio):
        """检查回撤限制"""
        peak = max(h['nav'] for h in portfolio.nav_history)
        current = portfolio.nav_history[-1]['nav']
        drawdown = (peak - current) / peak
        
        if drawdown > self.max_drawdown:
            return False, f"超过最大回撤限制 {self.max_drawdown}"
        return True, "OK"
```

### 5.2 组合优化器

```python
class PortfolioOptimizer:
    """组合优化器"""
    
    def optimize(self, expected_returns, cov_matrix, constraints):
        """
        组合优化
        
        目标函数：max w' * μ - λ * w' * Σ * w
        
        约束条件：
        - 权重和 = 1
        - 行业中性
        - 因子暴露限制
        - 个股权重限制
        """
        from scipy.optimize import minimize
        
        n = len(expected_returns)
        lambda_risk = constraints.get('risk_aversion', 1.0)
        
        def objective(w):
            return_rate = np.dot(w, expected_returns)
            risk = np.dot(w, np.dot(cov_matrix, w))
            return -(return_rate - lambda_risk * risk)
        
        # 约束条件
        cons = [
            {'type': 'eq', 'fun': lambda w: np.sum(w) - 1},  # 权重和=1
        ]
        
        # 行业中性
        if 'industry_neutral' in constraints:
            for industry, stocks in constraints['industry_neutral'].items():
                cons.append({
                    'type': 'eq',
                    'fun': lambda w, s=stocks: np.sum(w[s]) - constraints['benchmark_industry_weight'][industry]
                })
        
        # 边界
        bounds = [(0, constraints.get('max_weight', 0.1)) for _ in range(n)]
        
        result = minimize(
            objective,
            x0=np.ones(n) / n,
            method='SLSQP',
            bounds=bounds,
            constraints=cons
        )
        
        return result.x
```

---

## 六、绩效分析模块

### 6.1 绩效分析器

```python
class PerformanceAnalyzer:
    """绩效分析器"""
    
    def analyze(self, returns, benchmark_returns=None):
        """绩效分析"""
        return {
            'return_metrics': self._calc_return_metrics(returns),
            'risk_metrics': self._calc_risk_metrics(returns),
            'risk_adjusted_metrics': self._calc_risk_adjusted_metrics(returns, benchmark_returns),
            'trade_metrics': self._calc_trade_metrics(),
            'drawdown_analysis': self._analyze_drawdowns(returns)
        }
    
    def _calc_return_metrics(self, returns):
        """收益指标"""
        return {
            'total_return': (1 + returns).prod() - 1,
            'annual_return': (1 + returns).prod() ** (252 / len(returns)) - 1,
            'monthly_return': returns.mean() * 21,
            'daily_return_mean': returns.mean(),
            'positive_day_ratio': (returns > 0).mean(),
        }
    
    def _calc_risk_metrics(self, returns):
        """风险指标"""
        return {
            'volatility': returns.std() * np.sqrt(252),
            'downside_deviation': returns[returns < 0].std() * np.sqrt(252),
            'max_drawdown': self._calc_max_drawdown(returns),
            'var_95': returns.quantile(0.05),
            'cvar_95': returns[returns <= returns.quantile(0.05)].mean(),
        }
    
    def _calc_risk_adjusted_metrics(self, returns, benchmark_returns):
        """风险调整指标"""
        rf = 0.03 / 252  # 无风险利率
        excess_returns = returns - rf
        
        metrics = {
            'sharpe_ratio': excess_returns.mean() / returns.std() * np.sqrt(252),
            'sortino_ratio': excess_returns.mean() / returns[returns < 0].std() * np.sqrt(252),
            'calmar_ratio': (1 + returns).prod() ** (252/len(returns)) - 1) / abs(self._calc_max_drawdown(returns)),
        }
        
        if benchmark_returns is not None:
            excess = returns - benchmark_returns
            metrics['information_ratio'] = excess.mean() / excess.std() * np.sqrt(252)
            metrics['tracking_error'] = excess.std() * np.sqrt(252)
            metrics['beta'] = returns.cov(benchmark_returns) / benchmark_returns.var()
            metrics['alpha'] = (1 + returns).prod() - (1 + benchmark_returns).prod() * metrics['beta']
        
        return metrics
    
    def _calc_max_drawdown(self, returns):
        """计算最大回撤"""
        cum_returns = (1 + returns).cumprod()
        running_max = cum_returns.cummax()
        drawdowns = (cum_returns - running_max) / running_max
        return drawdowns.min()
    
    def _analyze_drawdowns(self, returns):
        """回撤分析"""
        cum_returns = (1 + returns).cumprod()
        running_max = cum_returns.cummax()
        drawdowns = (cum_returns - running_max) / running_max
        
        # 识别回撤期
        in_drawdown = drawdowns < 0
        drawdown_periods = []
        
        start = None
        for i, (date, dd) in enumerate(drawdowns.items()):
            if dd < 0 and start is None:
                start = date
            elif dd == 0 and start is not None:
                drawdown_periods.append({
                    'start': start,
                    'end': date,
                    'duration': (date - start).days,
                    'max_drawdown': drawdowns[start:date].min()
                })
                start = None
        
        return {
            'max_drawdown': drawdowns.min(),
            'max_drawdown_duration': max(p['duration'] for p in drawdown_periods) if drawdown_periods else 0,
            'num_drawdowns': len(drawdown_periods),
            'avg_drawdown': np.mean([p['max_drawdown'] for p in drawdown_periods]) if drawdown_periods else 0,
        }
```

---

## 七、API接口设计

### 7.1 因子回测接口

```python
@router.post("/backtest/factor")
async def backtest_factor(request: FactorBacktestRequest):
    """
    因子回测
    
    Args:
        factor_name: 因子名称
        start_date: 开始日期
        end_date: 结束日期
        horizon: 预测周期
        n_groups: 分组数
    """
    engine = BacktestEngine()
    result = engine.run_factor_backtest(
        factor_name=request.factor_name,
        start_date=request.start_date,
        end_date=request.end_date,
        horizon=request.horizon,
        n_groups=request.n_groups
    )
    return result
```

### 7.2 策略回测接口

```python
@router.post("/backtest/strategy")
async def backtest_strategy(request: StrategyBacktestRequest):
    """
    策略回测
    
    Args:
        strategy_id: 策略ID
        start_date: 开始日期
        end_date: 结束日期
        initial_capital: 初始资金
        commission: 佣金率
    """
    engine = BacktestEngine()
    result = engine.run_strategy_backtest(
        strategy_id=request.strategy_id,
        start_date=request.start_date,
        end_date=request.end_date,
        initial_capital=request.initial_capital,
        commission=request.commission
    )
    return result
```

---

## 八、文件结构

```
smart-quant/backend/app/
├── backtest/
│   ├── __init__.py
│   ├── engine.py          # 回测引擎
│   ├── factor_tester.py   # 因子检验
│   ├── group_tester.py    # 分组回测
│   ├── portfolio.py       # 组合管理
│   ├── execution.py       # 交易执行
│   ├── risk_manager.py    # 风险管理
│   ├── optimizer.py       # 组合优化
│   ├── performance.py     # 绩效分析
│   └── events.py          # 事件系统
├── api/
│   └── backtest.py        # 回测API
└── services/
    └── backtest_service.py # 回测服务
```

---

*框架版本: v1.0*
*设计时间: 2026-03-15*