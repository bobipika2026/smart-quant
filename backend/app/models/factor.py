"""
因子模型
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Date
from sqlalchemy.sql import func

from app.database import Base


class FactorValue(Base):
    """因子值表 - 存储每日因子数据"""
    __tablename__ = "factor_values"
    
    id = Column(Integer, primary_key=True, index=True)
    trade_date = Column(Date, nullable=False, index=True)  # 交易日期
    stock_code = Column(String(10), nullable=False, index=True)  # 股票代码
    
    # 基本面因子
    pe = Column(Float)  # 市盈率
    pb = Column(Float)  # 市净率
    ps = Column(Float)  # 市销率
    roe = Column(Float)  # 净资产收益率
    roa = Column(Float)  # 总资产收益率
    debt_ratio = Column(Float)  # 资产负债率
    net_profit_margin = Column(Float)  # 净利润率
    revenue_growth = Column(Float)  # 营收增长率
    profit_growth = Column(Float)  # 净利润增长率
    dividend_yield = Column(Float)  # 股息率
    
    # 市场因子
    market_cap = Column(Float)  # 总市值（亿）
    float_market_cap = Column(Float)  # 流通市值（亿）
    turnover_rate = Column(Float)  # 换手率
    volume_ratio = Column(Float)  # 量比
    volatility_20 = Column(Float)  # 20日波动率
    beta = Column(Float)  # Beta值
    
    # 技术因子
    ma_5 = Column(Float)  # 5日均线
    ma_20 = Column(Float)  # 20日均线
    rsi_14 = Column(Float)  # 14日RSI
    macd = Column(Float)  # MACD
    atr_14 = Column(Float)  # 14日ATR
    
    # 情绪因子
    north_flow = Column(Float)  # 北向资金净买入（万元）
    margin_balance = Column(Float)  # 融资余额（万元）
    
    created_at = Column(DateTime, server_default=func.now())


class FactorBacktest(Base):
    """因子回测结果表 - 记录每次回测的因子组合"""
    __tablename__ = "factor_backtests"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # 策略因子
    strategy_id = Column(String(50))  # 策略ID
    strategy_name = Column(String(100))  # 策略名称
    
    # 参数因子
    param_short_period = Column(Integer)  # 短周期参数
    param_long_period = Column(Integer)  # 长周期参数
    param_threshold = Column(Float)  # 阈值参数
    params_json = Column(Text)  # 其他参数JSON
    
    # 股票因子
    stock_code = Column(String(10))
    stock_name = Column(String(50))
    industry = Column(String(50))  # 行业
    market_cap_level = Column(String(20))  # 市值等级（大盘/中盘/小盘）
    
    # 时间因子
    start_date = Column(Date)
    end_date = Column(Date)
    holding_days = Column(Integer)  # 持仓天数
    trade_count = Column(Integer)  # 交易次数
    
    # 结果
    total_return = Column(Float)  # 总收益
    annual_return = Column(Float)  # 年化收益
    max_drawdown = Column(Float)  # 最大回撤
    sharpe_ratio = Column(Float)  # 夏普比率
    win_rate = Column(Float)  # 胜率
    
    created_at = Column(DateTime, server_default=func.now())


class FactorPerformance(Base):
    """因子表现分析表 - 单因子收益分析"""
    __tablename__ = "factor_performances"
    
    id = Column(Integer, primary_key=True, index=True)
    factor_name = Column(String(50), nullable=False)  # 因子名称
    factor_value = Column(String(100), nullable=False)  # 因子值（如"MA_5"表示MA参数为5）
    
    # 统计数据
    sample_count = Column(Integer)  # 样本数量
    avg_return = Column(Float)  # 平均收益
    win_rate = Column(Float)  # 胜率
    avg_sharpe = Column(Float)  # 平均夏普
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())