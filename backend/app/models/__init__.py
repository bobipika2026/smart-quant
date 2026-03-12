"""
数据模型
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text
from sqlalchemy.sql import func

from app.database import Base


class Strategy(Base):
    """策略模型"""
    __tablename__ = "strategies"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    strategy_type = Column(String(50))  # trend, oscillation, composite
    parameters = Column(Text)  # JSON 格式的参数
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())


class Stock(Base):
    """股票模型"""
    __tablename__ = "stocks"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(10), unique=True, nullable=False)
    name = Column(String(50))
    exchange = Column(String(10))  # SH, SZ, BJ
    industry = Column(String(50))
    is_monitoring = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())


class MonitorConfig(Base):
    """监控配置模型 - 股票+策略组合"""
    __tablename__ = "monitor_configs"
    
    id = Column(Integer, primary_key=True, index=True)
    stock_code = Column(String(10), nullable=False)
    stock_name = Column(String(50))
    strategy_id = Column(String(50), nullable=False)  # ma_cross, macd, kdj
    strategy_name = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())


class Signal(Base):
    """信号模型"""
    __tablename__ = "signals"
    
    id = Column(Integer, primary_key=True, index=True)
    stock_code = Column(String(10), nullable=False)
    strategy_id = Column(Integer)
    signal_type = Column(String(20))  # buy, sell, hold
    price = Column(Float)
    reason = Column(Text)
    is_notified = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())


class BacktestResult(Base):
    """回测结果模型"""
    __tablename__ = "backtest_results"
    
    id = Column(Integer, primary_key=True, index=True)
    strategy_id = Column(Integer)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    total_return = Column(Float)
    annual_return = Column(Float)
    max_drawdown = Column(Float)
    sharpe_ratio = Column(Float)
    win_rate = Column(Float)
    trade_count = Column(Integer)
    details = Column(Text)  # JSON 格式的详细数据
    created_at = Column(DateTime, server_default=func.now())


# 因子模型
from app.models.factor import FactorValue, FactorBacktest, FactorPerformance