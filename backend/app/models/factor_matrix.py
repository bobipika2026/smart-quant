"""
因子矩阵模型 - 01矩阵设计
每个因子取值 0 或 1，代表启用/不启用
"""
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class FactorDefinition(Base):
    """因子定义表 - 定义所有可用因子"""
    __tablename__ = "factor_definitions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    factor_code = Column(String(50), unique=True, nullable=False)  # 因子代码，如 ma_5_20
    factor_name = Column(String(100))  # 因子名称，如 MA金叉(5,20)
    factor_type = Column(String(20))  # 因子类型：strategy/condition/time
    factor_params = Column(Text)  # JSON格式的参数
    description = Column(String(200))  # 描述
    is_active = Column(Boolean, default=True)  # 是否启用
    created_at = Column(DateTime, default=datetime.now)


class FactorExperiment(Base):
    """因子实验表 - 每次实验是一行01组合"""
    __tablename__ = "factor_experiments"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    experiment_code = Column(String(50), unique=True)  # 实验编号
    
    # 股票信息
    stock_code = Column(String(10))
    stock_name = Column(String(50))
    
    # 时间范围
    start_date = Column(Date)
    end_date = Column(Date)
    
    # 因子组合（JSON格式，key是factor_code，value是0或1）
    factor_combination = Column(Text)  # {"ma_5_20": 1, "macd": 1, "rsi": 0, ...}
    
    # 启用的因子数量
    active_factor_count = Column(Integer)
    
    # 实验结果
    total_return = Column(Float)  # 总收益率%
    annual_return = Column(Float)  # 年化收益率%
    max_drawdown = Column(Float)  # 最大回撤%
    sharpe_ratio = Column(Float)  # 夏普比率
    win_rate = Column(Float)  # 胜率%
    trade_count = Column(Integer)  # 交易次数
    
    # 元数据
    created_at = Column(DateTime, default=datetime.now)
    notes = Column(String(500))  # 备注


class FactorContribution(Base):
    """因子贡献度表 - 分析每个因子的贡献"""
    __tablename__ = "factor_contributions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    factor_code = Column(String(50))  # 因子代码
    
    # 统计数据
    total_experiments = Column(Integer)  # 该因子参与的总实验数
    active_count = Column(Integer)  # 该因子=1的实验数
    
    # 因子=1时的表现
    avg_return_when_active = Column(Float)  # 启用时的平均收益
    avg_sharpe_when_active = Column(Float)  # 启用时的平均夏普
    
    # 因子=0时的表现
    avg_return_when_inactive = Column(Float)  # 不启用时的平均收益
    avg_sharpe_when_inactive = Column(Float)  # 不启用时的平均夏普
    
    # 贡献度
    contribution_score = Column(Float)  # 贡献度得分 = active_avg - inactive_avg
    
    # 统计时间
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class BestFactorCombination(Base):
    """最佳因子组合表 - 每只股票只保存Top 3"""
    __tablename__ = "best_factor_combinations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    combination_code = Column(String(100), unique=True, nullable=False)
    
    # 股票信息
    stock_code = Column(String(10), nullable=False, index=True)
    stock_name = Column(String(50))
    
    # 因子组合
    strategy_desc = Column(String(200))  # "MA(10,20)+RSI(80)(OR)"
    factor_combination = Column(Text)  # JSON
    active_factors = Column(Text)  # 启用的因子列表
    
    # 排名（该股票内的排名 1/2/3）
    rank_in_stock = Column(Integer, default=1, index=True)
    
    # 回测结果
    total_return = Column(Float)
    annual_return = Column(Float)  # 年化收益率%
    benchmark_return = Column(Float)  # 基准收益率%
    sharpe_ratio = Column(Float)
    max_drawdown = Column(Float)
    win_rate = Column(Float)
    profit_loss_ratio = Column(Float)  # 盈亏比
    trade_count = Column(Integer)
    composite_score = Column(Float, index=True)
    
    # 时间因子
    holding_period = Column(String(20))
    start_date = Column(Date)
    end_date = Column(Date)
    
    # 元数据
    backtest_date = Column(Date)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    is_active = Column(Boolean, default=True)
    notes = Column(String(500))