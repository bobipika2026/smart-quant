"""
因子库数据库模型

存储因子定义、检验结果、相关性分析
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean, Date
from sqlalchemy.sql import func
from sqlalchemy import UniqueConstraint

from app.database import Base


class FactorDefinition(Base):
    """因子定义表"""
    __tablename__ = "factor_definitions"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, nullable=False)  # 因子代码
    name = Column(String(100), nullable=False)  # 因子名称
    category = Column(String(50), nullable=False)  # 因子类别: value, growth, quality, momentum, sentiment, technical
    formula = Column(Text)  # 计算公式
    description = Column(Text)  # 描述
    weight = Column(Float, default=0.0)  # 默认权重
    direction = Column(Integer, default=1)  # 方向: 1=正向, -1=反向, 0=中性
    data_source = Column(String(50))  # 数据来源: day, daily_basic, financial, external
    update_freq = Column(String(20))  # 更新频率: daily, quarterly
    
    # 检验状态
    is_tested = Column(Boolean, default=False)  # 是否已检验
    is_valid = Column(Boolean, default=False)  # 是否有效(IC通过)
    is_selected = Column(Boolean, default=False)  # 是否被选中(相关性筛选后)
    is_removed = Column(Boolean, default=False)  # 是否被剔除
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())


class FactorTestResult(Base):
    """因子检验结果表"""
    __tablename__ = "factor_test_results"
    
    id = Column(Integer, primary_key=True, index=True)
    factor_code = Column(String(50), nullable=False, index=True)
    test_date = Column(DateTime, nullable=False)  # 测试日期
    
    # IC检验结果
    ic_mean = Column(Float)  # IC均值
    ic_std = Column(Float)  # IC标准差
    ir = Column(Float)  # 信息比率 IC/IC_std
    ic_positive_ratio = Column(Float)  # IC正值比例
    
    # 参数敏感性
    param_config = Column(Text)  # 参数配置JSON
    is_best_param = Column(Boolean, default=False)  # 是否最优参数
    
    # 测试条件
    horizon = Column(Integer, default=20)  # 预测周期
    n_stocks = Column(Integer)  # 测试股票数
    n_periods = Column(Integer)  # 有效周期数
    
    # 评级
    rating = Column(String(5))  # A/B+/B/B-/C/D
    
    created_at = Column(DateTime, server_default=func.now())
    
    __table_args__ = (
        UniqueConstraint('factor_code', 'test_date', name='uix_factor_test'),
    )


class FactorCorrelation(Base):
    """因子相关性表"""
    __tablename__ = "factor_correlations"
    
    id = Column(Integer, primary_key=True, index=True)
    factor1_code = Column(String(50), nullable=False, index=True)
    factor2_code = Column(String(50), nullable=False, index=True)
    correlation = Column(Float, nullable=False)  # 相关系数
    calc_date = Column(DateTime, nullable=False)  # 计算日期
    
    # 筛选信息
    is_high_corr = Column(Boolean, default=False)  # 是否高相关(>0.8)
    threshold = Column(Float, default=0.8)  # 使用的阈值
    
    created_at = Column(DateTime, server_default=func.now())
    
    __table_args__ = (
        UniqueConstraint('factor1_code', 'factor2_code', 'calc_date', name='uix_factor_corr'),
    )


class FactorParamSensitivity(Base):
    """因子参数敏感性测试结果"""
    __tablename__ = "factor_param_sensitivity"
    
    id = Column(Integer, primary_key=True, index=True)
    factor_type = Column(String(50), nullable=False)  # 因子类型: MOM, VOL_M, KDJ等
    factor_code = Column(String(50), nullable=False)  # 因子代码
    param_desc = Column(String(100))  # 参数描述
    test_date = Column(DateTime, nullable=False)
    
    # 测试结果
    ic = Column(Float)  # IC值
    n_stocks = Column(Integer)  # 测试股票数
    is_valid = Column(Boolean, default=True)
    is_best = Column(Boolean, default=False)  # 是否该类型最优参数
    
    created_at = Column(DateTime, server_default=func.now())


class FactorSelectionResult(Base):
    """因子筛选结果表"""
    __tablename__ = "factor_selection_results"
    
    id = Column(Integer, primary_key=True, index=True)
    selection_date = Column(DateTime, nullable=False)
    
    # 统计信息
    original_factors = Column(Integer)  # 原始因子数
    valid_factors = Column(Integer)  # 有效因子数
    selected_factors = Column(Integer)  # 选中因子数
    removed_factors = Column(Integer)  # 剔除因子数
    threshold = Column(Float, default=0.8)  # 相关性阈值
    
    # 筛选结果
    final_factor_codes = Column(Text)  # 最终因子列表JSON
    removed_factor_codes = Column(Text)  # 剔除因子列表JSON
    high_corr_pairs = Column(Text)  # 高相关因子对JSON
    
    created_at = Column(DateTime, server_default=func.now())