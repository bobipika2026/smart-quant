"""
Agent States - 智能体状态定义

参考 TradingAgents 的状态管理设计
"""
from typing import Annotated, List, Dict, Any, Optional
from typing_extensions import TypedDict
from datetime import date


class FactorAnalysisState(TypedDict):
    """因子分析状态"""
    stock_code: Annotated[str, "股票代码"]
    trade_date: Annotated[str, "交易日期"]
    
    # 因子计算结果
    value_factors: Annotated[Dict, "价值因子"]  # EP, BP, DIV_YIELD等
    growth_factors: Annotated[Dict, "成长因子"]  # REV_G, EPS_G等
    quality_factors: Annotated[Dict, "质量因子"]  # ROE, ROA, LEV等
    momentum_factors: Annotated[Dict, "动量因子"]  # MOM, RS等
    sentiment_factors: Annotated[Dict, "情绪因子"]  # TURN, NORTH_C等
    technical_factors: Annotated[Dict, "技术因子"]  # RSI, MACD, KDJ等
    
    # 综合评分
    composite_score: Annotated[float, "综合评分"]
    grade: Annotated[str, "评级 (A/B+/B/B-/C/D)"]
    
    # 因子报告
    factor_report: Annotated[str, "因子分析报告"]


class BacktestResultState(TypedDict):
    """回测结果状态"""
    strategy_name: Annotated[str, "策略名称"]
    stock_code: Annotated[str, "股票代码"]
    
    # 回测结果
    total_return: Annotated[float, "总收益率"]
    annual_return: Annotated[float, "年化收益率"]
    sharpe_ratio: Annotated[float, "夏普比率"]
    max_drawdown: Annotated[float, "最大回撤"]
    win_rate: Annotated[float, "胜率"]
    trade_count: Annotated[int, "交易次数"]
    
    # 基准对比
    benchmark_return: Annotated[float, "基准收益"]
    excess_return: Annotated[float, "超额收益"]
    
    # 回测报告
    backtest_report: Annotated[str, "回测报告"]


class RiskDebateState(TypedDict):
    """风险辩论状态（多空辩论）"""
    # 多方观点
    bull_history: Annotated[str, "多方观点历史"]
    bull_score: Annotated[float, "多方得分"]
    
    # 空方观点
    bear_history: Annotated[str, "空方观点历史"]
    bear_score: Annotated[float, "空方得分"]
    
    # 辩论历史
    history: Annotated[str, "辩论历史"]
    count: Annotated[int, "辩论轮次"]
    
    # 最终判断
    judge_decision: Annotated[str, "最终判断"]
    final_bias: Annotated[str, "最终倾向 (bullish/bearish/neutral)"]


class PortfolioState(TypedDict):
    """投资组合状态"""
    # 组合信息
    stocks: Annotated[List[str], "股票列表"]
    weights: Annotated[Dict[str, float], "权重分配"]
    
    # 组合指标
    expected_return: Annotated[float, "预期收益"]
    expected_risk: Annotated[float, "预期风险"]
    sharpe_ratio: Annotated[float, "夏普比率"]
    
    # 风险约束
    max_position: Annotated[float, "单只股票最大仓位"]
    max_sector_weight: Annotated[float, "单一板块最大权重"]
    
    # 组合报告
    portfolio_report: Annotated[str, "组合报告"]


class AgentState(TypedDict):
    """主智能体状态"""
    # 基本信息
    stock_code: Annotated[str, "股票代码"]
    stock_name: Annotated[str, "股票名称"]
    trade_date: Annotated[str, "交易日期"]
    
    # 发送者
    sender: Annotated[str, "发送消息的智能体"]
    
    # 因子分析结果
    factor_analysis: Annotated[FactorAnalysisState, "因子分析状态"]
    
    # 回测结果
    backtest_results: Annotated[List[BacktestResultState], "回测结果列表"]
    
    # 风险辩论
    risk_debate: Annotated[RiskDebateState, "风险辩论状态"]
    
    # 投资组合
    portfolio: Annotated[PortfolioState, "投资组合状态"]
    
    # 市场环境
    market_regime: Annotated[str, "市场环境 (bull/bear/sideways)"]
    market_timing_score: Annotated[float, "择时得分"]
    
    # 最终决策
    final_decision: Annotated[str, "最终交易决策"]
    action: Annotated[str, "操作 (buy/sell/hold)"]
    position_size: Annotated[float, "仓位大小"]
    confidence: Annotated[float, "信心度"]