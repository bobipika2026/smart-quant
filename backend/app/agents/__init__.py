"""
Smart Quant Multi-Agent System

多智能体量化交易系统

架构设计参考 TradingAgents，针对量化交易场景优化

核心Agent：
1. Factor Analyst - 因子分析
2. Strategy Backtester - 策略回测
3. Risk Manager - 风险管理
4. Portfolio Optimizer - 组合优化
5. Market Timer - 择时判断
"""
from app.agents.states import (
    AgentState,
    FactorAnalysisState,
    BacktestResultState,
    RiskDebateState,
    PortfolioState,
)
from app.agents.graph import SmartQuantGraph

__all__ = [
    "AgentState",
    "FactorAnalysisState", 
    "BacktestResultState",
    "RiskDebateState",
    "PortfolioState",
    "SmartQuantGraph",
]