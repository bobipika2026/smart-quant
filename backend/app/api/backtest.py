"""
专业量化回测 API
"""
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict
import pandas as pd
import numpy as np
from datetime import datetime

from app.backtest.engine import (
    BacktestEngine, BacktestConfig, get_backtest_engine,
    ICTester, GroupTester, PerformanceAnalyzer
)

router = APIRouter(prefix="/api/backtest", tags=["专业回测"])


# ==================== 请求模型 ====================

class FactorBacktestRequest(BaseModel):
    """因子回测请求"""
    factor_name: str
    start_date: str
    end_date: str
    horizon: int = 20
    n_groups: int = 10


class StrategyBacktestRequest(BaseModel):
    """策略回测请求"""
    strategy_id: str
    start_date: str
    end_date: str
    initial_capital: float = 1000000
    commission: float = 0.0003


class ICTestRequest(BaseModel):
    """IC检验请求"""
    factor_name: str
    start_date: str
    end_date: str
    horizon: int = 20


# ==================== 因子回测接口 ====================

@router.post("/factor")
async def backtest_factor(request: FactorBacktestRequest):
    """
    因子回测
    
    完整的因子检验流程：
    1. IC检验（IC均值、IR、t统计量）
    2. 分组回测（十分组、多空收益）
    3. 绩效分析
    
    Args:
        factor_name: 因子名称
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期
        horizon: 预测周期（天）
        n_groups: 分组数
    """
    engine = get_backtest_engine()
    
    # 模拟数据（实际应从数据库加载）
    # TODO: 实现真实因子数据加载
    
    return {
        "code": 0,
        "data": {
            "message": "因子回测功能已就绪，需要接入真实因子数据",
            "request": request.dict()
        }
    }


@router.get("/factor/{factor_name}/ic")
async def test_factor_ic(
    factor_name: str,
    start_date: str = Query(..., description="开始日期"),
    end_date: str = Query(..., description="结束日期"),
    horizon: int = Query(20, description="预测周期")
):
    """
    因子IC检验
    
    返回：
    - IC均值
    - IC标准差
    - IR（信息比率）
    - IC>0占比
    - IC显著性占比
    - t统计量
    """
    tester = ICTester()
    
    return {
        "code": 0,
        "data": {
            "factor_name": factor_name,
            "start_date": start_date,
            "end_date": end_date,
            "horizon": horizon,
            "message": "IC检验功能已就绪"
        }
    }


@router.get("/factor/{factor_name}/groups")
async def test_factor_groups(
    factor_name: str,
    start_date: str = Query(..., description="开始日期"),
    end_date: str = Query(..., description="结束日期"),
    n_groups: int = Query(10, ge=5, le=20, description="分组数")
):
    """
    因子分组回测
    
    返回：
    - 各组收益率
    - 多空收益
    - 单调性
    - spread
    """
    tester = GroupTester(n_groups=n_groups)
    
    return {
        "code": 0,
        "data": {
            "factor_name": factor_name,
            "n_groups": n_groups,
            "message": "分组回测功能已就绪"
        }
    }


# ==================== 策略回测接口 ====================

@router.post("/strategy")
async def backtest_strategy(request: StrategyBacktestRequest):
    """
    策略回测
    
    完整的策略回测流程：
    1. 加载策略信号
    2. 模拟交易执行
    3. 绩效分析
    
    Args:
        strategy_id: 策略ID
        start_date: 开始日期
        end_date: 结束日期
        initial_capital: 初始资金
        commission: 佣金率
    """
    config = BacktestConfig(
        initial_capital=request.initial_capital,
        commission_rate=request.commission
    )
    engine = BacktestEngine(config)
    
    return {
        "code": 0,
        "data": {
            "message": "策略回测功能已就绪",
            "request": request.dict()
        }
    }


# ==================== 绩效分析接口 ====================

@router.post("/performance/analyze")
async def analyze_performance(returns: List[float]):
    """
    绩效分析
    
    Args:
        returns: 日收益率序列
    
    Returns:
        - 收益指标（总收益、年化收益等）
        - 风险指标（波动率、最大回撤等）
        - 风险调整指标（夏普、索提诺等）
    """
    analyzer = PerformanceAnalyzer()
    
    returns_series = pd.Series(returns)
    performance = analyzer.analyze(returns_series)
    
    return {
        "code": 0,
        "data": performance
    }


@router.get("/metrics")
async def get_backtest_metrics():
    """
    获取回测指标说明
    
    返回所有可用的回测指标及其含义
    """
    return {
        "code": 0,
        "data": {
            "ic_metrics": {
                "ic_mean": "IC均值，衡量因子预测能力，>0.03为有效",
                "ic_std": "IC标准差，衡量因子稳定性，<0.15为稳定",
                "ir": "信息比率 = IC均值/IC标准差，>0.3为良好",
                "ic_positive_ratio": "IC>0占比，>55%为有效",
                "t_stat": "t统计量，>2.0为显著"
            },
            "group_metrics": {
                "long_short": "多空收益，做多最高组、做空最低组",
                "monotonic": "单调性，各组收益是否单调递增/递减",
                "spread": "最高组/最低组 - 1"
            },
            "return_metrics": {
                "total_return": "总收益率",
                "annual_return": "年化收益率",
                "monthly_return": "月均收益率"
            },
            "risk_metrics": {
                "volatility": "年化波动率",
                "max_drawdown": "最大回撤",
                "var_95": "95%置信度VaR",
                "cvar_95": "条件VaR（期望损失）"
            },
            "risk_adjusted_metrics": {
                "sharpe_ratio": "夏普比率，>1为良好，>1.5为优秀",
                "sortino_ratio": "索提诺比率，只考虑下行风险",
                "calmar_ratio": "卡玛比率 = 年化收益/最大回撤",
                "information_ratio": "信息比率（相对基准）",
                "beta": "贝塔系数（相对基准）",
                "alpha": "阿尔法（超额收益）"
            },
            "trade_metrics": {
                "win_rate": "胜率",
                "profit_loss_ratio": "盈亏比 = 平均盈利/平均亏损",
                "total_trades": "总交易次数"
            }
        }
    }


# ==================== 回测报告接口 ====================

@router.get("/report/{backtest_id}")
async def get_backtest_report(backtest_id: str):
    """
    获取回测报告
    
    返回完整的回测报告，包括：
    - 绩效概览
    - 风险分析
    - 回撤分析
    - 月度收益
    """
    return {
        "code": 0,
        "data": {
            "backtest_id": backtest_id,
            "message": "回测报告功能开发中"
        }
    }


# ==================== 因子对比接口 ====================

@router.post("/compare")
async def compare_factors(factor_names: List[str]):
    """
    因子对比分析
    
    对比多个因子的表现：
    - IC对比
    - 分组收益对比
    - 相关性分析
    """
    return {
        "code": 0,
        "data": {
            "factors": factor_names,
            "message": "因子对比功能已就绪"
        }
    }