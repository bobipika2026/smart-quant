"""
因子相关API
"""
from fastapi import APIRouter, HTTPException
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

from sqlalchemy.orm import Session
from app.database import SessionLocal, Base, engine
from app.models.factor import FactorValue, FactorBacktest, FactorPerformance
from app.services.factor_service import FactorService

router = APIRouter(prefix="/api/factor", tags=["多因子系统"])

# 确保表创建
Base.metadata.create_all(bind=engine)


class FactorQuery(BaseModel):
    """因子查询请求"""
    stock_code: str
    include_fundamental: bool = True
    include_market: bool = True
    include_technical: bool = True
    include_sentiment: bool = True


@router.get("/list")
async def list_available_factors():
    """列出所有可用因子"""
    return {
        "factors": {
            "fundamental": [
                {"id": "pe", "name": "市盈率", "description": "股价/每股收益"},
                {"id": "pb", "name": "市净率", "description": "股价/每股净资产"},
                {"id": "ps", "name": "市销率", "description": "市值/营业收入"},
                {"id": "roe", "name": "净资产收益率", "description": "净利润/净资产"},
                {"id": "roa", "name": "总资产收益率", "description": "净利润/总资产"},
                {"id": "debt_ratio", "name": "资产负债率", "description": "负债/总资产"},
                {"id": "net_profit_margin", "name": "净利润率", "description": "净利润/营业收入"},
                {"id": "revenue_growth", "name": "营收增长率", "description": "营收同比变化"},
                {"id": "profit_growth", "name": "净利润增长率", "description": "净利润同比变化"},
                {"id": "dividend_yield", "name": "股息率", "description": "年度分红/股价"},
            ],
            "market": [
                {"id": "market_cap", "name": "总市值", "description": "股票总市值"},
                {"id": "float_market_cap", "name": "流通市值", "description": "流通股市值"},
                {"id": "turnover_rate", "name": "换手率", "description": "成交量/流通股本"},
                {"id": "volatility_20", "name": "20日波动率", "description": "年化波动率"},
                {"id": "beta", "name": "Beta值", "description": "相对大盘的系统性风险"},
            ],
            "technical": [
                {"id": "ma_5", "name": "5日均线", "description": "短期趋势"},
                {"id": "ma_20", "name": "20日均线", "description": "中期趋势"},
                {"id": "rsi_14", "name": "14日RSI", "description": "相对强弱指标"},
                {"id": "macd", "name": "MACD", "description": "趋势动量指标"},
                {"id": "atr_14", "name": "14日ATR", "description": "真实波幅"},
            ],
            "sentiment": [
                {"id": "north_flow", "name": "北向资金", "description": "北向资金净买入"},
                {"id": "margin_balance", "name": "融资余额", "description": "融资余额"},
            ],
            "param": [
                {"id": "strategy_id", "name": "策略类型", "description": "MA/MACD/RSI等"},
                {"id": "param_short_period", "name": "短周期参数", "description": "均线短周期"},
                {"id": "param_long_period", "name": "长周期参数", "description": "均线长周期"},
                {"id": "param_threshold", "name": "阈值参数", "description": "超买超卖阈值"},
            ],
            "stock": [
                {"id": "stock_code", "name": "股票代码", "description": "个股标识"},
                {"id": "industry", "name": "行业", "description": "所属行业"},
                {"id": "market_cap_level", "name": "市值等级", "description": "大盘/中盘/小盘"},
            ],
            "time": [
                {"id": "start_date", "name": "开始日期", "description": "回测开始"},
                {"id": "end_date", "name": "结束日期", "description": "回测结束"},
                {"id": "holding_days", "name": "持仓天数", "description": "平均持仓周期"},
                {"id": "trade_count", "name": "交易次数", "description": "交易频率"},
            ],
        }
    }


@router.post("/get")
async def get_factors(query: FactorQuery):
    """获取单只股票的因子数据"""
    from app.services.data import DataService
    
    # 获取历史数据用于计算技术因子
    hist_data = None
    if query.include_technical:
        data_service = DataService()
        hist_data = await data_service.get_stock_history(query.stock_code)
    
    # 获取所有因子
    factors = await FactorService.get_all_factors(
        query.stock_code, 
        hist_data
    )
    
    return {
        "stock_code": query.stock_code,
        "trade_date": factors.get("trade_date"),
        "factors": factors
    }


@router.post("/save")
async def save_factors(query: FactorQuery):
    """获取并保存因子数据"""
    from app.services.data import DataService
    
    # 获取历史数据
    hist_data = None
    if query.include_technical:
        data_service = DataService()
        hist_data = await data_service.get_stock_history(query.stock_code)
    
    # 获取所有因子
    factors = await FactorService.get_all_factors(
        query.stock_code,
        hist_data
    )
    
    # 保存到数据库
    success = await FactorService.save_factors(factors)
    
    if success:
        return {
            "success": True,
            "message": f"已保存 {query.stock_code} 的因子数据",
            "factors": factors
        }
    else:
        raise HTTPException(status_code=500, detail="保存失败")


@router.get("/history/{stock_code}")
async def get_factor_history(stock_code: str, limit: int = 30):
    """获取因子历史数据"""
    db: Session = SessionLocal()
    try:
        records = db.query(FactorValue).filter(
            FactorValue.stock_code == stock_code
        ).order_by(FactorValue.trade_date.desc()).limit(limit).all()
        
        return {
            "stock_code": stock_code,
            "count": len(records),
            "data": [
                {
                    "trade_date": str(r.trade_date),
                    "pe": r.pe,
                    "pb": r.pb,
                    "roe": r.roe,
                    "market_cap": r.market_cap,
                    "turnover_rate": r.turnover_rate,
                    "volatility_20": r.volatility_20,
                    "rsi_14": r.rsi_14,
                    "macd": r.macd,
                }
                for r in records
            ]
        }
    finally:
        db.close()


@router.get("/backtest/list")
async def list_factor_backtests(limit: int = 20):
    """获取因子回测记录"""
    db: Session = SessionLocal()
    try:
        records = db.query(FactorBacktest).order_by(
            FactorBacktest.created_at.desc()
        ).limit(limit).all()
        
        return {
            "count": len(records),
            "backtests": [
                {
                    "id": r.id,
                    "strategy": r.strategy_name,
                    "stock": f"{r.stock_code} {r.stock_name}",
                    "industry": r.industry,
                    "return": r.total_return,
                    "sharpe": r.sharpe_ratio,
                    "win_rate": r.win_rate,
                    "date": str(r.created_at)
                }
                for r in records
            ]
        }
    finally:
        db.close()


@router.get("/performance/{factor_name}")
async def get_factor_performance(factor_name: str):
    """获取单因子表现分析"""
    db: Session = SessionLocal()
    try:
        records = db.query(FactorPerformance).filter(
            FactorPerformance.factor_name == factor_name
        ).order_by(FactorPerformance.avg_return.desc()).all()
        
        return {
            "factor_name": factor_name,
            "count": len(records),
            "performance": [
                {
                    "factor_value": r.factor_value,
                    "sample_count": r.sample_count,
                    "avg_return": r.avg_return,
                    "win_rate": r.win_rate,
                    "avg_sharpe": r.avg_sharpe,
                }
                for r in records
            ]
        }
    finally:
        db.close()