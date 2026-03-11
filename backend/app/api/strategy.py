"""
策略相关API
"""
from fastapi import APIRouter, HTTPException
from typing import List, Optional
from pydantic import BaseModel

from app.services.strategy import list_strategies, get_strategy
from app.services.data import DataService
from app.services.backtest import BacktestEngine

router = APIRouter(prefix="/api/strategy", tags=["策略管理"])


class BacktestRequest(BaseModel):
    """回测请求"""
    stock_code: str
    strategy_id: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    initial_capital: float = 100000
    params: Optional[dict] = None


@router.get("/list")
async def get_strategy_list():
    """获取策略列表"""
    strategies = list_strategies()
    return {"strategies": strategies}


@router.post("/backtest")
async def run_backtest(request: BacktestRequest):
    """运行回测"""
    try:
        # 获取历史数据
        data_service = DataService()
        df = await data_service.get_stock_history(
            code=request.stock_code,
            start_date=request.start_date,
            end_date=request.end_date
        )
        
        if df.empty:
            raise HTTPException(status_code=404, detail="未找到股票数据")
        
        # 获取策略
        strategy = get_strategy(request.strategy_id)
        if request.params:
            strategy.params.update(request.params)
        
        # 生成信号
        df_with_signals = strategy.generate_signals(df)
        
        # 运行回测
        engine = BacktestEngine(initial_capital=request.initial_capital)
        results = engine.run_backtest(df_with_signals)
        
        results['strategy_name'] = strategy.name
        results['stock_code'] = request.stock_code
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))