"""
策略相关API
"""
from fastapi import APIRouter, HTTPException
from typing import List, Optional
from pydantic import BaseModel

from app.services.strategy import list_strategies, get_strategy
from app.services.data import DataService
from app.services.backtest import BacktestEngine
from app.services.factor_matrix_service import FactorMatrixService

router = APIRouter(prefix="/api/strategy", tags=["策略管理"])


class BacktestRequest(BaseModel):
    """回测请求"""
    stock_code: str
    strategy_id: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    initial_capital: float = 100000
    params: Optional[dict] = None
    data_freq: str = "day"  # 数据频率: day, 60min, 1min


class ComboBacktestRequest(BaseModel):
    """组合策略回测请求"""
    stock_code: str
    strategy_ids: List[str]  # 策略ID列表
    combo_mode: str = "OR"  # OR 或 AND
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    initial_capital: float = 100000
    strategy_params: Optional[List[dict]] = None  # 每个策略的参数列表
    data_freq: str = "day"  # 数据频率: day, 60min, 1min


@router.get("/list")
async def get_strategy_list():
    """获取策略列表"""
    strategies = list_strategies()
    return {"strategies": strategies}


@router.post("/backtest")
async def run_backtest(request: BacktestRequest):
    """运行回测"""
    try:
        # 获取历史数据 - 优先使用缓存
        data_service = DataService()
        
        # 尝试从缓存获取
        df = DataService.get_cached_data(request.stock_code, request.data_freq)
        
        # 如果缓存没有数据，则在线获取
        if df.empty:
            print(f"[回测] 缓存无数据，在线获取: {request.stock_code}")
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
        
        # 保存回测因子到因子矩阵
        try:
            stock_name = request.stock_code  # 简化处理，可后续补充股票名称
            await FactorMatrixService.save_backtest_factors(
                backtest_result=results,
                strategy_id=request.strategy_id,
                strategy_name=strategy.name,
                stock_code=request.stock_code,
                stock_name=stock_name,
                params=request.params or strategy.params
            )
        except Exception as e:
            print(f"[回测] 保存因子失败: {e}")  # 不影响主流程
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/combo-backtest")
async def run_combo_backtest(request: ComboBacktestRequest):
    """
    组合策略回测
    
    - 支持多个策略组合
    - combo_mode: OR(任一信号买入) 或 AND(所有信号同时满足才买入)
    - strategy_params: 每个策略的参数列表，按 strategy_ids 顺序对应
    """
    import pandas as pd
    import numpy as np
    
    try:
        # 获取历史数据 - 优先使用缓存
        data_service = DataService()
        
        # 尝试从缓存获取
        df = DataService.get_cached_data(request.stock_code, request.data_freq)
        
        # 如果缓存没有数据，则在线获取
        if df.empty:
            print(f"[回测] 缓存无数据，在线获取: {request.stock_code} {request.data_freq}")
            if request.data_freq == 'day':
                df = await data_service.get_stock_history(
                    code=request.stock_code,
                    start_date=request.start_date,
                    end_date=request.end_date
                )
            elif request.data_freq in ['60min', '1min']:
                df = await data_service.get_minute_history(
                    code=request.stock_code,
                    freq=request.data_freq
                )
        
        if df.empty:
            raise HTTPException(status_code=404, detail="未找到股票数据")
        
        # 根据时间范围过滤数据
        if request.start_date or request.end_date:
            # 确保日期列存在
            if '日期' in df.columns:
                df['日期'] = pd.to_datetime(df['日期'].astype(str))
                df = df.sort_values('日期').reset_index(drop=True)
                
                if request.start_date:
                    start_str = request.start_date[:4] + '-' + request.start_date[4:6] + '-' + request.start_date[6:8]
                    start_dt = pd.to_datetime(start_str)
                    df = df[df['日期'] >= start_dt]
                
                if request.end_date:
                    end_str = request.end_date[:4] + '-' + request.end_date[4:6] + '-' + request.end_date[6:8]
                    end_dt = pd.to_datetime(end_str)
                    df = df[df['日期'] <= end_dt]
                
                df = df.reset_index(drop=True)
                print(f"[回测] 时间过滤后: {len(df)}条")
        
        # 记录数据来源
        print(f"[回测] 使用数据: {request.data_freq}, {len(df)}条")
        
        # 获取所有策略的信号
        signals_list = []
        strategy_names = []
        
        for i, strategy_id in enumerate(request.strategy_ids):
            try:
                strategy = get_strategy(strategy_id)
                
                # 应用参数
                if request.strategy_params and i < len(request.strategy_params):
                    params = request.strategy_params[i]
                    if params:
                        strategy.params.update(params)
                
                df_strategy = strategy.generate_signals(df.copy())
                
                # 提取信号列
                if 'signal' in df_strategy.columns:
                    signals_list.append(df_strategy['signal'].values)
                    strategy_names.append(strategy.name)
            except Exception as e:
                print(f"[组合回测] 策略 {strategy_id} 获取失败: {e}")
        
        if not signals_list:
            raise HTTPException(status_code=400, detail="没有有效的策略")
        
        # 组合信号
        signals_array = np.array(signals_list)
        
        if request.combo_mode.upper() == "AND":
            # AND模式：所有策略都发出买入信号才买入，都发出卖出信号才卖出
            buy_signal = np.all(signals_array == 1, axis=0).astype(int)
            sell_signal = np.all(signals_array == -1, axis=0).astype(int)
            combined_signal = buy_signal - sell_signal
        else:
            # OR模式（默认）：任一策略发出信号
            combined_signal = np.sum(signals_array, axis=0)
            combined_signal = np.clip(combined_signal, -1, 1)
        
        # 创建组合信号DataFrame
        df_combo = df.copy()
        df_combo['signal'] = combined_signal
        
        # 运行回测
        engine = BacktestEngine(initial_capital=request.initial_capital)
        results = engine.run_backtest(df_combo)
        
        results['strategy_name'] = f"组合策略({'+'.join(strategy_names)})"
        results['combo_mode'] = request.combo_mode
        results['strategy_ids'] = request.strategy_ids
        results['strategy_params'] = request.strategy_params
        results['stock_code'] = request.stock_code
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))