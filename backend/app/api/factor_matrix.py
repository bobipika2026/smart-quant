"""
因子矩阵API
"""
from fastapi import APIRouter, Query
from typing import Optional

from app.services.factor_matrix_service import FactorMatrixService
from app.services.factor_selector import FactorSelector

router = APIRouter(prefix="/api/factor-matrix", tags=["因子矩阵"])


@router.post("/generate")
async def generate_factor_matrix(
    limit: int = Query(100, ge=1, le=5000, description="股票数量")
):
    """
    生成因子矩阵
    
    为所有股票（或指定数量）生成因子数据
    """
    result = await FactorMatrixService.generate_factor_matrix(limit=limit)
    return result


@router.get("/data")
async def get_factor_matrix_data(
    trade_date: Optional[str] = Query(None, description="交易日期 YYYY-MM-DD"),
    limit: int = Query(100, ge=1, le=1000, description="返回条数")
):
    """
    获取因子矩阵数据
    
    返回股票×因子的矩阵数据
    """
    result = FactorMatrixService.get_factor_matrix_data(trade_date=trade_date, limit=limit)
    return result


@router.get("/statistics")
async def get_statistics():
    """获取因子矩阵统计信息"""
    return FactorMatrixService.get_statistics()


@router.get("/matrix")
async def get_factor_matrix(
    strategy_id: Optional[str] = Query(None, description="策略ID过滤"),
    stock_code: Optional[str] = Query(None, description="股票代码过滤"),
    industry: Optional[str] = Query(None, description="行业过滤"),
    limit: int = Query(100, ge=1, le=1000, description="返回条数")
):
    """
    查询因子矩阵
    
    返回回测记录的因子组合数据
    """
    result = FactorMatrixService.get_factor_matrix(
        strategy_id=strategy_id,
        stock_code=stock_code,
        industry=industry,
        limit=limit
    )
    return {
        "success": True,
        "count": len(result),
        "data": result
    }


@router.get("/analyze/{factor_name}")
async def analyze_factor(
    factor_name: str,
    top_n: int = Query(10, ge=1, le=50, description="返回前N个")
):
    """
    分析单个因子的收益表现
    
    支持的因子：
    - strategy_id: 策略ID
    - strategy_name: 策略名称
    - industry: 行业
    - market_cap_level: 市值等级
    - param_short: 短周期参数
    - param_long: 长周期参数
    """
    result = FactorMatrixService.analyze_factor_performance(factor_name, top_n)
    return result


@router.get("/correlation")
async def get_correlation():
    """
    获取因子与收益的相关性矩阵
    
    分析参数因子与收益指标的相关性
    """
    result = FactorMatrixService.get_correlation_matrix()
    return result


@router.get("/factors/list")
async def list_available_factors():
    """列出可分析的因子"""
    return {
        "factors": [
            {
                "name": "strategy_id",
                "description": "策略ID",
                "type": "categorical"
            },
            {
                "name": "strategy_name", 
                "description": "策略名称",
                "type": "categorical"
            },
            {
                "name": "industry",
                "description": "行业",
                "type": "categorical"
            },
            {
                "name": "market_cap_level",
                "description": "市值等级（大盘/中盘/小盘）",
                "type": "categorical"
            },
            {
                "name": "param_short",
                "description": "短周期参数",
                "type": "numeric"
            },
            {
                "name": "param_long",
                "description": "长周期参数",
                "type": "numeric"
            },
            {
                "name": "pe_range",
                "description": "市盈率分段（低/中低/中高/高）",
                "type": "numeric_range"
            },
            {
                "name": "pb_range",
                "description": "市净率分段",
                "type": "numeric_range"
            },
            {
                "name": "roe_range",
                "description": "净资产收益率分段",
                "type": "numeric_range"
            },
            {
                "name": "market_cap_range",
                "description": "市值分段",
                "type": "numeric_range"
            }
        ]
    }


# ==================== 因子选股 ====================

@router.get("/select/presets")
async def get_filter_presets():
    """获取预设筛选方案"""
    presets = await FactorSelector.get_filter_presets()
    return {"presets": presets}


@router.get("/select/stocks")
async def select_stocks(
    preset: Optional[str] = Query(None, description="预设方案: quality/value/growth"),
    roe_min: Optional[float] = Query(None, description="ROE最小值(%)"),
    debt_ratio_max: Optional[float] = Query(None, description="负债率最大值(%)"),
    market_cap_min: Optional[float] = Query(None, description="市值最小值(亿)"),
    north_holdings_min: Optional[float] = Query(None, description="北向持股最小值(%)"),
    pe_max: Optional[float] = Query(None, description="PE最大值"),
    pb_max: Optional[float] = Query(None, description="PB最大值"),
    sort_by: str = Query("roe", description="排序字段"),
    limit: int = Query(100, ge=1, le=500, description="返回数量")
):
    """
    基于因子筛选股票
    
    可以使用预设方案或自定义筛选条件
    """
    # 预设方案
    if preset:
        preset_map = {
            'quality': FactorSelector.QUALITY_STOCK_FILTER,
            'value': FactorSelector.VALUE_STOCK_FILTER,
            'growth': FactorSelector.GROWTH_STOCK_FILTER,
        }
        filters = preset_map.get(preset, FactorSelector.QUALITY_STOCK_FILTER)
    else:
        # 自定义筛选条件
        filters = {}
        if roe_min is not None:
            filters['roe_min'] = roe_min
        if debt_ratio_max is not None:
            filters['debt_ratio_max'] = debt_ratio_max
        if market_cap_min is not None:
            filters['market_cap_min'] = market_cap_min
        if north_holdings_min is not None:
            filters['north_holdings_min'] = north_holdings_min
        if pe_max is not None:
            filters['pe_max'] = pe_max
        if pb_max is not None:
            filters['pb_max'] = pb_max
        
        # 默认条件
        if not filters:
            filters = FactorSelector.QUALITY_STOCK_FILTER
    
    result = await FactorSelector.select_stocks(
        filters=filters,
        sort_by=sort_by,
        limit=limit
    )
    
    return result