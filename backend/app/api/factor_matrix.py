"""
因子矩阵API
"""
from fastapi import APIRouter, Query
from typing import Optional

from app.services.factor_matrix_service import FactorMatrixService

router = APIRouter(prefix="/api/factor-matrix", tags=["因子矩阵"])


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
            }
        ]
    }