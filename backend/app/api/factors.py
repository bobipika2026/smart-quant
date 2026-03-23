"""
因子库 API 接口
"""
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from enum import Enum

from app.factors.factor_library import get_factor_library, FactorCategory

router = APIRouter(prefix="/api/factors", tags=["因子库"])


class FactorCategoryEnum(str, Enum):
    """因子类别枚举"""
    VALUE = "value"
    GROWTH = "growth"
    QUALITY = "quality"
    MOMENTUM = "momentum"
    SENTIMENT = "sentiment"
    TECHNICAL = "technical"


@router.get("/list")
async def list_factors(
    category: Optional[FactorCategoryEnum] = Query(None, description="因子类别")
):
    """
    列出所有因子
    
    Args:
        category: 因子类别筛选（可选）
    
    Returns:
        因子列表，包含名称、代码、类别、权重、IC预期等
    """
    library = get_factor_library()
    
    category_enum = FactorCategory(category) if category else None
    factors = library.list_factors(category_enum)
    
    result = []
    for f in factors:
        result.append({
            "name": f.name,
            "code": f.code,
            "category": f.category.value,
            "formula": f.formula,
            "description": f.description,
            "weight": f.weight,
            "ic_expected": f.ic_expected,
            "direction": "正向" if f.direction == 1 else "反向" if f.direction == -1 else "中性",
            "data_source": f.data_source,
            "update_freq": f.update_freq
        })
    
    return {
        "code": 0,
        "data": {
            "total_count": len(result),
            "factors": result
        }
    }


@router.get("/categories")
async def list_categories():
    """
    列出因子类别
    
    Returns:
        各类别的因子数量
    """
    library = get_factor_library()
    
    categories = {}
    for cat in FactorCategory:
        codes = library.factors_by_category.get(cat, [])
        categories[cat.value] = {
            "name": {
                "value": "价值因子",
                "growth": "成长因子",
                "quality": "质量因子",
                "momentum": "动量因子",
                "sentiment": "情绪因子",
                "technical": "技术因子"
            }.get(cat.value, cat.value),
            "count": len(codes),
            "factors": codes
        }
    
    return {
        "code": 0,
        "data": categories
    }


@router.get("/{factor_code}")
async def get_factor_info(factor_code: str):
    """
    获取单个因子详情
    
    Args:
        factor_code: 因子代码
    """
    library = get_factor_library()
    factor = library.get_factor_info(factor_code)
    
    if not factor:
        raise HTTPException(status_code=404, detail=f"因子 {factor_code} 不存在")
    
    return {
        "code": 0,
        "data": {
            "name": factor.name,
            "code": factor.code,
            "category": factor.category.value,
            "formula": factor.formula,
            "description": factor.description,
            "weight": factor.weight,
            "ic_expected": factor.ic_expected,
            "direction": factor.direction,
            "data_source": factor.data_source,
            "update_freq": factor.update_freq
        }
    }


@router.get("/{factor_code}/calc")
async def calc_factor(
    factor_code: str,
    stock_code: str = Query(..., description="股票代码"),
    date: Optional[str] = Query(None, description="日期(YYYYMMDD)")
):
    """
    计算单个因子值
    
    Args:
        factor_code: 因子代码
        stock_code: 股票代码
        date: 日期（可选）
    """
    library = get_factor_library()
    value = library.calc_factor(factor_code, stock_code, date)
    
    return {
        "code": 0,
        "data": {
            "factor_code": factor_code,
            "stock_code": stock_code,
            "date": date,
            "value": value
        }
    }


@router.get("/calc/all")
async def calc_all_factors(
    stock_code: str = Query(..., description="股票代码"),
    date: Optional[str] = Query(None, description="日期(YYYYMMDD)")
):
    """
    计算所有因子值
    
    Args:
        stock_code: 股票代码
        date: 日期（可选）
    """
    library = get_factor_library()
    values = library.calc_all_factors(stock_code, date)
    
    # 按类别分组
    result = {}
    for cat in FactorCategory:
        cat_factors = library.factors_by_category.get(cat, [])
        result[cat.value] = {
            code: values.get(code) for code in cat_factors if code in values
        }
    
    return {
        "code": 0,
        "data": {
            "stock_code": stock_code,
            "date": date,
            "factors": result,
            "total_count": len(values)
        }
    }


@router.get("/calc/category/{category}")
async def calc_category_factors(
    category: FactorCategoryEnum,
    stock_code: str = Query(..., description="股票代码"),
    date: Optional[str] = Query(None, description="日期(YYYYMMDD)")
):
    """
    计算某类别的因子值
    
    Args:
        category: 因子类别
        stock_code: 股票代码
        date: 日期（可选）
    """
    library = get_factor_library()
    category_enum = FactorCategory(category.value)
    values = library.calc_category_factors(category_enum, stock_code, date)
    
    return {
        "code": 0,
        "data": {
            "category": category.value,
            "stock_code": stock_code,
            "date": date,
            "factors": values
        }
    }


@router.get("/weights/{category}")
async def get_factor_weights(category: FactorCategoryEnum):
    """
    获取某类别的因子权重
    
    Args:
        category: 因子类别
    """
    library = get_factor_library()
    category_enum = FactorCategory(category.value)
    weights = library.get_factor_weights(category_enum)
    
    return {
        "code": 0,
        "data": {
            "category": category.value,
            "weights": weights
        }
    }