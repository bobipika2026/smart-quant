"""
因子库API接口

提供因子库的查询、初始化等接口
"""
from typing import Optional
from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.factor_library_db import get_factor_library_service

router = APIRouter(prefix="/api/factor-library", tags=["factor-library"])


@router.get("/factors")
async def get_factors(
    category: Optional[str] = Query(None, description="因子类别筛选"),
    is_selected: Optional[bool] = Query(None, description="是否选中因子"),
    search: Optional[str] = Query(None, description="搜索关键词"),
    sort_by: Optional[str] = Query("ir", description="排序字段"),
    sort_order: Optional[str] = Query("desc", description="排序方向"),
    db: Session = Depends(get_db)
):
    """
    获取所有因子列表
    
    - category: 类别筛选 (value/growth/quality/momentum/sentiment/technical)
    - is_selected: 是否最终选中的因子
    - search: 搜索因子代码或名称
    - sort_by: 排序字段 (code/name/category/ic_mean/ir)
    - sort_order: 排序方向 (asc/desc)
    """
    service = get_factor_library_service(db)
    factors = service.get_all_factors(category=category, is_selected=is_selected)
    
    # 搜索过滤
    if search:
        search_lower = search.lower()
        factors = [
            f for f in factors
            if search_lower in f["code"].lower() or search_lower in f["name"].lower()
        ]
    
    # 排序
    if sort_by:
        reverse = sort_order == "desc"
        # 处理None值
        factors.sort(key=lambda x: x.get(sort_by) if x.get(sort_by) is not None else 0, reverse=reverse)
    
    return {
        "total": len(factors),
        "factors": factors
    }


@router.get("/factors/{code}")
async def get_factor_detail(
    code: str,
    db: Session = Depends(get_db)
):
    """
    获取因子详情
    
    - code: 因子代码
    """
    service = get_factor_library_service(db)
    detail = service.get_factor_detail(code)
    
    if not detail:
        return {"error": "因子不存在", "code": code}
    
    return detail


@router.get("/categories")
async def get_categories(db: Session = Depends(get_db)):
    """
    获取因子类别统计
    """
    service = get_factor_library_service(db)
    stats = service.get_category_stats()
    
    # 转换为列表格式
    category_names = {
        "value": "价值因子",
        "growth": "成长因子",
        "quality": "质量因子",
        "momentum": "动量因子",
        "sentiment": "情绪因子",
        "technical": "技术因子"
    }
    
    categories = []
    for cat, data in stats.items():
        categories.append({
            "category": cat,
            "category_name": category_names.get(cat, cat),
            **data
        })
    
    return {
        "total": len(categories),
        "categories": categories
    }


@router.get("/test-results")
async def get_test_results(
    limit: int = Query(100, description="返回数量限制"),
    db: Session = Depends(get_db)
):
    """
    获取因子检验结果
    """
    service = get_factor_library_service(db)
    results = service.get_test_results(limit=limit)
    
    return {
        "total": len(results),
        "results": results
    }


@router.get("/correlations")
async def get_correlations(
    threshold: float = Query(0.8, description="相关性阈值"),
    db: Session = Depends(get_db)
):
    """
    获取高相关因子对
    """
    service = get_factor_library_service(db)
    correlations = service.get_correlations(threshold=threshold)
    
    return {
        "total": len(correlations),
        "correlations": correlations
    }


@router.get("/param-sensitivity")
async def get_param_sensitivity(
    factor_type: Optional[str] = Query(None, description="因子类型"),
    db: Session = Depends(get_db)
):
    """
    获取参数敏感性测试结果
    """
    service = get_factor_library_service(db)
    results = service.get_param_sensitivity(factor_type=factor_type)
    
    # 按因子类型分组
    grouped = {}
    for r in results:
        ft = r["factor_type"]
        if ft not in grouped:
            grouped[ft] = {
                "factor_type": ft,
                "results": [],
                "best": None
            }
        grouped[ft]["results"].append(r)
        if r["is_best"]:
            grouped[ft]["best"] = r
    
    summary = list(grouped.values())
    
    return {
        "total": len(summary),
        "summary": summary
    }


@router.get("/selection")
async def get_selection_result(db: Session = Depends(get_db)):
    """
    获取最新的因子筛选结果
    """
    service = get_factor_library_service(db)
    result = service.get_selection_result()
    
    return result or {"error": "暂无筛选结果"}


@router.post("/init")
async def init_factor_library(db: Session = Depends(get_db)):
    """
    初始化因子库
    
    将导入因子定义、IC检验结果、参数敏感性结果、相关性分析结果
    """
    service = get_factor_library_service(db)
    result = service.init_all()
    
    return {
        "message": "因子库初始化完成",
        "result": result
    }


@router.get("/summary")
async def get_library_summary(db: Session = Depends(get_db)):
    """
    获取因子库摘要统计
    """
    service = get_factor_library_service(db)
    
    # 获取各类统计
    stats = service.get_category_stats()
    factors = service.get_all_factors()
    selection = service.get_selection_result()
    
    # 计算总体统计
    total_factors = len(factors)
    tested_factors = len([f for f in factors if f["is_tested"]])
    valid_factors = len([f for f in factors if f["is_valid"]])
    selected_factors = len([f for f in factors if f["is_selected"]])
    
    # 计算IC统计
    ics = [f["ic_mean"] for f in factors if f.get("ic_mean") is not None]
    avg_ic = sum(ics) / len(ics) if ics else 0
    max_ic = max(ics) if ics else 0
    min_ic = min(ics) if ics else 0
    
    # 类别统计
    category_names = {
        "value": "价值因子",
        "growth": "成长因子",
        "quality": "质量因子",
        "momentum": "动量因子",
        "sentiment": "情绪因子",
        "technical": "技术因子"
    }
    
    categories = []
    for cat, data in stats.items():
        categories.append({
            "category": cat,
            "category_name": category_names.get(cat, cat),
            **data
        })
    
    return {
        "total_factors": total_factors,
        "tested_factors": tested_factors,
        "valid_factors": valid_factors,
        "selected_factors": selected_factors,
        "test_rate": tested_factors / total_factors if total_factors > 0 else 0,
        "valid_rate": valid_factors / tested_factors if tested_factors > 0 else 0,
        "selection_rate": selected_factors / valid_factors if valid_factors > 0 else 0,
        "avg_ic": avg_ic,
        "max_ic": max_ic,
        "min_ic": min_ic,
        "categories": categories,
        "selection": selection
    }