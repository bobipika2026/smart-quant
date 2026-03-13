"""
因子矩阵V2 API - 01矩阵设计
"""
from fastapi import APIRouter, Query
from typing import Optional, List

from app.services.factor_matrix_v2_service import FactorMatrixV2

router = APIRouter(prefix="/api/factor-matrix-v2", tags=["因子矩阵V2"])


@router.get("/factors")
async def list_factors():
    """列出所有可用因子"""
    return {
        "strategy_factors": FactorMatrixV2.STRATEGY_FACTORS,
        "condition_factors": FactorMatrixV2.CONDITION_FACTORS,
        "time_factors": FactorMatrixV2.TIME_FACTORS,
        "total": len(FactorMatrixV2.ALL_FACTORS)
    }


@router.post("/init")
async def init_factors():
    """初始化因子定义"""
    FactorMatrixV2.init_factor_definitions()
    return {"success": True, "total": len(FactorMatrixV2.ALL_FACTORS)}


@router.post("/experiments/generate")
async def generate_experiments(
    stock_code: str = Query(..., description="股票代码"),
    max_combinations: int = Query(50, ge=1, le=1000, description="最大组合数")
):
    """生成因子组合实验"""
    experiments = FactorMatrixV2.generate_experiments(
        stock_code=stock_code,
        max_combinations=max_combinations
    )
    
    return {
        "stock_code": stock_code,
        "total_experiments": len(experiments),
        "experiments": experiments[:10]  # 返回前10个
    }


@router.post("/experiments/run")
async def run_batch_experiments(
    stock_code: str = Query(..., description="股票代码"),
    max_experiments: int = Query(50, ge=1, le=200, description="最大实验数")
):
    """批量运行实验"""
    result = await FactorMatrixV2.run_batch_experiments(
        stock_code=stock_code,
        max_experiments=max_experiments
    )
    return result


@router.get("/contributions")
async def analyze_contributions():
    """分析因子贡献度"""
    result = FactorMatrixV2.analyze_factor_contribution()
    return result


@router.get("/experiments/list")
async def list_experiments(
    stock_code: Optional[str] = Query(None, description="股票代码筛选"),
    limit: int = Query(20, ge=1, le=100, description="返回数量")
):
    """列出实验结果"""
    from app.database import SessionLocal
    from app.models.factor_matrix import FactorExperiment
    from sqlalchemy import desc
    
    db = SessionLocal()
    try:
        query = db.query(FactorExperiment)
        
        if stock_code:
            query = query.filter(FactorExperiment.stock_code == stock_code)
        
        query = query.order_by(desc(FactorExperiment.total_return)).limit(limit)
        records = query.all()
        
        results = []
        for r in records:
            results.append({
                "experiment_code": r.experiment_code,
                "stock_code": r.stock_code,
                "active_factor_count": r.active_factor_count,
                "total_return": r.total_return,
                "sharpe_ratio": r.sharpe_ratio,
                "max_drawdown": r.max_drawdown,
                "win_rate": r.win_rate,
                "created_at": str(r.created_at)
            })
        
        return {"count": len(results), "data": results}
        
    finally:
        db.close()