"""
因子矩阵V2 API - 01矩阵设计
"""
import json
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


@router.post("/experiments/run-batch")
async def run_batch_for_stocks(
    limit: int = Query(100, ge=1, le=500, description="股票数量")
):
    """
    批量运行100只股票的因子矩阵实验
    
    简化模式：每只股票 = 12策略 × 4时间 = 48个实验
    总计：100股票 × 48实验 = 4800个实验
    预计时间：约40分钟
    """
    from app.database import SessionLocal
    from app.models import Stock
    from app.models.factor_matrix import FactorExperiment
    from app.services.data import DataService
    import json
    
    db = SessionLocal()
    try:
        # 获取股票列表
        stocks = db.query(Stock.code, Stock.name).limit(limit).all()
        print(f"[因子矩阵] 获取 {len(stocks)} 只股票")
        
        # 生成所有实验
        all_experiments = []
        for code, name in stocks:
            experiments = FactorMatrixV2.generate_experiments(
                stock_code=code,
                simple_mode=True
            )
            all_experiments.extend(experiments)
        
        print(f"[因子矩阵] 生成 {len(all_experiments)} 个实验")
        
        # 获取历史数据缓存
        data_service = DataService()
        
        # 执行实验
        success_count = 0
        fail_count = 0
        results_to_save = []
        
        for i, exp in enumerate(all_experiments):
            try:
                stock_code = exp["stock_code"]
                hist_data = await data_service.get_stock_history(stock_code)
                
                if hist_data is not None and len(hist_data) >= 50:
                    result = await FactorMatrixV2.run_experiment(exp, hist_data)
                    
                    if "error" not in result and result.get("total_return") is not None:
                        success_count += 1
                        results_to_save.append({
                            "experiment_code": result["experiment_code"],
                            "stock_code": stock_code,
                            "factor_combination": json.dumps(result["factor_combination"]),
                            "active_factor_count": len(result["active_factors"]),
                            "total_return": result.get("total_return"),
                            "sharpe_ratio": result.get("sharpe_ratio"),
                            "max_drawdown": result.get("max_drawdown"),
                            "win_rate": result.get("win_rate"),
                            "trade_count": result.get("trade_count")
                        })
                    else:
                        fail_count += 1
                else:
                    fail_count += 1
                
                if (i + 1) % 100 == 0:
                    print(f"[因子矩阵] 进度: {i+1}/{len(all_experiments)}, 成功: {success_count}")
                    
            except Exception as e:
                fail_count += 1
        
        # 批量保存结果
        if results_to_save:
            for r in results_to_save:
                exp_record = FactorExperiment(**r)
                db.add(exp_record)
            db.commit()
            print(f"[因子矩阵] 保存 {len(results_to_save)} 条结果")
        
        # 更新因子贡献度
        FactorMatrixV2.analyze_factor_contribution()
        
        return {
            "total_experiments": len(all_experiments),
            "success_count": success_count,
            "fail_count": fail_count,
            "stocks_processed": len(stocks),
            "saved_records": len(results_to_save)
        }
        
    except Exception as e:
        db.rollback()
        return {"error": str(e)}
    finally:
        db.close()


@router.post("/experiments/parallel-run")
async def run_parallel_experiments(
    stock_code: str = Query(..., description="股票代码"),
    num_agents: int = Query(4, ge=1, le=16, description="并行agent数量"),
    include_conditions: bool = Query(True, description="是否包含条件因子"),
    top_n: int = Query(10, ge=1, le=100, description="返回top N结果")
):
    """
    使用多agent并行运行完整因子组合回测
    
    - 生成约90万+因子组合
    - 分批并行处理
    - 综合排名返回Top N
    - 预计时间：约30-60分钟（取决于agent数量）
    """
    import sys
    sys.path.insert(0, '.')
    
    from run_parallel_backtest import run_parallel_backtest
    
    result = await run_parallel_backtest(
        stock_code=stock_code,
        num_agents=num_agents,
        include_conditions=include_conditions,
        top_n=top_n
    )
    
    return result


@router.get("/best-combinations")
async def list_best_combinations(
    stock_code: Optional[str] = Query(None, description="股票代码筛选"),
    limit: int = Query(20, ge=1, le=100, description="返回数量")
):
    """
    获取最佳因子组合列表
    
    按综合得分排序，返回历史最优因子组合
    """
    from app.database import SessionLocal
    from app.models.factor_matrix import BestFactorCombination
    from sqlalchemy import desc
    
    db = SessionLocal()
    try:
        query = db.query(BestFactorCombination).filter(BestFactorCombination.is_active == True)
        
        if stock_code:
            query = query.filter(BestFactorCombination.stock_code == stock_code)
        
        query = query.order_by(desc(BestFactorCombination.composite_score)).limit(limit)
        records = query.all()
        
        results = []
        for r in records:
            results.append({
                "id": r.id,
                "combination_code": r.combination_code,
                "stock_code": r.stock_code,
                "stock_name": r.stock_name,
                "strategy_desc": r.strategy_desc,
                "factor_combination": json.loads(r.factor_combination) if r.factor_combination else {},
                "total_return": r.total_return,
                "sharpe_ratio": r.sharpe_ratio,
                "composite_score": r.composite_score,
                "holding_period": r.holding_period,
                "backtest_date": str(r.backtest_date) if r.backtest_date else None,
                "created_at": str(r.created_at) if r.created_at else None,
            })
        
        return {"count": len(results), "data": results}
        
    finally:
        db.close()


@router.get("/best-combinations/{combination_id}")
async def get_best_combination(combination_id: int):
    """获取单个最佳因子组合详情"""
    from app.database import SessionLocal
    from app.models.factor_matrix import BestFactorCombination
    
    db = SessionLocal()
    try:
        record = db.query(BestFactorCombination).filter(
            BestFactorCombination.id == combination_id
        ).first()
        
        if not record:
            return {"error": "组合不存在"}
        
        return {
            "id": record.id,
            "combination_code": record.combination_code,
            "stock_code": record.stock_code,
            "stock_name": record.stock_name,
            "strategy_desc": record.strategy_desc,
            "factor_combination": json.loads(record.factor_combination) if record.factor_combination else {},
            "total_return": record.total_return,
            "sharpe_ratio": record.sharpe_ratio,
            "max_drawdown": record.max_drawdown,
            "win_rate": record.win_rate,
            "trade_count": record.trade_count,
            "composite_score": record.composite_score,
            "holding_period": record.holding_period,
            "backtest_date": str(record.backtest_date) if record.backtest_date else None,
            "notes": record.notes,
        }
        
    finally:
        db.close()


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