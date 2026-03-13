"""
因子矩阵并行处理 - 使用多agent并行回测
"""
import asyncio
import sys
from typing import Dict, List
from datetime import datetime

sys.path.insert(0, '.')

from app.services.factor_matrix_v2_service import FactorMatrixV2
from app.services.data import DataService
from app.models.factor_matrix import FactorExperiment
from app.database import SessionLocal
import json


async def run_experiment_batch(
    experiments: List[Dict],
    hist_data,
    batch_id: int
) -> List[Dict]:
    """
    运行一批实验
    
    Args:
        experiments: 实验列表
        hist_data: 历史数据
        batch_id: 批次ID
    
    Returns:
        成功的实验结果列表
    """
    results = []
    
    for exp in experiments:
        try:
            result = await FactorMatrixV2.run_experiment(exp, hist_data)
            
            if "error" not in result and result.get("total_return") is not None:
                results.append({
                    "experiment_code": result["experiment_code"],
                    "stock_code": exp["stock_code"],
                    "factor_combination": json.dumps(result["factor_combination"]),
                    "active_factor_count": len(result["active_factors"]),
                    "total_return": result.get("total_return"),
                    "sharpe_ratio": result.get("sharpe_ratio"),
                    "max_drawdown": result.get("max_drawdown"),
                    "win_rate": result.get("win_rate"),
                    "trade_count": result.get("trade_count"),
                    "batch_id": batch_id
                })
        except Exception as e:
            continue
    
    return results


async def run_parallel_backtest(
    stock_code: str,
    num_agents: int = 4,
    include_conditions: bool = True,
    top_n: int = 10
) -> Dict:
    """
    使用多agent并行运行回测
    
    Args:
        stock_code: 股票代码
        num_agents: 并行agent数量
        include_conditions: 是否包含条件因子
        top_n: 返回top N结果
    
    Returns:
        排名前N的因子组合
    """
    print(f"[并行回测] 股票: {stock_code}, Agent数: {num_agents}")
    
    # 1. 生成所有实验
    print("[并行回测] 生成因子组合...")
    experiments = FactorMatrixV2.generate_all_experiments(
        stock_code=stock_code,
        include_conditions=include_conditions
    )
    print(f"[并行回测] 总实验数: {len(experiments)}")
    
    # 2. 获取历史数据
    print("[并行回测] 获取历史数据...")
    data_service = DataService()
    hist_data = await data_service.get_stock_history(stock_code)
    
    if hist_data is None or len(hist_data) < 50:
        return {"error": "数据不足"}
    
    print(f"[并行回测] 历史数据: {len(hist_data)} 条")
    
    # 3. 分批分配给多个agent
    batch_size = len(experiments) // num_agents + 1
    batches = []
    
    for i in range(num_agents):
        start = i * batch_size
        end = min(start + batch_size, len(experiments))
        if start < len(experiments):
            batches.append((i + 1, experiments[start:end]))
    
    print(f"[并行回测] 分配 {len(batches)} 个批次, 每批约 {batch_size} 个实验")
    
    # 4. 并行执行
    print("[并行回测] 开始并行处理...")
    start_time = datetime.now()
    
    tasks = [
        run_experiment_batch(batch, hist_data, batch_id)
        for batch_id, batch in batches
    ]
    
    all_results = await asyncio.gather(*tasks)
    
    # 5. 汇总结果
    print("[并行回测] 汇总结果...")
    all_results_flat = []
    for batch_results in all_results:
        all_results_flat.extend(batch_results)
    
    print(f"[并行回测] 成功实验数: {len(all_results_flat)}")
    
    # 6. 综合排名（综合得分 = 收益*0.4 + 夏普*0.3 - 最大回撤*0.3）
    for r in all_results_flat:
        ret = r.get("total_return", 0) or 0
        sharpe = r.get("sharpe_ratio", 0) or 0
        drawdown = abs(r.get("max_drawdown", 0) or 0)
        
        # 综合得分
        r["composite_score"] = ret * 0.4 + sharpe * 10 * 0.3 - drawdown * 0.3
    
    # 按综合得分排序
    all_results_flat.sort(key=lambda x: x.get("composite_score", 0), reverse=True)
    
    # 7. 取Top N
    top_results = all_results_flat[:top_n]
    
    # 8. 保存到数据库
    if top_results:
        db = SessionLocal()
        try:
            for r in top_results:
                exp_record = FactorExperiment(
                    experiment_code=r["experiment_code"],
                    stock_code=r["stock_code"],
                    factor_combination=r["factor_combination"],
                    active_factor_count=r["active_factor_count"],
                    total_return=r.get("total_return"),
                    sharpe_ratio=r.get("sharpe_ratio"),
                    max_drawdown=r.get("max_drawdown"),
                    win_rate=r.get("win_rate"),
                    trade_count=r.get("trade_count"),
                    notes=f"composite_score: {r.get('composite_score', 0):.2f}"
                )
                db.add(exp_record)
            db.commit()
            print(f"[并行回测] 保存 {len(top_results)} 条Top结果")
        except Exception as e:
            db.rollback()
            print(f"[并行回测] 保存失败: {e}")
        finally:
            db.close()
    
    # 统计
    elapsed = (datetime.now() - start_time).total_seconds()
    
    return {
        "stock_code": stock_code,
        "total_experiments": len(experiments),
        "successful_experiments": len(all_results_flat),
        "elapsed_seconds": elapsed,
        "experiments_per_second": len(experiments) / elapsed if elapsed > 0 else 0,
        "top_results": [
            {
                "rank": i + 1,
                "experiment_code": r["experiment_code"],
                "total_return": r.get("total_return"),
                "sharpe_ratio": r.get("sharpe_ratio"),
                "max_drawdown": r.get("max_drawdown"),
                "composite_score": round(r.get("composite_score", 0), 2),
                "active_factors": json.loads(r["factor_combination"]) if isinstance(r["factor_combination"], str) else r["factor_combination"]
            }
            for i, r in enumerate(top_results)
        ]
    }


if __name__ == "__main__":
    import sys
    
    stock_code = sys.argv[1] if len(sys.argv) > 1 else "000001"
    num_agents = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    
    result = asyncio.run(run_parallel_backtest(
        stock_code=stock_code,
        num_agents=num_agents,
        include_conditions=True,
        top_n=10
    ))
    
    print("\n" + "=" * 60)
    print(f"TOP 10 因子组合 (股票: {stock_code})")
    print("=" * 60)
    
    for r in result.get("top_results", []):
        print(f"\n#{r['rank']}: {r['experiment_code']}")
        print(f"  综合得分: {r['composite_score']}")
        print(f"  收益: {r['total_return']}%, 夏普: {r['sharpe_ratio']}, 回撤: {r['max_drawdown']}%")
        
        # 显示启用的因子
        factors = r["active_factors"]
        active = [k for k, v in factors.items() if v == 1]
        print(f"  启用因子: {active}")