"""
因子矩阵并行处理 - 使用本地缓存数据进行批量回测
"""
import asyncio
import sys
import os
from typing import Dict, List
from datetime import datetime
import pandas as pd

sys.path.insert(0, '.')

from app.services.factor_matrix_v2_service import FactorMatrixV2
from app.models.factor_matrix import FactorExperiment
from app.database import SessionLocal
import json


def load_local_data(stock_code: str) -> pd.DataFrame:
    """从本地缓存加载历史数据"""
    cache_file = f'data_cache/{stock_code}_history.csv'
    
    if os.path.exists(cache_file):
        df = pd.read_csv(cache_file)
        print(f"[本地缓存] 加载 {stock_code}: {len(df)} 条")
        return df
    else:
        print(f"[本地缓存] 文件不存在: {cache_file}")
        return None


async def run_experiment_batch_fast(
    experiments: List[Dict],
    hist_data: pd.DataFrame,
    batch_id: int
) -> List[Dict]:
    """
    快速运行一批实验（使用本地数据）
    """
    from app.services.strategy import get_strategy
    from app.services.backtest import BacktestEngine
    
    results = []
    
    for exp in experiments:
        try:
            active_factors = exp["active_factors"]
            stock_code = exp["stock_code"]
            
            # 提取策略因子
            strategy_factors = [f for f in active_factors if f.startswith(("ma_", "macd", "rsi", "kdj", "boll", "cci", "wr_"))]
            
            if not strategy_factors:
                continue
            
            # 提取策略配置
            strategy_config = None
            for f in FactorMatrixV2.STRATEGY_FACTORS:
                if f["code"] == strategy_factors[0]:
                    strategy_config = f
                    break
            
            if not strategy_config:
                continue
            
            # 提取策略类型
            factor_code = strategy_factors[0]
            if factor_code.startswith("ma_"):
                strategy_id = "ma_cross"
            elif factor_code.startswith("macd"):
                strategy_id = "macd"
            elif factor_code.startswith("rsi"):
                strategy_id = "rsi"
            elif factor_code.startswith("kdj"):
                strategy_id = "kdj"
            elif factor_code.startswith("boll"):
                strategy_id = "boll"
            elif factor_code.startswith("cci"):
                strategy_id = "cci"
            elif factor_code.startswith("wr"):
                strategy_id = "wr"
            else:
                continue
            
            # 获取策略并运行回测
            strategy = get_strategy(strategy_id)
            if strategy:
                strategy.params = strategy_config["params"]
                df_with_signals = strategy.generate_signals(hist_data)
                
                engine = BacktestEngine()
                backtest_result = engine.run_backtest(df_with_signals)
                
                if backtest_result.get("total_return") is not None:
                    results.append({
                        "experiment_code": exp["experiment_code"],
                        "stock_code": stock_code,
                        "factor_combination": json.dumps(exp["factor_combination"]),
                        "active_factor_count": len(active_factors),
                        "total_return": backtest_result.get("total_return"),
                        "sharpe_ratio": backtest_result.get("sharpe_ratio"),
                        "max_drawdown": backtest_result.get("max_drawdown"),
                        "win_rate": backtest_result.get("win_rate"),
                        "trade_count": backtest_result.get("trade_count"),
                        "batch_id": batch_id
                    })
        
        except Exception as e:
            continue
    
    return results


async def run_parallel_backtest_fast(
    stock_code: str,
    num_agents: int = 8,
    include_conditions: bool = True,
    top_n: int = 10
) -> Dict:
    """
    使用本地缓存数据进行并行回测
    
    优势：
    - 无API调用限制
    - 速度提升10-100倍
    - 预计耗时：几分钟（vs 几小时）
    """
    print(f"[并行回测] 股票: {stock_code}, Agent数: {num_agents}")
    print(f"[并行回测] 使用本地缓存数据")
    
    # 1. 生成所有实验
    print("[并行回测] 生成因子组合...")
    experiments = FactorMatrixV2.generate_all_experiments(
        stock_code=stock_code,
        include_conditions=include_conditions
    )
    print(f"[并行回测] 总实验数: {len(experiments):,}")
    
    # 2. 从本地加载历史数据
    print("[并行回测] 加载本地数据...")
    hist_data = load_local_data(stock_code)
    
    if hist_data is None or len(hist_data) < 50:
        return {"error": "数据不足"}
    
    # 3. 分批分配给多个agent
    batch_size = len(experiments) // num_agents + 1
    batches = []
    
    for i in range(num_agents):
        start = i * batch_size
        end = min(start + batch_size, len(experiments))
        if start < len(experiments):
            batches.append((i + 1, experiments[start:end]))
    
    print(f"[并行回测] 分配 {len(batches)} 个批次, 每批约 {batch_size:,} 个实验")
    
    # 4. 并行执行
    print("[并行回测] 开始并行处理...")
    start_time = datetime.now()
    
    tasks = [
        run_experiment_batch_fast(batch, hist_data, batch_id)
        for batch_id, batch in batches
    ]
    
    all_results = await asyncio.gather(*tasks)
    
    # 5. 汇总结果
    print("[并行回测] 汇总结果...")
    all_results_flat = []
    for batch_results in all_results:
        all_results_flat.extend(batch_results)
    
    print(f"[并行回测] 成功实验数: {len(all_results_flat):,}")
    
    # 6. 综合排名
    for r in all_results_flat:
        ret = r.get("total_return", 0) or 0
        sharpe = r.get("sharpe_ratio", 0) or 0
        drawdown = abs(r.get("max_drawdown", 0) or 0)
        r["composite_score"] = ret * 0.4 + sharpe * 10 * 0.3 - drawdown * 0.3
    
    all_results_flat.sort(key=lambda x: x.get("composite_score", 0), reverse=True)
    
    # 7. 取Top N
    top_results = all_results_flat[:top_n]
    
    # 8. 保存到数据库
    if top_results:
        db = SessionLocal()
        try:
            # 先清空旧数据
            db.query(FactorExperiment).filter(FactorExperiment.stock_code == stock_code).delete()
            
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
            print(f"[并行回测] 保存 Top {len(top_results)} 结果到数据库")
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
    stock_code = sys.argv[1] if len(sys.argv) > 1 else "000001"
    num_agents = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    
    result = asyncio.run(run_parallel_backtest_fast(
        stock_code=stock_code,
        num_agents=num_agents,
        include_conditions=True,
        top_n=10
    ))
    
    print("\n" + "=" * 60)
    print(f"TOP 10 因子组合 (股票: {stock_code})")
    print("=" * 60)
    print(f"总实验: {result['total_experiments']:,}")
    print(f"成功: {result['successful_experiments']:,}")
    print(f"耗时: {result['elapsed_seconds']:.1f}秒")
    print(f"速度: {result['experiments_per_second']:,.0f} 实验/秒")
    
    for r in result.get("top_results", []):
        print(f"\n#{r['rank']}: {r['experiment_code']}")
        print(f"  综合得分: {r['composite_score']}")
        print(f"  收益: {r['total_return']}%, 夏普: {r['sharpe_ratio']}, 回撤: {r['max_drawdown']}%")
        
        factors = r["active_factors"]
        active = [k for k, v in factors.items() if v == 1]
        print(f"  启用因子: {active}")