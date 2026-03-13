"""
因子矩阵服务 - 01矩阵设计
每个策略+参数 = 一个因子，取值0或1
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
import json
import itertools

from app.database import SessionLocal
from app.models.factor_matrix import FactorDefinition, FactorExperiment, FactorContribution


class FactorMatrixV2:
    """因子矩阵服务 - 01矩阵设计"""
    
    # ==================== 因子定义 ====================
    
    # 策略因子（策略+参数组合）
    STRATEGY_FACTORS = [
        # MA金叉
        {"code": "ma_5_20", "name": "MA金叉(5,20)", "type": "strategy", "params": {"short": 5, "long": 20}},
        {"code": "ma_5_30", "name": "MA金叉(5,30)", "type": "strategy", "params": {"short": 5, "long": 30}},
        {"code": "ma_10_20", "name": "MA金叉(10,20)", "type": "strategy", "params": {"short": 10, "long": 20}},
        {"code": "ma_10_30", "name": "MA金叉(10,30)", "type": "strategy", "params": {"short": 10, "long": 30}},
        
        # MACD
        {"code": "macd_default", "name": "MACD(12,26,9)", "type": "strategy", "params": {"fast": 12, "slow": 26, "signal": 9}},
        {"code": "macd_fast", "name": "MACD快速(8,17,9)", "type": "strategy", "params": {"fast": 8, "slow": 17, "signal": 9}},
        
        # RSI
        {"code": "rsi_14_70", "name": "RSI(14,超买70)", "type": "strategy", "params": {"period": 14, "upper": 70, "lower": 30}},
        {"code": "rsi_14_80", "name": "RSI(14,超买80)", "type": "strategy", "params": {"period": 14, "upper": 80, "lower": 20}},
        
        # KDJ
        {"code": "kdj_default", "name": "KDJ(9,3,3)", "type": "strategy", "params": {"n": 9, "m1": 3, "m2": 3}},
        
        # 布林带
        {"code": "boll_20_2", "name": "布林带(20,2)", "type": "strategy", "params": {"period": 20, "std": 2}},
        
        # 其他
        {"code": "cci_14", "name": "CCI(14)", "type": "strategy", "params": {"period": 14}},
        {"code": "wr_14", "name": "WR(14)", "type": "strategy", "params": {"period": 14}},
    ]
    
    # 条件因子（股票筛选条件）
    CONDITION_FACTORS = [
        {"code": "pe_lt_20", "name": "PE<20", "type": "condition", "params": {"field": "pe", "op": "<", "value": 20}},
        {"code": "pe_lt_15", "name": "PE<15", "type": "condition", "params": {"field": "pe", "op": "<", "value": 15}},
        {"code": "pb_lt_2", "name": "PB<2", "type": "condition", "params": {"field": "pb", "op": "<", "value": 2}},
        {"code": "roe_gt_10", "name": "ROE>10%", "type": "condition", "params": {"field": "roe", "op": ">", "value": 10}},
        {"code": "roe_gt_15", "name": "ROE>15%", "type": "condition", "params": {"field": "roe", "op": ">", "value": 15}},
        {"code": "debt_lt_60", "name": "负债率<60%", "type": "condition", "params": {"field": "debt_ratio", "op": "<", "value": 60}},
        {"code": "north_gt_1", "name": "北向持股>1%", "type": "condition", "params": {"field": "north_holdings_ratio", "op": ">", "value": 1}},
        {"code": "north_gt_3", "name": "北向持股>3%", "type": "condition", "params": {"field": "north_holdings_ratio", "op": ">", "value": 3}},
        {"code": "cap_gt_100", "name": "市值>100亿", "type": "condition", "params": {"field": "market_cap", "op": ">", "value": 100}},
        {"code": "cap_gt_500", "name": "市值>500亿", "type": "condition", "params": {"field": "market_cap", "op": ">", "value": 500}},
    ]
    
    # 时间因子
    TIME_FACTORS = [
        {"code": "period_3m", "name": "持仓3个月", "type": "time", "params": {"days": 90}},
        {"code": "period_6m", "name": "持仓6个月", "type": "time", "params": {"days": 180}},
        {"code": "period_1y", "name": "持仓1年", "type": "time", "params": {"days": 365}},
        {"code": "period_2y", "name": "持仓2年", "type": "time", "params": {"days": 730}},
    ]
    
    ALL_FACTORS = STRATEGY_FACTORS + CONDITION_FACTORS + TIME_FACTORS
    
    # ==================== 初始化因子定义 ====================
    
    @staticmethod
    def init_factor_definitions():
        """初始化因子定义到数据库"""
        db: Session = SessionLocal()
        try:
            for factor in FactorMatrixV2.ALL_FACTORS:
                existing = db.query(FactorDefinition).filter(
                    FactorDefinition.factor_code == factor["code"]
                ).first()
                
                if not existing:
                    db.add(FactorDefinition(
                        factor_code=factor["code"],
                        factor_name=factor["name"],
                        factor_type=factor["type"],
                        factor_params=json.dumps(factor["params"]),
                        description=factor["name"]
                    ))
            
            db.commit()
            print(f"[因子矩阵] 初始化 {len(FactorMatrixV2.ALL_FACTORS)} 个因子定义")
            
        except Exception as e:
            db.rollback()
            print(f"[因子矩阵] 初始化失败: {e}")
        finally:
            db.close()
    
    # ==================== 实验生成 ====================
    
    @staticmethod
    def generate_experiments(
        strategy_factors: List[str] = None,  # 策略因子列表
        condition_factors: List[str] = None,  # 条件因子列表
        time_factors: List[str] = None,  # 时间因子列表
        stock_code: str = None,
        max_combinations: int = 100
    ) -> List[Dict]:
        """
        生成因子组合实验
        
        Args:
            strategy_factors: 策略因子代码列表，None则使用全部
            condition_factors: 条件因子代码列表
            time_factors: 时间因子代码列表
            stock_code: 股票代码
            max_combinations: 最大组合数
        
        Returns:
            实验列表，每个实验是一个因子组合
        """
        # 使用默认因子
        if strategy_factors is None:
            strategy_factors = [f["code"] for f in FactorMatrixV2.STRATEGY_FACTORS[:5]]
        if condition_factors is None:
            condition_factors = [f["code"] for f in FactorMatrixV2.CONDITION_FACTORS[:5]]
        if time_factors is None:
            time_factors = [f["code"] for f in FactorMatrixV2.TIME_FACTORS[:2]]
        
        experiments = []
        exp_id = 1
        
        # 生成策略因子组合（选择1-3个策略）
        for num_strategies in range(1, min(4, len(strategy_factors) + 1)):
            for strategy_combo in itertools.combinations(strategy_factors, num_strategies):
                # 生成条件因子组合（选择0-3个条件）
                for num_conditions in range(0, min(4, len(condition_factors) + 1)):
                    for condition_combo in itertools.combinations(condition_factors, num_conditions):
                        # 时间因子（选1个）
                        for time_factor in time_factors:
                            # 构建因子组合
                            factor_combination = {}
                            all_factors = list(strategy_combo) + list(condition_combo) + [time_factor]
                            
                            for f in FactorMatrixV2.ALL_FACTORS:
                                factor_combination[f["code"]] = 1 if f["code"] in all_factors else 0
                            
                            experiments.append({
                                "experiment_code": f"EXP_{exp_id:04d}",
                                "stock_code": stock_code,
                                "factor_combination": factor_combination,
                                "active_factors": all_factors,
                                "active_count": len(all_factors)
                            })
                            
                            exp_id += 1
                            if len(experiments) >= max_combinations:
                                return experiments
        
        return experiments
    
    # ==================== 实验执行 ====================
    
    @staticmethod
    async def run_experiment(
        experiment: Dict,
        hist_data: pd.DataFrame = None
    ) -> Dict:
        """
        执行单个实验
        
        Args:
            experiment: 实验配置
            hist_data: 历史数据
        
        Returns:
            实验结果
        """
        from app.services.strategy import get_strategy
        from app.services.backtest import BacktestEngine
        
        factor_combination = experiment["factor_combination"]
        active_factors = experiment["active_factors"]
        stock_code = experiment["stock_code"]
        
        # 1. 提取启用的策略因子
        strategy_factors = [f for f in active_factors if f.startswith(("ma_", "macd", "rsi", "kdj", "boll", "cci", "wr_"))]
        
        # 2. 提取启用的条件因子
        condition_factors = [f for f in active_factors if f.startswith(("pe_", "pb_", "roe_", "debt_", "north_", "cap_"))]
        
        # 3. 提取时间因子
        time_factor = [f for f in active_factors if f.startswith("period_")]
        holding_days = 365  # 默认1年
        if time_factor:
            time_config = next((f for f in FactorMatrixV2.TIME_FACTORS if f["code"] == time_factor[0]), None)
            if time_config:
                holding_days = time_config["params"]["days"]
        
        # 4. 获取股票数据（如果未提供）
        if hist_data is None:
            from app.services.data import DataService
            data_service = DataService()
            start_date = (datetime.now() - timedelta(days=holding_days * 2)).strftime('%Y%m%d')
            hist_data = await data_service.get_stock_history(stock_code, start_date=start_date)
        
        if hist_data is None or len(hist_data) < 50:
            return {"error": "数据不足"}
        
        # 5. 简化处理：只用第一个策略因子执行回测
        # （完整实现需要组合多个策略的信号）
        if strategy_factors:
            # 查找策略配置
            strategy_config = next((f for f in FactorMatrixV2.STRATEGY_FACTORS if f["code"] == strategy_factors[0]), None)
            if strategy_config:
                try:
                    # 从因子代码提取策略类型
                    # ma_5_20 -> ma_cross, macd_default -> macd
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
                        strategy_id = factor_code.split("_")[0]
                    
                    strategy = get_strategy(strategy_id)
                    if strategy:
                        strategy.params = strategy_config["params"]
                        df_with_signals = strategy.generate_signals(hist_data)
                        
                        engine = BacktestEngine()
                        results = engine.run_backtest(df_with_signals)
                        
                        return {
                            "experiment_code": experiment["experiment_code"],
                            "factor_combination": factor_combination,
                            "active_factors": active_factors,
                            "total_return": results.get("total_return"),
                            "sharpe_ratio": results.get("sharpe_ratio"),
                            "max_drawdown": results.get("max_drawdown"),
                            "win_rate": results.get("win_rate"),
                            "trade_count": results.get("trade_count")
                        }
                except Exception as e:
                    return {"error": str(e)}
        
        return {"error": "无策略因子"}
    
    # ==================== 因子贡献分析 ====================
    
    @staticmethod
    def analyze_factor_contribution() -> Dict:
        """
        分析每个因子的贡献度
        
        Returns:
            每个因子的贡献度统计
        """
        db: Session = SessionLocal()
        try:
            experiments = db.query(FactorExperiment).filter(
                FactorExperiment.total_return.isnot(None)
            ).all()
            
            if len(experiments) < 10:
                return {"error": "实验数量不足，至少需要10个实验"}
            
            # 统计每个因子
            factor_stats = {}
            
            for exp in experiments:
                combo = json.loads(exp.factor_combination) if isinstance(exp.factor_combination, str) else exp.factor_combination
                
                for factor_code, value in combo.items():
                    if factor_code not in factor_stats:
                        factor_stats[factor_code] = {
                            "active_returns": [],
                            "inactive_returns": []
                        }
                    
                    if value == 1:
                        factor_stats[factor_code]["active_returns"].append(exp.total_return)
                    else:
                        factor_stats[factor_code]["inactive_returns"].append(exp.total_return)
            
            # 计算贡献度
            contributions = []
            for factor_code, stats in factor_stats.items():
                active_avg = np.mean(stats["active_returns"]) if stats["active_returns"] else 0
                inactive_avg = np.mean(stats["inactive_returns"]) if stats["inactive_returns"] else 0
                
                contribution = active_avg - inactive_avg
                
                # 查找因子名称
                factor_def = next((f for f in FactorMatrixV2.ALL_FACTORS if f["code"] == factor_code), None)
                factor_name = factor_def["name"] if factor_def else factor_code
                
                contributions.append({
                    "factor_code": factor_code,
                    "factor_name": factor_name,
                    "active_count": len(stats["active_returns"]),
                    "inactive_count": len(stats["inactive_returns"]),
                    "avg_return_when_active": round(active_avg, 2),
                    "avg_return_when_inactive": round(inactive_avg, 2),
                    "contribution": round(contribution, 2),
                })
            
            # 按贡献度排序
            contributions.sort(key=lambda x: x["contribution"], reverse=True)
            
            return {
                "total_experiments": len(experiments),
                "contributions": contributions
            }
            
        except Exception as e:
            return {"error": str(e)}
        finally:
            db.close()
    
    # ==================== 批量实验 ====================
    
    @staticmethod
    async def run_batch_experiments(
        stock_code: str,
        max_experiments: int = 50
    ) -> Dict:
        """
        批量运行实验
        
        Args:
            stock_code: 股票代码
            max_experiments: 最大实验数
        
        Returns:
            批量实验结果
        """
        from app.services.data import DataService
        
        # 生成实验
        experiments = FactorMatrixV2.generate_experiments(
            stock_code=stock_code,
            max_combinations=max_experiments
        )
        
        print(f"[因子矩阵] 生成 {len(experiments)} 个实验")
        
        # 获取历史数据
        data_service = DataService()
        hist_data = await data_service.get_stock_history(stock_code)
        
        # 执行实验
        results = []
        for i, exp in enumerate(experiments):
            result = await FactorMatrixV2.run_experiment(exp, hist_data)
            if "error" not in result:
                results.append(result)
            
            if (i + 1) % 10 == 0:
                print(f"[因子矩阵] 进度: {i+1}/{len(experiments)}")
        
        # 保存结果
        db: Session = SessionLocal()
        try:
            for result in results:
                exp_record = FactorExperiment(
                    experiment_code=result["experiment_code"],
                    stock_code=stock_code,
                    factor_combination=json.dumps(result["factor_combination"]),
                    active_factor_count=len(result["active_factors"]),
                    total_return=result.get("total_return"),
                    sharpe_ratio=result.get("sharpe_ratio"),
                    max_drawdown=result.get("max_drawdown"),
                    win_rate=result.get("win_rate"),
                    trade_count=result.get("trade_count")
                )
                db.add(exp_record)
            
            db.commit()
            print(f"[因子矩阵] 保存 {len(results)} 个实验结果")
            
        except Exception as e:
            db.rollback()
            print(f"[因子矩阵] 保存失败: {e}")
        finally:
            db.close()
        
        return {
            "total_experiments": len(experiments),
            "successful": len(results),
            "results": results[:10]  # 返回前10个结果
        }