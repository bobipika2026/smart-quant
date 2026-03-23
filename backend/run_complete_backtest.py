#!/usr/bin/env python3
"""
补充回测缺失数据的股票
使用正确的BacktestEngine
"""
import asyncio
import pandas as pd
import numpy as np
from datetime import date
import sqlite3
from app.database import SessionLocal
from app.models.factor_matrix import BestFactorCombination
from app.services.strategy import get_strategy
from app.services.backtest import BacktestEngine
from app.services.factor_matrix_v2_service import FactorMatrixV2
from app.services.data import DataService
import json
import functools

print = functools.partial(print, flush=True)

def get_stocks_needing_backtest():
    """获取需要补充回测的股票"""
    conn = sqlite3.connect('smart_quant.db')
    cursor = conn.cursor()
    
    # 已有完整数据的股票
    cursor.execute("""
        SELECT DISTINCT stock_code 
        FROM best_factor_combinations 
        WHERE is_active = 1 AND annual_return IS NOT NULL
    """)
    complete = set(row[0] for row in cursor.fetchall())
    
    # 筛选列表中的所有股票
    cursor.execute("SELECT code FROM stocks")
    all_stocks = set(row[0] for row in cursor.fetchall())
    
    conn.close()
    
    # 需要回测的股票
    return list(all_stocks - complete)

def get_cached_stocks():
    """获取已缓存日线的股票"""
    import os
    cache_dir = "data_cache/day"
    if not os.path.exists(cache_dir):
        return set()
    return set(f.replace("_day.csv", "") for f in os.listdir(cache_dir) if f.endswith("_day.csv"))

def get_strategy_id(factor_code):
    """从因子代码获取策略ID"""
    mapping = {
        'ma_5_20': 'ma_cross', 'ma_5_30': 'ma_cross', 'ma_10_20': 'ma_cross', 'ma_10_30': 'ma_cross',
        'macd_default': 'macd', 'macd_fast': 'macd',
        'rsi_14_70': 'rsi', 'rsi_14_80': 'rsi',
        'kdj_default': 'kdj',
        'boll_20_2': 'boll',
        'cci_14': 'cci', 'wr_14': 'wr'
    }
    return mapping.get(factor_code, 'ma_cross')

def run_backtest_for_stock(stock_code, strategy_factors, mutex_groups):
    """对单只股票进行回测"""
    data_service = DataService()
    engine = BacktestEngine()
    
    # 获取数据
    df = data_service.get_cached_data(stock_code, freq='day')
    if df is None or len(df) < 100:
        return [], 0
    
    # 计算基准收益
    benchmark_return = (df['收盘'].iloc[-1] / df['收盘'].iloc[0] - 1) * 100
    
    # 策略组合
    results = []
    
    # 单策略
    for sf in strategy_factors:
        sid = get_strategy_id(sf["code"])
        strategy = get_strategy(sid)
        strategy.params = sf["params"]
        df_signal = strategy.generate_signals(df.copy())
        
        result = engine.run_backtest(df_signal)
        
        if result.get("total_return", 0) > 0 and result["total_return"] > benchmark_return:
            results.append({
                'stock_code': stock_code,
                'codes': [sf["code"]],
                'desc': sf["name"],
                'benchmark': benchmark_return,
                **result
            })
    
    # 双策略组合
    for i, sf1 in enumerate(strategy_factors):
        for sf2 in strategy_factors[i+1:]:
            # 检查互斥
            is_mutex = False
            for group_factors in mutex_groups.values():
                if sf1["code"] in group_factors and sf2["code"] in group_factors:
                    is_mutex = True
                    break
            
            if is_mutex:
                continue
            
            # 组合回测
            signals = []
            names = []
            codes = []
            
            for sf in [sf1, sf2]:
                sid = get_strategy_id(sf["code"])
                strategy = get_strategy(sid)
                strategy.params = sf["params"]
                df_signal = strategy.generate_signals(df.copy())
                signals.append(df_signal['signal'].values)
                names.append(sf["name"])
                codes.append(sf["code"])
            
            # OR组合
            combined = np.clip(np.sum(signals, axis=0), -1, 1)
            df_test = df.copy()
            df_test['signal'] = combined
            
            result = engine.run_backtest(df_test)
            
            if result.get("total_return", 0) > 0 and result["total_return"] > benchmark_return:
                results.append({
                    'stock_code': stock_code,
                    'codes': codes,
                    'desc': '+'.join(names) + '(OR)',
                    'benchmark': benchmark_return,
                    **result
                })
    
    # 排序取Top 3
    results.sort(key=lambda x: (x.get('annual_return', 0), x.get('sharpe_ratio', 0)), reverse=True)
    top3 = results[:3]
    
    for i, r in enumerate(top3, 1):
        r['rank'] = i
    
    return top3, len(results)

async def main():
    print("=" * 70)
    print("补充回测缺失数据的股票")
    print("使用正确的BacktestEngine")
    print("=" * 70)
    
    # 获取需要回测的股票
    pending = get_stocks_needing_backtest()
    cached = get_cached_stocks()
    pending = [s for s in pending if s in cached]
    
    print(f"\n需要回测: {len(pending)} 只股票")
    
    if not pending:
        print("✅ 所有股票已有完整数据")
        return
    
    # 获取因子配置
    strategy_factors = FactorMatrixV2.STRATEGY_FACTORS
    mutex_groups = FactorMatrixV2.MUTEX_GROUPS
    
    # 回测
    all_results = []
    success, failed = 0, 0
    db = SessionLocal()  # 提前打开数据库连接
    
    for i, stock_code in enumerate(pending):
        print(f"\n[{i+1}/{len(pending)}] {stock_code}", end=" ")
        
        try:
            top3, valid_count = run_backtest_for_stock(stock_code, strategy_factors, mutex_groups)
            
            if top3:
                # 立即保存到数据库
                for r in top3:
                    factor_combo = {code: 1 for code in r['codes']}
                    record = BestFactorCombination(
                        combination_code=f"{r['stock_code']}:{':'.join(r['codes'])}",
                        stock_code=r['stock_code'],
                        stock_name=r['stock_code'],
                        strategy_desc=r['desc'],
                        factor_combination=json.dumps(factor_combo),
                        rank_in_stock=r['rank'],
                        total_return=r['total_return'],
                        annual_return=r.get('annual_return'),
                        benchmark_return=r.get('benchmark'),
                        sharpe_ratio=r['sharpe_ratio'],
                        max_drawdown=r.get('max_drawdown'),
                        win_rate=r.get('win_rate'),
                        profit_loss_ratio=r.get('profit_loss_ratio'),
                        trade_count=r['trade_count'],
                        composite_score=r['composite_score'],
                        holding_period='day_5y',
                        backtest_date=date.today(),
                        is_active=True
                    )
                    db.add(record)
                db.commit()
                
                success += 1
                print(f"✅ 有效策略 {valid_count}个, Top1年化 {top3[0].get('annual_return', 0):.1f}%")
            else:
                failed += 1
                print("⚠️ 无有效策略")
        except Exception as e:
            failed += 1
            print(f"❌ {e}")
            db.rollback()
    
    db.close()
    
    print(f"\n{'='*70}")
    print(f"回测完成! 成功: {success}, 失败: {failed}")
    print("=" * 70)

if __name__ == '__main__':
    asyncio.run(main())