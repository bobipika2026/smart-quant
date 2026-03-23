"""
股票池全因子回测
对评分最高的50只股票运行全因子回测，找出每只股票的最佳策略组合
"""
import sys
sys.path.insert(0, '/Users/hmh/.openclaw/workspace-pmofinclaw_agent/smart-quant/backend')

import pandas as pd
import numpy as np
import itertools
from datetime import datetime
import os

from app.services.stock_scoring_v3 import get_scoring_v3
from app.services.strategy import get_strategy, STRATEGY_REGISTRY
from app.services.backtest import BacktestEngine
from app.services.data import DataService


def get_stock_pool(top_n=50):
    """获取股票池"""
    print(f">>> 获取股票池 (Top {top_n})")
    
    service = get_scoring_v3()
    result = service.generate_stock_pool(top_n=top_n, min_score=0)
    
    stocks = []
    for s in result['stocks']:
        stocks.append({
            'code': s['stock_code'],
            'name': s['stock_name'],
            'score': s['composite_score'],
            'grade': s['grade'],
            'industry': s.get('industry', '')
        })
    
    print(f"  获取到 {len(stocks)} 只股票")
    return stocks


def get_strategy_configs():
    """获取策略配置"""
    # 核心策略（精简版）
    configs = [
        # 均线策略
        {"code": "ma_5_20", "name": "MA(5,20)", "strategy": "ma_cross", "params": {"short_period": 5, "long_period": 20}},
        {"code": "ma_10_30", "name": "MA(10,30)", "strategy": "ma_cross", "params": {"short_period": 10, "long_period": 30}},
        
        # MACD策略
        {"code": "macd_std", "name": "MACD", "strategy": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}},
        
        # KDJ策略
        {"code": "kdj_std", "name": "KDJ", "strategy": "kdj", "params": {"n": 9, "m1": 3, "m2": 3, "oversold": 20}},
        
        # RSI策略
        {"code": "rsi_14", "name": "RSI(14)", "strategy": "rsi", "params": {"period": 14, "oversold": 30, "overbought": 70}},
        
        # 布林带策略
        {"code": "boll_20", "name": "BOLL(20)", "strategy": "boll", "params": {"period": 20, "std_dev": 2.0}},
        
        # WR策略
        {"code": "wr_14", "name": "WR(14)", "strategy": "wr", "params": {"period": 14, "oversold": -80, "overbought": -20}},
        
        # CCI策略
        {"code": "cci_14", "name": "CCI(14)", "strategy": "cci", "params": {"period": 14, "oversold": -100, "overbought": 100}},
        
        # BIAS策略
        {"code": "bias_20", "name": "BIAS(20)", "strategy": "bias", "params": {"period": 20, "oversold": -10, "overbought": 10}},
    ]
    
    # 互斥组（同一组内策略只能选一个）
    mutex_groups = {
        "ma": ["ma_5_20", "ma_10_30"],
    }
    
    return configs, mutex_groups


def run_backtest_for_stock(stock_code, strategy_configs, mutex_groups):
    """对单只股票运行全因子回测"""
    
    # 读取缓存数据
    df = DataService.get_cached_data(stock_code, 'day')
    
    if df.empty or len(df) < 100:
        return [], 0, "数据不足"
    
    # 计算基准收益
    first_close = df['收盘'].iloc[0] if '收盘' in df.columns else df['close'].iloc[0]
    last_close = df['收盘'].iloc[-1] if '收盘' in df.columns else df['close'].iloc[-1]
    benchmark_return = (last_close - first_close) / first_close * 100
    
    # 准备数据
    col_map = {c.lower(): c for c in df.columns}
    df_test = df.copy()
    df_test.columns = [col_map.get(c.lower(), c) for c in df_test.columns]
    
    # 生成有效策略组合
    def is_mutex(f1, f2):
        for group in mutex_groups.values():
            if f1 in group and f2 in group:
                return True
        return False
    
    # 单策略组合
    valid_combos = [[cfg["code"]] for cfg in strategy_configs]
    
    # 双策略组合（OR逻辑）
    codes = [cfg["code"] for cfg in strategy_configs]
    for c1, c2 in itertools.combinations(codes, 2):
        if not is_mutex(c1, c2):
            valid_combos.append([c1, c2])
    
    # 回测
    engine = BacktestEngine()
    results = []
    
    for combo in valid_combos:
        try:
            signals = []
            names = []
            
            for code in combo:
                cfg = next((c for c in strategy_configs if c["code"] == code), None)
                if not cfg:
                    continue
                
                strategy = get_strategy(cfg["strategy"])
                strategy.params = cfg["params"]
                
                df_signal = strategy.generate_signals(df_test.copy())
                signals.append(df_signal['signal'].values)
                names.append(cfg["name"])
            
            if not signals:
                continue
            
            # OR逻辑：任一策略发出信号即执行
            combined = np.clip(np.sum(signals, axis=0), -1, 1)
            df_test_copy = df_test.copy()
            df_test_copy['signal'] = combined
            
            result = engine.run_backtest(df_test_copy)
            
            # 过滤条件：正收益 + 跑赢基准
            if result.get("total_return", 0) > 0 and result["total_return"] > benchmark_return:
                results.append({
                    'codes': combo,
                    'desc': ' OR '.join(names) if len(names) > 1 else names[0],
                    'benchmark': benchmark_return,
                    **result
                })
        except Exception as e:
            continue
    
    # 排序取Top 3
    results.sort(key=lambda x: (x.get('annual_return', 0), x.get('sharpe_ratio', 0)), reverse=True)
    top3 = results[:3]
    
    # 重新设置排名
    for i, r in enumerate(top3, 1):
        r['rank'] = i
    
    return top3, len(results), None


def main():
    print("=" * 70)
    print("股票池全因子回测")
    print("=" * 70)
    
    # 1. 获取股票池
    stocks = get_stock_pool(top_n=50)
    
    if not stocks:
        print("股票池为空")
        return
    
    # 2. 获取策略配置
    strategy_configs, mutex_groups = get_strategy_configs()
    print(f"\n>>> 策略配置: {len(strategy_configs)}个策略")
    for cfg in strategy_configs:
        print(f"  - {cfg['name']}")
    
    # 3. 对每只股票回测
    all_results = []
    stats = []
    
    print(f"\n>>> 开始回测 ({len(stocks)}只股票)...")
    print("-" * 70)
    
    for i, stock in enumerate(stocks, 1):
        code = stock['code']
        name = stock['name']
        score = stock['score']
        
        print(f"\n[{i}/{len(stocks)}] {code} {name} (评分: {score:.1f})")
        
        top3, valid_count, error = run_backtest_for_stock(code, strategy_configs, mutex_groups)
        
        if error:
            print(f"  ⚠️  {error}")
            stats.append({
                'code': code,
                'name': name,
                'score': score,
                'valid': 0,
                'error': error
            })
            continue
        
        if top3:
            all_results.extend(top3)
            benchmark = top3[0]['benchmark']
            best = top3[0]
            
            print(f"  ✅ 基准: {benchmark:.1f}% | 有效策略: {valid_count}个")
            print(f"     Top1: {best['desc']} | 年化: {best.get('annual_return', 0):.1f}% | 夏普: {best.get('sharpe_ratio', 0):.2f}")
            
            stats.append({
                'code': code,
                'name': name,
                'score': score,
                'valid': valid_count,
                'benchmark': benchmark,
                'top1_return': best['total_return'],
                'top1_annual': best.get('annual_return', 0),
                'top1_sharpe': best.get('sharpe_ratio', 0),
                'top1_strategy': best['desc'],
                'top1_max_dd': best.get('max_drawdown', 0),
                'error': None
            })
        else:
            print(f"  ❌ 无有效策略")
            stats.append({
                'code': code,
                'name': name,
                'score': score,
                'valid': 0,
                'error': '无有效策略'
            })
    
    # 4. 输出汇总报告
    print("\n" + "=" * 70)
    print("📊 回测汇总报告")
    print("=" * 70)
    
    # 过滤有效结果
    valid_stats = [s for s in stats if s.get('top1_annual') is not None]
    valid_stats.sort(key=lambda x: x['top1_annual'], reverse=True)
    
    print(f"\n股票池: {len(stocks)}只 | 有效回测: {len(valid_stats)}只")
    
    if valid_stats:
        print(f"\n{'排名':<4} {'代码':<8} {'名称':<8} {'评分':<6} {'基准%':<8} {'年化%':<8} {'夏普':<6} {'最佳策略'}")
        print("-" * 80)
        
        for i, s in enumerate(valid_stats[:20], 1):
            print(f"#{i:<3} {s['code']:<8} {s['name']:<8} {s['score']:<6.1f} {s['benchmark']:>6.1f}% {s['top1_annual']:>6.1f}% {s['top1_sharpe']:>5.2f}  {s['top1_strategy']}")
    
    # 5. 导出结果
    if valid_stats:
        export_dir = "/Users/hmh/.openclaw/workspace-pmofinclaw_agent/smart-quant/backend/data_cache/exports"
        os.makedirs(export_dir, exist_ok=True)
        
        df_export = pd.DataFrame(valid_stats)
        file_name = f"stock_pool_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        file_path = os.path.join(export_dir, file_name)
        df_export.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ 结果已导出: {file_path}")
    
    # 6. 统计摘要
    if valid_stats:
        avg_annual = np.mean([s['top1_annual'] for s in valid_stats])
        avg_sharpe = np.mean([s['top1_sharpe'] for s in valid_stats])
        avg_max_dd = np.mean([s['top1_max_dd'] for s in valid_stats])
        
        print(f"\n📈 统计摘要:")
        print(f"   平均年化收益: {avg_annual:.2f}%")
        print(f"   平均夏普比率: {avg_sharpe:.2f}")
        print(f"   平均最大回撤: {avg_max_dd:.2f}%")
        
        # 策略分布
        strategy_count = {}
        for s in valid_stats:
            st = s['top1_strategy']
            strategy_count[st] = strategy_count.get(st, 0) + 1
        
        print(f"\n🎯 最佳策略分布:")
        for st, cnt in sorted(strategy_count.items(), key=lambda x: -x[1]):
            print(f"   {st}: {cnt}只")


if __name__ == '__main__':
    main()