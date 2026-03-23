"""
多股票全因子回测
自动获取股票列表，排除ST股和市值<50亿的股票
"""
import asyncio
import pandas as pd
import numpy as np
from datetime import date
from app.database import SessionLocal
from app.models.factor_matrix import BestFactorCombination
from app.models import Stock
from app.services.strategy import get_strategy
from app.services.backtest import BacktestEngine
from app.services.factor_matrix_v2_service import FactorMatrixV2
from app.services.data_sync import DataSyncService
from app.services.data import DataService
import json
import itertools
import tushare as ts

# Tushare Token
TS_TOKEN = "21cbce2d06540b12e14765850fee73749ccfb0cd7570f466bf7d8e45"

def get_qualified_stocks(min_market_cap=50):
    """
    获取符合条件的股票列表
    - 排除ST股
    - 排除市值小于min_market_cap亿的股票
    """
    print(f">>> 获取股票列表（排除ST股和市值<{min_market_cap}亿）")
    
    pro = ts.pro_api(TS_TOKEN)
    
    # 获取所有A股
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,market,list_date')
    
    # 排除ST股
    df = df[~df['name'].str.contains('ST', case=False, na=False)]
    df = df[~df['name'].str.contains('\*', case=False, na=False)]
    
    # 获取市值数据
    print("  获取市值数据...")
    try:
        # 获取每日基本面指标
        df_basic = pro.daily_basic(ts_code='', trade_date=date.today().strftime('%Y%m%d'), 
                                    fields='ts_code,total_mv', skip=True)
        
        if df_basic is not None and len(df_basic) > 0:
            # 市值单位是万元，转换为亿元
            df_basic['market_cap'] = df_basic['total_mv'] / 10000
            
            # 合并市值数据
            df = df.merge(df_basic[['ts_code', 'market_cap']], on='ts_code', how='left')
            
            # 过滤市值
            df = df[df['market_cap'] >= min_market_cap]
            print(f"  市值>{min_market_cap}亿: {len(df)}只")
    except Exception as e:
        print(f"  获取市值失败，跳过市值过滤: {e}")
    
    # 提取股票代码（去掉交易所后缀）
    codes = df['symbol'].tolist()
    
    print(f"  最终股票数: {len(codes)}只")
    
    return codes[:50]  # 限制最多50只，避免运行太久


# 股票列表（会被自动获取替换）
STOCK_LIST = []


async def sync_stock_data(codes):
    """同步股票数据"""
    print(f"\n>>> 同步数据 ({len(codes)}只股票)")
    sync_service = DataSyncService()
    
    success = 0
    for code in codes:
        try:
            df = await sync_service.sync_day_data_tushare(code, years=10)
            if len(df) > 0:
                print(f"  {code}: {len(df)}条")
                success += 1
            await asyncio.sleep(0.5)  # 避免Tushare限频
        except Exception as e:
            print(f"  {code}: 失败 - {str(e)[:50]}")
    
    print(f"\n同步完成: {success}/{len(codes)}")
    return success


def run_backtest_for_stock(stock_code, strategy_factors, mutex_groups):
    """对单只股票运行全因子回测"""
    
    # 读取缓存数据
    df = DataService.get_cached_data(stock_code, 'day')
    
    if df.empty:
        return [], 0
    
    # 计算基准收益
    benchmark_return = (df['收盘'].iloc[-1] - df['收盘'].iloc[0]) / df['收盘'].iloc[0] * 100
    
    # 准备数据
    df_test = df.rename(columns={
        '日期': 'date', '开盘': 'open', '最高': 'high', 
        '最低': 'low', '收盘': 'close', '成交量': 'volume'
    })
    
    # 生成有效策略组合
    def get_strategy_id(factor_code):
        if factor_code.startswith("ma_"): return "ma_cross"
        elif factor_code.startswith("macd"): return "macd"
        elif factor_code.startswith("rsi"): return "rsi"
        elif factor_code.startswith("kdj"): return "kdj"
        elif factor_code.startswith("boll"): return "boll"
        elif factor_code.startswith("cci"): return "cci"
        elif factor_code.startswith("wr"): return "wr"
        return factor_code.split("_")[0]
    
    def is_mutex(f1, f2):
        for group in mutex_groups.values():
            if f1 in group and f2 in group:
                return True
        return False
    
    valid_combos = []
    sf_codes = [sf["code"] for sf in strategy_factors]
    
    for sf in strategy_factors:
        valid_combos.append([sf["code"]])
    
    for r in range(2, len(strategy_factors) + 1):
        for combo in itertools.combinations(sf_codes, r):
            is_valid = True
            for f1, f2 in itertools.combinations(combo, 2):
                if is_mutex(f1, f2):
                    is_valid = False
                    break
            if is_valid:
                valid_combos.append(list(combo))
    
    # 回测
    engine = BacktestEngine()
    results = []
    
    for strat_combo in valid_combos:
        signals, names, codes = [], [], []
        
        for fc in strat_combo:
            sid = get_strategy_id(fc)
            sf = next((f for f in strategy_factors if f["code"] == fc), None)
            if sf:
                strategy = get_strategy(sid)
                strategy.params = sf["params"]
                df_signal = strategy.generate_signals(df_test.copy())
                signals.append(df_signal['signal'].values)
                names.append(sf["name"])
                codes.append(fc)
        
        if not signals:
            continue
        
        combined = np.clip(np.sum(signals, axis=0), -1, 1)
        df_test_copy = df_test.copy()
        df_test_copy['signal'] = combined
        
        result = engine.run_backtest(df_test_copy)
        
        # 过滤：正收益 + 跑赢基准
        if result.get("total_return") and result["total_return"] > 0 and result["total_return"] > benchmark_return:
            results.append({
                'stock_code': stock_code,
                'codes': codes,
                'desc': '+'.join(names) + '(OR)' if len(names) > 1 else names[0],
                'benchmark': benchmark_return,
                **result
            })
    
    # 排序取Top 3
    results.sort(key=lambda x: (x.get('annual_return', 0), x.get('sharpe_ratio', 0)), reverse=True)
    top3 = results[:3]
    
    # 重新设置排名
    for i, r in enumerate(top3, 1):
        r['rank'] = i
    
    return top3, len(results)


async def main():
    print("=" * 70)
    print("多股票全因子回测")
    print("（排除ST股和市值<50亿）")
    print("=" * 70)
    
    # 1. 获取符合条件的股票列表
    global STOCK_LIST
    STOCK_LIST = get_qualified_stocks(min_market_cap=50)
    
    if not STOCK_LIST:
        print("没有符合条件的股票")
        return
    
    # 2. 同步数据
    await sync_stock_data(STOCK_LIST)
    
    # 3. 获取因子配置
    strategy_factors = FactorMatrixV2.STRATEGY_FACTORS
    mutex_groups = FactorMatrixV2.MUTEX_GROUPS
    
    # 4. 对每只股票回测
    all_results = []
    stats = []
    
    print("\n>>> 开始回测...")
    for stock_code in STOCK_LIST:
        print(f"\n{stock_code}...", end=" ")
        
        top3, valid_count = run_backtest_for_stock(stock_code, strategy_factors, mutex_groups)
        
        if top3:
            all_results.extend(top3)
            benchmark = top3[0]['benchmark']
            print(f"基准 {benchmark:.1f}%, 有效策略 {valid_count}个, Top1年化 {top3[0].get('annual_return', 0):.1f}%")
        else:
            print("无有效策略")
        
        stats.append({
            'code': stock_code,
            'valid': valid_count,
            'top1_return': top3[0]['total_return'] if top3 else 0,
            'top1_annual': top3[0].get('annual_return', 0) if top3 else 0
        })
    
    # 5. 保存到数据库
    print(f"\n>>> 保存结果: {len(all_results)}条")
    
    db = SessionLocal()
    db.query(BestFactorCombination).delete()
    db.commit()
    
    for r in all_results:
        factor_combo = {code: 1 for code in r['codes']}
        record = BestFactorCombination(
            combination_code=f"{r['stock_code']}:{':'.join(r['codes'])}",
            stock_code=r['stock_code'],
            stock_name=r.get('stock_name', r['stock_code']),
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
            backtest_date=date.today()
        )
        db.add(record)
    
    db.commit()
    db.close()
    
    # 6. 输出汇总
    print("\n" + "=" * 70)
    print("回测完成")
    print("=" * 70)
    
    # 按年化收益排序
    stats.sort(key=lambda x: x['top1_annual'], reverse=True)
    
    print(f"\n{'排名':<4} {'股票':<10} {'有效策略':<8} {'Top1收益':<10} {'Top1年化':<10}")
    print("-" * 50)
    for i, s in enumerate(stats, 1):
        print(f"#{i:<3} {s['code']:<10} {s['valid']:<8} {s['top1_return']:>7.1f}% {s['top1_annual']:>7.1f}%")


if __name__ == '__main__':
    asyncio.run(main())