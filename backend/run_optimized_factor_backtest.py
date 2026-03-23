"""
优化因子组合策略回测

基于因子评估结果，使用最有效的因子组合：
1. Amihud非流动性 (IC=0.1642) - 权重40%
2. 威廉指标 WR (IC=0.0493) - 权重25%
3. KDJ-J值 (IC=0.0421) - 权重20%
4. 布林带位置 (IC=0.0341) - 权重15%

组合方式：因子标准化后加权得分
"""
import sys
sys.path.insert(0, '/Users/hmh/.openclaw/workspace-pmofinclaw_agent/smart-quant/backend')

import pandas as pd
import numpy as np
from datetime import datetime
import os

from app.services.stock_scoring_v3 import get_scoring_v3
from app.services.backtest import BacktestEngine
from app.services.data import DataService
from app.services.market_timing import (
    get_stock_industry, 
    get_industry_description,
    INDUSTRY_CONFIG
)


def get_stock_pool(top_n=50):
    """获取股票池"""
    print(f">>> 获取股票池 v3 (Top {top_n})")
    
    service = get_scoring_v3()
    result = service.generate_stock_pool(top_n=top_n, min_score=0)
    
    stocks = []
    for s in result['stocks']:
        industry = get_stock_industry(s['stock_code'])
        industry_desc = get_industry_description(s['stock_code'])
        
        stocks.append({
            'code': s['stock_code'],
            'name': s['stock_name'],
            'score': s['composite_score'],
            'industry': industry,
            'industry_desc': industry_desc
        })
    
    print(f"  获取到 {len(stocks)} 只股票")
    return stocks


class OptimizedFactorStrategy:
    """优化因子组合策略"""
    
    # 因子权重
    FACTOR_WEIGHTS = {
        'amihud': 0.40,      # Amihud非流动性
        'wr': 0.25,          # 威廉指标
        'kdj_j': 0.20,       # KDJ-J值
        'boll': 0.15,        # 布林带位置
    }
    
    # 交易参数
    BUY_THRESHOLD = 0.6     # 买入阈值
    SELL_THRESHOLD = -0.4   # 卖出阈值
    MIN_HOLD_DAYS = 5       # 最少持有天数
    STOP_LOSS = 0.06        # 止损6%
    TAKE_PROFIT = 0.12      # 止盈12%
    
    def __init__(self, stock_code=None):
        self.stock_code = stock_code
    
    # ==================== 因子计算 ====================
    
    def calc_amihud(self, df):
        """
        Amihud非流动性因子（IC=0.1642，最强因子）
        公式：abs(ret) / (volume * close)
        含义：价格对成交额的敏感度，值越大流动性越差
        """
        ret = df['close'].pct_change().abs()
        dollar_vol = df['volume'] * df['close']
        illiq = ret / (dollar_vol + 1)
        # 取20日均值
        return illiq.rolling(20).mean()
    
    def calc_williams_r(self, df, period=14):
        """
        威廉指标 WR（IC=0.0493）
        公式：-100 * (high - close) / (high - low)
        含义：超买超卖指标，<-80超卖，>-20超买
        """
        high = df['high'].rolling(period).max()
        low = df['low'].rolling(period).min()
        wr = -100 * (high - df['close']) / (high - low + 0.0001)
        # 反转：WR越小越超卖，应该买入，所以取负
        return -wr  # 反转后，值越大越应该买入
    
    def calc_kdj_j(self, df):
        """
        KDJ-J值（IC=0.0421）
        公式：J = 3*K - 2*D
        含义：比K、D更敏感，能提前反映趋势
        """
        low_9 = df['low'].rolling(9).min()
        high_9 = df['high'].rolling(9).max()
        rsv = (df['close'] - low_9) / (high_9 - low_9 + 0.0001) * 100
        k = rsv.ewm(alpha=1/3).mean()
        d = k.ewm(alpha=1/3).mean()
        j = 3 * k - 2 * d
        # J值在0-100之间，标准化
        return (j - 50) / 50  # 标准化到[-1, 1]
    
    def calc_bollinger_position(self, df, window=20):
        """
        布林带位置（IC=0.0341）
        公式：(close - ma) / (2 * std)
        含义：价格在布林带中的位置
        """
        ma = df['close'].rolling(window).mean()
        std = df['close'].rolling(window).std()
        boll = (df['close'] - ma) / (2 * std + 0.0001)
        # 布林带下轨为买入信号
        return -boll  # 反转：下轨（负值）-> 正值（买入）
    
    def calculate_all_factors(self, df):
        """计算所有因子"""
        df = df.copy()
        
        df['factor_amihud'] = self.calc_amihud(df)
        df['factor_wr'] = self.calc_williams_r(df)
        df['factor_kdj_j'] = self.calc_kdj_j(df)
        df['factor_boll'] = self.calc_bollinger_position(df)
        
        return df
    
    def normalize_factors(self, df, lookback=252):
        """
        因子标准化（滚动标准化）
        每个因子减去均值除以标准差
        """
        df = df.copy()
        
        factor_cols = ['factor_amihud', 'factor_wr', 'factor_kdj_j', 'factor_boll']
        
        for col in factor_cols:
            if col in df.columns:
                # 滚动标准化
                roll_mean = df[col].rolling(lookback, min_periods=60).mean()
                roll_std = df[col].rolling(lookback, min_periods=60).std()
                df[f'{col}_z'] = (df[col] - roll_mean) / (roll_std + 0.0001)
        
        return df
    
    def calculate_composite_score(self, df):
        """
        计算综合得分
        加权求和
        """
        df = df.copy()
        df['composite_score'] = 0.0
        
        # 加权求和
        weight_map = {
            'factor_amihud_z': self.FACTOR_WEIGHTS['amihud'],
            'factor_wr_z': self.FACTOR_WEIGHTS['wr'],
            'factor_kdj_j_z': self.FACTOR_WEIGHTS['kdj_j'],
            'factor_boll_z': self.FACTOR_WEIGHTS['boll'],
        }
        
        for col, weight in weight_map.items():
            if col in df.columns:
                df['composite_score'] += df[col].fillna(0) * weight
        
        # 再次标准化综合得分
        roll_mean = df['composite_score'].rolling(252, min_periods=60).mean()
        roll_std = df['composite_score'].rolling(252, min_periods=60).std()
        df['score_z'] = (df['composite_score'] - roll_mean) / (roll_std + 0.0001)
        
        return df
    
    def generate_signals(self, df):
        """
        生成交易信号
        """
        df = df.copy()
        df['signal'] = 0
        
        in_position = False
        entry_idx = 0
        entry_price = 0.0
        peak_price = 0.0
        
        for idx in range(252, len(df)):
            score_z = df['score_z'].iloc[idx]
            current_price = df['close'].iloc[idx]
            
            # 更新持仓最高价
            if in_position and current_price > peak_price:
                peak_price = current_price
            
            # 计算盈亏
            if in_position and entry_price > 0:
                pnl = (current_price - entry_price) / entry_price
                
                # 止损检查
                if pnl < -self.STOP_LOSS:
                    df.iloc[idx, df.columns.get_loc('signal')] = -1
                    in_position = False
                    continue
                
                # 止盈检查
                if pnl > self.TAKE_PROFIT:
                    df.iloc[idx, df.columns.get_loc('signal')] = -1
                    in_position = False
                    continue
            
            days_held = idx - entry_idx if in_position else 0
            
            # 买入信号
            if score_z > self.BUY_THRESHOLD and not in_position:
                df.iloc[idx, df.columns.get_loc('signal')] = 1
                in_position = True
                entry_idx = idx
                entry_price = current_price
                peak_price = current_price
            
            # 卖出信号
            elif score_z < self.SELL_THRESHOLD and in_position and days_held >= self.MIN_HOLD_DAYS:
                df.iloc[idx, df.columns.get_loc('signal')] = -1
                in_position = False
            
            # 持仓信号
            elif in_position:
                df.iloc[idx, df.columns.get_loc('signal')] = 1
        
        return df
    
    def run_strategy(self, df):
        """运行完整策略"""
        df = self.calculate_all_factors(df)
        df = self.normalize_factors(df)
        df = self.calculate_composite_score(df)
        df = self.generate_signals(df)
        return df


def run_backtest(stock_code, stock_name, industry, industry_desc):
    """运行回测"""
    
    # 读取数据
    df = DataService.get_cached_data(stock_code, 'day')
    
    if df.empty or len(df) < 500:
        return None, "数据不足"
    
    # 准备数据
    rename_map = {
        '日期': 'trade_date',
        '开盘': 'open',
        '最高': 'high',
        '最低': 'low',
        '收盘': 'close',
        '成交量': 'volume'
    }
    df = df.rename(columns=rename_map)
    
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in required_cols:
        if col not in df.columns:
            return None, f"缺少列: {col}"
    
    # 处理日期格式
    if 'trade_date' in df.columns:
        df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
    
    # 删除ts_code列（如果存在）
    if 'ts_code' in df.columns:
        df = df.drop(columns=['ts_code'])
    
    # 运行策略
    strategy = OptimizedFactorStrategy(stock_code)
    
    try:
        df_result = strategy.run_strategy(df)
        
        if df_result.empty or 'signal' not in df_result.columns:
            return None, "策略运行失败"
        
        # 回测
        engine = BacktestEngine()
        backtest_result = engine.run_backtest(df_result, signal_col='signal')
        
        backtest_result['stock_code'] = stock_code
        backtest_result['stock_name'] = stock_name
        backtest_result['industry'] = industry
        backtest_result['industry_desc'] = industry_desc
        
        return backtest_result, None
        
    except Exception as e:
        import traceback
        return None, f"策略异常: {str(e)[:100]}"


def main():
    print("=" * 70)
    print("优化因子组合策略回测")
    print("=" * 70)
    
    # 显示因子组合
    print("\n>>> 因子组合:")
    print("  1. Amihud非流动性 (IC=0.1642) - 权重40%")
    print("  2. 威廉指标 WR (IC=0.0493) - 权重25%")
    print("  3. KDJ-J值 (IC=0.0421) - 权重20%")
    print("  4. 布林带位置 (IC=0.0341) - 权重15%")
    
    print("\n>>> 交易参数:")
    print(f"  买入阈值: {OptimizedFactorStrategy.BUY_THRESHOLD}")
    print(f"  卖出阈值: {OptimizedFactorStrategy.SELL_THRESHOLD}")
    print(f"  止损: {OptimizedFactorStrategy.STOP_LOSS*100}%")
    print(f"  止盈: {OptimizedFactorStrategy.TAKE_PROFIT*100}%")
    
    # 获取股票池
    stocks = get_stock_pool(top_n=50)
    
    if not stocks:
        print("股票池为空")
        return
    
    # 回测
    results = []
    errors = []
    
    print(f"\n>>> 开始回测 ({len(stocks)}只股票)...")
    print("-" * 70)
    
    for i, stock in enumerate(stocks, 1):
        code = stock['code']
        name = stock['name']
        industry = stock['industry']
        industry_desc = stock['industry_desc']
        
        print(f"\n[{i}/{len(stocks)}] {code} {name} ({industry_desc})")
        
        result, error = run_backtest(code, name, industry, industry_desc)
        
        if error:
            print(f"  ⚠️  {error}")
            errors.append({'code': code, 'name': name, 'error': error})
            continue
        
        if result:
            results.append(result)
            
            annual = result.get('annual_return', 0)
            sharpe = result.get('sharpe_ratio', 0)
            max_dd = result.get('max_drawdown', 0)
            win_rate = result.get('win_rate', 0)
            benchmark = result.get('benchmark_return', 0)
            excess = result.get('excess_return', 0)
            
            print(f"  ✅ 基准: {benchmark:.1f}% | 年化: {annual:.1f}% | 超额: {excess:.1f}%")
            print(f"     夏普: {sharpe:.2f} | 回撤: {max_dd:.1f}% | 胜率: {win_rate:.1f}%")
    
    # 汇总报告
    print("\n" + "=" * 70)
    print("📊 优化因子组合回测汇总")
    print("=" * 70)
    
    if not results:
        print("无有效回测结果")
        return
    
    results.sort(key=lambda x: x.get('annual_return', 0), reverse=True)
    
    print(f"\n有效回测: {len(results)}只 | 失败: {len(errors)}只")
    
    # Top 20
    print(f"\n{'排名':<4} {'代码':<8} {'名称':<8} {'行业':<10} {'基准%':<8} {'年化%':<8} {'夏普':<6} {'回撤%':<8} {'胜率%'}")
    print("-" * 90)
    
    for i, r in enumerate(results[:20], 1):
        print(f"#{i:<3} {r['stock_code']:<8} {r['stock_name']:<8} {r['industry']:<10} "
              f"{r.get('benchmark_return', 0):>6.1f}% {r.get('annual_return', 0):>6.1f}% "
              f"{r.get('sharpe_ratio', 0):>5.2f} {r.get('max_drawdown', 0):>6.1f}% "
              f"{r.get('win_rate', 0):>5.1f}%")
    
    # 总体统计
    print("\n" + "=" * 70)
    print("📊 总体统计")
    print("=" * 70)
    
    all_annual = [r.get('annual_return', 0) for r in results]
    all_sharpe = [r.get('sharpe_ratio', 0) for r in results]
    all_dd = [r.get('max_drawdown', 0) for r in results]
    all_excess = [r.get('excess_return', 0) for r in results]
    
    beat_benchmark = sum(1 for e in all_excess if e > 0)
    positive_return = sum(1 for a in all_annual if a > 0)
    
    print(f"\n  平均年化收益: {np.mean(all_annual):.2f}%")
    print(f"  平均夏普比率: {np.mean(all_sharpe):.2f}")
    print(f"  平均最大回撤: {np.mean(all_dd):.2f}%")
    print(f"  正收益占比: {positive_return}/{len(results)} ({positive_return/len(results)*100:.1f}%)")
    print(f"  跑赢基准占比: {beat_benchmark}/{len(results)} ({beat_benchmark/len(results)*100:.1f}%)")
    
    # 与v1.7对比
    print("\n" + "=" * 70)
    print("📊 与v1.7对比")
    print("=" * 70)
    
    print(f"\n  {'版本':<15} {'平均年化':<12} {'平均夏普':<12} {'平均回撤':<12} {'跑赢基准'}")
    print(f"  {'v1.7':<15} {'4.05%':<12} {'0.12':<12} {'47.61%':<12} {'18.0%'}")
    print(f"  {'优化因子组合':<15} {np.mean(all_annual):.2f}%{'':<6} {np.mean(all_sharpe):.2f}{'':<8} {np.mean(all_dd):.2f}%{'':<6} {beat_benchmark/len(results)*100:.1f}%")
    
    # 导出
    export_dir = "/Users/hmh/.openclaw/workspace-pmofinclaw_agent/smart-quant/backend/data_cache/exports"
    os.makedirs(export_dir, exist_ok=True)
    
    export_data = []
    for r in results:
        export_data.append({
            '代码': r['stock_code'],
            '名称': r['stock_name'],
            '行业': r['industry'],
            '基准收益%': r.get('benchmark_return', 0),
            '总收益%': r.get('total_return', 0),
            '年化收益%': r.get('annual_return', 0),
            '超额收益%': r.get('excess_return', 0),
            '夏普比率': r.get('sharpe_ratio', 0),
            '最大回撤%': r.get('max_drawdown', 0),
            '胜率%': r.get('win_rate', 0),
            '交易次数': r.get('trade_count', 0),
        })
    
    df_export = pd.DataFrame(export_data)
    file_name = f"optimized_factor_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(export_dir, file_name)
    df_export.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 结果已导出: {file_path}")


if __name__ == '__main__':
    main()