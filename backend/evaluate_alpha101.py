"""
Alpha101 因子有效性评估

Alpha101是WorldQuant发布的101个经典因子公式，我们来测试其中几个代表性因子在A股市场的有效性
"""
import sys
sys.path.insert(0, '/Users/hmh/.openclaw/workspace-pmofinclaw_agent/smart-quant/backend')

import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime
import os

from app.services.data import DataService


class Alpha101Factors:
    """Alpha101 因子计算"""
    
    @staticmethod
    def alpha_001(df, window=20):
        """
        Alpha#1: (rank(Ts_ArgMax(Sign(((close - delay(close, 1)) * 0 - ((close - delay(close, 1)) * 0)), 20), 20)) * -1)
        简化版：过去20天收益率符号变化的排名
        """
        close = df['close']
        ret = close.pct_change()
        sign_change = np.sign(ret).diff().abs()
        return sign_change.rolling(window).sum().rank(ascending=True) * -1
    
    @staticmethod
    def alpha_006(df, window=10):
        """
        Alpha#6: -1 * correlation(open, volume, 10)
        开盘价与成交量的负相关
        """
        corr = df['open'].rolling(window).corr(df['volume'])
        return -1 * corr
    
    @staticmethod
    def alpha_010(df, window=20):
        """
        Alpha#10: rank(((close - open) / ((high - low) + 0.001)))
        日内涨幅相对于波动的排名
        """
        intraday_ret = (df['close'] - df['open']) / (df['high'] - df['low'] + 0.001)
        return intraday_ret.rolling(window).rank(pct=True)
    
    @staticmethod
    def alpha_014(df, window=5):
        """
        Alpha#14: -1 * rank(delta(return, 3)) * correlation(close, volume, 5)
        收益变化与量价相关的组合
        """
        ret = df['close'].pct_change()
        delta_ret = ret.diff(3)
        corr = df['close'].rolling(window).corr(df['volume'])
        return -1 * delta_ret.rank(pct=True) * corr
    
    @staticmethod
    def alpha_016(df, window=5):
        """
        Alpha#16: -1 * rank(covariance(rank(close), rank(volume), 5))
        收盘价排名与成交量排名的协方差
        """
        close_rank = df['close'].rolling(window).rank(pct=True)
        vol_rank = df['volume'].rolling(window).rank(pct=True)
        cov = close_rank.rolling(window).cov(vol_rank)
        return -1 * cov.rank(pct=True)
    
    @staticmethod
    def alpha_018(df, window=20):
        """
        Alpha#18: -1 * correlation(close, open, 10)
        收盘价与开盘价的负相关
        """
        return -1 * df['close'].rolling(window).corr(df['open'])
    
    @staticmethod
    def alpha_019(df, window=10):
        """
        Alpha#19: ((close - delay(close, 7)) / delay(close, 7) - 1) * 100
        7日收益率
        """
        return (df['close'] / df['close'].shift(7) - 1) * 100
    
    @staticmethod
    def alpha_020(df, window=6):
        """
        Alpha#20: ((close - delay(close, 6)) / delay(close, 6)) * volume
        6日收益率加权成交量
        """
        ret_6 = df['close'] / df['close'].shift(6) - 1
        return ret_6 * df['volume']
    
    @staticmethod
    def alpha_023(df, window=20):
        """
        Alpha#23: (((high - low) / ((close - open) + 0.001)) > 1) ? 1 : -1
        日内波动相对于涨幅
        """
        ratio = (df['high'] - df['low']) / (df['close'] - df['open'] + 0.001)
        return np.where(ratio > 1, 1, -1)
    
    @staticmethod
    def alpha_026(df, window=20):
        """
        Alpha#26: -1 * ts_max(correlation(ts_rank(volume, 5), ts_rank(high, 5), 5), 3)
        成交量排名与最高价排名的最大相关性
        """
        vol_rank = df['volume'].rolling(5).rank(pct=True)
        high_rank = df['high'].rolling(5).rank(pct=True)
        corr = vol_rank.rolling(5).corr(high_rank)
        return -1 * corr.rolling(3).max()
    
    @staticmethod
    def alpha_028(df, window=6):
        """
        Alpha#28: scale(((correlation adv20, low, 5) + ((high + low) / 2)) - close)
        平均成交额与低价的相关性
        """
        adv20 = df['volume'].rolling(20).mean() * df['close']
        corr = adv20.rolling(5).corr(df['low'])
        mid = (df['high'] + df['low']) / 2
        return (corr + mid - df['close']) / (corr + mid - df['close']).rolling(window).std()
    
    @staticmethod
    def alpha_053(df, window=5):
        """
        Alpha#53: close - delay(close, 5)
        5日价格变化
        """
        return df['close'] - df['close'].shift(5)
    
    @staticmethod
    def alpha_054(df, window=20):
        """
        Alpha#54: ((-1 * ((low - close) * (open^5))) / ((low - high) * (close^5))) * -1
        简化版：基于价格位置的因子
        """
        numerator = (df['low'] - df['close']) * (df['open'] ** 5)
        denominator = (df['low'] - df['high'] + 0.001) * (df['close'] ** 5 + 0.001)
        return (numerator / denominator) * -1
    
    @staticmethod
    def alpha_101(df, window=20):
        """
        Alpha#101: (close - open) / ((high - low) + 0.001)
        日内涨幅相对于波动
        """
        return (df['close'] - df['open']) / (df['high'] - df['low'] + 0.001)


def calculate_ic(factor_values, future_returns, periods=20):
    """
    计算IC（信息系数）
    
    Args:
        factor_values: 因子值序列
        future_returns: 未来收益率
        periods: 预测周期
    
    Returns:
        IC值、IC的p值
    """
    # 去除NaN
    valid_mask = ~(factor_values.isna() | future_returns.isna())
    factor_clean = factor_values[valid_mask]
    returns_clean = future_returns[valid_mask]
    
    if len(factor_clean) < 30:
        return 0, 1
    
    # Spearman相关系数
    ic, pvalue = stats.spearmanr(factor_clean, returns_clean)
    
    return ic, pvalue


def evaluate_alpha101(stock_code, alpha_func, alpha_name, prediction_period=20):
    """
    评估单个Alpha101因子在特定股票上的有效性
    
    Args:
        stock_code: 股票代码
        alpha_func: 因子计算函数
        alpha_name: 因子名称
        prediction_period: 预测周期（天）
    
    Returns:
        评估结果字典
    """
    # 读取数据
    df = DataService.get_cached_data(stock_code, 'day')
    
    if df.empty or len(df) < 500:
        return None
    
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
            return None
    
    try:
        # 计算因子
        factor_values = alpha_func(df)
        
        # 计算未来收益率
        future_returns = df['close'].pct_change(prediction_period).shift(-prediction_period)
        
        # 计算IC
        ic, pvalue = calculate_ic(factor_values, future_returns, prediction_period)
        
        # 分组测试：将因子值分为5组，计算各组平均收益
        factor_df = pd.DataFrame({
            'factor': factor_values,
            'return': future_returns
        }).dropna()
        
        if len(factor_df) < 50:
            return None
        
        # 分5组
        factor_df['group'] = pd.qcut(factor_df['factor'], 5, labels=['Q1', 'Q2', 'Q3', 'Q4', 'Q5'], duplicates='drop')
        
        group_returns = factor_df.groupby('group')['return'].mean()
        
        # 多空收益：Q5 - Q1
        if 'Q5' in group_returns.index and 'Q1' in group_returns.index:
            long_short = group_returns['Q5'] - group_returns['Q1']
        else:
            long_short = 0
        
        # IC的时间序列（滚动IC）
        rolling_ic = []
        for i in range(252, len(factor_values) - prediction_period, 20):
            ic_window, _ = calculate_ic(
                factor_values.iloc[i-252:i],
                future_returns.iloc[i-252:i],
                prediction_period
            )
            rolling_ic.append(ic_window)
        
        ic_mean = np.mean(rolling_ic) if rolling_ic else 0
        ic_std = np.std(rolling_ic) if rolling_ic else 0
        ic_ir = ic_mean / ic_std if ic_std > 0 else 0  # IC信息比率
        
        # 胜率：IC > 0的比例
        ic_positive_rate = sum(1 for ic in rolling_ic if ic > 0) / len(rolling_ic) if rolling_ic else 0
        
        return {
            'stock_code': stock_code,
            'alpha_name': alpha_name,
            'ic': ic,
            'ic_pvalue': pvalue,
            'ic_mean': ic_mean,
            'ic_std': ic_std,
            'ic_ir': ic_ir,
            'ic_positive_rate': ic_positive_rate,
            'long_short_return': long_short * 100,
            'group_returns': group_returns.to_dict() if hasattr(group_returns, 'to_dict') else {},
            'data_points': len(factor_df)
        }
        
    except Exception as e:
        return None


def main():
    print("=" * 70)
    print("Alpha101 因子有效性评估")
    print("=" * 70)
    
    # 定义要测试的因子
    alpha_factors = {
        'Alpha#001': Alpha101Factors.alpha_001,
        'Alpha#006': Alpha101Factors.alpha_006,
        'Alpha#010': Alpha101Factors.alpha_010,
        'Alpha#014': Alpha101Factors.alpha_014,
        'Alpha#016': Alpha101Factors.alpha_016,
        'Alpha#018': Alpha101Factors.alpha_018,
        'Alpha#019': Alpha101Factors.alpha_019,
        'Alpha#020': Alpha101Factors.alpha_020,
        'Alpha#023': Alpha101Factors.alpha_023,
        'Alpha#026': Alpha101Factors.alpha_026,
        'Alpha#053': Alpha101Factors.alpha_053,
        'Alpha#054': Alpha101Factors.alpha_054,
        'Alpha#101': Alpha101Factors.alpha_101,
    }
    
    # 测试股票池（选择有代表性的股票）
    test_stocks = [
        '600519',  # 贵州茅台
        '601318',  # 中国平安
        '000001',  # 平安银行
        '600036',  # 招商银行
        '300059',  # 东方财富
        '300274',  # 阳光电源
        '002594',  # 比亚迪
        '601012',  # 隆基绿能
        '600900',  # 长江电力
        '000858',  # 五粮液
    ]
    
    print(f"\n>>> 测试股票: {len(test_stocks)}只")
    print(f">>> 测试因子: {len(alpha_factors)}个")
    print(f">>> 预测周期: 20天")
    
    # 收集所有结果
    all_results = []
    
    print("\n>>> 开始评估...")
    print("-" * 70)
    
    for i, stock_code in enumerate(test_stocks, 1):
        print(f"\n[{i}/{len(test_stocks)}] {stock_code}")
        
        for alpha_name, alpha_func in alpha_factors.items():
            result = evaluate_alpha101(stock_code, alpha_func, alpha_name)
            
            if result:
                all_results.append(result)
                ic = result['ic']
                ir = result['ic_ir']
                status = '✅' if ic > 0.03 else ('❌' if ic < -0.03 else '➖')
                print(f"  {alpha_name}: IC={ic:.4f}, IR={ir:.2f} {status}")
    
    # 汇总分析
    print("\n" + "=" * 70)
    print("📊 因子有效性汇总")
    print("=" * 70)
    
    if not all_results:
        print("无有效结果")
        return
    
    # 按因子分组统计
    factor_stats = {}
    for r in all_results:
        name = r['alpha_name']
        if name not in factor_stats:
            factor_stats[name] = {
                'ics': [],
                'irs': [],
                'positive_rates': [],
                'long_short_returns': []
            }
        
        factor_stats[name]['ics'].append(r['ic'])
        factor_stats[name]['irs'].append(r['ic_ir'])
        factor_stats[name]['positive_rates'].append(r['ic_positive_rate'])
        factor_stats[name]['long_short_returns'].append(r['long_short_return'])
    
    # 输出因子排序
    print(f"\n{'因子':<12} {'平均IC':<10} {'IC显著性':<10} {'IC>0占比':<10} {'多空收益%':<10} {'评级'}")
    print("-" * 70)
    
    factor_summary = []
    for name, stats in factor_stats.items():
        avg_ic = np.mean(stats['ics'])
        avg_ir = np.mean(stats['irs'])
        avg_positive = np.mean(stats['positive_rates'])
        avg_ls = np.mean(stats['long_short_returns'])
        
        # 评级标准
        if avg_ic > 0.05 and avg_positive > 0.55:
            rating = '⭐⭐⭐ 有效'
        elif avg_ic > 0.02 and avg_positive > 0.50:
            rating = '⭐⭐ 弱有效'
        elif avg_ic > 0:
            rating = '⭐ 略有效'
        else:
            rating = '❌ 无效'
        
        factor_summary.append({
            'name': name,
            'avg_ic': avg_ic,
            'avg_ir': avg_ir,
            'avg_positive': avg_positive,
            'avg_ls': avg_ls,
            'rating': rating
        })
    
    # 按IC排序
    factor_summary.sort(key=lambda x: x['avg_ic'], reverse=True)
    
    for fs in factor_summary:
        print(f"{fs['name']:<12} {fs['avg_ic']:>8.4f}  {fs['avg_ir']:>8.2f}  {fs['avg_positive']*100:>7.1f}%  {fs['avg_ls']:>8.2f}%  {fs['rating']}")
    
    # 总结
    print("\n" + "=" * 70)
    print("📊 结论")
    print("=" * 70)
    
    effective_count = sum(1 for fs in factor_summary if '有效' in fs['rating'])
    print(f"\n  测试因子数: {len(factor_summary)}")
    print(f"  有效因子数: {effective_count}")
    print(f"  有效比例: {effective_count/len(factor_summary)*100:.1f}%")
    
    # Top 3有效因子
    top3 = factor_summary[:3]
    print(f"\n  Top 3 有效因子:")
    for i, fs in enumerate(top3, 1):
        print(f"    {i}. {fs['name']}: 平均IC={fs['avg_ic']:.4f}")
    
    # 无效因子
    invalid = [fs for fs in factor_summary if '无效' in fs['rating']]
    if invalid:
        print(f"\n  无效因子（避免使用）:")
        for fs in invalid:
            print(f"    - {fs['name']}: 平均IC={fs['avg_ic']:.4f}")
    
    # 导出结果
    export_dir = "/Users/hmh/.openclaw/workspace-pmofinclaw_agent/smart-quant/backend/data_cache/exports"
    os.makedirs(export_dir, exist_ok=True)
    
    df_export = pd.DataFrame(all_results)
    file_name = f"alpha101_evaluation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(export_dir, file_name)
    df_export.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 结果已导出: {file_path}")


if __name__ == '__main__':
    main()