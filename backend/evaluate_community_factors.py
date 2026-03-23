"""
量化社区常见有效因子汇总

基于聚宽、米筐、优矿等平台的公开研究，以及学术论文验证的因子
"""
import sys
sys.path.insert(0, '/Users/hmh/.openclaw/workspace-pmofinclaw_agent/smart-quant/backend')

import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime
import os

from app.services.data import DataService


class CommunityFactors:
    """量化社区常见有效因子"""
    
    # ==================== 动量因子 ====================
    
    @staticmethod
    def momentum_1m(df):
        """1月动量（涨跌幅）"""
        return df['close'].pct_change(20)
    
    @staticmethod
    def momentum_3m(df):
        """3月动量"""
        return df['close'].pct_change(60)
    
    @staticmethod
    def momentum_6m(df):
        """6月动量"""
        return df['close'].pct_change(120)
    
    @staticmethod
    def momentum_12m(df):
        """12月动量（经典动量因子）"""
        return df['close'].pct_change(252)
    
    @staticmethod
    def momentum_skip1m(df):
        """12月动量跳过最近1月（学术经典）"""
        # 跳过最近一个月，避免短期反转
        return df['close'].shift(20) / df['close'].shift(252) - 1
    
    @staticmethod
    def rsi_momentum(df, period=14):
        """RSI动量"""
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
        rs = gain / (loss + 0.0001)
        return 100 - 100 / (1 + rs)
    
    @staticmethod
    def price_acceleration(df, window=20):
        """价格加速度（二阶动量）"""
        ret = df['close'].pct_change()
        return ret.diff(window)  # 收益率的变化
    
    # ==================== 反转因子 ====================
    
    @staticmethod
    def short_reversal_5d(df):
        """5日反转因子"""
        return -df['close'].pct_change(5)
    
    @staticmethod
    def short_reversal_10d(df):
        """10日反转因子"""
        return -df['close'].pct_change(10)
    
    @staticmethod
    def volume_reversal(df, window=20):
        """成交量反转"""
        vol_ma = df['volume'].rolling(window).mean()
        return -df['volume'] / vol_ma
    
    # ==================== 波动率因子 ====================
    
    @staticmethod
    def volatility_20d(df):
        """20日波动率"""
        return df['close'].pct_change().rolling(20).std() * np.sqrt(252)
    
    @staticmethod
    def volatility_60d(df):
        """60日波动率"""
        return df['close'].pct_change().rolling(60).std() * np.sqrt(252)
    
    @staticmethod
    def high_low_spread(df, window=20):
        """高低价差（流动性代理）"""
        spread = (df['high'] - df['low']) / df['close']
        return spread.rolling(window).mean()
    
    @staticmethod
    def amihud_illiquidity(df, window=20):
        """Amihud非流动性因子"""
        ret = df['close'].pct_change().abs()
        dollar_vol = df['volume'] * df['close']
        illiq = ret / (dollar_vol + 1)
        return illiq.rolling(window).mean()
    
    @staticmethod
    def range_volatility(df, window=20):
        """Parkinson波动率"""
        high_low = np.log(df['high'] / df['low'])
        return np.sqrt((high_low ** 2).rolling(window).mean() / (4 * np.log(2)))
    
    # ==================== 成交量因子 ====================
    
    @staticmethod
    def volume_ma_ratio(df, short=5, long=60):
        """成交量均线比率"""
        short_ma = df['volume'].rolling(short).mean()
        long_ma = df['volume'].rolling(long).mean()
        return short_ma / long_ma
    
    @staticmethod
    def turnover_rate(df, window=20):
        """换手率（需要流通股本数据，这里用代理）"""
        return df['volume'].rolling(window).sum() / df['volume'].rolling(252).mean() / window
    
    @staticmethod
    def obv_momentum(df, window=20):
        """OBV动量"""
        obv = (np.sign(df['close'].diff()) * df['volume']).cumsum()
        return obv.pct_change(window)
    
    @staticmethod
    def volume_price_trend(df, window=20):
        """量价趋势"""
        vpt = (df['close'].pct_change() * df['volume']).cumsum()
        return vpt.pct_change(window)
    
    # ==================== 技术形态因子 ====================
    
    @staticmethod
    def ma_cross(df, short=5, long=20):
        """均线交叉"""
        ma_short = df['close'].rolling(short).mean()
        ma_long = df['close'].rolling(long).mean()
        return (ma_short / ma_long - 1) * 100
    
    @staticmethod
    def bollinger_position(df, window=20):
        """布林带位置"""
        ma = df['close'].rolling(window).mean()
        std = df['close'].rolling(window).std()
        return (df['close'] - ma) / (2 * std + 0.0001)
    
    @staticmethod
    def macd_hist(df):
        """MACD柱状图"""
        ema12 = df['close'].ewm(span=12).mean()
        ema26 = df['close'].ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        return macd - signal
    
    @staticmethod
    def kdj_j(df):
        """KDJ的J值"""
        low_9 = df['low'].rolling(9).min()
        high_9 = df['high'].rolling(9).max()
        rsv = (df['close'] - low_9) / (high_9 - low_9 + 0.0001) * 100
        k = rsv.ewm(alpha=1/3).mean()
        d = k.ewm(alpha=1/3).mean()
        return 3 * k - 2 * d
    
    @staticmethod
    def williams_r(df, period=14):
        """威廉指标"""
        high = df['high'].rolling(period).max()
        low = df['low'].rolling(period).min()
        return -100 * (high - df['close']) / (high - low + 0.0001)
    
    @staticmethod
    def cci(df, period=20):
        """商品通道指数"""
        tp = (df['high'] + df['low'] + df['close']) / 3
        ma = tp.rolling(period).mean()
        md = tp.rolling(period).apply(lambda x: np.abs(x - x.mean()).mean())
        return (tp - ma) / (0.015 * md + 0.0001)
    
    # ==================== 流动性因子 ====================
    
    @staticmethod
    def close_to_open(df, window=20):
        """收盘价与开盘价关系"""
        return (df['close'] - df['open']) / df['close'].rolling(window).mean()
    
    @staticmethod
    def gap_factor(df, window=20):
        """跳空因子"""
        gap = (df['open'] - df['close'].shift(1)) / df['close'].shift(1)
        return gap.rolling(window).mean()
    
    @staticmethod
    def intraday_range(df, window=20):
        """日内波动幅度"""
        return ((df['high'] - df['low']) / df['open']).rolling(window).mean()
    
    # ==================== 情绪因子 ====================
    
    @staticmethod
    def adv_20(df):
        """20日平均成交额"""
        return (df['volume'] * df['close']).rolling(20).mean()
    
    @staticmethod
    def volume_spike(df, window=20):
        """成交量突增"""
        vol_ma = df['volume'].rolling(window).mean()
        vol_std = df['volume'].rolling(window).std()
        return (df['volume'] - vol_ma) / (vol_std + 0.0001)
    
    @staticmethod
    def price_volume_corr(df, window=20):
        """量价相关性"""
        return df['close'].rolling(window).corr(df['volume'])


def evaluate_factor(stock_code, factor_func, factor_name, prediction_period=20):
    """评估单个因子"""
    df = DataService.get_cached_data(stock_code, 'day')
    
    if df.empty or len(df) < 500:
        return None
    
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
        factor_values = factor_func(df)
        future_returns = df['close'].pct_change(prediction_period).shift(-prediction_period)
        
        valid_mask = ~(factor_values.isna() | future_returns.isna())
        factor_clean = factor_values[valid_mask]
        returns_clean = future_returns[valid_mask]
        
        if len(factor_clean) < 50:
            return None
        
        ic, pvalue = stats.spearmanr(factor_clean, returns_clean)
        
        # 滚动IC
        rolling_ic = []
        for i in range(252, len(factor_values) - prediction_period, 20):
            fv = factor_values.iloc[i-252:i]
            fr = future_returns.iloc[i-252:i]
            mask = ~(fv.isna() | fr.isna())
            if mask.sum() > 30:
                ic_w, _ = stats.spearmanr(fv[mask], fr[mask])
                rolling_ic.append(ic_w)
        
        ic_mean = np.mean(rolling_ic) if rolling_ic else 0
        ic_std = np.std(rolling_ic) if rolling_ic else 0
        ic_ir = ic_mean / ic_std if ic_std > 0 else 0
        ic_positive_rate = sum(1 for ic in rolling_ic if ic > 0) / len(rolling_ic) if rolling_ic else 0
        
        return {
            'stock_code': stock_code,
            'factor_name': factor_name,
            'ic': ic,
            'ic_mean': ic_mean,
            'ic_std': ic_std,
            'ic_ir': ic_ir,
            'ic_positive_rate': ic_positive_rate,
        }
        
    except Exception as e:
        return None


def main():
    print("=" * 70)
    print("量化社区常见有效因子评估")
    print("=" * 70)
    
    # 因子分类
    factor_categories = {
        "动量因子": {
            "momentum_1m": ("1月动量", CommunityFactors.momentum_1m),
            "momentum_3m": ("3月动量", CommunityFactors.momentum_3m),
            "momentum_6m": ("6月动量", CommunityFactors.momentum_6m),
            "momentum_12m": ("12月动量", CommunityFactors.momentum_12m),
            "momentum_skip1m": ("动量跳过1月", CommunityFactors.momentum_skip1m),
            "rsi_momentum": ("RSI动量", CommunityFactors.rsi_momentum),
        },
        "反转因子": {
            "short_reversal_5d": ("5日反转", CommunityFactors.short_reversal_5d),
            "short_reversal_10d": ("10日反转", CommunityFactors.short_reversal_10d),
            "volume_reversal": ("成交量反转", CommunityFactors.volume_reversal),
        },
        "波动率因子": {
            "volatility_20d": ("20日波动率", CommunityFactors.volatility_20d),
            "volatility_60d": ("60日波动率", CommunityFactors.volatility_60d),
            "high_low_spread": ("高低价差", CommunityFactors.high_low_spread),
            "amihud_illiquidity": ("Amihud非流动性", CommunityFactors.amihud_illiquidity),
        },
        "成交量因子": {
            "volume_ma_ratio": ("量比", CommunityFactors.volume_ma_ratio),
            "obv_momentum": ("OBV动量", CommunityFactors.obv_momentum),
            "volume_price_trend": ("量价趋势", CommunityFactors.volume_price_trend),
        },
        "技术形态因子": {
            "ma_cross": ("均线交叉", CommunityFactors.ma_cross),
            "bollinger_position": ("布林带位置", CommunityFactors.bollinger_position),
            "macd_hist": ("MACD柱", CommunityFactors.macd_hist),
            "kdj_j": ("KDJ-J值", CommunityFactors.kdj_j),
            "williams_r": ("威廉指标", CommunityFactors.williams_r),
            "cci": ("CCI", CommunityFactors.cci),
        },
        "流动情绪因子": {
            "close_to_open": ("收盘-开盘", CommunityFactors.close_to_open),
            "gap_factor": ("跳空因子", CommunityFactors.gap_factor),
            "intraday_range": ("日内波动", CommunityFactors.intraday_range),
            "volume_spike": ("量突增", CommunityFactors.volume_spike),
            "price_volume_corr": ("量价相关", CommunityFactors.price_volume_corr),
        },
    }
    
    test_stocks = [
        '600519', '601318', '000001', '600036', '300059',
        '300274', '002594', '601012', '600900', '000858',
    ]
    
    print(f"\n>>> 测试股票: {len(test_stocks)}只")
    print(f">>> 因子分类: {len(factor_categories)}类")
    print(f">>> 预测周期: 20天")
    
    all_results = []
    
    for category, factors in factor_categories.items():
        print(f"\n>>> 测试 {category} ({len(factors)}个因子)")
        print("-" * 50)
        
        for factor_key, (factor_name, factor_func) in factors.items():
            results = []
            for stock_code in test_stocks:
                result = evaluate_factor(stock_code, factor_func, factor_name)
                if result:
                    results.append(result)
            
            if results:
                avg_ic = np.mean([r['ic'] for r in results])
                avg_ic_ir = np.mean([r['ic_ir'] for r in results])
                avg_positive = np.mean([r['ic_positive_rate'] for r in results])
                
                status = '✅' if avg_ic > 0.03 else ('❌' if avg_ic < -0.03 else '➖')
                
                print(f"  {factor_name:<15} IC={avg_ic:>7.4f}  IR={avg_ic_ir:>5.2f}  胜率={avg_positive*100:>5.1f}%  {status}")
                
                all_results.append({
                    'category': category,
                    'factor_name': factor_name,
                    'avg_ic': avg_ic,
                    'avg_ic_ir': avg_ic_ir,
                    'avg_positive': avg_positive,
                })
    
    # 汇总
    print("\n" + "=" * 70)
    print("📊 因子有效性排名（按IC排序）")
    print("=" * 70)
    
    all_results.sort(key=lambda x: x['avg_ic'], reverse=True)
    
    print(f"\n{'排名':<4} {'因子':<18} {'类别':<12} {'平均IC':<10} {'IC_IR':<8} {'胜率%':<8} {'评级'}")
    print("-" * 75)
    
    for i, r in enumerate(all_results[:25], 1):
        if r['avg_ic'] > 0.04:
            rating = '⭐⭐⭐ 强'
        elif r['avg_ic'] > 0.02:
            rating = '⭐⭐ 中'
        elif r['avg_ic'] > 0:
            rating = '⭐ 弱'
        else:
            rating = '❌ 反'
        
        print(f"#{i:<3} {r['factor_name']:<18} {r['category']:<12} {r['avg_ic']:>8.4f}  {r['avg_ic_ir']:>6.2f}  {r['avg_positive']*100:>6.1f}%  {rating}")
    
    # 按类别汇总
    print("\n" + "=" * 70)
    print("📊 按类别汇总")
    print("=" * 70)
    
    category_stats = {}
    for r in all_results:
        cat = r['category']
        if cat not in category_stats:
            category_stats[cat] = []
        category_stats[cat].append(r['avg_ic'])
    
    print(f"\n{'类别':<15} {'因子数':<8} {'平均IC':<10} {'最佳因子'}")
    print("-" * 60)
    
    for cat, ics in sorted(category_stats.items(), key=lambda x: np.mean(x[1]), reverse=True):
        avg = np.mean(ics)
        best = max([r for r in all_results if r['category'] == cat], key=lambda x: x['avg_ic'])
        print(f"{cat:<15} {len(ics):<8} {avg:>8.4f}  {best['factor_name']}")
    
    # 结论
    print("\n" + "=" * 70)
    print("📊 结论与建议")
    print("=" * 70)
    
    strong_factors = [r for r in all_results if r['avg_ic'] > 0.04]
    medium_factors = [r for r in all_results if 0.02 < r['avg_ic'] <= 0.04]
    weak_factors = [r for r in all_results if 0 < r['avg_ic'] <= 0.02]
    
    print(f"\n  强有效因子 (IC>0.04): {len(strong_factors)}个")
    for f in strong_factors:
        print(f"    ✅ {f['factor_name']} ({f['category']}): IC={f['avg_ic']:.4f}")
    
    print(f"\n  中等有效因子 (0.02<IC≤0.04): {len(medium_factors)}个")
    for f in medium_factors[:5]:
        print(f"    ⚠️ {f['factor_name']} ({f['category']}): IC={f['avg_ic']:.4f}")
    
    print(f"\n  弱有效因子 (0<IC≤0.02): {len(weak_factors)}个")
    
    # 导出
    export_dir = "/Users/hmh/.openclaw/workspace-pmofinclaw_agent/smart-quant/backend/data_cache/exports"
    os.makedirs(export_dir, exist_ok=True)
    
    df_export = pd.DataFrame(all_results)
    file_name = f"community_factors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(export_dir, file_name)
    df_export.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 结果已导出: {file_path}")


if __name__ == '__main__':
    main()