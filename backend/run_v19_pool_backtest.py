"""
股票池 v1.9 策略回测

v1.9 平衡版：在v1.7收益和v1.8风险控制之间找到平衡

优化策略：
1. 基础仓位：75%（介于v1.7的85%和v1.8的60%之间）
2. 最大仓位：80%（v1.7:95%, v1.8:70%）
3. 动态回撤控制：更温和（回撤>20%才降仓）
4. 止损优化：跟踪止损而非固定止损
5. 信号阈值：适度提高但不激进
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
    get_industry_factor_weights,
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
            'grade': s['grade'],
            'industry': industry,
            'industry_desc': industry_desc
        })
    
    print(f"  获取到 {len(stocks)} 只股票")
    return stocks


# v1.9 配置
V19_CONFIG = {
    # 仓位控制
    'base_position': 0.75,      # 基础仓位75%
    'max_position': 0.80,       # 最大仓位80%
    'min_position': 0.0,
    
    # 市场环境仓位调整
    'regime_position': {
        'bull': 0.80,           # 牛市80%
        'bear': 0.25,           # 熊市25%
        'sideways': 0.45,       # 震荡市45%
    },
    
    # 动态回撤控制（更温和）
    'drawdown_control': {
        'level_1': 0.15,        # 回撤15%，仓位×0.85
        'level_2': 0.22,        # 回撤22%，仓位×0.6
        'level_3': 0.30,        # 回撤30%，仓位×0.3
    },
    
    # 跟踪止损（更智能）
    'trailing_stop': {
        'enabled': True,
        'activation': 0.08,     # 盈利8%后启动跟踪止损
        'distance': 0.05,       # 回撤5%触发止损
    },
    
    # 信号阈值
    'signal_threshold': {
        'buy': 0.65,            # 买入阈值
        'sell': -0.55,          # 卖出阈值
        'vote_min': 3,          # 最少因子确认数
    },
    
    # 止损止盈
    'stop_loss': 0.06,
    'take_profit': 0.12,
}


class V19Strategy:
    """v1.9 策略：平衡版"""
    
    def __init__(self, stock_code=None):
        self.stock_code = stock_code
        if stock_code:
            self.industry = get_stock_industry(stock_code)
            self.industry_weights = get_industry_factor_weights(stock_code)
            self.industry_desc = get_industry_description(stock_code)
        else:
            self.industry = 'default'
            self.industry_weights = None
            self.industry_desc = '默认配置'
    
    def calculate_factors(self, df):
        """计算因子"""
        result = df.copy()
        
        low_14 = result['low'].rolling(14).min()
        high_14 = result['high'].rolling(14).max()
        rsv = (result['close'] - low_14) / (high_14 - low_14 + 0.0001) * 100
        result['KDJ'] = rsv.ewm(alpha=1/3).mean().ewm(alpha=1/3).mean()
        
        ma20 = result['close'].rolling(20).mean()
        std20 = result['close'].rolling(20).std()
        result['BOLL'] = (result['close'] - ma20) / (2 * std20 + 0.0001)
        
        result['MOM'] = result['close'].pct_change(63)
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        result['ROE'] = result['close'].pct_change().rolling(252).mean() / \
                       (result['close'].pct_change().rolling(252).std() + 0.0001)
        result['TURN'] = result['volume'].rolling(5).mean() / \
                        (result['volume'].rolling(60).mean() + 0.0001) - 1
        result['EP'] = 1 / (result['close'] + 0.0001) * 100
        result['BP'] = 1 / (result['close'] + 0.0001) * 50
        
        return result
    
    def detect_regime_rolling(self, df):
        """滚动识别市场环境"""
        result = df.copy()
        
        result['ma_short'] = result['close'].rolling(20).mean()
        result['ma_long'] = result['close'].rolling(60).mean()
        result['trend'] = (result['ma_short'] / result['ma_long'] - 1) * 100
        result['ret_60'] = result['close'].pct_change(60) * 100
        
        result['regime'] = 'sideways'
        result.loc[(result['trend'] > 2.0) & (result['ret_60'] > 5.0), 'regime'] = 'bull'
        result.loc[(result['trend'] < -2.0) & (result['ret_60'] < -5.0), 'regime'] = 'bear'
        
        return result
    
    def get_factor_weights(self, regime):
        """获取因子权重"""
        base_weights = {
            'bull': {'MOM': 0.35, 'KDJ': 0.20, 'BOLL': 0.15, 'TURN': 0.15, 'ROE': 0.15},
            'bear': {'LEV': 0.30, 'ROE': 0.25, 'EP': 0.25, 'BP': 0.20},
            'sideways': {'KDJ': 0.25, 'BOLL': 0.25, 'TURN': 0.20, 'MOM': 0.15, 'ROE': 0.15}
        }
        
        weights = base_weights.get(regime, base_weights['sideways']).copy()
        
        if self.industry_weights:
            for factor in weights:
                base_w = weights.get(factor, 0)
                ind_w = self.industry_weights.get(factor, 0)
                weights[factor] = base_w * 0.6 + ind_w * 0.4
        
        return weights
    
    def calculate_score(self, df):
        """计算综合得分"""
        result = df.copy()
        result['score'] = 0.0
        
        for idx in range(252, len(result)):
            regime = result['regime'].iloc[idx]
            weights = self.get_factor_weights(regime)
            
            score = 0.0
            for factor, weight in weights.items():
                if factor in result.columns:
                    factor_val = result[factor].iloc[idx]
                    roll_mean = result[factor].iloc[idx-252:idx].mean()
                    roll_std = result[factor].iloc[idx-252:idx].std()
                    if roll_std > 0 and not np.isnan(factor_val):
                        factor_std = (factor_val - roll_mean) / roll_std
                        score += factor_std * weight
            
            result.iloc[idx, result.columns.get_loc('score')] = score
        
        result['score_z'] = result['score'].rolling(252).apply(
            lambda x: (x.iloc[-1] - x.mean()) / (x.std() + 0.0001) if len(x) > 0 and x.std() > 0 else 0,
            raw=False
        )
        
        return result
    
    def generate_signal(self, df):
        """生成交易信号（v1.9：平衡版）"""
        result = df.copy()
        result['signal'] = 0
        result['vote'] = 0
        
        # 多因子投票
        for factor in ['KDJ', 'BOLL', 'MOM', 'LEV', 'EP', 'BP', 'ROE']:
            if factor in result.columns:
                for idx in range(252, len(result)):
                    factor_val = result[factor].iloc[idx]
                    roll_mean = result[factor].iloc[idx-252:idx].mean()
                    roll_std = result[factor].iloc[idx-252:idx].std()
                    if roll_std > 0 and not np.isnan(factor_val):
                        factor_std = (factor_val - roll_mean) / roll_std
                        if factor_std > 0.3:
                            result.iloc[idx, result.columns.get_loc('vote')] += 1
                        elif factor_std < -0.3:
                            result.iloc[idx, result.columns.get_loc('vote')] -= 1
        
        # 布林带
        result['boll_upper'] = result['close'].rolling(20).mean() + 2 * result['close'].rolling(20).std()
        result['boll_lower'] = result['close'].rolling(20).mean() - 2 * result['close'].rolling(20).std()
        
        # 持仓状态和跟踪止损
        in_position = False
        entry_idx = 0
        entry_price = 0.0
        peak_price = 0.0
        trailing_stop_active = False
        trailing_stop_price = 0.0
        
        for idx in range(252, len(result)):
            regime = result['regime'].iloc[idx]
            
            # 获取市场环境对应的基础仓位
            regime_position = V19_CONFIG['regime_position'].get(regime, 0.50)
            
            buy_threshold = V19_CONFIG['signal_threshold']['buy']
            sell_threshold = V19_CONFIG['signal_threshold']['sell']
            vote_min = V19_CONFIG['signal_threshold']['vote_min']
            
            score_z = result['score_z'].iloc[idx]
            vote = result['vote'].iloc[idx]
            current_price = result['close'].iloc[idx]
            
            # 更新持仓期间最高价
            if in_position and current_price > peak_price:
                peak_price = current_price
                # 跟踪止损更新
                if trailing_stop_active:
                    trailing_stop_price = peak_price * (1 - V19_CONFIG['trailing_stop']['distance'])
            
            # 计算当前回撤
            current_drawdown = 0.0
            if in_position and peak_price > 0:
                current_drawdown = (peak_price - current_price) / peak_price
            
            # 动态回撤控制（更温和）
            drawdown_adj = 1.0
            if current_drawdown >= 0.30:
                drawdown_adj = 0.3
            elif current_drawdown >= 0.22:
                drawdown_adj = 0.6
            elif current_drawdown >= 0.15:
                drawdown_adj = 0.85
            
            # 跟踪止损逻辑
            if in_position:
                profit_pct = (current_price - entry_price) / entry_price
                
                # 盈利超过阈值后启动跟踪止损
                if profit_pct >= V19_CONFIG['trailing_stop']['activation']:
                    trailing_stop_active = True
                    if trailing_stop_price == 0:
                        trailing_stop_price = peak_price * (1 - V19_CONFIG['trailing_stop']['distance'])
                
                # 跟踪止损触发
                if trailing_stop_active and current_price < trailing_stop_price:
                    result.iloc[idx, result.columns.get_loc('signal')] = -1
                    in_position = False
                    trailing_stop_active = False
                    trailing_stop_price = 0
                    continue
                
                # 固定止损
                if profit_pct < -V19_CONFIG['stop_loss']:
                    result.iloc[idx, result.columns.get_loc('signal')] = -1
                    in_position = False
                    trailing_stop_active = False
                    trailing_stop_price = 0
                    continue
            
            days_in_position = idx - entry_idx if in_position else 0
            can_sell = not in_position or days_in_position >= 8
            
            # 震荡市区间交易
            if regime == 'sideways':
                boll_upper = result['boll_upper'].iloc[idx]
                boll_lower = result['boll_lower'].iloc[idx]
                
                if not in_position and current_price < boll_lower * 1.02 and vote >= 2:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
                    in_position = True
                    entry_idx = idx
                    entry_price = current_price
                    peak_price = current_price
                elif in_position and current_price > boll_upper * 0.98 and can_sell:
                    result.iloc[idx, result.columns.get_loc('signal')] = -1
                    in_position = False
                    trailing_stop_active = False
                    trailing_stop_price = 0
                elif in_position:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
            else:
                # 普通模式
                if score_z > buy_threshold and vote >= vote_min and not in_position:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
                    in_position = True
                    entry_idx = idx
                    entry_price = current_price
                    peak_price = current_price
                elif score_z < sell_threshold and vote <= -vote_min and in_position and can_sell:
                    result.iloc[idx, result.columns.get_loc('signal')] = -1
                    in_position = False
                    trailing_stop_active = False
                    trailing_stop_price = 0
                elif in_position:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
        
        return result
    
    def run_strategy(self, df):
        """运行完整策略"""
        df = self.calculate_factors(df)
        df = self.detect_regime_rolling(df)
        df = self.calculate_score(df)
        df = self.generate_signal(df)
        return df


def run_v19_backtest(stock_code, stock_name, industry, industry_desc):
    """运行 v1.9 策略回测"""
    
    df = DataService.get_cached_data(stock_code, 'day')
    
    if df.empty or len(df) < 500:
        return None, "数据不足"
    
    df_test = df.copy()
    
    rename_map = {
        '日期': 'trade_date',
        '开盘': 'open',
        '最高': 'high',
        '最低': 'low',
        '收盘': 'close',
        '成交量': 'volume'
    }
    df_test = df_test.rename(columns=rename_map)
    
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in required_cols:
        if col not in df_test.columns:
            return None, f"缺少列: {col}"
    
    if 'trade_date' in df_test.columns:
        df_test['trade_date'] = pd.to_datetime(df_test['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
    
    if 'ts_code' in df_test.columns:
        df_test = df_test.drop(columns=['ts_code'])
    
    strategy = V19Strategy(stock_code)
    
    try:
        df_result = strategy.run_strategy(df_test)
        
        if df_result.empty or 'signal' not in df_result.columns:
            return None, "策略运行失败"
        
        engine = BacktestEngine()
        backtest_result = engine.run_backtest(df_result, signal_col='signal')
        
        backtest_result['stock_code'] = stock_code
        backtest_result['stock_name'] = stock_name
        backtest_result['industry'] = industry
        backtest_result['industry_desc'] = industry_desc
        
        if 'regime' in df_result.columns:
            regime_counts = df_result['regime'].value_counts().to_dict()
            backtest_result['regime_distribution'] = regime_counts
        
        return backtest_result, None
        
    except Exception as e:
        import traceback
        return None, f"策略异常: {str(e)[:100]}"


def main():
    print("=" * 70)
    print("股票池 v1.9 策略回测")
    print("（平衡版：收益与风险的最优平衡）")
    print("=" * 70)
    
    # 显示v1.9核心改进
    print("\n>>> v1.9 平衡优化:")
    print("  1. 基础仓位: 75%（v1.7:85%, v1.8:60%）")
    print("  2. 最大仓位: 80%（v1.7:95%, v1.8:70%）")
    print("  3. 动态回撤控制: 回撤>22%时仓位×0.6")
    print("  4. 跟踪止损: 盈利8%后启动，回撤5%触发")
    print("  5. 信号阈值: 适中（买入0.65, 卖出-0.55）")
    
    stocks = get_stock_pool(top_n=50)
    
    if not stocks:
        print("股票池为空")
        return
    
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
        
        result, error = run_v19_backtest(code, name, industry, industry_desc)
        
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
    
    print("\n" + "=" * 70)
    print("📊 v1.9 策略回测汇总报告")
    print("=" * 70)
    
    if not results:
        print("无有效回测结果")
        return
    
    results.sort(key=lambda x: x.get('annual_return', 0), reverse=True)
    
    print(f"\n有效回测: {len(results)}只 | 失败: {len(errors)}只")
    
    print(f"\n{'排名':<4} {'代码':<8} {'名称':<8} {'行业':<10} {'基准%':<8} {'年化%':<8} {'夏普':<6} {'回撤%':<8} {'胜率%'}")
    print("-" * 90)
    
    for i, r in enumerate(results[:20], 1):
        print(f"#{i:<3} {r['stock_code']:<8} {r['stock_name']:<8} {r['industry']:<10} "
              f"{r.get('benchmark_return', 0):>6.1f}% {r.get('annual_return', 0):>6.1f}% "
              f"{r.get('sharpe_ratio', 0):>5.2f} {r.get('max_drawdown', 0):>6.1f}% "
              f"{r.get('win_rate', 0):>5.1f}%")
    
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
    
    # 版本对比
    print("\n" + "=" * 70)
    print("📊 版本对比")
    print("=" * 70)
    
    print(f"\n  {'版本':<8} {'年化收益':<10} {'夏普':<8} {'回撤':<10} {'正收益占比'}")
    print(f"  {'v1.7':<8} {'4.05%':<10} {'0.12':<8} {'47.61%':<10} {'68.0%'}")
    print(f"  {'v1.8':<8} {'1.90%':<10} {'-0.08':<8} {'39.04%':<10} {'52.0%'}")
    print(f"  {'v1.9':<8} {np.mean(all_annual):.2f}%{'':<5} {np.mean(all_sharpe):.2f}{'':<4} {np.mean(all_dd):.2f}%{'':<5} {positive_return/len(results)*100:.1f}%")
    
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
    file_name = f"v19_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(export_dir, file_name)
    df_export.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 结果已导出: {file_path}")


if __name__ == '__main__':
    main()