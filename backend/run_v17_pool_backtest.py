"""
股票池 v1.7 策略回测（简化版）

v1.7 特性：
1. 市场环境识别：使用股票自身的MA20/MA60判断趋势
2. 行业轮动：不同行业配置不同因子权重
3. 动态参数：牛/熊/震荡市不同参数配置
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
from app.config.best_factor_config import REGIME_PARAMS


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


class V17StrategySimplified:
    """v1.7 策略简化版（不含外部大盘数据）"""
    
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
        
        # KDJ
        low_14 = result['low'].rolling(14).min()
        high_14 = result['high'].rolling(14).max()
        rsv = (result['close'] - low_14) / (high_14 - low_14 + 0.0001) * 100
        result['KDJ'] = rsv.ewm(alpha=1/3).mean().ewm(alpha=1/3).mean()
        
        # BOLL位置
        ma20 = result['close'].rolling(20).mean()
        std20 = result['close'].rolling(20).std()
        result['BOLL'] = (result['close'] - ma20) / (2 * std20 + 0.0001)
        
        # 动量（63天）
        result['MOM'] = result['close'].pct_change(63)
        
        # LEV（负波动率）
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        
        # ROE代理（收益/风险）
        result['ROE'] = result['close'].pct_change().rolling(252).mean() / \
                       (result['close'].pct_change().rolling(252).std() + 0.0001)
        
        # 换手率代理
        result['TURN'] = result['volume'].rolling(5).mean() / \
                        (result['volume'].rolling(60).mean() + 0.0001) - 1
        
        # EP、BP代理
        result['EP'] = 1 / (result['close'] + 0.0001) * 100
        result['BP'] = 1 / (result['close'] + 0.0001) * 50
        
        return result
    
    def detect_regime_rolling(self, df):
        """滚动识别市场环境"""
        result = df.copy()
        
        # 计算趋势
        result['ma_short'] = result['close'].rolling(20).mean()
        result['ma_long'] = result['close'].rolling(60).mean()
        result['trend'] = (result['ma_short'] / result['ma_long'] - 1) * 100
        
        # 计算60日收益
        result['ret_60'] = result['close'].pct_change(60) * 100
        
        # 判断市场环境
        trend_th = 2.0
        return_th = 5.0
        
        result['regime'] = 'sideways'
        result.loc[(result['trend'] > trend_th) & (result['ret_60'] > return_th), 'regime'] = 'bull'
        result.loc[(result['trend'] < -trend_th) & (result['ret_60'] < -return_th), 'regime'] = 'bear'
        
        return result
    
    def get_factor_weights(self, regime):
        """获取因子权重（结合行业轮动）"""
        # 基础权重（按市场环境）
        base_weights = {
            'bull': {'MOM': 0.40, 'KDJ': 0.20, 'BOLL': 0.15, 'TURN': 0.15, 'ROE': 0.10},
            'bear': {'LEV': 0.30, 'ROE': 0.25, 'EP': 0.20, 'BP': 0.15, 'BOLL': 0.10},
            'sideways': {'KDJ': 0.25, 'BOLL': 0.25, 'TURN': 0.20, 'MOM': 0.15, 'ROE': 0.15}
        }
        
        weights = base_weights.get(regime, base_weights['sideways']).copy()
        
        # 行业权重调整
        if self.industry_weights:
            for factor in weights:
                base_w = weights.get(factor, 0)
                ind_w = self.industry_weights.get(factor, 0)
                # 市场权重60% + 行业权重40%
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
        
        # 标准化
        result['score_z'] = result['score'].rolling(252).apply(
            lambda x: (x.iloc[-1] - x.mean()) / (x.std() + 0.0001) if len(x) > 0 and x.std() > 0 else 0,
            raw=False
        )
        
        return result
    
    def generate_signal(self, df):
        """生成交易信号"""
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
        
        # 计算布林带
        result['boll_upper'] = result['close'].rolling(20).mean() + 2 * result['close'].rolling(20).std()
        result['boll_lower'] = result['close'].rolling(20).mean() - 2 * result['close'].rolling(20).std()
        
        # 持仓状态
        in_position = False
        entry_idx = 0
        
        for idx in range(252, len(result)):
            regime = result['regime'].iloc[idx]
            params = REGIME_PARAMS.get(regime, REGIME_PARAMS['sideways'])
            
            buy_threshold = params.get('signal_threshold', 0.6)
            sell_threshold = -buy_threshold
            vote_min = params.get('vote_min', 4)
            range_trade = params.get('range_trade', False)
            
            score_z = result['score_z'].iloc[idx]
            vote = result['vote'].iloc[idx]
            current_price = result['close'].iloc[idx]
            
            days_in_position = idx - entry_idx if in_position else 0
            can_sell = not in_position or days_in_position >= 10
            
            # 震荡市区间交易
            if range_trade and regime == 'sideways':
                boll_upper = result['boll_upper'].iloc[idx]
                boll_lower = result['boll_lower'].iloc[idx]
                
                if not in_position and current_price < boll_lower * 1.02 and vote >= 2:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
                    in_position = True
                    entry_idx = idx
                elif in_position and current_price > boll_upper * 0.98 and can_sell:
                    result.iloc[idx, result.columns.get_loc('signal')] = -1
                    in_position = False
                elif in_position:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
            else:
                # 普通模式
                if score_z > buy_threshold and vote >= vote_min and not in_position:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
                    in_position = True
                    entry_idx = idx
                elif score_z < sell_threshold and vote <= -vote_min and in_position and can_sell:
                    result.iloc[idx, result.columns.get_loc('signal')] = -1
                    in_position = False
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


def run_v17_backtest(stock_code, stock_name, industry, industry_desc):
    """运行 v1.7 策略回测"""
    
    # 1. 加载数据
    df = DataService.get_cached_data(stock_code, 'day')
    
    if df.empty or len(df) < 500:
        return None, "数据不足"
    
    # 2. 准备数据格式
    df_test = df.copy()
    
    # 重命名列
    rename_map = {
        '日期': 'trade_date',
        '开盘': 'open',
        '最高': 'high',
        '最低': 'low',
        '收盘': 'close',
        '成交量': 'volume'
    }
    df_test = df_test.rename(columns=rename_map)
    
    # 确保必要的列存在
    required_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in required_cols:
        if col not in df_test.columns:
            return None, f"缺少列: {col}"
    
    # 处理日期格式（可能是整数20160316或字符串）
    if 'trade_date' in df_test.columns:
        # 转换为字符串再解析
        df_test['trade_date'] = pd.to_datetime(df_test['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
    
    # 删除ts_code列（如果存在）
    if 'ts_code' in df_test.columns:
        df_test = df_test.drop(columns=['ts_code'])
    
    # 3. 运行策略
    strategy = V17StrategySimplified(stock_code)
    
    try:
        df_result = strategy.run_strategy(df_test)
        
        if df_result.empty or 'signal' not in df_result.columns:
            return None, "策略运行失败"
        
        # 4. 回测
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
    print("股票池 v1.7 策略回测")
    print("（市场环境识别 + 行业轮动）")
    print("=" * 70)
    
    # 1. 获取股票池
    stocks = get_stock_pool(top_n=50)
    
    if not stocks:
        print("股票池为空")
        return
    
    # 2. 行业分布
    print("\n>>> 行业分布:")
    industry_count = {}
    for s in stocks:
        ind = s['industry']
        industry_count[ind] = industry_count.get(ind, 0) + 1
    
    for ind, cnt in sorted(industry_count.items(), key=lambda x: -x[1]):
        desc = next((c['description'] for i, c in INDUSTRY_CONFIG.items() if i == ind), '默认配置')
        print(f"  {ind}: {cnt}只 ({desc})")
    
    # 3. 策略参数
    print("\n>>> v1.7 策略参数:")
    for regime, params in REGIME_PARAMS.items():
        print(f"  {regime}: 基础仓位{params.get('base_position', 0)*100:.0f}%, "
              f"信号阈值{params.get('signal_threshold', 0)}, "
              f"最少确认{params.get('vote_min', 0)}个因子")
    
    # 4. 回测
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
        
        result, error = run_v17_backtest(code, name, industry, industry_desc)
        
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
            
            regime_dist = result.get('regime_distribution', {})
            regime_str = ", ".join([f"{k}:{v}" for k, v in regime_dist.items()])
            
            print(f"  ✅ 基准: {benchmark:.1f}% | 年化: {annual:.1f}% | 超额: {excess:.1f}%")
            print(f"     夏普: {sharpe:.2f} | 回撤: {max_dd:.1f}% | 胜率: {win_rate:.1f}%")
            if regime_str:
                print(f"     市场环境: {regime_str}")
    
    # 5. 汇总报告
    print("\n" + "=" * 70)
    print("📊 v1.7 策略回测汇总报告")
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
    
    # 行业统计
    print("\n" + "=" * 70)
    print("📈 按行业分组统计")
    print("=" * 70)
    
    industry_stats = {}
    for r in results:
        ind = r['industry']
        if ind not in industry_stats:
            industry_stats[ind] = {
                'count': 0, 'annual_returns': [], 'sharpe_ratios': [],
                'max_drawdowns': [], 'excess_returns': []
            }
        industry_stats[ind]['count'] += 1
        industry_stats[ind]['annual_returns'].append(r.get('annual_return', 0))
        industry_stats[ind]['sharpe_ratios'].append(r.get('sharpe_ratio', 0))
        industry_stats[ind]['max_drawdowns'].append(r.get('max_drawdown', 0))
        industry_stats[ind]['excess_returns'].append(r.get('excess_return', 0))
    
    print(f"\n{'行业':<12} {'数量':<6} {'平均年化%':<10} {'平均夏普':<10} {'跑赢基准%'}")
    print("-" * 60)
    
    for ind, stats in sorted(industry_stats.items(), key=lambda x: np.mean(x[1]['annual_returns']), reverse=True):
        avg_annual = np.mean(stats['annual_returns'])
        avg_sharpe = np.mean(stats['sharpe_ratios'])
        beat_count = sum(1 for e in stats['excess_returns'] if e > 0)
        beat_pct = beat_count / stats['count'] * 100 if stats['count'] > 0 else 0
        
        print(f"{ind:<12} {stats['count']:<6} {avg_annual:>8.2f}% {avg_sharpe:>8.2f} {beat_pct:>8.1f}%")
    
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
            '盈亏比': r.get('profit_loss_ratio', 0),
            '交易次数': r.get('trade_count', 0),
        })
    
    df_export = pd.DataFrame(export_data)
    file_name = f"v17_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(export_dir, file_name)
    df_export.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 结果已导出: {file_path}")


if __name__ == '__main__':
    main()