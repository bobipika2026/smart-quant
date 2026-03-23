"""
低估值板块估值择时策略 v2

核心逻辑：
1. 基于真实PE/PB分位数进行仓位管理
2. 低估值时逐步加仓，高估值时逐步减仓
3. 针对银行、非银金融等低估值板块

数据来源：Tushare指数估值数据
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List
import json
import tushare as ts

ts.set_token('21cbce2d06540b12e14765850fee73749ccfb0cd7570f466bf7d8e45')
pro = ts.pro_api()

DAY_CACHE_DIR = "data_cache/day"
OUTPUT_DIR = "data_cache/backtest_results"


class ValuationTimingStrategyV2:
    """估值择时策略v2 - 使用真实估值数据"""
    
    # 指数代码
    INDEX_CODE = '000001.SH'  # 上证指数
    
    # 仓位调整规则（基于PE分位数）
    POSITION_RULES = {
        # PE分位数范围 -> 目标仓位
        (0, 10): 0.90,    # 极度低估：90%仓位
        (10, 20): 0.80,   # 非常低估：80%
        (20, 30): 0.70,   # 低估：70%
        (30, 40): 0.60,   # 偏低：60%
        (40, 50): 0.50,   # 中性偏低：50%
        (50, 60): 0.40,   # 中性偏高：40%
        (60, 70): 0.30,   # 偏高：30%
        (70, 80): 0.20,   # 高估：20%
        (80, 90): 0.10,   # 非常高估：10%
        (90, 100): 0.05,  # 极度高估：5%
    }
    
    def __init__(self, initial_capital: float = 1000000):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self.initial_capital = initial_capital
    
    def load_index_valuation(self) -> pd.DataFrame:
        """加载指数估值数据"""
        print("加载指数估值和价格数据...")
        
        # 获取估值数据
        valuation_df = pro.index_dailybasic(
            ts_code=self.INDEX_CODE,
            start_date='20160101',
            end_date='20260322',
            fields='ts_code,trade_date,pe,pe_ttm,pb,turnover_rate'
        )
        
        # 获取价格数据
        price_df = pro.index_daily(
            ts_code=self.INDEX_CODE,
            start_date='20160101',
            end_date='20260322',
            fields='ts_code,trade_date,close,open,high,low,pct_chg'
        )
        
        # 合并
        df = valuation_df.merge(price_df[['trade_date', 'close', 'pct_chg']], on='trade_date', how='inner')
        
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
        df = df.sort_values('trade_date').reset_index(drop=True)
        
        print(f"  数据范围: {df['trade_date'].min()} ~ {df['trade_date'].max()}")
        print(f"  数据条数: {len(df)}")
        
        return df
    
    def calculate_valuation_percentile(self, df: pd.DataFrame, window: int = 756) -> pd.DataFrame:
        """计算估值分位数（过去3年）"""
        result = df.copy()
        
        # PE分位数
        result['pe_pct'] = result['pe_ttm'].rolling(window, min_periods=252).apply(
            lambda x: (x.iloc[-1] <= x).mean() * 100 if len(x) >= 252 else 50
        )
        
        # PB分位数
        result['pb_pct'] = result['pb'].rolling(window, min_periods=252).apply(
            lambda x: (x.iloc[-1] <= x).mean() * 100 if len(x) >= 252 else 50
        )
        
        # 综合估值分位数（PE权重60%，PB权重40%）
        result['valuation_pct'] = (
            result['pe_pct'].fillna(50) * 0.6 +
            result['pb_pct'].fillna(50) * 0.4
        )
        
        return result
    
    def get_target_position(self, valuation_pct: float) -> float:
        """根据估值分位数获取目标仓位"""
        for (low, high), position in self.POSITION_RULES.items():
            if low <= valuation_pct < high:
                return position
        return 0.05
    
    def load_sector_index(self, sector_name: str) -> pd.DataFrame:
        """加载板块指数数据"""
        # 板块对应的股票代码
        sector_stocks = {
            '银行': ['601398', '601288', '601939', '601988', '600036'],  # 工商、农业、建设、中国、招商
            '非银金融': ['601318', '601688', '600030', '000166', '601211'],  # 中国平安、华泰、中信等
            '低估值组合': None  # 使用上证指数
        }
        
        if sector_name == '低估值组合':
            # 使用上证指数
            df = pro.index_dailybasic(
                ts_code='000001.SH',
                start_date='20160101',
                end_date='20260322',
                fields='ts_code,trade_date,close,pe,pe_ttm,pb'
            )
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            return df.sort_values('trade_date').reset_index(drop=True)
        
        # 构建板块等权指数
        all_returns = []
        for code in sector_stocks.get(sector_name, []):
            try:
                file_path = os.path.join(DAY_CACHE_DIR, f"{code}_day.csv")
                if os.path.exists(file_path):
                    stock_df = pd.read_csv(file_path, encoding='utf-8')
                    if '日期' in stock_df.columns:
                        stock_df['trade_date'] = pd.to_datetime(stock_df['日期'].astype(str), format='%Y%m%d')
                    if '收盘' in stock_df.columns:
                        stock_df['close'] = stock_df['收盘']
                    all_returns.append(stock_df[['trade_date', 'close']])
            except:
                continue
        
        if not all_returns:
            return pd.DataFrame()
        
        merged = all_returns[0]
        for i, ret_df in enumerate(all_returns[1:], 1):
            merged = merged.merge(ret_df, on='trade_date', how='outer', suffixes=('', f'_{i}'))
        
        close_cols = [c for c in merged.columns if c == 'close' or c.startswith('close_')]
        merged['close'] = merged[close_cols].mean(axis=1)
        
        return merged[['trade_date', 'close']].sort_values('trade_date').reset_index(drop=True)
    
    def run_backtest(self, sector_name: str = '低估值组合') -> Dict:
        """运行回测"""
        print(f"\n{'='*70}")
        print(f"估值择时策略回测 - {sector_name}")
        print(f"{'='*70}")
        
        # 加载估值数据
        valuation_df = self.load_index_valuation()
        valuation_df = self.calculate_valuation_percentile(valuation_df)
        
        # 加载价格数据
        if sector_name == '低估值组合':
            price_df = valuation_df[['trade_date', 'close']].copy()
        else:
            price_df = self.load_sector_index(sector_name)
        
        if price_df.empty:
            print("无法加载价格数据")
            return {}
        
        # 合并数据（估值数据已包含close）
        df = valuation_df.copy()
        
        # 初始化
        cash = self.initial_capital
        position = 0
        current_position_pct = 0.50  # 初始50%仓位
        
        trades = []
        portfolio_values = []
        
        # 遍历
        for i in range(756, len(df)):  # 跳过前3年（计算分位数）
            date = df['trade_date'].iloc[i]
            price = df['close'].iloc[i]
            valuation_pct = df['valuation_pct'].iloc[i]
            
            # 当前组合价值
            current_value = cash + position * price
            
            # 获取目标仓位
            target_position_pct = self.get_target_position(valuation_pct)
            
            # 分步调整（每次最多调整10%）
            max_change = 0.10
            if target_position_pct > current_position_pct + max_change:
                adjusted_pct = current_position_pct + max_change
            elif target_position_pct < current_position_pct - max_change:
                adjusted_pct = current_position_pct - max_change
            else:
                adjusted_pct = target_position_pct
            
            # 计算需要调整的金额
            target_value = current_value * adjusted_pct
            current_position_value = position * price
            trade_value = target_value - current_position_value
            
            # 执行交易
            if trade_value > 1000 and cash > trade_value * 1.001:  # 加仓
                shares = trade_value / price
                cost = shares * price * 1.0003
                position += shares
                cash -= cost
                trades.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'action': 'buy',
                    'price': price,
                    'value': trade_value,
                    'valuation_pct': round(valuation_pct, 1),
                    'position': round(adjusted_pct * 100, 1)
                })
                current_position_pct = adjusted_pct
                
            elif trade_value < -1000 and position > 0:  # 减仓
                shares = min(-trade_value / price, position)
                revenue = shares * price * 0.9997
                position -= shares
                cash += revenue
                trades.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'action': 'sell',
                    'price': price,
                    'value': -trade_value,
                    'valuation_pct': round(valuation_pct, 1),
                    'position': round(adjusted_pct * 100, 1)
                })
                current_position_pct = adjusted_pct
            
            # 记录
            portfolio_values.append({
                'date': date,
                'value': cash + position * price,
                'position_pct': current_position_pct,
                'valuation_pct': valuation_pct,
                'pe': df['pe_ttm'].iloc[i],
                'pb': df['pb'].iloc[i]
            })
        
        # 计算业绩
        portfolio_df = pd.DataFrame(portfolio_values)
        portfolio_df['returns'] = portfolio_df['value'].pct_change()
        
        total_return = portfolio_df['value'].iloc[-1] / self.initial_capital - 1
        years = len(portfolio_df) / 252
        annual_return = (1 + total_return) ** (1 / years) - 1
        
        sharpe = portfolio_df['returns'].mean() / portfolio_df['returns'].std() * np.sqrt(252)
        
        cummax = portfolio_df['value'].cummax()
        drawdown = (portfolio_df['value'] - cummax) / cummax
        max_drawdown = drawdown.min()
        
        # 基准收益
        benchmark_return = df['close'].iloc[-1] / df['close'].iloc[756] - 1
        
        # 年度收益
        portfolio_df['year'] = portfolio_df['date'].dt.year
        yearly_returns = portfolio_df.groupby('year').apply(
            lambda x: (1 + x['returns'].dropna()).prod() - 1
        ).to_dict()
        
        # 打印结果
        print(f"\n{'='*70}")
        print("📊 回测结果")
        print(f"{'='*70}")
        
        print(f"\n【策略收益】")
        print(f"  总收益: {total_return*100:.2f}%")
        print(f"  年化收益: {annual_return*100:.2f}%")
        print(f"  夏普比率: {sharpe:.3f}")
        print(f"  最大回撤: {max_drawdown*100:.2f}%")
        
        print(f"\n【基准收益（买入持有）】")
        print(f"  总收益: {benchmark_return*100:.2f}%")
        print(f"  年化收益: {(1+benchmark_return)**(1/years)-1:.2%}")
        
        print(f"\n【超额收益】")
        print(f"  总超额: {(total_return - benchmark_return)*100:.2f}%")
        
        print(f"\n【年度收益】")
        for year, ret in yearly_returns.items():
            print(f"  {year}: {ret*100:.2f}%")
        
        # 仓位统计
        print(f"\n【仓位统计】")
        avg_position = portfolio_df['position_pct'].mean()
        print(f"  平均仓位: {avg_position*100:.1f}%")
        print(f"  最高仓位: {portfolio_df['position_pct'].max()*100:.1f}%")
        print(f"  最低仓位: {portfolio_df['position_pct'].min()*100:.1f}%")
        
        # 估值区间统计
        print(f"\n【估值分位数分布】")
        valuation_bins = [(0, 20, '低估'), (20, 40, '偏低'), (40, 60, '中性'), (60, 80, '偏高'), (80, 100, '高估')]
        for low, high, label in valuation_bins:
            count = ((portfolio_df['valuation_pct'] >= low) & (portfolio_df['valuation_pct'] < high)).sum()
            pct = count / len(portfolio_df) * 100
            print(f"  {label}（{low}-{high}%）: {pct:.1f}%")
        
        # 保存
        result = {
            'strategy': f'估值择时策略 - {sector_name}',
            'backtest_period': {
                'start': portfolio_df['date'].min().strftime('%Y-%m-%d'),
                'end': portfolio_df['date'].max().strftime('%Y-%m-%d'),
                'years': round(years, 1)
            },
            'performance': {
                'total_return': f"{total_return*100:.2f}%",
                'annual_return': f"{annual_return*100:.2f}%",
                'sharpe_ratio': round(sharpe, 3),
                'max_drawdown': f"{max_drawdown*100:.2f}%",
            },
            'benchmark': {
                'total_return': f"{benchmark_return*100:.2f}%",
                'annual_return': f"{(1+benchmark_return)**(1/years)-1:.2%}",
            },
            'excess_return': f"{(total_return - benchmark_return)*100:.2f}%",
            'yearly_returns': {str(k): f"{v*100:.2f}%" for k, v in yearly_returns.items()},
            'position_stats': {
                'avg_position': f"{avg_position*100:.1f}%",
                'max_position': f"{portfolio_df['position_pct'].max()*100:.1f}%",
                'min_position': f"{portfolio_df['position_pct'].min()*100:.1f}%",
            },
            'trade_count': len(trades),
            'final_value': round(portfolio_df['value'].iloc[-1], 2)
        }
        
        output_file = os.path.join(
            OUTPUT_DIR,
            f"valuation_timing_v2_{sector_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存: {output_file}")
        
        return result


def main():
    strategy = ValuationTimingStrategyV2(initial_capital=1000000)
    return strategy.run_backtest('低估值组合')


if __name__ == '__main__':
    main()