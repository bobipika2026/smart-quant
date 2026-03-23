"""
估值+趋势双重择时策略 v3

改进：
1. 估值分位数 + 趋势判断双重确认
2. 低估值+上升趋势：加仓
3. 高估值+下降趋势：减仓
4. 其他情况：维持当前仓位
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime
import json
import tushare as ts

ts.set_token('21cbce2d06540b12e14765850fee73749ccfb0cd7570f466bf7d8e45')
pro = ts.pro_api()

OUTPUT_DIR = "data_cache/backtest_results"


class ValuationTrendStrategy:
    """估值+趋势双重择时策略"""
    
    # 指数代码
    INDEX_CODE = '000001.SH'  # 上证指数
    
    # 改进的仓位规则（更激进）
    POSITION_RULES = {
        # (估值分位数区间, 趋势方向) -> 仓位变化
        # 趋势: 1=上升, 0=震荡, -1=下降
        
        # 低估值区间
        (0, 20): {1: 0.85, 0: 0.70, -1: 0.50},    # 低估值：上升趋势加仓
        (20, 40): {1: 0.70, 0: 0.55, -1: 0.40},   # 偏低：上升趋势加仓
        
        # 中性区间
        (40, 60): {1: 0.60, 0: 0.50, -1: 0.35},   # 中性：跟随趋势
        
        # 高估值区间
        (60, 80): {1: 0.45, 0: 0.30, -1: 0.20},   # 偏高：下降趋势减仓
        (80, 100): {1: 0.30, 0: 0.15, -1: 0.05},  # 高估：下降趋势清仓
    }
    
    def __init__(self, initial_capital: float = 1000000):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self.initial_capital = initial_capital
    
    def load_data(self) -> pd.DataFrame:
        """加载估值和价格数据"""
        print("加载数据...")
        
        # 估值数据
        valuation_df = pro.index_dailybasic(
            ts_code=self.INDEX_CODE,
            start_date='20160101',
            end_date='20260322',
            fields='ts_code,trade_date,pe,pe_ttm,pb'
        )
        
        # 价格数据
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
        return df
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算估值分位数和趋势"""
        result = df.copy()
        
        # PE分位数（过去3年）
        result['pe_pct'] = result['pe_ttm'].rolling(756, min_periods=252).apply(
            lambda x: (x.iloc[-1] <= x).mean() * 100 if len(x) >= 252 else 50
        )
        
        # PB分位数
        result['pb_pct'] = result['pb'].rolling(756, min_periods=252).apply(
            lambda x: (x.iloc[-1] <= x).mean() * 100 if len(x) >= 252 else 50
        )
        
        # 综合估值分位数
        result['valuation_pct'] = result['pe_pct'] * 0.6 + result['pb_pct'] * 0.4
        
        # 趋势判断（MA20 vs MA60）
        result['ma20'] = result['close'].rolling(20).mean()
        result['ma60'] = result['close'].rolling(60).mean()
        result['trend'] = 0
        result.loc[result['ma20'] > result['ma60'] * 1.02, 'trend'] = 1   # 上升趋势
        result.loc[result['ma20'] < result['ma60'] * 0.98, 'trend'] = -1  # 下降趋势
        
        # 价格动量（60日涨幅）
        result['momentum'] = result['close'].pct_change(60) * 100
        
        return result
    
    def get_target_position(self, valuation_pct: float, trend: int) -> float:
        """根据估值和趋势获取目标仓位"""
        for (low, high), trend_dict in self.POSITION_RULES.items():
            if low <= valuation_pct < high:
                return trend_dict.get(trend, 0.50)
        return 0.50
    
    def run_backtest(self) -> dict:
        """运行回测"""
        print(f"\n{'='*70}")
        print("估值+趋势双重择时策略回测")
        print(f"{'='*70}")
        
        df = self.load_data()
        df = self.calculate_indicators(df)
        
        # 初始化
        cash = self.initial_capital
        position = 0
        current_pct = 0.50
        
        trades = []
        portfolio_values = []
        
        # 遍历（跳过前3年）
        for i in range(756, len(df)):
            date = df['trade_date'].iloc[i]
            price = df['close'].iloc[i]
            valuation_pct = df['valuation_pct'].iloc[i]
            trend = df['trend'].iloc[i]
            
            # 当前价值
            current_value = cash + position * price
            
            # 目标仓位
            target_pct = self.get_target_position(valuation_pct, trend)
            
            # 分步调整（每次最多15%）
            max_change = 0.15
            if target_pct > current_pct + max_change:
                adjusted_pct = current_pct + max_change
            elif target_pct < current_pct - max_change:
                adjusted_pct = current_pct - max_change
            else:
                adjusted_pct = target_pct
            
            # 计算交易金额
            target_value = current_value * adjusted_pct
            position_value = position * price
            trade_value = target_value - position_value
            
            # 执行交易
            if trade_value > 1000 and cash > trade_value * 1.001:
                shares = trade_value / price
                cost = shares * price * 1.0003
                position += shares
                cash -= cost
                trades.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'action': 'buy',
                    'valuation': round(valuation_pct, 1),
                    'trend': trend,
                    'position': round(adjusted_pct * 100, 1)
                })
                current_pct = adjusted_pct
                
            elif trade_value < -1000 and position > 0:
                shares = min(-trade_value / price, position)
                revenue = shares * price * 0.9997
                position -= shares
                cash += revenue
                trades.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'action': 'sell',
                    'valuation': round(valuation_pct, 1),
                    'trend': trend,
                    'position': round(adjusted_pct * 100, 1)
                })
                current_pct = adjusted_pct
            
            portfolio_values.append({
                'date': date,
                'value': cash + position * price,
                'position_pct': current_pct,
                'valuation_pct': valuation_pct,
                'trend': trend
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
        
        # 基准
        benchmark_return = df['close'].iloc[-1] / df['close'].iloc[756] - 1
        
        # 年度收益
        portfolio_df['year'] = portfolio_df['date'].dt.year
        yearly = portfolio_df.groupby('year').apply(
            lambda x: (1 + x['returns'].dropna()).prod() - 1
        ).to_dict()
        
        # 打印
        print(f"\n{'='*70}")
        print("📊 回测结果")
        print(f"{'='*70}")
        
        print(f"\n【策略收益】")
        print(f"  总收益: {total_return*100:.2f}%")
        print(f"  年化收益: {annual_return*100:.2f}%")
        print(f"  夏普比率: {sharpe:.3f}")
        print(f"  最大回撤: {max_drawdown*100:.2f}%")
        
        print(f"\n【基准收益】")
        print(f"  总收益: {benchmark_return*100:.2f}%")
        
        print(f"\n【超额收益】")
        print(f"  {(total_return - benchmark_return)*100:.2f}%")
        
        print(f"\n【仓位统计】")
        print(f"  平均仓位: {portfolio_df['position_pct'].mean()*100:.1f}%")
        
        print(f"\n【年度收益】")
        for y, r in yearly.items():
            print(f"  {y}: {r*100:.2f}%")
        
        # 按趋势-估值分组统计
        print(f"\n【趋势+估值组合表现】")
        portfolio_df['val_group'] = pd.cut(portfolio_df['valuation_pct'], [0, 40, 60, 100], labels=['低估', '中性', '高估'])
        portfolio_df['trend_group'] = portfolio_df['trend'].map({1: '上升', 0: '震荡', -1: '下降'})
        
        for val in ['低估', '中性', '高估']:
            for trend in ['上升', '震荡', '下降']:
                subset = portfolio_df[(portfolio_df['val_group'] == val) & (portfolio_df['trend_group'] == trend)]
                if len(subset) > 0:
                    avg_ret = subset['returns'].mean() * 252 * 100
                    count = len(subset)
                    print(f"  {val}+{trend}: 年化{avg_ret:.1f}%, {count}天")
        
        # 保存
        result = {
            'strategy': '估值+趋势双重择时',
            'performance': {
                'total_return': f"{total_return*100:.2f}%",
                'annual_return': f"{annual_return*100:.2f}%",
                'sharpe': round(sharpe, 3),
                'max_drawdown': f"{max_drawdown*100:.2f}%",
            },
            'benchmark': f"{benchmark_return*100:.2f}%",
            'excess': f"{(total_return - benchmark_return)*100:.2f}%",
            'yearly': {str(k): f"{v*100:.2f}%" for k, v in yearly.items()},
            'avg_position': f"{portfolio_df['position_pct'].mean()*100:.1f}%",
            'trades': len(trades),
            'final_value': round(portfolio_df['value'].iloc[-1], 2)
        }
        
        output_file = os.path.join(OUTPUT_DIR, f"valuation_trend_strategy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存: {output_file}")
        return result


if __name__ == '__main__':
    strategy = ValuationTrendStrategy()
    strategy.run_backtest()