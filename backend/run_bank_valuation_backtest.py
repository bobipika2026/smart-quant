"""
银行板块估值择时回测

核心逻辑：
1. 银行板块PE<6时逐步加仓
2. PE>10时逐步减仓
3. 银行板块估值区间相对稳定，适合估值择时
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
DAY_CACHE_DIR = "data_cache/day"


class BankValuationTiming:
    """银行板块估值择时策略"""
    
    # 银行板块估值仓位规则（基于PE-TTM）
    PE_POSITION_RULES = {
        # PE区间 -> 目标仓位
        (0, 5.0): 0.95,     # 极度低估：重仓
        (5.0, 6.0): 0.85,   # 非常低估
        (6.0, 7.0): 0.70,   # 低估
        (7.0, 8.0): 0.55,   # 合理偏低
        (8.0, 9.0): 0.45,   # 合理
        (9.0, 10.0): 0.35,  # 合理偏高
        (10.0, 12.0): 0.20, # 偏高
        (12.0, 999): 0.10,  # 高估：轻仓
    }
    
    # 银行股列表（主要银行）
    BANK_STOCKS = [
        '601398',  # 工商银行
        '601288',  # 农业银行
        '601939',  # 建设银行
        '601988',  # 中国银行
        '600036',  # 招商银行
        '601166',  # 兴业银行
        '600000',  # 浦发银行
        '601328',  # 交通银行
        '600016',  # 民生银行
        '601818',  # 光大银行
    ]
    
    def __init__(self, initial_capital: float = 1000000):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self.initial_capital = initial_capital
    
    def load_bank_pe_data(self) -> pd.DataFrame:
        """加载银行板块PE数据"""
        print("加载银行板块估值数据...")
        
        # 获取申万银行指数估值
        try:
            # 申万银行指数 801780.SI
            df = pro.index_dailybasic(
                ts_code='801780.SI',
                start_date='20160101',
                end_date='20260322',
                fields='ts_code,trade_date,pe,pe_ttm,pb'
            )
            if len(df) > 0:
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
                return df.sort_values('trade_date').reset_index(drop=True)
        except:
            pass
        
        # 备用：使用个股数据估算板块PE
        print("  使用个股数据估算板块PE...")
        all_pe = []
        
        for code in self.BANK_STOCKS:
            try:
                # 获取每日基本面数据
                df = pro.daily_basic(
                    ts_code=f'{code}.SH',
                    start_date='20160101',
                    end_date='20260322',
                    fields='ts_code,trade_date,pe_ttm,pb'
                )
                if len(df) > 0:
                    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
                    all_pe.append(df[['trade_date', 'pe_ttm', 'pb']])
            except:
                continue
        
        if not all_pe:
            return pd.DataFrame()
        
        # 合并，取平均PE
        merged = all_pe[0]
        for i, df in enumerate(all_pe[1:], 1):
            merged = merged.merge(df, on='trade_date', how='outer', suffixes=('', f'_{i}'))
        
        pe_cols = [c for c in merged.columns if c == 'pe_ttm' or c.startswith('pe_ttm_')]
        pb_cols = [c for c in merged.columns if c == 'pb' or c.startswith('pb_')]
        
        merged['pe_ttm'] = merged[pe_cols].mean(axis=1)
        merged['pb'] = merged[pb_cols].mean(axis=1)
        
        return merged[['trade_date', 'pe_ttm', 'pb']].sort_values('trade_date').reset_index(drop=True)
    
    def load_bank_price_data(self) -> pd.DataFrame:
        """加载银行板块价格数据"""
        print("加载银行板块价格数据...")
        
        all_prices = []
        
        for code in self.BANK_STOCKS:
            try:
                file_path = os.path.join(DAY_CACHE_DIR, f"{code}_day.csv")
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path, encoding='utf-8')
                    if '日期' in df.columns:
                        df['trade_date'] = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d')
                    if '收盘' in df.columns:
                        df['close'] = df['收盘']
                    all_prices.append(df[['trade_date', 'close']])
            except:
                continue
        
        if not all_prices:
            # 从Tushare获取
            for code in self.BANK_STOCKS:
                try:
                    df = pro.daily(ts_code=f'{code}.SH', start_date='20160101', end_date='20260322')
                    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
                    all_prices.append(df[['trade_date', 'close']])
                except:
                    continue
        
        if not all_prices:
            return pd.DataFrame()
        
        # 构建等权指数
        merged = all_prices[0]
        for i, df in enumerate(all_prices[1:], 1):
            merged = merged.merge(df, on='trade_date', how='outer', suffixes=('', f'_{i}'))
        
        close_cols = [c for c in merged.columns if c == 'close' or c.startswith('close_')]
        merged['close'] = merged[close_cols].mean(axis=1)
        
        return merged[['trade_date', 'close']].sort_values('trade_date').reset_index(drop=True)
    
    def get_target_position(self, pe: float) -> float:
        """根据PE获取目标仓位"""
        for (low, high), position in self.PE_POSITION_RULES.items():
            if low <= pe < high:
                return position
        return 0.50
    
    def run_backtest(self) -> dict:
        """运行回测"""
        print(f"\n{'='*70}")
        print("银行板块估值择时策略回测")
        print(f"{'='*70}")
        
        # 加载数据
        pe_df = self.load_bank_pe_data()
        price_df = self.load_bank_price_data()
        
        if pe_df.empty or price_df.empty:
            print("数据加载失败")
            return {}
        
        # 合并
        df = pe_df.merge(price_df, on='trade_date', how='inner')
        df = df.dropna()
        
        print(f"  数据范围: {df['trade_date'].min()} ~ {df['trade_date'].max()}")
        print(f"  数据条数: {len(df)}")
        print(f"  PE范围: {df['pe_ttm'].min():.2f} ~ {df['pe_ttm'].max():.2f}")
        
        # 初始化
        cash = self.initial_capital
        position = 0
        current_pct = 0.50
        
        trades = []
        portfolio_values = []
        
        # 遍历
        for i in range(252, len(df)):  # 跳过第一年
            date = df['trade_date'].iloc[i]
            price = df['close'].iloc[i]
            pe = df['pe_ttm'].iloc[i]
            
            if pd.isna(pe) or pe <= 0:
                continue
            
            # 当前价值
            current_value = cash + position * price
            
            # 目标仓位
            target_pct = self.get_target_position(pe)
            
            # 分步调整
            max_change = 0.10
            if target_pct > current_pct + max_change:
                adjusted_pct = current_pct + max_change
            elif target_pct < current_pct - max_change:
                adjusted_pct = current_pct - max_change
            else:
                adjusted_pct = target_pct
            
            # 计算交易
            target_value = current_value * adjusted_pct
            position_value = position * price
            trade_value = target_value - position_value
            
            # 执行
            if trade_value > 1000 and cash > trade_value * 1.001:
                shares = trade_value / price
                cost = shares * price * 1.0003
                position += shares
                cash -= cost
                trades.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'action': 'buy',
                    'pe': round(pe, 2),
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
                    'pe': round(pe, 2),
                    'position': round(adjusted_pct * 100, 1)
                })
                current_pct = adjusted_pct
            
            portfolio_values.append({
                'date': date,
                'value': cash + position * price,
                'position_pct': current_pct,
                'pe': pe
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
        benchmark_return = df['close'].iloc[-1] / df['close'].iloc[252] - 1
        
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
        
        print(f"\n【基准收益（银行板块买入持有）】")
        print(f"  总收益: {benchmark_return*100:.2f}%")
        print(f"  年化收益: {(1+benchmark_return)**(1/years)-1:.2%}")
        
        print(f"\n【超额收益】")
        print(f"  {(total_return - benchmark_return)*100:.2f}%")
        
        print(f"\n【仓位统计】")
        print(f"  平均仓位: {portfolio_df['position_pct'].mean()*100:.1f}%")
        print(f"  最高仓位: {portfolio_df['position_pct'].max()*100:.1f}%")
        print(f"  最低仓位: {portfolio_df['position_pct'].min()*100:.1f}%")
        
        print(f"\n【年度收益】")
        for y, r in yearly.items():
            print(f"  {y}: {r*100:.2f}%")
        
        # PE区间统计
        print(f"\n【PE区间分布】")
        pe_bins = [(0, 6, '极低'), (6, 7, '低'), (7, 8, '合理偏低'), (8, 10, '合理'), (10, 999, '高')]
        for low, high, label in pe_bins:
            count = ((portfolio_df['pe'] >= low) & (portfolio_df['pe'] < high)).sum()
            pct = count / len(portfolio_df) * 100
            print(f"  {label}（{low}-{high}）: {pct:.1f}%")
        
        # 保存
        result = {
            'strategy': '银行板块估值择时',
            'performance': {
                'total_return': f"{total_return*100:.2f}%",
                'annual_return': f"{annual_return*100:.2f}%",
                'sharpe': round(sharpe, 3),
                'max_drawdown': f"{max_drawdown*100:.2f}%",
            },
            'benchmark': f"{benchmark_return*100:.2f}%",
            'excess': f"{(total_return - benchmark_return)*100:.2f}%",
            'yearly': {str(k): f"{v*100:.2f}%" for k, v in yearly.items()},
            'position': {
                'avg': f"{portfolio_df['position_pct'].mean()*100:.1f}%",
                'max': f"{portfolio_df['position_pct'].max()*100:.1f}%",
                'min': f"{portfolio_df['position_pct'].min()*100:.1f}%",
            },
            'trades': len(trades),
            'final_value': round(portfolio_df['value'].iloc[-1], 2)
        }
        
        output_file = os.path.join(OUTPUT_DIR, f"bank_valuation_timing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存: {output_file}")
        return result


if __name__ == '__main__':
    strategy = BankValuationTiming()
    strategy.run_backtest()