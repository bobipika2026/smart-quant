"""
回测引擎
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime


class BacktestEngine:
    """回测引擎"""
    
    def __init__(self, initial_capital: float = 100000, commission: float = 0.0003):
        """
        初始化回测引擎
        
        Args:
            initial_capital: 初始资金
            commission: 手续费率
        """
        self.initial_capital = initial_capital
        self.commission = commission
    
    def run_backtest(
        self,
        df: pd.DataFrame,
        signal_col: str = 'signal'
    ) -> Dict:
        """
        运行回测
        
        Args:
            df: 包含价格和信号的数据
            signal_col: 信号列名
        
        Returns:
            dict: 回测结果
        """
        df = df.copy()
        
        # 确保有价格列
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        date_col = col_map.get('date', '日期')
        
        # 初始化
        cash = self.initial_capital
        position = 0  # 持仓数量
        trades = []  # 交易记录
        
        # 遍历数据
        for i in range(len(df)):
            if i == 0:
                continue
            
            price = df[close_col].iloc[i]
            signal = df[signal_col].iloc[i] if signal_col in df.columns else 0
            date = df[date_col].iloc[i] if date_col in df.columns else i
            
            # 买入信号
            if signal == 1 and cash > 0:
                shares = int(cash / price)
                if shares > 0:
                    cost = shares * price * (1 + self.commission)
                    cash -= cost
                    position += shares
                    trades.append({
                        'date': date,
                        'type': 'buy',
                        'price': price,
                        'shares': shares,
                        'value': cost
                    })
            
            # 卖出信号
            elif signal == -1 and position > 0:
                revenue = position * price * (1 - self.commission)
                cash += revenue
                trades.append({
                    'date': date,
                    'type': 'sell',
                    'price': price,
                    'shares': position,
                    'value': revenue
                })
                position = 0
        
        # 计算最终市值
        final_price = df[close_col].iloc[-1]
        final_value = cash + position * final_price
        
        # 计算指标
        results = self._calculate_metrics(df, final_value, trades, close_col, date_col)
        
        return results
    
    def _calculate_metrics(
        self,
        df: pd.DataFrame,
        final_value: float,
        trades: List[Dict],
        close_col: str,
        date_col: str
    ) -> Dict:
        """计算回测指标"""
        
        # 总收益率
        total_return = (final_value - self.initial_capital) / self.initial_capital * 100
        
        # 年化收益率
        if date_col in df.columns:
            start_date = pd.to_datetime(df[date_col].iloc[0])
            end_date = pd.to_datetime(df[date_col].iloc[-1])
            days = (end_date - start_date).days
            years = max(days / 365, 1)
            annual_return = (pow(final_value / self.initial_capital, 1/years) - 1) * 100
        else:
            annual_return = total_return
        
        # 最大回撤
        prices = df[close_col]
        cummax = prices.cummax()
        drawdown = (prices - cummax) / cummax
        max_drawdown = drawdown.min() * 100
        
        # 夏普比率（简化计算）
        returns = prices.pct_change().dropna()
        if len(returns) > 0 and returns.std() > 0:
            sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(252)
        else:
            sharpe_ratio = 0
        
        # 胜率
        win_trades = 0
        total_trades = len(trades) // 2  # 买卖成对
        
        for i in range(0, len(trades) - 1, 2):
            if i + 1 < len(trades):
                buy = trades[i]
                sell = trades[i + 1]
                if sell['price'] > buy['price']:
                    win_trades += 1
        
        win_rate = win_trades / total_trades * 100 if total_trades > 0 else 0
        
        return {
            'initial_capital': self.initial_capital,
            'final_value': round(final_value, 2),
            'total_return': round(total_return, 2),
            'annual_return': round(annual_return, 2),
            'max_drawdown': round(max_drawdown, 2),
            'sharpe_ratio': round(sharpe_ratio, 2),
            'win_rate': round(win_rate, 2),
            'trade_count': len(trades),
            'trades': trades[:20]  # 只返回前20条交易记录
        }