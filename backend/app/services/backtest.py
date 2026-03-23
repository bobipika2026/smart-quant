"""
回测引擎 - 完整评价体系
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime


class BacktestEngine:
    """回测引擎"""
    
    # 无风险利率（年化，假设3%）
    RISK_FREE_RATE = 0.03
    
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
        
        # 确保有价格列和日期列
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        # 日期列：支持 日线(日期/date) 和 分钟线(时间/day)
        date_col = None
        for dc in ['日期', 'date', '时间', 'day', 'trade_time']:
            if dc in df.columns:
                date_col = dc
                break
        if date_col is None:
            date_col = df.columns[0]  # 默认第一列
        
        # 初始化
        cash = self.initial_capital
        position = 0  # 持仓数量
        trades = []  # 交易记录
        equity_curve = []  # 收益曲线
        
        # 遍历数据
        for i in range(len(df)):
            price = df[close_col].iloc[i]
            signal = df[signal_col].iloc[i] if signal_col in df.columns else 0
            date = df[date_col].iloc[i] if date_col in df.columns else i
            
            # 记录当日市值
            current_equity = cash + position * price
            equity_curve.append({
                'date': str(date),
                'equity': round(current_equity, 2),
                'cash': round(cash, 2),
                'position': position,
                'price': round(price, 2)
            })
            
            if i == 0:
                continue
            
            # 买入信号
            if signal == 1 and cash > 0:
                shares = int(cash / price)
                if shares > 0:
                    cost = shares * price * (1 + self.commission)
                    cash -= cost
                    position += shares
                    trades.append({
                        'date': str(date),
                        'type': 'buy',
                        'price': round(price, 2),
                        'shares': shares,
                        'value': round(cost, 2)
                    })
            
            # 卖出信号
            elif signal == -1 and position > 0:
                revenue = position * price * (1 - self.commission)
                cash += revenue
                trades.append({
                    'date': str(date),
                    'type': 'sell',
                    'price': round(price, 2),
                    'shares': position,
                    'value': round(revenue, 2)
                })
                position = 0
        
        # 计算最终市值
        final_price = df[close_col].iloc[-1]
        final_value = cash + position * final_price
        
        # 计算指标
        results = self._calculate_metrics(df, final_value, trades, equity_curve, close_col, date_col)
        
        return results
    
    def _calculate_metrics(
        self,
        df: pd.DataFrame,
        final_value: float,
        trades: List[Dict],
        equity_curve: List[Dict],
        close_col: str,
        date_col: str
    ) -> Dict:
        """计算完整回测指标"""
        
        # ==================== 基础收益指标 ====================
        
        # 总收益率
        total_return = (final_value - self.initial_capital) / self.initial_capital * 100
        
        # 年化收益率
        if date_col in df.columns:
            try:
                # 处理不同日期格式
                date_val = df[date_col].iloc[0]
                if isinstance(date_val, (int, np.integer)):
                    # 整数格式 YYYYMMDD
                    start_date = pd.to_datetime(str(df[date_col].iloc[0]))
                    end_date = pd.to_datetime(str(df[date_col].iloc[-1]))
                else:
                    start_date = pd.to_datetime(df[date_col].iloc[0])
                    end_date = pd.to_datetime(df[date_col].iloc[-1])
                
                days = max((end_date - start_date).days, 1)
                years = max(days / 365, 1/365)
                annual_return = (pow(final_value / self.initial_capital, 1/years) - 1) * 100
            except:
                # 解析失败时使用数据条数估算
                days = len(df)
                years = max(days / 252, 1/252)
                annual_return = (pow(final_value / self.initial_capital, 1/years) - 1) * 100
        else:
            days = len(df)
            years = max(days / 252, 1/252)
            annual_return = (pow(final_value / self.initial_capital, 1/years) - 1) * 100
        
        # 基准收益（买入持有）
        first_price = df[close_col].iloc[0]
        last_price = df[close_col].iloc[-1]
        benchmark_return = (last_price - first_price) / first_price * 100
        
        # 超额收益
        excess_return = total_return - benchmark_return
        
        # ==================== 风险指标 ====================
        
        # 策略净值日收益率（使用权益曲线）
        if len(equity_curve) > 1:
            equity_series = pd.Series([e['equity'] for e in equity_curve])
            equity_returns = equity_series.pct_change().dropna()
        else:
            equity_returns = pd.Series([0])
        
        # 策略波动率（年化）- 使用策略净值收益率
        if len(equity_returns) > 0 and equity_returns.std() > 0:
            volatility = equity_returns.std() * np.sqrt(252) * 100
        else:
            volatility = 0
        
        # 最大回撤 - 使用权益曲线
        if len(equity_curve) > 0:
            equity_values = [e['equity'] for e in equity_curve]
            equity_cummax = pd.Series(equity_values).cummax()
            equity_drawdown = (pd.Series(equity_values) - equity_cummax) / equity_cummax
            max_drawdown = abs(equity_drawdown.min() * 100)
        else:
            max_drawdown = 0
        
        # 下行风险（只计算负收益）
        negative_returns = equity_returns[equity_returns < 0]
        if len(negative_returns) > 0:
            downside_risk = negative_returns.std() * np.sqrt(252) * 100
        else:
            downside_risk = 0
        
        # ==================== 风险调整收益指标 ====================
        
        # 夏普比率 - 使用策略净值收益率
        if len(equity_returns) > 0 and equity_returns.std() > 0:
            daily_rf = self.RISK_FREE_RATE / 252
            excess_daily = equity_returns.mean() - daily_rf
            sharpe_ratio = (excess_daily / equity_returns.std()) * np.sqrt(252)
        else:
            sharpe_ratio = 0
        
        # 索提诺比率（只惩罚下行风险）- 使用策略净值收益率
        if len(negative_returns) > 0 and negative_returns.std() > 0:
            daily_rf = self.RISK_FREE_RATE / 252
            excess_daily = equity_returns.mean() - daily_rf
            sortino_ratio = (excess_daily / negative_returns.std()) * np.sqrt(252)
        else:
            sortino_ratio = sharpe_ratio  # 无负收益时等于夏普
        
        # 卡玛比率（收益/最大回撤）
        if max_drawdown > 0:
            calmar_ratio = annual_return / max_drawdown
        else:
            calmar_ratio = 999  # 无回撤
        
        # 信息比率（超额收益/跟踪误差）
        if len(equity_returns) > 1:
            tracking_error = equity_returns.std() * np.sqrt(252)
            if tracking_error > 0:
                information_ratio = excess_return / 100 / tracking_error
            else:
                information_ratio = 0
        else:
            information_ratio = 0
        
        # ==================== 交易质量指标 ====================
        
        # 胜率和盈亏比
        win_trades = 0
        loss_trades = 0
        total_profit = 0
        total_loss = 0
        total_trades = len(trades) // 2  # 买卖成对
        
        for i in range(0, len(trades) - 1, 2):
            if i + 1 < len(trades):
                buy = trades[i]
                sell = trades[i + 1]
                profit = (sell['price'] - buy['price']) * buy['shares']
                if profit > 0:
                    win_trades += 1
                    total_profit += profit
                else:
                    loss_trades += 1
                    total_loss += abs(profit)
        
        win_rate = win_trades / total_trades * 100 if total_trades > 0 else 0
        profit_loss_ratio = total_profit / total_loss if total_loss > 0 else 999
        
        # 平均持仓天数
        if len(trades) >= 2 and total_trades > 0:
            total_holding_days = 0
            for i in range(0, len(trades) - 1, 2):
                if i + 1 < len(trades):
                    buy_date = pd.to_datetime(trades[i]['date'])
                    sell_date = pd.to_datetime(trades[i + 1]['date'])
                    total_holding_days += (sell_date - buy_date).days
            avg_holding_days = total_holding_days / total_trades
        else:
            avg_holding_days = days  # 未交易则等于总天数
        
        # ==================== 综合评分 ====================
        
        # 评分标准（满分100分）
        score = 0
        
        # 年化收益评分（满分40分）
        if annual_return > 30:
            score += 40
        elif annual_return > 20:
            score += 35
        elif annual_return > 10:
            score += 25
        elif annual_return > 0:
            score += 15
        else:
            score += max(0, 10 + annual_return / 10)  # 亏损扣分
        
        # 夏普比率评分（满分30分）
        if sharpe_ratio > 2:
            score += 30
        elif sharpe_ratio > 1:
            score += 25
        elif sharpe_ratio > 0.5:
            score += 15
        elif sharpe_ratio > 0:
            score += 10
        else:
            score += max(0, 5 + sharpe_ratio)  # 负夏普扣分
        
        # 最大回撤评分（满分20分）- 回撤越小分越高
        if max_drawdown < 10:
            score += 20
        elif max_drawdown < 15:
            score += 18
        elif max_drawdown < 20:
            score += 15
        elif max_drawdown < 30:
            score += 10
        else:
            score += max(0, 10 - (max_drawdown - 30) / 5)
        
        # 胜率评分（满分10分）
        if win_rate > 60:
            score += 10
        elif win_rate > 50:
            score += 8
        elif win_rate > 40:
            score += 5
        else:
            score += max(0, win_rate / 10)
        
        # ==================== 评级 ====================
        
        if score >= 80:
            rating = "A+"
        elif score >= 70:
            rating = "A"
        elif score >= 60:
            rating = "B+"
        elif score >= 50:
            rating = "B"
        elif score >= 40:
            rating = "C"
        else:
            rating = "D"
        
        return {
            # 基础指标
            'initial_capital': self.initial_capital,
            'final_value': round(final_value, 2),
            'total_return': round(total_return, 2),
            'annual_return': round(annual_return, 2),
            'benchmark_return': round(benchmark_return, 2),
            'excess_return': round(excess_return, 2),
            
            # 风险指标
            'volatility': round(volatility, 2),
            'max_drawdown': round(max_drawdown, 2),
            'downside_risk': round(downside_risk, 2),
            
            # 风险调整指标
            'sharpe_ratio': round(sharpe_ratio, 2),
            'sortino_ratio': round(sortino_ratio, 2),
            'calmar_ratio': round(calmar_ratio, 2),
            'information_ratio': round(information_ratio, 2),
            
            # 交易质量
            'win_rate': round(win_rate, 2),
            'profit_loss_ratio': round(profit_loss_ratio, 2),
            'trade_count': len(trades),
            'avg_holding_days': round(avg_holding_days, 1),
            
            # 综合评价
            'composite_score': round(score, 1),
            'rating': rating,
            
            # 详细数据
            'trades': trades,
            'equity_curve': equity_curve
        }