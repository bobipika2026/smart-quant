"""
因子服务 - 获取各类因子数据
"""
import akshare as ak
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import asyncio
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.factor import FactorValue


class FactorService:
    """因子数据服务"""
    
    # ==================== 基本面因子 ====================
    
    @staticmethod
    async def get_fundamental_factors(stock_code: str) -> Dict:
        """获取基本面因子（PE、PB、ROE等）"""
        factors = {}
        
        try:
            # 使用AkShare获取估值数据
            df = await asyncio.to_thread(
                ak.stock_a_lg_indicator,
                symbol=stock_code
            )
            
            if df is not None and len(df) > 0:
                latest = df.iloc[-1]
                factors['pe'] = latest.get('pe', None)
                factors['pb'] = latest.get('pb', None)
                factors['ps'] = latest.get('ps', None)
                factors['roe'] = latest.get('roe', None)
                factors['debt_ratio'] = latest.get('debt_ratio', None)
                
        except Exception as e:
            print(f"[因子] 获取基本面因子失败 {stock_code}: {e}")
        
        return factors
    
    @staticmethod
    async def get_financial_factors(stock_code: str) -> Dict:
        """获取财务因子（营收增长、利润增长等）"""
        factors = {}
        
        try:
            # 获取财务指标
            df = await asyncio.to_thread(
                ak.stock_financial_analysis_indicator,
                symbol=stock_code
            )
            
            if df is not None and len(df) > 0:
                latest = df.iloc[-1]
                factors['revenue_growth'] = latest.get('营业收入同比增长率(%)', None)
                factors['profit_growth'] = latest.get('净利润同比增长率(%)', None)
                factors['net_profit_margin'] = latest.get('销售净利率(%)', None)
                factors['roa'] = latest.get('总资产净利率ROA(%)', None)
                
        except Exception as e:
            print(f"[因子] 获取财务因子失败 {stock_code}: {e}")
        
        return factors
    
    # ==================== 市场因子 ====================
    
    @staticmethod
    async def get_market_factors(stock_code: str, hist_data: pd.DataFrame = None) -> Dict:
        """获取市场因子（市值、换手率、波动率等）"""
        factors = {}
        
        try:
            # 获取实时行情
            df = await asyncio.to_thread(
                ak.stock_individual_info_em,
                symbol=stock_code
            )
            
            if df is not None and len(df) > 0:
                # 转换为字典
                info = dict(zip(df['item'], df['value']))
                
                # 市值（转换为亿）
                total_mv = info.get('总市值', 0)
                factors['market_cap'] = float(total_mv) / 100000000 if total_mv else None
                
                float_mv = info.get('流通市值', 0)
                factors['float_market_cap'] = float(float_mv) / 100000000 if float_mv else None
                
        except Exception as e:
            print(f"[因子] 获取市值因子失败 {stock_code}: {e}")
        
        # 从历史数据计算波动率
        if hist_data is not None and len(hist_data) > 20:
            close_col = '收盘' if '收盘' in hist_data.columns else 'close'
            returns = hist_data[close_col].pct_change()
            factors['volatility_20'] = returns.rolling(window=20).std().iloc[-1] * np.sqrt(252)  # 年化波动率
            
            # 计算换手率
            volume_col = '成交量' if '成交量' in hist_data.columns else 'volume'
            if factors.get('float_market_cap'):
                avg_volume = hist_data[volume_col].iloc[-20:].mean()
                factors['turnover_rate'] = avg_volume / (factors['float_market_cap'] * 100000000) * 100
        
        return factors
    
    # ==================== 技术因子 ====================
    
    @staticmethod
    def calculate_technical_factors(hist_data: pd.DataFrame) -> Dict:
        """计算技术因子"""
        factors = {}
        
        if hist_data is None or len(hist_data) < 20:
            return factors
        
        close_col = '收盘' if '收盘' in hist_data.columns else 'close'
        high_col = '最高' if '最高' in hist_data.columns else 'high'
        low_col = '最低' if '最低' in hist_data.columns else 'low'
        
        # 均线
        factors['ma_5'] = hist_data[close_col].rolling(window=5).mean().iloc[-1]
        factors['ma_20'] = hist_data[close_col].rolling(window=20).mean().iloc[-1]
        
        # RSI
        delta = hist_data[close_col].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        factors['rsi_14'] = (100 - (100 / (1 + rs))).iloc[-1]
        
        # MACD
        ema12 = hist_data[close_col].ewm(span=12).mean()
        ema26 = hist_data[close_col].ewm(span=26).mean()
        factors['macd'] = (ema12 - ema26).iloc[-1]
        
        # ATR
        tr = np.maximum(
            hist_data[high_col] - hist_data[low_col],
            np.maximum(
                np.abs(hist_data[high_col] - hist_data[close_col].shift(1)),
                np.abs(hist_data[low_col] - hist_data[close_col].shift(1))
            )
        )
        factors['atr_14'] = pd.Series(tr).rolling(window=14).mean().iloc[-1]
        
        return factors
    
    # ==================== 情绪因子 ====================
    
    @staticmethod
    async def get_sentiment_factors(stock_code: str) -> Dict:
        """获取情绪因子（北向资金、融资余额等）"""
        factors = {}
        
        try:
            # 获取个股北向资金
            df = await asyncio.to_thread(
                ak.stock_hsgt_individual_em,
                symbol=stock_code
            )
            
            if df is not None and len(df) > 0:
                factors['north_flow'] = df['北向资金买入额(万元)'].iloc[-1]
                
        except Exception as e:
            print(f"[因子] 获取北向资金失败 {stock_code}: {e}")
        
        return factors
    
    # ==================== 综合获取 ====================
    
    @staticmethod
    async def get_all_factors(stock_code: str, hist_data: pd.DataFrame = None) -> Dict:
        """获取所有因子"""
        all_factors = {
            'stock_code': stock_code,
            'trade_date': datetime.now().strftime('%Y-%m-%d')
        }
        
        # 并行获取各类因子
        fundamental = await FactorService.get_fundamental_factors(stock_code)
        financial = await FactorService.get_financial_factors(stock_code)
        market = await FactorService.get_market_factors(stock_code, hist_data)
        sentiment = await FactorService.get_sentiment_factors(stock_code)
        technical = FactorService.calculate_technical_factors(hist_data)
        
        all_factors.update(fundamental)
        all_factors.update(financial)
        all_factors.update(market)
        all_factors.update(sentiment)
        all_factors.update(technical)
        
        return all_factors
    
    # ==================== 因子存储 ====================
    
    @staticmethod
    async def save_factors(factors: Dict) -> bool:
        """保存因子到数据库"""
        db: Session = SessionLocal()
        try:
            factor_value = FactorValue(
                trade_date=factors.get('trade_date'),
                stock_code=factors.get('stock_code'),
                
                # 基本面
                pe=factors.get('pe'),
                pb=factors.get('pb'),
                ps=factors.get('ps'),
                roe=factors.get('roe'),
                roa=factors.get('roa'),
                debt_ratio=factors.get('debt_ratio'),
                net_profit_margin=factors.get('net_profit_margin'),
                revenue_growth=factors.get('revenue_growth'),
                profit_growth=factors.get('profit_growth'),
                
                # 市场
                market_cap=factors.get('market_cap'),
                float_market_cap=factors.get('float_market_cap'),
                turnover_rate=factors.get('turnover_rate'),
                volatility_20=factors.get('volatility_20'),
                
                # 技术
                ma_5=factors.get('ma_5'),
                ma_20=factors.get('ma_20'),
                rsi_14=factors.get('rsi_14'),
                macd=factors.get('macd'),
                atr_14=factors.get('atr_14'),
                
                # 情绪
                north_flow=factors.get('north_flow'),
            )
            
            db.add(factor_value)
            db.commit()
            return True
            
        except Exception as e:
            db.rollback()
            print(f"[因子] 保存失败: {e}")
            return False
        finally:
            db.close()