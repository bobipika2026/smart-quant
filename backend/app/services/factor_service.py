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
        """获取基本面因子（PE、PB、ROE等）- 使用AkShare"""
        factors = {}
        
        try:
            # 方法1: 获取估值数据 (百度数据)
            df = await asyncio.to_thread(
                ak.stock_zh_valuation_baidu,
                symbol=stock_code,
                indicator="总市值"
            )
            
            if df is not None and len(df) > 0:
                # 获取最新市值
                latest = df.iloc[-1]
                factors['market_cap'] = float(latest.get('value', 0) or 0)
                
        except Exception as e:
            print(f"[因子] 获取估值数据失败 {stock_code}: {e}")
        
        try:
            # 方法2: 获取个股市净率
            df = await asyncio.to_thread(ak.stock_a_all_pb)
            
            if df is not None and len(df) > 0:
                # 查找对应股票
                stock_df = df[df['code'] == stock_code]
                if len(stock_df) > 0:
                    factors['pb'] = float(stock_df.iloc[-1].get('pb', 0) or 0)
                
        except Exception as e:
            print(f"[因子] 获取市净率失败 {stock_code}: {e}")
        
        try:
            # 方法3: 获取个股信息
            df = await asyncio.to_thread(
                ak.stock_individual_info_em,
                symbol=stock_code
            )
            
            if df is not None and len(df) > 0:
                info = dict(zip(df['item'], df['value']))
                
                # 市值
                total_mv = info.get('总市值', '0').replace('亿', '')
                if total_mv and not factors.get('market_cap'):
                    factors['market_cap'] = float(total_mv)
                
                float_mv = info.get('流通市值', '0').replace('亿', '')
                factors['float_market_cap'] = float(float_mv) if float_mv else None
                
        except Exception as e:
            print(f"[因子] 获取个股信息失败 {stock_code}: {e}")
        
        return factors
    
    @staticmethod
    async def get_financial_factors(stock_code: str) -> Dict:
        """获取财务因子（营收增长、利润增长等）"""
        factors = {}
        
        try:
            # 获取实时行情数据（包含PE、PB等）
            df = await asyncio.to_thread(
                ak.stock_zh_a_spot_em
            )
            
            if df is not None and len(df) > 0:
                stock = df[df['代码'] == stock_code]
                if len(stock) > 0:
                    latest = stock.iloc[0]
                    factors['pe'] = float(latest.get('市盈率-动态', 0) or 0) or None
                    factors['pb'] = float(latest.get('市净率', 0) or 0) or None
                    factors['turnover_rate'] = float(latest.get('换手率', 0) or 0) or None
                    factors['volume_ratio'] = float(latest.get('量比', 0) or 0) or None
                
        except Exception as e:
            print(f"[因子] 获取实时行情失败 {stock_code}: {e}")
        
        return factors
    
    # ==================== 市场因子 ====================
    
    @staticmethod
    async def get_market_factors(stock_code: str, hist_data: pd.DataFrame = None) -> Dict:
        """获取市场因子（市值、换手率、波动率等）"""
        factors = {}
        
        # 从历史数据计算技术相关市场因子
        if hist_data is not None and len(hist_data) > 20:
            close_col = '收盘' if '收盘' in hist_data.columns else 'close'
            high_col = '最高' if '最高' in hist_data.columns else 'high'
            low_col = '最低' if '最低' in hist_data.columns else 'low'
            volume_col = '成交量' if '成交量' in hist_data.columns else 'volume'
            
            # 波动率
            returns = hist_data[close_col].pct_change()
            factors['volatility_20'] = float(returns.rolling(window=20).std().iloc[-1] * np.sqrt(252)) if len(returns) > 20 else None
            
            # 量比
            if len(hist_data) > 5:
                avg_volume_5 = hist_data[volume_col].iloc[-5:].mean()
                today_volume = hist_data[volume_col].iloc[-1]
                factors['volume_ratio'] = float(today_volume / avg_volume_5) if avg_volume_5 > 0 else None
            
            # 换手率（需要流通市值）
            if factors.get('float_market_cap'):
                avg_volume = hist_data[volume_col].iloc[-20:].mean()
                factors['turnover_rate'] = float(avg_volume / (factors['float_market_cap'] * 100000000) * 100) if factors['float_market_cap'] > 0 else None
        
        return factors
    
    # ==================== 情绪因子 ====================
    
    @staticmethod
    async def get_sentiment_factors(stock_code: str) -> Dict:
        """获取情绪因子（北向资金、融资余额等）"""
        factors = {}
        
        try:
            # 获取个股北向资金持股
            df = await asyncio.to_thread(
                ak.stock_hsgt_hold_stock_em,
                market="北向"
            )
            
            if df is not None and len(df) > 0:
                stock_df = df[df['代码'] == stock_code]
                if len(stock_df) > 0:
                    factors['north_flow'] = float(stock_df.iloc[-1].get('北向资金流入', 0) or 0)
                
        except Exception as e:
            print(f"[因子] 获取北向资金失败 {stock_code}: {e}")
        
        try:
            # 获取融资融券数据 (深交所)
            df = await asyncio.to_thread(
                ak.stock_margin_underlying_info_szse
            )
            
            if df is not None and len(df) > 0:
                stock_df = df[df['证券代码'] == stock_code]
                if len(stock_df) > 0:
                    factors['margin_balance'] = float(stock_df.iloc[-1].get('融资余额', 0) or 0)
                
        except Exception as e:
            print(f"[因子] 获取融资余额失败 {stock_code}: {e}")
        
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
        
        try:
            # 均线
            factors['ma_5'] = float(hist_data[close_col].rolling(window=5).mean().iloc[-1])
            factors['ma_10'] = float(hist_data[close_col].rolling(window=10).mean().iloc[-1])
            factors['ma_20'] = float(hist_data[close_col].rolling(window=20).mean().iloc[-1])
            
            # RSI
            delta = hist_data[close_col].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss.replace(0, np.nan)
            factors['rsi_14'] = float((100 - (100 / (1 + rs))).iloc[-1])
            
            # MACD
            ema12 = hist_data[close_col].ewm(span=12, adjust=False).mean()
            ema26 = hist_data[close_col].ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            factors['macd'] = float(macd_line.iloc[-1])
            factors['macd_signal'] = float(signal_line.iloc[-1])
            factors['macd_hist'] = float((macd_line - signal_line).iloc[-1])
            
            # ATR
            tr = np.maximum(
                hist_data[high_col] - hist_data[low_col],
                np.maximum(
                    np.abs(hist_data[high_col] - hist_data[close_col].shift(1)),
                    np.abs(hist_data[low_col] - hist_data[close_col].shift(1))
                )
            )
            factors['atr_14'] = float(pd.Series(tr).rolling(window=14).mean().iloc[-1])
            
            # 布林带
            ma20 = hist_data[close_col].rolling(window=20).mean()
            std20 = hist_data[close_col].rolling(window=20).std()
            factors['boll_upper'] = float((ma20 + 2 * std20).iloc[-1])
            factors['boll_lower'] = float((ma20 - 2 * std20).iloc[-1])
            factors['boll_mid'] = float(ma20.iloc[-1])
            
        except Exception as e:
            print(f"[因子] 计算技术因子失败: {e}")
        
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
                volume_ratio=factors.get('volume_ratio'),
                volatility_20=factors.get('volatility_20'),
                
                # 技术
                ma_5=factors.get('ma_5'),
                ma_10=factors.get('ma_10'),
                ma_20=factors.get('ma_20'),
                rsi_14=factors.get('rsi_14'),
                macd=factors.get('macd'),
                atr_14=factors.get('atr_14'),
                boll_upper=factors.get('boll_upper'),
                boll_lower=factors.get('boll_lower'),
                
                # 情绪
                north_flow=factors.get('north_flow'),
                margin_balance=factors.get('margin_balance'),
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
    
    # ==================== 批量获取 ====================
    
    @staticmethod
    async def get_factors_for_stocks(stock_codes: List[str]) -> List[Dict]:
        """批量获取多只股票的因子"""
        from app.services.data import DataService
        
        results = []
        data_service = DataService()
        
        for code in stock_codes:
            try:
                # 获取历史数据
                hist_data = await data_service.get_stock_history(code)
                
                # 获取因子
                factors = await FactorService.get_all_factors(code, hist_data)
                results.append(factors)
                
                print(f"[因子] {code} 获取成功")
                
            except Exception as e:
                print(f"[因子] {code} 获取失败: {e}")
        
        return results