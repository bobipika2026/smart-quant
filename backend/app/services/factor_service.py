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
            # 获取估值数据 (百度数据)
            df = await asyncio.to_thread(
                ak.stock_zh_valuation_baidu,
                symbol=stock_code,
                indicator="总市值"
            )
            
            if df is not None and len(df) > 0:
                # 获取最新市值
                latest = df.iloc[-1]
                mv = latest.get('value', 0)
                if mv:
                    factors['market_cap'] = float(mv)
                
        except Exception as e:
            print(f"[因子] 获取估值数据失败 {stock_code}: {e}")
        
        try:
            # 获取个股市净率
            df = await asyncio.to_thread(ak.stock_a_all_pb)
            
            if df is not None and len(df) > 0:
                # 查找对应股票 - 字段名可能是 'code' 或 '代码'
                code_col = 'code' if 'code' in df.columns else '代码'
                stock_df = df[df[code_col] == stock_code]
                if len(stock_df) > 0:
                    pb_col = 'pb' if 'pb' in df.columns else '市净率'
                    factors['pb'] = float(stock_df.iloc[-1].get(pb_col, 0) or 0)
                
        except Exception as e:
            print(f"[因子] 获取市净率失败 {stock_code}: {e}")
        
        try:
            # 获取个股信息
            df = await asyncio.to_thread(
                ak.stock_individual_info_em,
                symbol=stock_code
            )
            
            if df is not None and len(df) > 0:
                info = dict(zip(df['item'], df['value']))
                
                # 市值 - 处理带"亿"的字符串
                total_mv = info.get('总市值', '0')
                if total_mv and not factors.get('market_cap'):
                    if isinstance(total_mv, str):
                        total_mv = total_mv.replace('亿', '').strip()
                    factors['market_cap'] = float(total_mv) if total_mv else None
                
                float_mv = info.get('流通市值', '0')
                if float_mv:
                    if isinstance(float_mv, str):
                        float_mv = float_mv.replace('亿', '').strip()
                    factors['float_market_cap'] = float(float_mv) if float_mv else None
                
                # PE
                pe = info.get('市盈率', info.get('市盈率(动态)', '0'))
                if pe:
                    if isinstance(pe, str):
                        pe = pe.strip()
                    try:
                        factors['pe'] = float(pe)
                    except:
                        pass
                
                print(f"[因子] 个股信息获取成功 {stock_code}")
                
        except Exception as e:
            print(f"[因子] 获取个股信息失败 {stock_code}: {e}")
        
        return factors
    
    @staticmethod
    async def get_financial_factors(stock_code: str) -> Dict:
        """获取财务因子（营收增长、利润增长等）"""
        factors = {}
        
        # 尝试多个接口获取实时行情
        apis_to_try = [
            # 方法1: 东方财富实时行情
            ('stock_zh_a_spot_em', {}),
            # 方法2: 新浪实时行情
            ('stock_zh_a_spot', {}),
        ]
        
        for api_name, api_params in apis_to_try:
            try:
                if api_name == 'stock_zh_a_spot_em':
                    df = await asyncio.to_thread(ak.stock_zh_a_spot_em)
                    if df is not None and len(df) > 0:
                        stock = df[df['代码'] == stock_code]
                        if len(stock) > 0:
                            latest = stock.iloc[0]
                            factors['pe'] = float(latest.get('市盈率-动态', 0) or 0) or None
                            factors['pb'] = float(latest.get('市净率', 0) or 0) or None
                            factors['turnover_rate'] = float(latest.get('换手率', 0) or 0) or None
                            factors['volume_ratio'] = float(latest.get('量比', 0) or 0) or None
                            if factors.get('market_cap') is None:
                                mv = latest.get('总市值', 0)
                                factors['market_cap'] = float(mv) / 100000000 if mv else None
                            print(f"[因子] 实时行情获取成功 {stock_code}")
                            break
                elif api_name == 'stock_zh_a_spot':
                    # 新浪接口作为备用
                    df = await asyncio.to_thread(ak.stock_zh_a_spot)
                    if df is not None and len(df) > 0:
                        stock = df[df['code'] == stock_code]
                        if len(stock) > 0:
                            latest = stock.iloc[0]
                            # 新浪接口字段不同，尝试映射
                            pass  # 简化处理，主要依赖东方财富
                
            except Exception as e:
                print(f"[因子] {api_name} 接口失败 {stock_code}: {e}")
                continue
        
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
        
        # 北向资金持股
        try:
            df = await asyncio.to_thread(
                ak.stock_hsgt_hold_stock_em,
                market="北向"
            )
            
            if df is not None and len(df) > 0:
                stock_df = df[df['代码'] == stock_code]
                if len(stock_df) > 0:
                    latest = stock_df.iloc[-1]
                    # 使用正确的字段名
                    factors['north_holdings'] = float(latest.get('今日持股-市值', 0) or 0)
                    factors['north_holdings_ratio'] = float(latest.get('今日持股-占流通股比', 0) or 0)
                    factors['north_5d_change'] = float(latest.get('5日增持估计-市值增幅', 0) or 0)
                    print(f"[因子] 北向资金获取成功 {stock_code}")
                
        except Exception as e:
            print(f"[因子] 获取北向资金失败 {stock_code}: {e}")
        
        # 融资融券 - 尝试深交所
        try:
            df = await asyncio.to_thread(
                ak.stock_margin_detail_szse,
                date=datetime.now().strftime('%Y%m%d')
            )
            
            if df is not None and len(df) > 0:
                stock_df = df[df['证券代码'] == stock_code]
                if len(stock_df) > 0:
                    latest = stock_df.iloc[-1]
                    # 融资余额（单位：元，转换为亿）
                    margin = latest.get('融资余额', 0)
                    factors['margin_balance'] = float(margin) / 100000000 if margin else None
                    print(f"[因子] 融资余额获取成功 {stock_code}")
                
        except Exception as e:
            # 深交所失败，尝试其他接口
            print(f"[因子] 深交所融资融券失败 {stock_code}: {e}")
        
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
        volume_col = '成交量' if '成交量' in hist_data.columns else 'volume'
        
        try:
            # ========== 均线系列 ==========
            factors['ma_5'] = float(hist_data[close_col].rolling(window=5).mean().iloc[-1])
            factors['ma_10'] = float(hist_data[close_col].rolling(window=10).mean().iloc[-1])
            factors['ma_20'] = float(hist_data[close_col].rolling(window=20).mean().iloc[-1])
            factors['ma_60'] = float(hist_data[close_col].rolling(window=60).mean().iloc[-1]) if len(hist_data) >= 60 else None
            
            # ========== RSI ==========
            delta = hist_data[close_col].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss.replace(0, np.nan)
            factors['rsi_14'] = float((100 - (100 / (1 + rs))).iloc[-1])
            
            # ========== MACD ==========
            ema12 = hist_data[close_col].ewm(span=12, adjust=False).mean()
            ema26 = hist_data[close_col].ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            factors['macd'] = float(macd_line.iloc[-1])
            factors['macd_signal'] = float(signal_line.iloc[-1])
            factors['macd_hist'] = float((macd_line - signal_line).iloc[-1])
            
            # ========== ATR ==========
            tr = np.maximum(
                hist_data[high_col] - hist_data[low_col],
                np.maximum(
                    np.abs(hist_data[high_col] - hist_data[close_col].shift(1)),
                    np.abs(hist_data[low_col] - hist_data[close_col].shift(1))
                )
            )
            factors['atr_14'] = float(pd.Series(tr).rolling(window=14).mean().iloc[-1])
            
            # ========== 布林带 ==========
            ma20 = hist_data[close_col].rolling(window=20).mean()
            std20 = hist_data[close_col].rolling(window=20).std()
            factors['boll_upper'] = float((ma20 + 2 * std20).iloc[-1])
            factors['boll_lower'] = float((ma20 - 2 * std20).iloc[-1])
            factors['boll_mid'] = float(ma20.iloc[-1])
            factors['boll_width'] = float((factors['boll_upper'] - factors['boll_lower']) / factors['boll_mid'] * 100)  # 带宽
            
            # ========== KDJ ==========
            low_min = hist_data[low_col].rolling(window=9).min()
            high_max = hist_data[high_col].rolling(window=9).max()
            rsv = (hist_data[close_col] - low_min) / (high_max - low_min) * 100
            rsv = rsv.fillna(50)
            factors['kdj_k'] = float(rsv.ewm(com=2, adjust=False).mean().iloc[-1])
            factors['kdj_d'] = float(pd.Series(factors['kdj_k']).ewm(com=2, adjust=False).mean().iloc[-1] if factors['kdj_k'] else 50)
            factors['kdj_j'] = 3 * factors['kdj_k'] - 2 * factors['kdj_d']
            
            # ========== CCI ==========
            tp = (hist_data[high_col] + hist_data[low_col] + hist_data[close_col]) / 3
            ma_tp = tp.rolling(window=14).mean()
            md_tp = tp.rolling(window=14).apply(lambda x: np.abs(x - x.mean()).mean())
            factors['cci_14'] = float(((tp - ma_tp) / (0.015 * md_tp)).iloc[-1]) if md_tp.iloc[-1] > 0 else 0
            
            # ========== WR 威廉指标 ==========
            high_n = hist_data[high_col].rolling(window=14).max()
            low_n = hist_data[low_col].rolling(window=14).min()
            factors['wr_14'] = float(((high_n - hist_data[close_col]) / (high_n - low_n) * -100).iloc[-1]) if (high_n - low_n).iloc[-1] > 0 else -50
            
            # ========== DMI ==========
            up_move = hist_data[high_col] - hist_data[high_col].shift(1)
            down_move = hist_data[low_col].shift(1) - hist_data[low_col]
            plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
            minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
            
            atr = pd.Series(tr).rolling(window=14).mean()
            plus_di = 100 * pd.Series(plus_dm).rolling(window=14).mean() / atr
            minus_di = 100 * pd.Series(minus_dm).rolling(window=14).mean() / atr
            dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
            
            factors['dmi_plus'] = float(plus_di.iloc[-1]) if not np.isnan(plus_di.iloc[-1]) else None
            factors['dmi_minus'] = float(minus_di.iloc[-1]) if not np.isnan(minus_di.iloc[-1]) else None
            factors['dmi_adx'] = float(dx.iloc[-1]) if not np.isnan(dx.iloc[-1]) else None
            
            # ========== OBV ==========
            obv_series = pd.Series(np.where(hist_data[close_col] > hist_data[close_col].shift(1), 1, -1) * hist_data[volume_col]).cumsum()
            factors['obv'] = float(obv_series.iloc[-1]) if len(obv_series) > 0 else None
            
            # ========== BIAS 乖离率 ==========
            factors['bias_6'] = float(((hist_data[close_col] - hist_data[close_col].rolling(window=6).mean()) / hist_data[close_col].rolling(window=6).mean() * 100).iloc[-1])
            factors['bias_12'] = float(((hist_data[close_col] - hist_data[close_col].rolling(window=12).mean()) / hist_data[close_col].rolling(window=12).mean() * 100).iloc[-1])
            factors['bias_24'] = float(((hist_data[close_col] - hist_data[close_col].rolling(window=24).mean()) / hist_data[close_col].rolling(window=24).mean() * 100).iloc[-1]) if len(hist_data) >= 24 else None
            
            # ========== VWAP ==========
            typical_price = (hist_data[high_col] + hist_data[low_col] + hist_data[close_col]) / 3
            cumulative_tp_volume = (typical_price * hist_data[volume_col]).cumsum()
            cumulative_volume = hist_data[volume_col].cumsum()
            factors['vwap'] = float((cumulative_tp_volume / cumulative_volume).iloc[-1]) if cumulative_volume.iloc[-1] > 0 else None
            
            # ========== Aroon 阿隆指标 ==========
            period = 14
            aroon_up = 100 * (hist_data[high_col].rolling(window=period).apply(lambda x: x.argmax()) / period)
            aroon_down = 100 * (hist_data[low_col].rolling(window=period).apply(lambda x: x.argmin()) / period)
            factors['aroon_up'] = float(aroon_up.iloc[-1]) if not np.isnan(aroon_up.iloc[-1]) else None
            factors['aroon_down'] = float(aroon_down.iloc[-1]) if not np.isnan(aroon_down.iloc[-1]) else None
            factors['aroon_osc'] = factors['aroon_up'] - factors['aroon_down'] if factors['aroon_up'] is not None and factors['aroon_down'] is not None else None
            
            # ========== MOM 动量 ==========
            factors['mom_10'] = float((hist_data[close_col] - hist_data[close_col].shift(10)).iloc[-1]) if len(hist_data) >= 10 else None
            
            # ========== ROC 变动率 ==========
            factors['roc_10'] = float(((hist_data[close_col] - hist_data[close_col].shift(10)) / hist_data[close_col].shift(10) * 100).iloc[-1]) if len(hist_data) >= 10 else None
            
            # ========== 唐奇安通道 ==========
            factors['donchian_high'] = float(hist_data[high_col].rolling(window=20).max().iloc[-1])
            factors['donchian_low'] = float(hist_data[low_col].rolling(window=20).min().iloc[-1])
            factors['donchian_mid'] = (factors['donchian_high'] + factors['donchian_low']) / 2
            
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
        
        # 获取行业信息
        try:
            df = await asyncio.to_thread(ak.stock_individual_info_em, symbol=stock_code)
            if df is not None and len(df) > 0:
                info = dict(zip(df['item'], df['value']))
                all_factors['industry'] = info.get('行业')
        except:
            pass
        
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
                
                # 技术因子 - 均线
                ma_5=factors.get('ma_5'),
                ma_10=factors.get('ma_10'),
                ma_20=factors.get('ma_20'),
                ma_60=factors.get('ma_60'),
                
                # 技术因子 - 趋势
                rsi_14=factors.get('rsi_14'),
                macd=factors.get('macd'),
                macd_signal=factors.get('macd_signal'),
                macd_hist=factors.get('macd_hist'),
                atr_14=factors.get('atr_14'),
                
                # 技术因子 - 布林带
                boll_upper=factors.get('boll_upper'),
                boll_lower=factors.get('boll_lower'),
                boll_mid=factors.get('boll_mid'),
                boll_width=factors.get('boll_width'),
                
                # 技术因子 - KDJ
                kdj_k=factors.get('kdj_k'),
                kdj_d=factors.get('kdj_d'),
                kdj_j=factors.get('kdj_j'),
                
                # 技术因子 - 其他
                cci_14=factors.get('cci_14'),
                wr_14=factors.get('wr_14'),
                dmi_plus=factors.get('dmi_plus'),
                dmi_minus=factors.get('dmi_minus'),
                dmi_adx=factors.get('dmi_adx'),
                obv=factors.get('obv'),
                bias_6=factors.get('bias_6'),
                bias_12=factors.get('bias_12'),
                bias_24=factors.get('bias_24'),
                vwap=factors.get('vwap'),
                aroon_up=factors.get('aroon_up'),
                aroon_down=factors.get('aroon_down'),
                aroon_osc=factors.get('aroon_osc'),
                mom_10=factors.get('mom_10'),
                roc_10=factors.get('roc_10'),
                donchian_high=factors.get('donchian_high'),
                donchian_low=factors.get('donchian_low'),
                donchian_mid=factors.get('donchian_mid'),
                
                # 情绪
                north_holdings=factors.get('north_holdings'),
                north_holdings_ratio=factors.get('north_holdings_ratio'),
                north_5d_change=factors.get('north_5d_change'),
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