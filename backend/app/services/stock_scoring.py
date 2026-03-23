"""
股票多因子评分服务

功能：
1. 财务因子评分（盈利、成长、财务健康）
2. 市场因子评分（估值、流动性、资金面）
3. 技术因子评分（趋势、动量）
4. 综合评分计算
5. 股票池生成（Top 50）
"""
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import sqlite3


class StockScoringService:
    """股票多因子评分服务"""
    
    # 数据目录
    FINANCIAL_DIR = "data_cache/financial"
    DAY_CACHE_DIR = "data_cache/day"
    
    # ==================== 因子权重配置 ====================
    
    # 财务因子权重（总权重40%）
    FINANCIAL_WEIGHTS = {
        'roe': 0.08,              # ROE
        'net_margin': 0.04,       # 净利率
        'gross_margin': 0.03,     # 毛利率
        'revenue_growth': 0.05,   # 营收增长
        'profit_growth': 0.05,    # 利润增长
        'roe_growth': 0.05,       # ROE增长
        'debt_ratio': 0.05,       # 负债率（反向）
        'current_ratio': 0.03,    # 流动比率
        'cash_flow': 0.02,        # 现金流
    }
    
    # 市场因子权重（总权重30%）
    MARKET_WEIGHTS = {
        'pe': 0.05,               # PE（反向）
        'pb': 0.05,               # PB（反向）
        'ps': 0.05,               # PS（反向）
        'turnover': 0.03,         # 换手率
        'market_cap': 0.04,       # 市值规模
        'volume_ratio': 0.03,     # 量比
        'north_holding': 0.05,    # 北向持股
    }
    
    # 技术因子权重（总权重20%）
    TECHNICAL_WEIGHTS = {
        'ma_trend': 0.05,         # 均线趋势
        'macd': 0.03,             # MACD
        'boll_position': 0.02,    # 布林位置
        'rsi': 0.03,              # RSI强度
        'breakout': 0.04,         # 突破形态
        'volume_trend': 0.03,     # 量价配合
    }
    
    # 行业因子权重（总权重10%）
    INDUSTRY_WEIGHTS = {
        'industry_trend': 0.05,   # 行业景气
        'industry_rotation': 0.05, # 行业轮动
    }
    
    # ==================== 评分阈值配置 ====================
    
    # 财务因子阈值
    ROE_THRESHOLDS = [(20, 100), (15, 80), (10, 60), (5, 40), (0, 20)]
    NET_MARGIN_THRESHOLDS = [(20, 100), (10, 70), (5, 50), (0, 30)]
    GROSS_MARGIN_THRESHOLDS = [(40, 100), (25, 70), (15, 50), (0, 30)]
    GROWTH_THRESHOLDS = [(30, 100), (15, 70), (5, 50), (0, 30), (-100, 10)]
    DEBT_THRESHOLDS = [(30, 100), (50, 70), (70, 40), (100, 20)]  # 反向
    CURRENT_RATIO_THRESHOLDS = [(2, 100), (1.5, 70), (1, 50), (0, 20)]
    CASH_FLOW_THRESHOLDS = [(5, 100), (2, 70), (0, 50), (-100, 20)]
    
    # 市场因子阈值
    PE_THRESHOLDS = [(10, 100), (20, 70), (30, 50), (50, 30), (200, 10)]  # 反向
    PB_THRESHOLDS = [(1, 100), (2, 70), (3, 50), (5, 30), (20, 10)]       # 反向
    PS_THRESHOLDS = [(1, 100), (3, 70), (5, 50), (10, 30), (50, 10)]      # 反向
    TURNOVER_THRESHOLDS = [(5, 100), (3, 80), (1, 60), (0.5, 40)]
    MARKET_CAP_THRESHOLDS = [(500, 100), (200, 80), (100, 60), (50, 40), (0, 20)]  # 亿
    VOLUME_RATIO_THRESHOLDS = [(1.5, 100), (1, 80), (0.5, 60), (0, 40)]
    NORTH_HOLDING_THRESHOLDS = [(5, 100), (2, 70), (1, 50), (0, 20)]
    
    # 技术因子阈值
    RSI_THRESHOLDS = [(70, 80), (50, 100), (30, 60), (0, 40)]  # 50-70最佳
    
    def __init__(self):
        self.db_path = 'smart_quant.db'
        
    # ==================== 评分函数 ====================
    
    def _score_threshold(self, value: float, thresholds: List[Tuple[float, float]], 
                         reverse: bool = False, default_score: float = 50) -> float:
        """
        阶梯式评分
        
        Args:
            value: 因子值
            thresholds: [(阈值, 分数), ...]
            reverse: 是否反向评分（如负债率、PE等越低越好）
            default_score: 数据缺失时的默认分数
        """
        if pd.isna(value) or value is None:
            return float(default_score)
        
        # 转为float避免numpy类型问题
        try:
            value = float(value)
        except:
            return float(default_score)
        
        if reverse:
            # 反向评分：值越低分数越高
            for threshold, score in thresholds:
                if value <= threshold:
                    return float(score)
            return float(thresholds[-1][1])
        else:
            # 正向评分：值越高分数越高
            for threshold, score in thresholds:
                if value >= threshold:
                    return float(score)
            return float(thresholds[-1][1])
    
    def _load_financial_data(self, stock_code: str) -> Optional[Dict]:
        """加载财务数据（最新一期）"""
        file_path = os.path.join(self.FINANCIAL_DIR, f"{stock_code}_fina_indicator.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            
            # 取最新一期（按end_date排序）
            df = df.sort_values('end_date', ascending=False)
            latest = df.iloc[0]
            
            return {
                'roe': latest.get('roe'),
                'roa': latest.get('roa'),
                'net_margin': latest.get('netprofit_margin'),
                'gross_margin': latest.get('grossprofit_margin'),
                'revenue_growth': latest.get('tr_yoy'),
                'profit_growth': latest.get('netprofit_yoy'),
                'roe_growth': latest.get('roe_yoy'),
                'debt_ratio': latest.get('debt_to_assets'),
                'current_ratio': latest.get('current_ratio'),
                'cash_flow': latest.get('ocfps'),
                'eps': latest.get('eps'),
            }
        except Exception as e:
            return None
    
    def _load_daily_basic_data(self, stock_code: str) -> Optional[Dict]:
        """加载每日基本面数据（最新一日）"""
        file_path = os.path.join(self.FINANCIAL_DIR, f"{stock_code}_daily_basic.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            
            # 取最新一日
            df = df.sort_values('trade_date', ascending=False)
            latest = df.iloc[0]
            
            return {
                'trade_date': latest.get('trade_date'),
                'close': latest.get('close'),
                'pe': latest.get('pe_ttm'),
                'pb': latest.get('pb'),
                'ps': latest.get('ps_ttm'),
                'market_cap': latest.get('total_mv'),  # 万元
                'turnover': latest.get('turnover_rate'),
                'volume_ratio': latest.get('volume_ratio'),
            }
        except Exception as e:
            return None
    
    def _load_day_kline(self, stock_code: str, days: int = 120) -> Optional[pd.DataFrame]:
        """加载日K线数据"""
        file_path = os.path.join(self.DAY_CACHE_DIR, f"{stock_code}_day.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            
            # 取最近N天
            df = df.sort_values('trade_date', ascending=False).head(days)
            df = df.sort_values('trade_date', ascending=True)
            
            return df
        except Exception as e:
            return None
    
    # ==================== 因子评分计算 ====================
    
    def _score_financial(self, fina_data: Dict) -> Dict:
        """计算财务因子评分"""
        scores = {}
        
        # ROE
        roe = fina_data.get('roe')
        scores['roe'] = {
            'value': _to_serializable(roe),
            'score': self._score_threshold(roe, self.ROE_THRESHOLDS),
            'weight': self.FINANCIAL_WEIGHTS['roe']
        }
        
        # 净利率
        net_margin = fina_data.get('net_margin')
        scores['net_margin'] = {
            'value': _to_serializable(net_margin),
            'score': self._score_threshold(net_margin, self.NET_MARGIN_THRESHOLDS),
            'weight': self.FINANCIAL_WEIGHTS['net_margin']
        }
        
        # 毛利率
        gross_margin = fina_data.get('gross_margin')
        scores['gross_margin'] = {
            'value': _to_serializable(gross_margin),
            'score': self._score_threshold(gross_margin, self.GROSS_MARGIN_THRESHOLDS),
            'weight': self.FINANCIAL_WEIGHTS['gross_margin']
        }
        
        # 营收增长
        revenue_growth = fina_data.get('revenue_growth')
        scores['revenue_growth'] = {
            'value': _to_serializable(revenue_growth),
            'score': self._score_threshold(revenue_growth, self.GROWTH_THRESHOLDS),
            'weight': self.FINANCIAL_WEIGHTS['revenue_growth']
        }
        
        # 利润增长
        profit_growth = fina_data.get('profit_growth')
        scores['profit_growth'] = {
            'value': _to_serializable(profit_growth),
            'score': self._score_threshold(profit_growth, self.GROWTH_THRESHOLDS),
            'weight': self.FINANCIAL_WEIGHTS['profit_growth']
        }
        
        # ROE增长
        roe_growth = fina_data.get('roe_growth')
        scores['roe_growth'] = {
            'value': _to_serializable(roe_growth),
            'score': self._score_threshold(roe_growth, self.GROWTH_THRESHOLDS),
            'weight': self.FINANCIAL_WEIGHTS['roe_growth']
        }
        
        # 负债率（反向）
        debt_ratio = fina_data.get('debt_ratio')
        # 原始数据已经是百分比形式（如91.0表示91%）
        scores['debt_ratio'] = {
            'value': _to_serializable(debt_ratio),
            'score': self._score_threshold(debt_ratio, self.DEBT_THRESHOLDS, reverse=True),
            'weight': self.FINANCIAL_WEIGHTS['debt_ratio']
        }
        
        # 流动比率
        current_ratio = fina_data.get('current_ratio')
        scores['current_ratio'] = {
            'value': _to_serializable(current_ratio),
            'score': self._score_threshold(current_ratio, self.CURRENT_RATIO_THRESHOLDS),
            'weight': self.FINANCIAL_WEIGHTS['current_ratio']
        }
        
        # 现金流
        cash_flow = fina_data.get('cash_flow')
        scores['cash_flow'] = {
            'value': _to_serializable(cash_flow),
            'score': self._score_threshold(cash_flow, self.CASH_FLOW_THRESHOLDS),
            'weight': self.FINANCIAL_WEIGHTS['cash_flow']
        }
        
        return scores
    
    def _score_market(self, daily_data: Dict) -> Dict:
        """计算市场因子评分"""
        scores = {}
        
        # PE（反向）
        pe = daily_data.get('pe')
        scores['pe'] = {
            'value': _to_serializable(pe),
            'score': self._score_threshold(pe, self.PE_THRESHOLDS, reverse=True),
            'weight': self.MARKET_WEIGHTS['pe']
        }
        
        # PB（反向）
        pb = daily_data.get('pb')
        scores['pb'] = {
            'value': _to_serializable(pb),
            'score': self._score_threshold(pb, self.PB_THRESHOLDS, reverse=True),
            'weight': self.MARKET_WEIGHTS['pb']
        }
        
        # PS（反向）
        ps = daily_data.get('ps')
        scores['ps'] = {
            'value': _to_serializable(ps),
            'score': self._score_threshold(ps, self.PS_THRESHOLDS, reverse=True),
            'weight': self.MARKET_WEIGHTS['ps']
        }
        
        # 换手率
        turnover = daily_data.get('turnover')
        scores['turnover'] = {
            'value': _to_serializable(turnover),
            'score': self._score_threshold(turnover, self.TURNOVER_THRESHOLDS),
            'weight': self.MARKET_WEIGHTS['turnover']
        }
        
        # 市值规模（亿）
        market_cap = daily_data.get('market_cap')
        if market_cap is not None:
            market_cap = market_cap / 10000  # 万元转亿
        scores['market_cap'] = {
            'value': _to_serializable(market_cap),
            'score': self._score_threshold(market_cap, self.MARKET_CAP_THRESHOLDS),
            'weight': self.MARKET_WEIGHTS['market_cap']
        }
        
        # 量比
        volume_ratio = daily_data.get('volume_ratio')
        scores['volume_ratio'] = {
            'value': _to_serializable(volume_ratio),
            'score': self._score_threshold(volume_ratio, self.VOLUME_RATIO_THRESHOLDS),
            'weight': self.MARKET_WEIGHTS['volume_ratio']
        }
        
        # 北向持股（暂无数据，默认50分）
        scores['north_holding'] = {
            'value': None,
            'score': 50,
            'weight': self.MARKET_WEIGHTS['north_holding']
        }
        
        return scores
    
    def _score_technical(self, kline_df: pd.DataFrame, daily_data: Dict) -> Dict:
        """计算技术因子评分"""
        scores = {}
        
        if kline_df is None or len(kline_df) < 60:
            # 数据不足，给默认分
            for key, weight in self.TECHNICAL_WEIGHTS.items():
                scores[key] = {'value': None, 'score': 50, 'weight': weight}
            return scores
        
        close = kline_df['close'].values
        
        # 1. 均线趋势
        ma5 = pd.Series(close).rolling(5).mean().values
        ma10 = pd.Series(close).rolling(10).mean().values
        ma20 = pd.Series(close).rolling(20).mean().values
        ma60 = pd.Series(close).rolling(60).mean().values
        
        # 多头排列得分
        ma_trend_score = 0
        if not pd.isna(ma5[-1]) and not pd.isna(ma10[-1]):
            if ma5[-1] > ma10[-1]:
                ma_trend_score += 25
        if not pd.isna(ma10[-1]) and not pd.isna(ma20[-1]):
            if ma10[-1] > ma20[-1]:
                ma_trend_score += 25
        if not pd.isna(ma20[-1]) and not pd.isna(ma60[-1]):
            if ma20[-1] > ma60[-1]:
                ma_trend_score += 25
        if not pd.isna(ma5[-1]) and not pd.isna(ma20[-1]):
            if ma5[-1] > ma20[-1]:
                ma_trend_score += 25
        
        scores['ma_trend'] = {
            'value': float(ma_trend_score),
            'score': float(ma_trend_score),
            'weight': self.TECHNICAL_WEIGHTS['ma_trend']
        }
        
        # 2. MACD
        # 简化MACD计算
        ema12 = pd.Series(close).ewm(span=12).mean()
        ema26 = pd.Series(close).ewm(span=26).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9).mean()
        macd_hist = (dif - dea) * 2
        
        macd_score = 50
        if dif.iloc[-1] > dea.iloc[-1]:  # 金叉
            macd_score = 80
            if macd_hist.iloc[-1] > macd_hist.iloc[-2]:  # 柱状放大
                macd_score = 100
        elif dif.iloc[-1] < dea.iloc[-1]:  # 死叉
            macd_score = 30
        
        scores['macd'] = {
            'value': float(macd_hist.iloc[-1]) if not pd.isna(macd_hist.iloc[-1]) else 0,
            'score': float(macd_score),
            'weight': self.TECHNICAL_WEIGHTS['macd']
        }
        
        # 3. 布林带位置
        ma20_val = pd.Series(close).rolling(20).mean()
        std20 = pd.Series(close).rolling(20).std()
        boll_upper = ma20_val + 2 * std20
        boll_lower = ma20_val - 2 * std20
        
        boll_score = 50
        if close[-1] > ma20_val.iloc[-1]:
            boll_score = 70
        if close[-1] > boll_upper.iloc[-1]:
            boll_score = 90  # 突破上轨
        
        scores['boll_position'] = {
            'value': float((close[-1] - ma20_val.iloc[-1]) / std20.iloc[-1]) if std20.iloc[-1] > 0 else 0.0,
            'score': float(boll_score),
            'weight': self.TECHNICAL_WEIGHTS['boll_position']
        }
        
        # 4. RSI
        delta = pd.Series(close).diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        rsi_val = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else 50.0
        rsi_score = self._score_threshold(rsi_val, self.RSI_THRESHOLDS)
        
        scores['rsi'] = {
            'value': float(rsi_val),
            'score': float(rsi_score),
            'weight': self.TECHNICAL_WEIGHTS['rsi']
        }
        
        # 5. 突破形态
        breakout_score = 50
        if close[-1] > ma20[-1] and close[-2] <= ma20[-2]:
            breakout_score = 80  # 突破MA20
        if close[-1] > ma60[-1] and close[-2] <= ma60[-2]:
            breakout_score = 100  # 突破MA60
        
        scores['breakout'] = {
            'value': float(close[-1] / ma20[-1] - 1) if ma20[-1] > 0 else 0.0,
            'score': float(breakout_score),
            'weight': self.TECHNICAL_WEIGHTS['breakout']
        }
        
        # 6. 量价配合
        volume_score = 50
        volume = kline_df['vol'].values if 'vol' in kline_df.columns else None
        vol_change_val = 1.0
        if volume is not None and len(volume) >= 2:
            vol_change = volume[-1] / volume[-2] if volume[-2] > 0 else 1
            vol_change_val = float(vol_change)
            price_change = close[-1] / close[-2] - 1
            
            if price_change > 0 and vol_change > 1.2:  # 放量上涨
                volume_score = 90
            elif price_change > 0 and vol_change < 0.8:  # 缩量上涨
                volume_score = 70
            elif price_change < 0 and vol_change < 0.8:  # 缩量下跌
                volume_score = 60
        
        scores['volume_trend'] = {
            'value': vol_change_val,
            'score': float(volume_score),
            'weight': self.TECHNICAL_WEIGHTS['volume_trend']
        }
        
        return scores
    
    def _score_industry(self, stock_code: str) -> Dict:
        """计算行业因子评分（简化版）"""
        # 暂时给默认分，后续可接入行业数据
        scores = {
            'industry_trend': {
                'value': None,
                'score': 50,
                'weight': self.INDUSTRY_WEIGHTS['industry_trend']
            },
            'industry_rotation': {
                'value': None,
                'score': 50,
                'weight': self.INDUSTRY_WEIGHTS['industry_rotation']
            }
        }
        return scores
    
    # ==================== 综合评分 ====================
    
    def calculate_score(self, stock_code: str) -> Optional[Dict]:
        """
        计算单只股票的综合评分
        
        Returns:
            {
                'stock_code': str,
                'stock_name': str,
                'total_score': float,
                'grade': str,
                'scores': {
                    'financial': float,
                    'market': float,
                    'technical': float,
                    'industry': float
                },
                'details': {...}
            }
        """
        # 加载数据
        fina_data = self._load_financial_data(stock_code)
        daily_data = self._load_daily_basic_data(stock_code)
        kline_df = self._load_day_kline(stock_code, 120)
        
        if fina_data is None or daily_data is None:
            return None
        
        # 计算各维度评分
        financial_scores = self._score_financial(fina_data)
        market_scores = self._score_market(daily_data)
        technical_scores = self._score_technical(kline_df, daily_data)
        industry_scores = self._score_industry(stock_code)
        
        # 计算维度总分
        financial_total = sum(s['score'] * s['weight'] for s in financial_scores.values())
        market_total = sum(s['score'] * s['weight'] for s in market_scores.values())
        technical_total = sum(s['score'] * s['weight'] for s in technical_scores.values())
        industry_total = sum(s['score'] * s['weight'] for s in industry_scores.values())
        
        # 综合评分
        total_score = financial_total + market_total + technical_total + industry_total
        
        # 评级
        if total_score >= 85:
            grade = 'A+'
        elif total_score >= 75:
            grade = 'A'
        elif total_score >= 65:
            grade = 'B+'
        elif total_score >= 55:
            grade = 'B'
        elif total_score >= 45:
            grade = 'C'
        else:
            grade = 'D'
        
        # 获取股票名称
        stock_name = self._get_stock_name(stock_code)
        
        result = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'total_score': round(float(total_score), 1),
            'grade': grade,
            'scores': {
                'financial': round(float(financial_total), 1),
                'market': round(float(market_total), 1),
                'technical': round(float(technical_total), 1),
                'industry': round(float(industry_total), 1)
            },
            'details': {
                'financial': financial_scores,
                'market': market_scores,
                'technical': technical_scores,
                'industry': industry_scores
            },
            'trade_date': int(daily_data.get('trade_date', 0)) if daily_data.get('trade_date') else None,
            'close': float(daily_data.get('close', 0)) if daily_data.get('close') else None,
        }
        
        # 确保可序列化
        return _to_serializable(result)
    
    def _get_stock_name(self, stock_code: str) -> str:
        """获取股票名称"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM stocks WHERE code = ?", (stock_code,))
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else stock_code
        except:
            return stock_code
    
    # ==================== 股票池生成 ====================
    
    def generate_stock_pool(self, top_n: int = 50, min_score: float = 50.0) -> Dict:
        """
        生成股票池（评分Top N）
        
        Args:
            top_n: 取前N只股票
            min_score: 最低评分门槛
        
        Returns:
            {
                'stocks': [...],
                'generated_at': str,
                'total_count': int,
                'qualified_count': int
            }
        """
        # 获取所有股票代码
        stock_codes = self._get_all_stock_codes()
        
        results = []
        for i, code in enumerate(stock_codes):
            score_data = self.calculate_score(code)
            if score_data and score_data['total_score'] >= min_score:
                results.append(score_data)
        
        # 按总分排序
        results.sort(key=lambda x: x['total_score'], reverse=True)
        
        # 取Top N
        top_stocks = results[:top_n]
        
        return {
            'stocks': top_stocks,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_count': len(stock_codes),
            'qualified_count': len(results),
            'top_n': top_n
        }
    
    def _get_all_stock_codes(self) -> List[str]:
        """获取所有股票代码"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT code FROM stocks ORDER BY code")
            codes = [row[0] for row in cursor.fetchall()]
            conn.close()
            return codes
        except:
            return []
    
    # ==================== 快速评分（用于批量计算） ====================
    
    def calculate_score_fast(self, stock_code: str) -> Optional[Dict]:
        """
        快速评分（仅财务+市场因子，跳过技术因子）
        用于批量计算时提升速度
        """
        fina_data = self._load_financial_data(stock_code)
        daily_data = self._load_daily_basic_data(stock_code)
        
        if fina_data is None or daily_data is None:
            return None
        
        financial_scores = self._score_financial(fina_data)
        market_scores = self._score_market(daily_data)
        
        financial_total = sum(s['score'] * s['weight'] for s in financial_scores.values())
        market_total = sum(s['score'] * s['weight'] for s in market_scores.values())
        
        # 技术+行业给默认分
        technical_total = 50 * 0.20  # 10分
        industry_total = 50 * 0.10   # 5分
        
        total_score = financial_total + market_total + technical_total + industry_total
        
        return {
            'stock_code': stock_code,
            'stock_name': self._get_stock_name(stock_code),
            'total_score': round(total_score, 1),
            'financial_score': round(financial_total, 1),
            'market_score': round(market_total, 1),
        }


# ==================== 单例 ====================
_scoring_service = None

def get_scoring_service() -> StockScoringService:
    global _scoring_service
    if _scoring_service is None:
        _scoring_service = StockScoringService()
    return _scoring_service


def _to_serializable(obj):
    """转换为可JSON序列化的格式"""
    import numpy as np
    
    if isinstance(obj, dict):
        return {k: _to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_to_serializable(v) for v in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif pd.isna(obj):
        return None
    return obj