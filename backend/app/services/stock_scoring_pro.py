"""
专业因子评分服务

参考Barra、MSCI等专业量化机构因子框架
五大类因子：价值、成长、质量、动量、情绪
"""
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import sqlite3


class ProStockScoringService:
    """专业因子评分服务"""
    
    # 数据目录
    FINANCIAL_DIR = "data_cache/financial"
    DAY_CACHE_DIR = "data_cache/day"
    DB_PATH = "smart_quant.db"
    
    # ==================== 大类因子权重 ====================
    # 基于历史IC优化后的权重配置
    STYLE_WEIGHTS = {
        'value': 0.25,      # 价值因子
        'growth': 0.20,     # 成长因子
        'quality': 0.25,    # 质量因子
        'momentum': 0.15,   # 动量因子
        'sentiment': 0.15,  # 情绪因子
    }
    
    # ==================== 细分因子权重 ====================
    
    # 价值因子权重
    VALUE_WEIGHTS = {
        'ep': 0.32,         # EP (盈利收益率)
        'bp': 0.24,         # BP (账面市值比)
        'sp': 0.16,         # SP (销售市值比)
        'ncfp': 0.16,       # NCFP (现金流市值比)
        'div_yield': 0.12,  # 股息率
    }
    
    # 成长因子权重
    GROWTH_WEIGHTS = {
        'revenue_growth': 0.25,   # 营收增长
        'profit_growth': 0.25,    # 利润增长
        'roe_change': 0.20,       # ROE变化
        'growth_accel': 0.15,     # 增长加速度
        'eps_growth': 0.15,       # EPS增长
    }
    
    # 质量因子权重
    QUALITY_WEIGHTS = {
        'roe': 0.28,         # ROE
        'roa': 0.16,         # ROA
        'gross_margin': 0.16, # 毛利率
        'net_margin': 0.12,   # 净利率
        'leverage': 0.12,     # 财务杠杆（反向）
        'current_ratio': 0.08, # 流动比率
        'accruals': 0.08,     # 应计项目（反向）
    }
    
    # 动量因子权重
    MOMENTUM_WEIGHTS = {
        'price_momentum': 0.33,    # 价格动量
        'relative_strength': 0.20, # 相对强度
        'volume_momentum': 0.13,   # 成交量动量
        'ma_deviation': 0.17,      # 均线偏离
        'breakout': 0.17,          # 突破形态
    }
    
    # 情绪因子权重
    SENTIMENT_WEIGHTS = {
        'north_fund': 0.33,    # 北向资金
        'turnover': 0.20,      # 换手率（适度为佳）
        'volume_ratio': 0.20,  # 量比
        'margin_balance': 0.13, # 融资余额
        'volatility': 0.14,    # 波动率（反向）
    }
    
    def __init__(self):
        self._load_stock_list()
    
    def _load_stock_list(self):
        """加载股票列表"""
        try:
            conn = sqlite3.connect(self.DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT code, name FROM stocks")
            self.stock_names = {row[0]: row[1] for row in cursor.fetchall()}
            self.stock_codes = list(self.stock_names.keys())
            conn.close()
        except:
            self.stock_names = {}
            self.stock_codes = []
    
    # ==================== 数据加载 ====================
    
    def _load_financial_data(self, stock_code: str) -> Optional[Dict]:
        """加载财务数据"""
        file_path = os.path.join(self.FINANCIAL_DIR, f"{stock_code}_fina_indicator.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            
            df = df.sort_values('end_date', ascending=False)
            latest = df.iloc[0]
            
            # 获取同比数据（需要至少两期）
            if len(df) >= 4:
                yoy = df.iloc[3] if len(df) > 3 else df.iloc[-1]
            else:
                yoy = latest
            
            return {
                # 盈利能力
                'roe': self._safe_float(latest.get('roe')),
                'roa': self._safe_float(latest.get('roa')),
                'net_margin': self._safe_float(latest.get('netprofit_margin')),
                'gross_margin': self._safe_float(latest.get('grossprofit_margin')),
                
                # 增长能力
                'revenue_growth': self._safe_float(latest.get('tr_yoy')),
                'profit_growth': self._safe_float(latest.get('netprofit_yoy')),
                'roe_change': self._safe_float(latest.get('roe_yoy')),
                'eps': self._safe_float(latest.get('eps')),
                'eps_growth': self._safe_float(latest.get('dt_eps_yoy')),
                
                # 财务健康
                'debt_ratio': self._safe_float(latest.get('debt_to_assets')),
                'current_ratio': self._safe_float(latest.get('current_ratio')),
                'ocfps': self._safe_float(latest.get('ocfps')),
                
                # 每股指标
                'bps': self._safe_float(latest.get('bps')),
            }
        except:
            return None
    
    def _load_daily_basic_data(self, stock_code: str) -> Optional[Dict]:
        """加载每日基本面数据"""
        file_path = os.path.join(self.FINANCIAL_DIR, f"{stock_code}_daily_basic.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            
            df = df.sort_values('trade_date', ascending=False)
            latest = df.iloc[0]
            
            return {
                'trade_date': int(latest.get('trade_date', 0)),
                'close': self._safe_float(latest.get('close')),
                'pe': self._safe_float(latest.get('pe_ttm')),
                'pb': self._safe_float(latest.get('pb')),
                'ps': self._safe_float(latest.get('ps_ttm')),
                'market_cap': self._safe_float(latest.get('total_mv')),  # 万元
                'circ_mv': self._safe_float(latest.get('circ_mv')),
                'turnover': self._safe_float(latest.get('turnover_rate')),
                'volume_ratio': self._safe_float(latest.get('volume_ratio')),
                'dv_ratio': self._safe_float(latest.get('dv_ratio')),  # 股息率%
            }
        except:
            return None
    
    def _load_day_kline(self, stock_code: str, days: int = 250) -> Optional[pd.DataFrame]:
        """加载日K线数据"""
        file_path = os.path.join(self.DAY_CACHE_DIR, f"{stock_code}_day.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            
            df = df.sort_values('trade_date', ascending=False).head(days)
            df = df.sort_values('trade_date', ascending=True)
            return df
        except:
            return None
    
    def _safe_float(self, val) -> Optional[float]:
        """安全转换为float"""
        if val is None or pd.isna(val):
            return None
        try:
            return float(val)
        except:
            return None
    
    # ==================== 极值处理 ====================
    
    def _winsorize(self, value: float, lower: float, upper: float) -> float:
        """缩尾处理"""
        if value is None:
            return None
        return max(lower, min(upper, value))
    
    def _percentile_score(self, value: float, min_val: float, max_val: float, 
                          reverse: bool = False) -> float:
        """
        百分位评分
        
        将值映射到0-100分
        """
        if value is None:
            return 50.0  # 数据缺失给中位数
        
        if max_val == min_val:
            return 50.0
        
        if reverse:
            # 反向评分：值越低分数越高
            score = (max_val - value) / (max_val - min_val) * 100
        else:
            # 正向评分：值越高分数越高
            score = (value - min_val) / (max_val - min_val) * 100
        
        return max(0, min(100, score))
    
    # ==================== 价值因子计算 ====================
    
    def _calc_value_score(self, fina_data: Dict, daily_data: Dict) -> Tuple[float, Dict]:
        """
        计算价值因子得分
        
        核心逻辑：寻找被低估的股票
        """
        details = {}
        
        # EP (盈利收益率) = 1/PE
        pe = daily_data.get('pe')
        if pe and pe > 0:
            ep = 100 / pe  # 转为百分比
            # EP越高越便宜，评分越高
            # 正常范围：0-25%，大于15%为优秀
            ep_score = self._percentile_score(ep, 0, 25)
        else:
            ep_score = 50.0
            ep = None
        details['ep'] = {'value': ep, 'score': ep_score}
        
        # BP (账面市值比) = 1/PB
        pb = daily_data.get('pb')
        if pb and pb > 0:
            bp = 100 / pb
            # BP越高越便宜
            # 正常范围：0-200%，大于100%为低估
            bp_score = self._percentile_score(bp, 0, 200)
        else:
            bp_score = 50.0
            bp = None
        details['bp'] = {'value': bp, 'score': bp_score}
        
        # SP (销售市值比) = 1/PS
        ps = daily_data.get('ps')
        if ps and ps > 0:
            sp = 100 / ps
            sp_score = self._percentile_score(sp, 0, 500)
        else:
            sp_score = 50.0
            sp = None
        details['sp'] = {'value': sp, 'score': sp_score}
        
        # NCFP (现金流市值比)
        ocfps = fina_data.get('ocfps')  # 每股经营现金流
        close = daily_data.get('close')
        if ocfps and close and close > 0:
            ncfp = ocfps / close * 100  # 现金流收益率%
            ncfp_score = self._percentile_score(ncfp, -50, 50)
        else:
            ncfp_score = 50.0
            ncfp = None
        details['ncfp'] = {'value': ncfp, 'score': ncfp_score}
        
        # 股息率
        div_yield = daily_data.get('dv_ratio')  # 已经是百分比
        if div_yield:
            # 股息率0-8%为正常范围
            div_score = self._percentile_score(div_yield, 0, 8)
        else:
            div_score = 50.0
        details['div_yield'] = {'value': div_yield, 'score': div_score}
        
        # 加权合成
        total_score = (
            ep_score * self.VALUE_WEIGHTS['ep'] +
            bp_score * self.VALUE_WEIGHTS['bp'] +
            sp_score * self.VALUE_WEIGHTS['sp'] +
            ncfp_score * self.VALUE_WEIGHTS['ncfp'] +
            div_score * self.VALUE_WEIGHTS['div_yield']
        )
        
        return total_score, details
    
    # ==================== 成长因子计算 ====================
    
    def _calc_growth_score(self, fina_data: Dict) -> Tuple[float, Dict]:
        """
        计算成长因子得分
        
        核心逻辑：寻找高增长公司
        """
        details = {}
        
        # 营收增长率
        rev_growth = fina_data.get('revenue_growth')
        if rev_growth is not None:
            # -50%到100%为正常范围
            rev_score = self._percentile_score(rev_growth, -50, 100)
        else:
            rev_score = 50.0
        details['revenue_growth'] = {'value': rev_growth, 'score': rev_score}
        
        # 利润增长率
        profit_growth = fina_data.get('profit_growth')
        if profit_growth is not None:
            # -100%到200%为正常范围
            profit_score = self._percentile_score(profit_growth, -100, 200)
        else:
            profit_score = 50.0
        details['profit_growth'] = {'value': profit_growth, 'score': profit_score}
        
        # ROE变化
        roe_change = fina_data.get('roe_change')
        if roe_change is not None:
            # -20到20为正常范围
            roe_chg_score = self._percentile_score(roe_change, -20, 20)
        else:
            roe_chg_score = 50.0
        details['roe_change'] = {'value': roe_change, 'score': roe_chg_score}
        
        # 增长加速度（用利润增长变化近似）
        # 由于数据限制，暂时用利润增长本身替代
        growth_accel = profit_growth
        if growth_accel is not None:
            accel_score = self._percentile_score(growth_accel, -50, 50)
        else:
            accel_score = 50.0
        details['growth_accel'] = {'value': growth_accel, 'score': accel_score}
        
        # EPS增长
        eps_growth = fina_data.get('eps_growth')
        if eps_growth is not None:
            eps_score = self._percentile_score(eps_growth, -100, 200)
        else:
            eps_score = 50.0
        details['eps_growth'] = {'value': eps_growth, 'score': eps_score}
        
        # 加权合成
        total_score = (
            rev_score * self.GROWTH_WEIGHTS['revenue_growth'] +
            profit_score * self.GROWTH_WEIGHTS['profit_growth'] +
            roe_chg_score * self.GROWTH_WEIGHTS['roe_change'] +
            accel_score * self.GROWTH_WEIGHTS['growth_accel'] +
            eps_score * self.GROWTH_WEIGHTS['eps_growth']
        )
        
        return total_score, details
    
    # ==================== 质量因子计算 ====================
    
    def _calc_quality_score(self, fina_data: Dict) -> Tuple[float, Dict]:
        """
        计算质量因子得分
        
        核心逻辑：筛选财务健康的优质公司
        """
        details = {}
        
        # ROE
        roe = fina_data.get('roe')
        if roe is not None:
            # ROE 0-30%为正常范围，>20%为优秀
            roe_score = self._percentile_score(roe, 0, 30)
        else:
            roe_score = 50.0
        details['roe'] = {'value': roe, 'score': roe_score}
        
        # ROA
        roa = fina_data.get('roa')
        if roa is not None:
            roa_score = self._percentile_score(roa, 0, 15)
        else:
            roa_score = 50.0
        details['roa'] = {'value': roa, 'score': roa_score}
        
        # 毛利率
        gross_margin = fina_data.get('gross_margin')
        if gross_margin is not None:
            # 0-60%为正常范围
            gm_score = self._percentile_score(gross_margin, 0, 60)
        else:
            gm_score = 50.0
        details['gross_margin'] = {'value': gross_margin, 'score': gm_score}
        
        # 净利率
        net_margin = fina_data.get('net_margin')
        if net_margin is not None:
            nm_score = self._percentile_score(net_margin, -10, 40)
        else:
            nm_score = 50.0
        details['net_margin'] = {'value': net_margin, 'score': nm_score}
        
        # 财务杠杆（负债率，反向）
        debt_ratio = fina_data.get('debt_ratio')
        if debt_ratio is not None:
            # 负债率0-100%，越低越好
            leverage_score = self._percentile_score(debt_ratio, 0, 100, reverse=True)
        else:
            leverage_score = 50.0
        details['leverage'] = {'value': debt_ratio, 'score': leverage_score}
        
        # 流动比率
        current_ratio = fina_data.get('current_ratio')
        if current_ratio is not None:
            # 0-3为正常范围，>1.5为健康
            cr_score = self._percentile_score(current_ratio, 0, 3)
        else:
            cr_score = 50.0
        details['current_ratio'] = {'value': current_ratio, 'score': cr_score}
        
        # 应计项目（简化：用现金流质量近似）
        ocfps = fina_data.get('ocfps')
        eps = fina_data.get('eps')
        if ocfps is not None and eps is not None:
            # 现金流/利润，越接近1越好
            accruals = ocfps / eps if eps != 0 else 0
            # 应计项目越小说明盈利质量越高
            accruals_score = self._percentile_score(accruals, -2, 2)
        else:
            accruals_score = 50.0
        details['accruals'] = {'value': accruals if 'accruals' in dir() else None, 'score': accruals_score}
        
        # 加权合成
        total_score = (
            roe_score * self.QUALITY_WEIGHTS['roe'] +
            roa_score * self.QUALITY_WEIGHTS['roa'] +
            gm_score * self.QUALITY_WEIGHTS['gross_margin'] +
            nm_score * self.QUALITY_WEIGHTS['net_margin'] +
            leverage_score * self.QUALITY_WEIGHTS['leverage'] +
            cr_score * self.QUALITY_WEIGHTS['current_ratio'] +
            accruals_score * self.QUALITY_WEIGHTS['accruals']
        )
        
        return total_score, details
    
    # ==================== 动量因子计算 ====================
    
    def _calc_momentum_score(self, kline_df: pd.DataFrame, daily_data: Dict) -> Tuple[float, Dict]:
        """
        计算动量因子得分
        
        核心逻辑：强者恒强，跟随趋势
        """
        details = {}
        
        if kline_df is None or len(kline_df) < 60:
            # 数据不足
            for key in self.MOMENTUM_WEIGHTS.keys():
                details[key] = {'value': None, 'score': 50.0}
            return 50.0, details
        
        close = kline_df['close'].values
        volume = kline_df['vol'].values if 'vol' in kline_df.columns else None
        
        # 1. 价格动量（过去N月收益）
        if len(close) >= 120:
            # 剔除最近1月的12月动量
            momentum_11m = close[-25] / close[-120] - 1  # 5个月动量
            momentum_score = self._percentile_score(momentum_11m * 100, -50, 100)
        else:
            momentum_11m = None
            momentum_score = 50.0
        details['price_momentum'] = {'value': momentum_11m, 'score': momentum_score}
        
        # 2. 相对强度（价格与MA的关系）
        ma20 = pd.Series(close).rolling(20).mean().values
        if ma20[-1] > 0:
            relative_strength = close[-1] / ma20[-1] - 1
            rs_score = self._percentile_score(relative_strength * 100, -20, 30)
        else:
            rs_score = 50.0
            relative_strength = None
        details['relative_strength'] = {'value': relative_strength, 'score': rs_score}
        
        # 3. 成交量动量
        if volume is not None and len(volume) >= 20:
            vol_ma5 = volume[-5:].mean()
            vol_ma20 = volume[-20:].mean()
            vol_momentum = vol_ma5 / vol_ma20 if vol_ma20 > 0 else 1
            # 量比在0.5-2之间为健康
            vm_score = self._percentile_score(vol_momentum, 0.5, 2)
        else:
            vol_momentum = None
            vm_score = 50.0
        details['volume_momentum'] = {'value': vol_momentum, 'score': vm_score}
        
        # 4. 均线偏离
        if ma20[-1] > 0:
            ma_deviation = (close[-1] - ma20[-1]) / ma20[-1] * 100
            # 偏离-15%到20%为正常
            ma_dev_score = self._percentile_score(ma_deviation, -15, 20)
        else:
            ma_dev_score = 50.0
            ma_deviation = None
        details['ma_deviation'] = {'value': ma_deviation, 'score': ma_dev_score}
        
        # 5. 突破形态
        ma60 = pd.Series(close).rolling(60).mean().values
        breakout_score = 50.0
        if len(close) >= 2 and not pd.isna(ma20[-1]) and not pd.isna(ma60[-1]):
            if close[-1] > ma20[-1] and close[-2] <= ma20[-2]:
                breakout_score = 80  # 突破MA20
            if close[-1] > ma60[-1] and close[-2] <= ma60[-2]:
                breakout_score = 100  # 突破MA60
        details['breakout'] = {'value': None, 'score': breakout_score}
        
        # 加权合成
        total_score = (
            momentum_score * self.MOMENTUM_WEIGHTS['price_momentum'] +
            rs_score * self.MOMENTUM_WEIGHTS['relative_strength'] +
            vm_score * self.MOMENTUM_WEIGHTS['volume_momentum'] +
            ma_dev_score * self.MOMENTUM_WEIGHTS['ma_deviation'] +
            breakout_score * self.MOMENTUM_WEIGHTS['breakout']
        )
        
        return total_score, details
    
    # ==================== 情绪因子计算 ====================
    
    def _calc_sentiment_score(self, daily_data: Dict, kline_df: pd.DataFrame) -> Tuple[float, Dict]:
        """
        计算情绪因子得分
        
        核心逻辑：捕捉市场情绪和资金流向
        """
        details = {}
        
        # 1. 北向资金（暂无数据，默认50分）
        north_score = 50.0
        details['north_fund'] = {'value': None, 'score': north_score}
        
        # 2. 换手率（适度为佳）
        turnover = daily_data.get('turnover')
        if turnover is not None:
            # 换手率1-5%为健康区间
            if 1 <= turnover <= 5:
                to_score = 80
            elif 0.5 <= turnover < 1 or 5 < turnover <= 10:
                to_score = 60
            elif turnover < 0.5:
                to_score = 40  # 过低
            else:
                to_score = 30  # 过热
        else:
            to_score = 50.0
        details['turnover'] = {'value': turnover, 'score': to_score}
        
        # 3. 量比
        volume_ratio = daily_data.get('volume_ratio')
        if volume_ratio is not None:
            # 量比0.8-1.5为健康
            if 0.8 <= volume_ratio <= 1.5:
                vr_score = 80
            elif 0.5 <= volume_ratio < 0.8 or 1.5 < volume_ratio <= 2:
                vr_score = 60
            else:
                vr_score = 40
        else:
            vr_score = 50.0
        details['volume_ratio'] = {'value': volume_ratio, 'score': vr_score}
        
        # 4. 融资余额（暂无数据）
        margin_score = 50.0
        details['margin_balance'] = {'value': None, 'score': margin_score}
        
        # 5. 波动率（反向，越低越稳定）
        if kline_df is not None and len(kline_df) >= 20:
            returns = pd.Series(kline_df['close'].values).pct_change().dropna()
            volatility = returns.std() * np.sqrt(252) * 100  # 年化波动率%
            # 波动率20-40%为正常
            if volatility < 20:
                vol_score = 90  # 非常稳定
            elif 20 <= volatility <= 40:
                vol_score = 70
            elif 40 < volatility <= 60:
                vol_score = 50
            else:
                vol_score = 30  # 波动过大
        else:
            vol_score = 50.0
            volatility = None
        details['volatility'] = {'value': volatility, 'score': vol_score}
        
        # 加权合成
        total_score = (
            north_score * self.SENTIMENT_WEIGHTS['north_fund'] +
            to_score * self.SENTIMENT_WEIGHTS['turnover'] +
            vr_score * self.SENTIMENT_WEIGHTS['volume_ratio'] +
            margin_score * self.SENTIMENT_WEIGHTS['margin_balance'] +
            vol_score * self.SENTIMENT_WEIGHTS['volatility']
        )
        
        return total_score, details
    
    # ==================== 综合评分计算 ====================
    
    def calculate_score(self, stock_code: str) -> Optional[Dict]:
        """
        计算单只股票的综合评分
        
        Returns:
            {
                'stock_code': str,
                'stock_name': str,
                'composite_score': float,  # 综合评分 0-100
                'grade': str,              # 评级 A+/A/B+/B/B-/C/D
                'style_scores': {          # 风格因子得分
                    'value': float,
                    'growth': float,
                    'quality': float,
                    'momentum': float,
                    'sentiment': float
                },
                'details': {...}           # 细分因子详情
            }
        """
        # 加载数据
        fina_data = self._load_financial_data(stock_code)
        daily_data = self._load_daily_basic_data(stock_code)
        kline_df = self._load_day_kline(stock_code, 250)
        
        if fina_data is None or daily_data is None:
            return None
        
        # 计算各风格因子得分
        value_score, value_details = self._calc_value_score(fina_data, daily_data)
        growth_score, growth_details = self._calc_growth_score(fina_data)
        quality_score, quality_details = self._calc_quality_score(fina_data)
        momentum_score, momentum_details = self._calc_momentum_score(kline_df, daily_data)
        sentiment_score, sentiment_details = self._calc_sentiment_score(daily_data, kline_df)
        
        # 综合评分
        composite_score = (
            value_score * self.STYLE_WEIGHTS['value'] +
            growth_score * self.STYLE_WEIGHTS['growth'] +
            quality_score * self.STYLE_WEIGHTS['quality'] +
            momentum_score * self.STYLE_WEIGHTS['momentum'] +
            sentiment_score * self.STYLE_WEIGHTS['sentiment']
        )
        
        # 评级
        if composite_score >= 85:
            grade = 'A+'
        elif composite_score >= 75:
            grade = 'A'
        elif composite_score >= 65:
            grade = 'B+'
        elif composite_score >= 55:
            grade = 'B'
        elif composite_score >= 45:
            grade = 'B-'
        elif composite_score >= 35:
            grade = 'C'
        else:
            grade = 'D'
        
        return {
            'stock_code': stock_code,
            'stock_name': self.stock_names.get(stock_code, stock_code),
            'composite_score': round(composite_score, 1),
            'grade': grade,
            'style_scores': {
                'value': round(value_score, 1),
                'growth': round(growth_score, 1),
                'quality': round(quality_score, 1),
                'momentum': round(momentum_score, 1),
                'sentiment': round(sentiment_score, 1),
            },
            'details': {
                'value': value_details,
                'growth': growth_details,
                'quality': quality_details,
                'momentum': momentum_details,
                'sentiment': sentiment_details,
            },
            'trade_date': daily_data.get('trade_date'),
            'close': daily_data.get('close'),
        }
    
    # ==================== 股票池生成 ====================
    
    def generate_stock_pool(self, top_n: int = 50, min_score: float = 0) -> Dict:
        """
        生成股票池
        
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
        results = []
        
        for code in self.stock_codes:
            try:
                score_data = self.calculate_score(code)
                if score_data and score_data['composite_score'] >= min_score:
                    results.append(score_data)
            except:
                continue
        
        # 按综合评分排序
        results.sort(key=lambda x: x['composite_score'], reverse=True)
        
        # 取Top N
        top_stocks = results[:top_n]
        
        return {
            'stocks': top_stocks,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_count': len(self.stock_codes),
            'qualified_count': len(results),
        }
    
    # ==================== 风格分析 ====================
    
    def analyze_style(self, stock_code: str) -> Optional[Dict]:
        """
        分析股票风格特征
        """
        score_data = self.calculate_score(stock_code)
        if not score_data:
            return None
        
        style_scores = score_data['style_scores']
        
        # 找出主导风格
        dominant_style = max(style_scores, key=style_scores.get)
        
        # 风格标签
        tags = []
        if style_scores['value'] >= 70:
            tags.append('价值')
        if style_scores['growth'] >= 70:
            tags.append('成长')
        if style_scores['quality'] >= 70:
            tags.append('质量')
        if style_scores['momentum'] >= 70:
            tags.append('动量')
        if style_scores['sentiment'] >= 70:
            tags.append('热门')
        
        return {
            'stock_code': stock_code,
            'stock_name': score_data['stock_name'],
            'dominant_style': dominant_style,
            'style_tags': tags if tags else ['均衡'],
            'style_profile': style_scores,
            'composite_score': score_data['composite_score'],
        }


# ==================== 单例 ====================
_pro_scoring_service = None

def get_pro_scoring_service() -> ProStockScoringService:
    global _pro_scoring_service
    if _pro_scoring_service is None:
        _pro_scoring_service = ProStockScoringService()
    return _pro_scoring_service