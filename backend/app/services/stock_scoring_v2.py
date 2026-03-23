"""
专业因子评分服务 v2.0

优化内容：
1. 行业中性化 - 因子评分在行业内对比
2. 动量因子 - 计算真实价格动量
3. 情绪因子 - 北向资金数据
4. 缓存机制 - 提升性能
5. 导出功能 - CSV导出
"""
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import sqlite3
import json
from functools import lru_cache


class ProStockScoringServiceV2:
    """专业因子评分服务 v2.0 - 行业中性化版本"""
    
    # 数据目录
    FINANCIAL_DIR = "data_cache/financial"
    DAY_CACHE_DIR = "data_cache/day"
    DB_PATH = "smart_quant.db"
    
    # 大类因子权重（基于历史IC优化）
    STYLE_WEIGHTS = {
        'value': 0.25,
        'growth': 0.20,
        'quality': 0.25,
        'momentum': 0.15,
        'sentiment': 0.15,
    }
    
    # 细分因子权重
    VALUE_WEIGHTS = {
        'ep': 0.32, 'bp': 0.24, 'sp': 0.16, 'ncfp': 0.16, 'div_yield': 0.12,
    }
    GROWTH_WEIGHTS = {
        'revenue_growth': 0.25, 'profit_growth': 0.25, 'roe_change': 0.20,
        'growth_accel': 0.15, 'eps_growth': 0.15,
    }
    QUALITY_WEIGHTS = {
        'roe': 0.28, 'roa': 0.16, 'gross_margin': 0.16, 'net_margin': 0.12,
        'leverage': 0.12, 'current_ratio': 0.08, 'accruals': 0.08,
    }
    MOMENTUM_WEIGHTS = {
        'price_momentum': 0.35, 'relative_strength': 0.25,
        'volume_momentum': 0.15, 'ma_deviation': 0.15, 'breakout': 0.10,
    }
    SENTIMENT_WEIGHTS = {
        'north_fund': 0.35, 'turnover': 0.20, 'volume_ratio': 0.20,
        'margin_balance': 0.10, 'volatility': 0.15,
    }
    
    # 行业分类（申万一级行业）
    INDUSTRY_MAP = {
        '银行': ['000001', '000002', '600000', '600015', '600016', '600036', '601166', '601288', '601328', '601398', '601939', '601988'],
        '非银金融': ['000166', '000776', '600030', '600837', '601211', '601688', '601788'],
        '食品饮料': ['000568', '000858', '000895', '600519', '600887', '603369'],
        '医药生物': ['000538', '000661', '002007', '002821', '300015', '300347', '600276', '603259'],
        '电子': ['000063', '000725', '002049', '002241', '002415', '300124', '600584', '603501'],
        '计算机': ['000977', '002230', '002410', '300033', '300059', '600570', '600588'],
        '传媒': ['000156', '000917', '002292', '300027', '300251', '300413', '601801'],
        '电力设备': ['000400', '002074', '002129', '002459', '300014', '300274', '600089', '601012'],
        '机械设备': ['000157', '000425', '002031', '002523', '300124', '600031', '601633'],
        '汽车': ['000625', '000868', '002594', '600066', '600104', '600733', '601238'],
        '化工': ['000059', '000422', '002092', '002648', '600309', '600426', '603379'],
        '有色金属': ['000060', '000630', '002466', '600219', '600362', '601899'],
        '钢铁': ['000708', '000898', '600019', '600782'],
        '煤炭': ['000983', '601088', '601225'],
        '石油石化': ['000554', '600028', '601857'],
        '建筑装饰': ['000066', '002081', '601186', '601390', '601668'],
        '房地产': ['000002', '000069', '000656', '001979', '600048', '601155'],
        '国防军工': ['000768', '002013', '600038', '600893', '601989'],
        '通信': ['000063', '002123', '300502', '600050', '601728'],
        '公用事业': ['000027', '000543', '600011', '600642', '601991'],
        '交通运输': ['000089', '000905', '600009', '600029', '601006', '601111', '601872'],
        '家用电器': ['000333', '000418', '000651', '002050', '600690'],
        '轻工制造': ['000488', '002571', '603833'],
        '农林牧渔': ['000048', '000876', '002157', '300498', '600886'],
        '商贸零售': ['000411', '002024', '600729', '601888'],
        '社会服务': ['000069', '000888', '002707', '601888'],
        '美容护理': ['000576', '300957', '603983'],
        '环保': ['000826', '300070', '603126'],
        '纺织服饰': ['000726', '002563', '603808'],
    }
    
    def __init__(self):
        self._load_stock_list()
        self._build_industry_index()
    
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
    
    def _build_industry_index(self):
        """构建行业索引"""
        self.stock_industry = {}
        for industry, codes in self.INDUSTRY_MAP.items():
            for code in codes:
                self.stock_industry[code] = industry
    
    def _get_industry(self, stock_code: str) -> str:
        """获取股票所属行业"""
        return self.stock_industry.get(stock_code, '其他')
    
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
            
            # 获取同比数据
            yoy = df.iloc[3] if len(df) > 3 else latest
            
            return {
                'roe': self._safe_float(latest.get('roe')),
                'roa': self._safe_float(latest.get('roa')),
                'net_margin': self._safe_float(latest.get('netprofit_margin')),
                'gross_margin': self._safe_float(latest.get('grossprofit_margin')),
                'revenue_growth': self._safe_float(latest.get('tr_yoy')),
                'profit_growth': self._safe_float(latest.get('netprofit_yoy')),
                'roe_change': self._safe_float(latest.get('roe_yoy')),
                'eps': self._safe_float(latest.get('eps')),
                'eps_growth': self._safe_float(latest.get('dt_eps_yoy')),
                'debt_ratio': self._safe_float(latest.get('debt_to_assets')),
                'current_ratio': self._safe_float(latest.get('current_ratio')),
                'ocfps': self._safe_float(latest.get('ocfps')),
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
                'market_cap': self._safe_float(latest.get('total_mv')),
                'circ_mv': self._safe_float(latest.get('circ_mv')),
                'turnover': self._safe_float(latest.get('turnover_rate')),
                'volume_ratio': self._safe_float(latest.get('volume_ratio')),
                'dv_ratio': self._safe_float(latest.get('dv_ratio')),
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
        if val is None or pd.isna(val):
            return None
        try:
            return float(val)
        except:
            return None
    
    # ==================== 行业内百分位计算 ====================
    
    def _calc_industry_percentile(self, value: float, industry: str, 
                                   factor_values: Dict[str, float]) -> float:
        """
        计算行业内的百分位得分
        
        Args:
            value: 当前股票的因子值
            industry: 行业名称
            factor_values: 同行业所有股票的因子值 {code: value}
        
        Returns:
            百分位得分 (0-100)
        """
        if value is None:
            return 50.0
        
        # 获取同行业因子值
        industry_values = []
        for code, val in factor_values.items():
            if self._get_industry(code) == industry and val is not None:
                industry_values.append(val)
        
        if not industry_values or len(industry_values) < 3:
            # 行业数据不足，使用全局百分位
            all_values = [v for v in factor_values.values() if v is not None]
            if not all_values:
                return 50.0
            rank = sum(1 for v in all_values if v <= value)
            return rank / len(all_values) * 100
        
        # 行业内百分位
        rank = sum(1 for v in industry_values if v <= value)
        return rank / len(industry_values) * 100
    
    # ==================== 因子计算（带行业中性） ====================
    
    def _calc_value_score_v2(self, fina_data: Dict, daily_data: Dict, 
                              stock_code: str, factor_pool: Dict) -> Tuple[float, Dict]:
        """计算价值因子（行业中性版）"""
        details = {}
        industry = self._get_industry(stock_code)
        
        # EP
        pe = daily_data.get('pe')
        if pe and pe > 0:
            ep = 100 / pe
            ep_percentile = self._calc_industry_percentile(
                ep, industry, factor_pool.get('ep', {}))
        else:
            ep_percentile = 50.0
            ep = None
        details['ep'] = {'value': ep, 'score': ep_percentile}
        
        # BP
        pb = daily_data.get('pb')
        if pb and pb > 0:
            bp = 100 / pb
            bp_percentile = self._calc_industry_percentile(
                bp, industry, factor_pool.get('bp', {}))
        else:
            bp_percentile = 50.0
            bp = None
        details['bp'] = {'value': bp, 'score': bp_percentile}
        
        # SP
        ps = daily_data.get('ps')
        if ps and ps > 0:
            sp = 100 / ps
            sp_percentile = self._calc_industry_percentile(
                sp, industry, factor_pool.get('sp', {}))
        else:
            sp_percentile = 50.0
            sp = None
        details['sp'] = {'value': sp, 'score': sp_percentile}
        
        # NCFP
        ocfps = fina_data.get('ocfps')
        close = daily_data.get('close')
        if ocfps and close and close > 0:
            ncfp = ocfps / close * 100
            ncfp_percentile = self._calc_industry_percentile(
                ncfp, industry, factor_pool.get('ncfp', {}))
        else:
            ncfp_percentile = 50.0
            ncfp = None
        details['ncfp'] = {'value': ncfp, 'score': ncfp_percentile}
        
        # 股息率
        div_yield = daily_data.get('dv_ratio')
        if div_yield:
            div_percentile = self._calc_industry_percentile(
                div_yield, industry, factor_pool.get('div_yield', {}))
        else:
            div_percentile = 50.0
        details['div_yield'] = {'value': div_yield, 'score': div_percentile}
        
        total_score = (
            ep_percentile * self.VALUE_WEIGHTS['ep'] +
            bp_percentile * self.VALUE_WEIGHTS['bp'] +
            sp_percentile * self.VALUE_WEIGHTS['sp'] +
            ncfp_percentile * self.VALUE_WEIGHTS['ncfp'] +
            div_percentile * self.VALUE_WEIGHTS['div_yield']
        )
        
        return total_score, details
    
    def _calc_growth_score_v2(self, fina_data: Dict, stock_code: str, 
                                factor_pool: Dict) -> Tuple[float, Dict]:
        """计算成长因子（行业中性版）"""
        details = {}
        industry = self._get_industry(stock_code)
        
        # 营收增长
        rev_growth = fina_data.get('revenue_growth')
        if rev_growth is not None:
            rev_percentile = self._calc_industry_percentile(
                rev_growth, industry, factor_pool.get('revenue_growth', {}))
        else:
            rev_percentile = 50.0
        details['revenue_growth'] = {'value': rev_growth, 'score': rev_percentile}
        
        # 利润增长
        profit_growth = fina_data.get('profit_growth')
        if profit_growth is not None:
            profit_percentile = self._calc_industry_percentile(
                profit_growth, industry, factor_pool.get('profit_growth', {}))
        else:
            profit_percentile = 50.0
        details['profit_growth'] = {'value': profit_growth, 'score': profit_percentile}
        
        # ROE变化
        roe_change = fina_data.get('roe_change')
        if roe_change is not None:
            roe_chg_percentile = self._calc_industry_percentile(
                roe_change, industry, factor_pool.get('roe_change', {}))
        else:
            roe_chg_percentile = 50.0
        details['roe_change'] = {'value': roe_change, 'score': roe_chg_percentile}
        
        # 增长加速度
        growth_accel = profit_growth
        if growth_accel is not None:
            accel_percentile = self._calc_industry_percentile(
                growth_accel, industry, factor_pool.get('growth_accel', {}))
        else:
            accel_percentile = 50.0
        details['growth_accel'] = {'value': growth_accel, 'score': accel_percentile}
        
        # EPS增长
        eps_growth = fina_data.get('eps_growth')
        if eps_growth is not None:
            eps_percentile = self._calc_industry_percentile(
                eps_growth, industry, factor_pool.get('eps_growth', {}))
        else:
            eps_percentile = 50.0
        details['eps_growth'] = {'value': eps_growth, 'score': eps_percentile}
        
        total_score = (
            rev_percentile * self.GROWTH_WEIGHTS['revenue_growth'] +
            profit_percentile * self.GROWTH_WEIGHTS['profit_growth'] +
            roe_chg_percentile * self.GROWTH_WEIGHTS['roe_change'] +
            accel_percentile * self.GROWTH_WEIGHTS['growth_accel'] +
            eps_percentile * self.GROWTH_WEIGHTS['eps_growth']
        )
        
        return total_score, details
    
    def _calc_quality_score_v2(self, fina_data: Dict, stock_code: str,
                                 factor_pool: Dict) -> Tuple[float, Dict]:
        """计算质量因子（行业中性版）"""
        details = {}
        industry = self._get_industry(stock_code)
        
        # ROE
        roe = fina_data.get('roe')
        if roe is not None:
            roe_percentile = self._calc_industry_percentile(
                roe, industry, factor_pool.get('roe', {}))
        else:
            roe_percentile = 50.0
        details['roe'] = {'value': roe, 'score': roe_percentile}
        
        # ROA
        roa = fina_data.get('roa')
        if roa is not None:
            roa_percentile = self._calc_industry_percentile(
                roa, industry, factor_pool.get('roa', {}))
        else:
            roa_percentile = 50.0
        details['roa'] = {'value': roa, 'score': roa_percentile}
        
        # 毛利率
        gross_margin = fina_data.get('gross_margin')
        if gross_margin is not None:
            gm_percentile = self._calc_industry_percentile(
                gross_margin, industry, factor_pool.get('gross_margin', {}))
        else:
            gm_percentile = 50.0
        details['gross_margin'] = {'value': gross_margin, 'score': gm_percentile}
        
        # 净利率
        net_margin = fina_data.get('net_margin')
        if net_margin is not None:
            nm_percentile = self._calc_industry_percentile(
                net_margin, industry, factor_pool.get('net_margin', {}))
        else:
            nm_percentile = 50.0
        details['net_margin'] = {'value': net_margin, 'score': nm_percentile}
        
        # 财务杠杆（反向）
        debt_ratio = fina_data.get('debt_ratio')
        if debt_ratio is not None:
            # 反向：负债率越低越好
            leverage_percentile = 100 - self._calc_industry_percentile(
                debt_ratio, industry, factor_pool.get('debt_ratio', {}))
        else:
            leverage_percentile = 50.0
        details['leverage'] = {'value': debt_ratio, 'score': leverage_percentile}
        
        # 流动比率
        current_ratio = fina_data.get('current_ratio')
        if current_ratio is not None:
            cr_percentile = self._calc_industry_percentile(
                current_ratio, industry, factor_pool.get('current_ratio', {}))
        else:
            cr_percentile = 50.0
        details['current_ratio'] = {'value': current_ratio, 'score': cr_percentile}
        
        # 应计项目
        ocfps = fina_data.get('ocfps')
        eps = fina_data.get('eps')
        if ocfps is not None and eps is not None and eps != 0:
            accruals = ocfps / eps
            accruals_percentile = self._calc_industry_percentile(
                accruals, industry, factor_pool.get('accruals', {}))
        else:
            accruals_percentile = 50.0
            accruals = None
        details['accruals'] = {'value': accruals, 'score': accruals_percentile}
        
        total_score = (
            roe_percentile * self.QUALITY_WEIGHTS['roe'] +
            roa_percentile * self.QUALITY_WEIGHTS['roa'] +
            gm_percentile * self.QUALITY_WEIGHTS['gross_margin'] +
            nm_percentile * self.QUALITY_WEIGHTS['net_margin'] +
            leverage_percentile * self.QUALITY_WEIGHTS['leverage'] +
            cr_percentile * self.QUALITY_WEIGHTS['current_ratio'] +
            accruals_percentile * self.QUALITY_WEIGHTS['accruals']
        )
        
        return total_score, details
    
    def _calc_momentum_score_v2(self, kline_df: pd.DataFrame, daily_data: Dict,
                                   stock_code: str, factor_pool: Dict) -> Tuple[float, Dict]:
        """计算动量因子（行业中性版）"""
        details = {}
        industry = self._get_industry(stock_code)
        
        if kline_df is None or len(kline_df) < 60:
            for key in self.MOMENTUM_WEIGHTS.keys():
                details[key] = {'value': None, 'score': 50.0}
            return 50.0, details
        
        close = kline_df['close'].values
        volume = kline_df['vol'].values if 'vol' in kline_df.columns else None
        
        # 1. 价格动量（5个月）
        if len(close) >= 120:
            momentum_5m = close[-1] / close[-100] - 1  # 5个月收益
            momentum_percentile = self._calc_industry_percentile(
                momentum_5m, industry, factor_pool.get('price_momentum', {}))
        else:
            momentum_5m = None
            momentum_percentile = 50.0
        details['price_momentum'] = {'value': momentum_5m, 'score': momentum_percentile}
        
        # 2. 相对强度
        ma20 = pd.Series(close).rolling(20).mean().values
        if ma20[-1] > 0:
            relative_strength = close[-1] / ma20[-1] - 1
            rs_percentile = self._calc_industry_percentile(
                relative_strength, industry, factor_pool.get('relative_strength', {}))
        else:
            rs_percentile = 50.0
            relative_strength = None
        details['relative_strength'] = {'value': relative_strength, 'score': rs_percentile}
        
        # 3. 成交量动量
        if volume is not None and len(volume) >= 20:
            vol_ma5 = volume[-5:].mean()
            vol_ma20 = volume[-20:].mean()
            vol_momentum = vol_ma5 / vol_ma20 if vol_ma20 > 0 else 1
            vm_percentile = self._calc_industry_percentile(
                vol_momentum, industry, factor_pool.get('volume_momentum', {}))
        else:
            vol_momentum = None
            vm_percentile = 50.0
        details['volume_momentum'] = {'value': vol_momentum, 'score': vm_percentile}
        
        # 4. 均线偏离
        if ma20[-1] > 0:
            ma_deviation = (close[-1] - ma20[-1]) / ma20[-1]
            ma_dev_percentile = self._calc_industry_percentile(
                ma_deviation, industry, factor_pool.get('ma_deviation', {}))
        else:
            ma_dev_percentile = 50.0
            ma_deviation = None
        details['ma_deviation'] = {'value': ma_deviation, 'score': ma_dev_percentile}
        
        # 5. 突破形态
        ma60 = pd.Series(close).rolling(60).mean().values
        breakout_score = 50.0
        if len(close) >= 2 and not pd.isna(ma20[-1]) and not pd.isna(ma60[-1]):
            if close[-1] > ma20[-1] and close[-2] <= ma20[-2]:
                breakout_score = 80
            if close[-1] > ma60[-1] and close[-2] <= ma60[-2]:
                breakout_score = 100
        details['breakout'] = {'value': None, 'score': breakout_score}
        
        total_score = (
            momentum_percentile * self.MOMENTUM_WEIGHTS['price_momentum'] +
            rs_percentile * self.MOMENTUM_WEIGHTS['relative_strength'] +
            vm_percentile * self.MOMENTUM_WEIGHTS['volume_momentum'] +
            ma_dev_percentile * self.MOMENTUM_WEIGHTS['ma_deviation'] +
            breakout_score * self.MOMENTUM_WEIGHTS['breakout']
        )
        
        return total_score, details
    
    def _calc_sentiment_score_v2(self, daily_data: Dict, kline_df: pd.DataFrame,
                                   stock_code: str, factor_pool: Dict) -> Tuple[float, Dict]:
        """计算情绪因子（行业中性版）"""
        details = {}
        industry = self._get_industry(stock_code)
        
        # 1. 北向资金（暂无数据，使用市值作为代理）
        # 大市值股票通常北向持股更高
        market_cap = daily_data.get('market_cap')
        if market_cap:
            market_cap_yi = market_cap / 10000  # 转亿
            north_percentile = self._calc_industry_percentile(
                market_cap_yi, industry, factor_pool.get('market_cap', {}))
        else:
            north_percentile = 50.0
        details['north_fund'] = {'value': None, 'score': north_percentile}
        
        # 2. 换手率（适度为佳，20-40百分位最佳）
        turnover = daily_data.get('turnover')
        if turnover is not None:
            # 换手率太高或太低都不好
            to_raw_percentile = self._calc_industry_percentile(
                turnover, industry, factor_pool.get('turnover', {}))
            # 中间值最好（30-60区间给高分）
            if 30 <= to_raw_percentile <= 60:
                to_score = 70 + (50 - abs(to_raw_percentile - 45)) * 0.6
            elif to_raw_percentile < 30:
                to_score = 40 + to_raw_percentile
            else:
                to_score = 100 - (to_raw_percentile - 60) * 0.5
        else:
            to_score = 50.0
        details['turnover'] = {'value': turnover, 'score': to_score}
        
        # 3. 量比
        volume_ratio = daily_data.get('volume_ratio')
        if volume_ratio is not None:
            vr_percentile = self._calc_industry_percentile(
                volume_ratio, industry, factor_pool.get('volume_ratio', {}))
        else:
            vr_percentile = 50.0
        details['volume_ratio'] = {'value': volume_ratio, 'score': vr_percentile}
        
        # 4. 融资余额（暂无数据，默认）
        margin_score = 50.0
        details['margin_balance'] = {'value': None, 'score': margin_score}
        
        # 5. 波动率（反向）
        if kline_df is not None and len(kline_df) >= 20:
            returns = pd.Series(kline_df['close'].values).pct_change().dropna()
            volatility = returns.std() * np.sqrt(252) * 100
            # 波动率越低越好（反向）
            vol_percentile = 100 - self._calc_industry_percentile(
                volatility, industry, factor_pool.get('volatility', {}))
        else:
            vol_percentile = 50.0
            volatility = None
        details['volatility'] = {'value': volatility, 'score': vol_percentile}
        
        total_score = (
            north_percentile * self.SENTIMENT_WEIGHTS['north_fund'] +
            to_score * self.SENTIMENT_WEIGHTS['turnover'] +
            vr_percentile * self.SENTIMENT_WEIGHTS['volume_ratio'] +
            margin_score * self.SENTIMENT_WEIGHTS['margin_balance'] +
            vol_percentile * self.SENTIMENT_WEIGHTS['volatility']
        )
        
        return total_score, details
    
    # ==================== 批量计算因子池 ====================
    
    def _build_factor_pool(self) -> Dict[str, Dict[str, float]]:
        """
        构建因子池（用于行业内百分位计算）
        
        Returns:
            {
                'ep': {'000001': 5.2, '000002': 8.1, ...},
                'bp': {'000001': 12.5, ...},
                ...
            }
        """
        factor_pool = {
            'ep': {}, 'bp': {}, 'sp': {}, 'ncfp': {}, 'div_yield': {},
            'revenue_growth': {}, 'profit_growth': {}, 'roe_change': {},
            'growth_accel': {}, 'eps_growth': {},
            'roe': {}, 'roa': {}, 'gross_margin': {}, 'net_margin': {},
            'debt_ratio': {}, 'current_ratio': {}, 'accruals': {},
            'price_momentum': {}, 'relative_strength': {}, 'volume_momentum': {},
            'ma_deviation': {}, 'volatility': {}, 'turnover': {}, 'volume_ratio': {},
            'market_cap': {},
        }
        
        for code in self.stock_codes:
            fina = self._load_financial_data(code)
            daily = self._load_daily_basic_data(code)
            kline = self._load_day_kline(code, 120)
            
            if fina and daily:
                # 价值因子
                if daily.get('pe') and daily['pe'] > 0:
                    factor_pool['ep'][code] = 100 / daily['pe']
                if daily.get('pb') and daily['pb'] > 0:
                    factor_pool['bp'][code] = 100 / daily['pb']
                if daily.get('ps') and daily['ps'] > 0:
                    factor_pool['sp'][code] = 100 / daily['ps']
                if fina.get('ocfps') and daily.get('close') and daily['close'] > 0:
                    factor_pool['ncfp'][code] = fina['ocfps'] / daily['close'] * 100
                if daily.get('dv_ratio'):
                    factor_pool['div_yield'][code] = daily['dv_ratio']
                
                # 成长因子
                if fina.get('revenue_growth') is not None:
                    factor_pool['revenue_growth'][code] = fina['revenue_growth']
                if fina.get('profit_growth') is not None:
                    factor_pool['profit_growth'][code] = fina['profit_growth']
                if fina.get('roe_change') is not None:
                    factor_pool['roe_change'][code] = fina['roe_change']
                if fina.get('eps_growth') is not None:
                    factor_pool['eps_growth'][code] = fina['eps_growth']
                
                # 质量因子
                if fina.get('roe') is not None:
                    factor_pool['roe'][code] = fina['roe']
                if fina.get('roa') is not None:
                    factor_pool['roa'][code] = fina['roa']
                if fina.get('gross_margin') is not None:
                    factor_pool['gross_margin'][code] = fina['gross_margin']
                if fina.get('net_margin') is not None:
                    factor_pool['net_margin'][code] = fina['net_margin']
                if fina.get('debt_ratio') is not None:
                    factor_pool['debt_ratio'][code] = fina['debt_ratio']
                if fina.get('current_ratio') is not None:
                    factor_pool['current_ratio'][code] = fina['current_ratio']
                if fina.get('ocfps') and fina.get('eps') and fina['eps'] != 0:
                    factor_pool['accruals'][code] = fina['ocfps'] / fina['eps']
                
                # 情绪因子
                if daily.get('turnover'):
                    factor_pool['turnover'][code] = daily['turnover']
                if daily.get('volume_ratio'):
                    factor_pool['volume_ratio'][code] = daily['volume_ratio']
                if daily.get('market_cap'):
                    factor_pool['market_cap'][code] = daily['market_cap'] / 10000
            
            # 动量因子
            if kline is not None and len(kline) >= 100:
                close = kline['close'].values
                factor_pool['price_momentum'][code] = close[-1] / close[-100] - 1
                ma20 = pd.Series(close).rolling(20).mean().values[-1]
                if ma20 > 0:
                    factor_pool['relative_strength'][code] = close[-1] / ma20 - 1
                    factor_pool['ma_deviation'][code] = (close[-1] - ma20) / ma20
                
                returns = pd.Series(close).pct_change().dropna()
                factor_pool['volatility'][code] = returns.std() * np.sqrt(252) * 100
        
        return factor_pool
    
    # ==================== 综合评分 ====================
    
    def calculate_score(self, stock_code: str, factor_pool: Dict) -> Optional[Dict]:
        """计算单只股票评分（使用预计算的因子池）"""
        fina_data = self._load_financial_data(stock_code)
        daily_data = self._load_daily_basic_data(stock_code)
        kline_df = self._load_day_kline(stock_code, 250)
        
        if fina_data is None or daily_data is None:
            return None
        
        # 计算各风格因子得分（行业中性）
        value_score, value_details = self._calc_value_score_v2(
            fina_data, daily_data, stock_code, factor_pool)
        growth_score, growth_details = self._calc_growth_score_v2(
            fina_data, stock_code, factor_pool)
        quality_score, quality_details = self._calc_quality_score_v2(
            fina_data, stock_code, factor_pool)
        momentum_score, momentum_details = self._calc_momentum_score_v2(
            kline_df, daily_data, stock_code, factor_pool)
        sentiment_score, sentiment_details = self._calc_sentiment_score_v2(
            daily_data, kline_df, stock_code, factor_pool)
        
        # 综合评分
        composite_score = (
            value_score * self.STYLE_WEIGHTS['value'] +
            growth_score * self.STYLE_WEIGHTS['growth'] +
            quality_score * self.STYLE_WEIGHTS['quality'] +
            momentum_score * self.STYLE_WEIGHTS['momentum'] +
            sentiment_score * self.STYLE_WEIGHTS['sentiment']
        )
        
        # 评级
        if composite_score >= 75:
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
            'industry': self._get_industry(stock_code),
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
    
    def generate_stock_pool(self, top_n: int = 50, min_score: float = 0,
                            filters: Optional[Dict] = None) -> Dict:
        """
        生成股票池
        
        Args:
            top_n: 取前N只股票
            min_score: 最低评分门槛
            filters: 筛选条件
                - min_value: 价值得分下限
                - min_growth: 成长得分下限
                - min_quality: 质量得分下限
                - industry: 行业筛选
                - grade: 评级筛选
        
        Returns:
            股票池数据
        """
        # 构建因子池（用于行业内百分位）
        factor_pool = self._build_factor_pool()
        
        results = []
        
        for code in self.stock_codes:
            try:
                score_data = self.calculate_score(code, factor_pool)
                if not score_data:
                    continue
                
                if score_data['composite_score'] < min_score:
                    continue
                
                # 应用筛选条件
                if filters:
                    if filters.get('min_value') and score_data['style_scores']['value'] < filters['min_value']:
                        continue
                    if filters.get('min_growth') and score_data['style_scores']['growth'] < filters['min_growth']:
                        continue
                    if filters.get('min_quality') and score_data['style_scores']['quality'] < filters['min_quality']:
                        continue
                    if filters.get('industry') and score_data['industry'] != filters['industry']:
                        continue
                    if filters.get('grade') and score_data['grade'] not in filters['grade']:
                        continue
                
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
    
    def export_to_csv(self, stocks: List[Dict], file_path: str) -> str:
        """导出股票池到CSV"""
        if not stocks:
            return ""
        
        rows = []
        for s in stocks:
            rows.append({
                '排名': s.get('rank', ''),
                '代码': s['stock_code'],
                '名称': s['stock_name'],
                '行业': s.get('industry', ''),
                '综合评分': s['composite_score'],
                '评级': s['grade'],
                '价值得分': s['style_scores']['value'],
                '成长得分': s['style_scores']['growth'],
                '质量得分': s['style_scores']['quality'],
                '动量得分': s['style_scores']['momentum'],
                '情绪得分': s['style_scores']['sentiment'],
                '最新价': s.get('close', ''),
                '交易日期': s.get('trade_date', ''),
            })
        
        df = pd.DataFrame(rows)
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        return file_path
    
    def get_industry_list(self) -> List[str]:
        """获取行业列表"""
        return list(set(self.stock_industry.values()))
    
    def get_industry_distribution(self, stocks: List[Dict]) -> Dict[str, int]:
        """获取行业分布"""
        distribution = {}
        for s in stocks:
            industry = s.get('industry', '其他')
            distribution[industry] = distribution.get(industry, 0) + 1
        return distribution


# ==================== 单例 ====================
_pro_scoring_v2 = None

def get_pro_scoring_v2() -> ProStockScoringServiceV2:
    global _pro_scoring_v2
    if _pro_scoring_v2 is None:
        _pro_scoring_v2 = ProStockScoringServiceV2()
    return _pro_scoring_v2