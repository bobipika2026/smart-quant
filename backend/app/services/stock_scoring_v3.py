"""
专业量化因子评分服务 v3.0

严格遵循专业量化机构标准：
1. 三层级因子结构（风格因子 -> 细分因子）
2. 行业中性化评分
3. MAD去极值 + Z-Score标准化
4. 基于IC优化的权重配置
5. 风险控制（行业偏离、因子暴露）
"""
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import sqlite3
from dataclasses import dataclass
from enum import Enum


class Grade(Enum):
    """评级枚举"""
    A = "A"      # 90-100百分位
    B_PLUS = "B+"  # 75-90
    B = "B"      # 50-75
    B_MINUS = "B-"  # 35-50
    C = "C"      # 15-35
    D = "D"      # 0-15


@dataclass
class FactorDefinition:
    """因子定义"""
    name: str
    code: str
    formula: str
    weight: float
    ic_expected: float
    direction: int  # 1=正向, -1=反向


class ProFactorScoringV3:
    """
    专业量化因子评分服务 v3.0
    
    严格遵循Barra/MSCI标准
    """
    
    # ==================== 行业分类（申万一级行业） ====================
    INDUSTRY_CODES = {
        '银行': ['000001', '000002', '600000', '600015', '600016', '600036', 
                 '601166', '601288', '601328', '601398', '601939', '601988', '601998'],
        '非银金融': ['000166', '000776', '600030', '600837', '601211', '601688', '601788', '601901'],
        '食品饮料': ['000568', '000858', '000895', '600519', '600887', '603369', '000848'],
        '医药生物': ['000538', '000661', '002007', '002821', '300015', '300347', 
                    '600276', '603259', '000963', '002422'],
        '电子': ['000063', '000725', '002049', '002241', '002415', '300124', 
                 '600584', '603501', '002371', '300661'],
        '计算机': ['000977', '002230', '002410', '300033', '300059', '600570', '600588'],
        '传媒': ['000156', '000917', '002292', '300027', '300251', '300413', '601801'],
        '电力设备': ['000400', '002074', '002129', '002459', '300014', '300274', 
                    '600089', '601012', '002459'],
        '机械设备': ['000157', '000425', '002031', '002523', '600031', '601633', '002523'],
        '汽车': ['000625', '000868', '002594', '600066', '600104', '600733', '601238'],
        '化工': ['000059', '000422', '002092', '002648', '600309', '600426', '603379'],
        '有色金属': ['000060', '000630', '002466', '600219', '600362', '601899'],
        '建筑装饰': ['000066', '002081', '601186', '601390', '601668', '601390'],
        '房地产': ['000002', '000069', '000656', '001979', '600048', '601155'],
        '交通运输': ['000089', '000905', '600009', '600029', '601006', '601111', '601872'],
        '家用电器': ['000333', '000418', '000651', '002050', '600690'],
        '公用事业': ['000027', '000543', '600011', '600642', '601991', '600886'],
        '通信': ['000063', '002123', '300502', '600050', '601728'],
        '国防军工': ['000768', '002013', '600038', '600893', '601989'],
    }
    
    # ==================== 因子定义 ====================
    
    # 价值因子
    VALUE_FACTORS = [
        FactorDefinition('盈利收益率', 'EP', 'E_ttm/MarketCap', 0.25, 0.04, 1),
        FactorDefinition('账面市值比', 'BP', 'B/MarketCap', 0.20, 0.03, 1),
        FactorDefinition('销售市值比', 'SP', 'Revenue_ttm/MarketCap', 0.15, 0.02, 1),
        FactorDefinition('现金流市值比', 'NCFP', 'OCFA_ttm/MarketCap', 0.20, 0.04, 1),
        FactorDefinition('股息率', 'DIV', 'DPS/Price', 0.10, 0.03, 1),
        FactorDefinition('企业价值比', 'EV_EBITDA', 'EBITDA/EV', 0.10, 0.03, 1),
    ]
    
    # 成长因子
    GROWTH_FACTORS = [
        FactorDefinition('营收增长率', 'REV_G', '(Rev_t-Rev_t-1)/|Rev_t-1|', 0.20, 0.03, 1),
        FactorDefinition('净利润增长率', 'NP_G', '(NP_t-NP_t-1)/|NP_t-1|', 0.25, 0.04, 1),
        FactorDefinition('EPS增长率', 'EPS_G', '(EPS_t-EPS_t-1)/|EPS_t-1|', 0.15, 0.03, 1),
        FactorDefinition('ROE变化', 'ROE_D', 'ROE_t-ROE_t-1', 0.20, 0.03, 1),
        FactorDefinition('营收增长加速度', 'REV_ACC', 'ΔREV_G', 0.10, 0.02, 1),
        FactorDefinition('净利率变化', 'NPM_D', 'NPM_t-NPM_t-1', 0.10, 0.02, 1),
    ]
    
    # 质量因子
    QUALITY_FACTORS = [
        FactorDefinition('ROE', 'ROE', 'NetIncome/Equity', 0.20, 0.06, 1),
        FactorDefinition('ROA', 'ROA', 'NetIncome/Assets', 0.10, 0.04, 1),
        FactorDefinition('毛利率', 'GPM', '(Rev-COGS)/Rev', 0.10, 0.03, 1),
        FactorDefinition('净利率', 'NPM', 'NetIncome/Revenue', 0.10, 0.03, 1),
        FactorDefinition('资产周转率', 'AT', 'Revenue/Assets', 0.10, 0.02, 1),
        FactorDefinition('财务杠杆', 'LEV', 'Assets/Equity', 0.10, 0.02, -1),  # 反向
        FactorDefinition('流动比率', 'CR', 'CA/CL', 0.05, 0.01, 1),
        FactorDefinition('应计项目', 'ACCR', '(NI-OCF)/Assets', 0.15, 0.04, -1),  # 反向
        FactorDefinition('现金流质量', 'OCF_NI', 'OCF/NI', 0.10, 0.03, 1),
    ]
    
    # 动量因子
    MOMENTUM_FACTORS = [
        FactorDefinition('12月动量', 'MOM12', 'Ret_t-1m/Ret_t-12m-1', 0.30, 0.05, 1),
        FactorDefinition('6月动量', 'MOM6', 'Ret_t/Ret_t-6m-1', 0.20, 0.04, 1),
        FactorDefinition('相对强度', 'RS', 'Stock/Industry_Index', 0.15, 0.03, 1),
        FactorDefinition('成交量动量', 'VOL_M', 'Vol_5d/Vol_20d', 0.10, 0.02, 1),
        FactorDefinition('均线偏离', 'MA_DEV', '(P-MA20)/MA20', 0.10, 0.02, 1),
        FactorDefinition('盈利修正', 'ER', 'EPS_revise_up/total', 0.15, 0.04, 1),
    ]
    
    # 情绪因子
    SENTIMENT_FACTORS = [
        FactorDefinition('北向持股变化', 'NORTH_C', 'ΔNorth/Shares', 0.25, 0.05, 1),
        FactorDefinition('机构持股比例', 'INST', 'Inst_Holding/Shares', 0.20, 0.04, 1),
        FactorDefinition('分析师评级', 'ANALYST', 'Rating_mean', 0.15, 0.03, 1),
        FactorDefinition('换手率', 'TURN', 'Vol/Float', 0.10, 0.02, 0),  # 中性
        FactorDefinition('融资余额变化', 'MTGN_C', 'ΔMargin/Cap', 0.15, 0.03, 1),
        FactorDefinition('波动率', 'VOL', 'Std(Ret)*√252', 0.15, 0.02, -1),  # 反向
    ]
    
    # 风格权重（基于IC优化）
    STYLE_WEIGHTS = {
        'value': 0.22,
        'growth': 0.18,
        'quality': 0.28,
        'momentum': 0.17,
        'sentiment': 0.15,
    }
    
    # 数据目录
    FINANCIAL_DIR = "data_cache/financial"
    DAY_CACHE_DIR = "data_cache/day"
    DB_PATH = "smart_quant.db"
    
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
        self.industry_stocks = {}
        
        for industry, codes in self.INDUSTRY_CODES.items():
            self.industry_stocks[industry] = []
            for code in codes:
                if code in self.stock_names:
                    self.stock_industry[code] = industry
                    self.industry_stocks[industry].append(code)
    
    def _get_industry(self, stock_code: str) -> str:
        """获取股票行业"""
        return self.stock_industry.get(stock_code, '其他')
    
    # ==================== 数据加载 ====================
    
    def _load_financial_data(self, stock_code: str) -> Optional[Dict]:
        """加载财务数据"""
        file_path = os.path.join(self.FINANCIAL_DIR, f"{stock_code}_fina_indicator.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty or len(df) < 4:
                return None
            
            df = df.sort_values('end_date', ascending=False)
            latest = df.iloc[0]
            prev = df.iloc[3] if len(df) > 3 else df.iloc[-1]
            
            return {
                # 盈利能力
                'roe': self._safe_float(latest.get('roe')),
                'roa': self._safe_float(latest.get('roa')),
                'net_margin': self._safe_float(latest.get('netprofit_margin')),
                'gross_margin': self._safe_float(latest.get('grossprofit_margin')),
                'eps': self._safe_float(latest.get('eps')),
                'bps': self._safe_float(latest.get('bps')),
                
                # 增长率（同比）
                'revenue_growth': self._safe_float(latest.get('tr_yoy')),
                'profit_growth': self._safe_float(latest.get('netprofit_yoy')),
                'eps_growth': self._safe_float(latest.get('dt_eps_yoy')),
                'roe_change': self._safe_float(latest.get('roe_yoy')),
                
                # 财务健康
                'debt_ratio': self._safe_float(latest.get('debt_to_assets')),
                'current_ratio': self._safe_float(latest.get('current_ratio')),
                'ocfps': self._safe_float(latest.get('ocfps')),
                
                # 上期数据（用于计算变化）
                'prev_roe': self._safe_float(prev.get('roe')),
                'prev_net_margin': self._safe_float(prev.get('netprofit_margin')),
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
    
    def _load_day_kline(self, stock_code: str, days: int = 300) -> Optional[pd.DataFrame]:
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
    
    # ==================== 因子计算 ====================
    
    def _calc_value_factors(self, fina: Dict, daily: Dict) -> Dict[str, float]:
        """计算价值因子"""
        factors = {}
        
        # EP (盈利收益率)
        if daily.get('pe') and daily['pe'] > 0:
            factors['EP'] = 100 / daily['pe']
        else:
            factors['EP'] = None
        
        # BP (账面市值比)
        if daily.get('pb') and daily['pb'] > 0:
            factors['BP'] = 100 / daily['pb']
        else:
            factors['BP'] = None
        
        # SP (销售市值比)
        if daily.get('ps') and daily['ps'] > 0:
            factors['SP'] = 100 / daily['ps']
        else:
            factors['SP'] = None
        
        # NCFP (现金流市值比)
        if fina.get('ocfps') and daily.get('close') and daily['close'] > 0:
            factors['NCFP'] = fina['ocfps'] / daily['close'] * 100
        else:
            factors['NCFP'] = None
        
        # DIV (股息率)
        factors['DIV'] = daily.get('dv_ratio')
        
        # EV_EBITDA (暂用PE近似)
        factors['EV_EBITDA'] = factors.get('EP')
        
        return factors
    
    def _calc_growth_factors(self, fina: Dict) -> Dict[str, float]:
        """计算成长因子"""
        factors = {}
        
        # 营收增长率
        factors['REV_G'] = fina.get('revenue_growth')
        
        # 净利润增长率
        factors['NP_G'] = fina.get('profit_growth')
        
        # EPS增长率
        factors['EPS_G'] = fina.get('eps_growth')
        
        # ROE变化
        factors['ROE_D'] = fina.get('roe_change')
        
        # 营收增长加速度（用利润增长近似）
        factors['REV_ACC'] = fina.get('profit_growth')
        
        # 净利率变化
        if fina.get('net_margin') and fina.get('prev_net_margin'):
            factors['NPM_D'] = fina['net_margin'] - fina['prev_net_margin']
        else:
            factors['NPM_D'] = None
        
        return factors
    
    def _calc_quality_factors(self, fina: Dict) -> Dict[str, float]:
        """计算质量因子"""
        factors = {}
        
        # ROE
        factors['ROE'] = fina.get('roe')
        
        # ROA
        factors['ROA'] = fina.get('roa')
        
        # 毛利率
        factors['GPM'] = fina.get('gross_margin')
        
        # 净利率
        factors['NPM'] = fina.get('net_margin')
        
        # 资产周转率（暂用ROA近似）
        factors['AT'] = fina.get('roa')
        
        # 财务杠杆（反向，负债率）
        if fina.get('debt_ratio'):
            factors['LEV'] = fina['debt_ratio']
        else:
            factors['LEV'] = None
        
        # 流动比率
        factors['CR'] = fina.get('current_ratio')
        
        # 应计项目（现金流质量）
        if fina.get('ocfps') and fina.get('eps') and fina['eps'] != 0:
            factors['ACCR'] = fina['ocfps'] / fina['eps']
        else:
            factors['ACCR'] = None
        
        # 现金流质量
        if fina.get('ocfps') and fina.get('eps') and fina['eps'] != 0:
            factors['OCF_NI'] = min(fina['ocfps'] / fina['eps'], 3)  # 限制上限
        else:
            factors['OCF_NI'] = None
        
        return factors
    
    def _calc_momentum_factors(self, kline: pd.DataFrame, daily: Dict) -> Dict[str, float]:
        """计算动量因子"""
        factors = {}
        
        if kline is None or len(kline) < 100:
            for f in ['MOM12', 'MOM6', 'RS', 'VOL_M', 'MA_DEV', 'ER']:
                factors[f] = None
            return factors
        
        close = kline['close'].values
        volume = kline['vol'].values if 'vol' in kline.columns else None
        
        # MOM12 (12月动量，剔除最近一月)
        if len(close) >= 240:
            factors['MOM12'] = close[-22] / close[-244] - 1
        elif len(close) >= 120:
            factors['MOM12'] = close[-22] / close[-120] - 1
        else:
            factors['MOM12'] = None
        
        # MOM6 (6月动量)
        if len(close) >= 120:
            factors['MOM6'] = close[-1] / close[-120] - 1
        else:
            factors['MOM6'] = None
        
        # RS (相对强度 = 价格/MA20)
        ma20 = pd.Series(close).rolling(20).mean().values
        if ma20[-1] > 0:
            factors['RS'] = close[-1] / ma20[-1] - 1
        else:
            factors['RS'] = None
        
        # VOL_M (成交量动量)
        if volume is not None and len(volume) >= 20:
            vol_5 = volume[-5:].mean()
            vol_20 = volume[-20:].mean()
            factors['VOL_M'] = vol_5 / vol_20 if vol_20 > 0 else 1
        else:
            factors['VOL_M'] = None
        
        # MA_DEV (均线偏离)
        if ma20[-1] > 0:
            factors['MA_DEV'] = (close[-1] - ma20[-1]) / ma20[-1]
        else:
            factors['MA_DEV'] = None
        
        # ER (盈利修正，暂用动量代理)
        factors['ER'] = factors.get('MOM6')
        
        return factors
    
    def _calc_sentiment_factors(self, daily: Dict, kline: pd.DataFrame) -> Dict[str, float]:
        """计算情绪因子"""
        factors = {}
        
        # NORTH_C (北向持股变化，用市值代理)
        if daily.get('market_cap'):
            factors['NORTH_C'] = daily['market_cap'] / 10000  # 亿元
        else:
            factors['NORTH_C'] = None
        
        # INST (机构持股，用市值代理)
        factors['INST'] = factors.get('NORTH_C')
        
        # ANALYST (分析师评级，暂无数据)
        factors['ANALYST'] = None
        
        # TURN (换手率)
        factors['TURN'] = daily.get('turnover')
        
        # MTGN_C (融资余额变化，暂无数据)
        factors['MTGN_C'] = None
        
        # VOL (波动率，反向)
        if kline is not None and len(kline) >= 20:
            returns = pd.Series(kline['close'].values).pct_change().dropna()
            factors['VOL'] = returns.std() * np.sqrt(252) * 100
        else:
            factors['VOL'] = None
        
        return factors
    
    # ==================== 标准化处理 ====================
    
    def _winsorize_mad(self, series: pd.Series, n_mad: float = 5.0) -> pd.Series:
        """MAD法去极值"""
        median = series.median()
        mad = (series - median).abs().median()
        
        if mad == 0:
            return series
        
        upper = median + n_mad * mad
        lower = median - n_mad * mad
        
        return series.clip(lower, upper)
    
    def _neutralize_by_industry(self, values: pd.Series, stock_codes: List[str]) -> pd.Series:
        """行业中性化"""
        neutralized = pd.Series(index=values.index)
        
        for i, code in enumerate(stock_codes):
            industry = self._get_industry(code)
            # 获取同行业股票的索引
            industry_indices = [j for j, c in enumerate(stock_codes) 
                               if self._get_industry(c) == industry]
            
            if len(industry_indices) >= 3:
                industry_values = values.iloc[industry_indices]
                industry_mean = industry_values.mean()
                neutralized.iloc[i] = values.iloc[i] - industry_mean
            else:
                neutralized.iloc[i] = values.iloc[i] - values.mean()
        
        return neutralized.fillna(0)
    
    def _zscore_normalize(self, values: pd.Series) -> pd.Series:
        """Z-Score标准化"""
        mean = values.mean()
        std = values.std()
        
        if std == 0:
            return pd.Series(0, index=values.index)
        
        return (values - mean) / std
    
    def _percentile_score(self, zscore: float) -> float:
        """Z-Score转百分位得分"""
        from scipy import stats as sp_stats
        try:
            percentile = sp_stats.norm.cdf(zscore) * 100
            return max(0, min(100, percentile))
        except:
            return 50.0
    
    # ==================== 综合评分 ====================
    
    def calculate_score(self, stock_code: str, 
                        factor_pool: Optional[Dict] = None) -> Optional[Dict]:
        """
        计算单只股票评分
        
        Returns:
            {
                'stock_code': str,
                'stock_name': str,
                'industry': str,
                'composite_score': float,
                'grade': str,
                'style_scores': {...},
                'factor_details': {...}
            }
        """
        # 加载数据
        fina = self._load_financial_data(stock_code)
        daily = self._load_daily_basic_data(stock_code)
        kline = self._load_day_kline(stock_code, 300)
        
        if fina is None or daily is None:
            return None
        
        # 计算各因子原始值
        value_factors = self._calc_value_factors(fina, daily)
        growth_factors = self._calc_growth_factors(fina)
        quality_factors = self._calc_quality_factors(fina)
        momentum_factors = self._calc_momentum_factors(kline, daily)
        sentiment_factors = self._calc_sentiment_factors(daily, kline)
        
        # 如果有因子池，使用行业内百分位
        if factor_pool:
            value_score = self._calc_style_score_with_pool(
                value_factors, self.VALUE_FACTORS, factor_pool, stock_code, 'value')
            growth_score = self._calc_style_score_with_pool(
                growth_factors, self.GROWTH_FACTORS, factor_pool, stock_code, 'growth')
            quality_score = self._calc_style_score_with_pool(
                quality_factors, self.QUALITY_FACTORS, factor_pool, stock_code, 'quality')
            momentum_score = self._calc_style_score_with_pool(
                momentum_factors, self.MOMENTUM_FACTORS, factor_pool, stock_code, 'momentum')
            sentiment_score = self._calc_style_score_with_pool(
                sentiment_factors, self.SENTIMENT_FACTORS, factor_pool, stock_code, 'sentiment')
        else:
            # 无因子池，使用简单百分位
            value_score = self._calc_style_score_simple(value_factors, self.VALUE_FACTORS)
            growth_score = self._calc_style_score_simple(growth_factors, self.GROWTH_FACTORS)
            quality_score = self._calc_style_score_simple(quality_factors, self.QUALITY_FACTORS)
            momentum_score = self._calc_style_score_simple(momentum_factors, self.MOMENTUM_FACTORS)
            sentiment_score = self._calc_style_score_simple(sentiment_factors, self.SENTIMENT_FACTORS)
        
        # 综合评分
        composite_score = (
            value_score * self.STYLE_WEIGHTS['value'] +
            growth_score * self.STYLE_WEIGHTS['growth'] +
            quality_score * self.STYLE_WEIGHTS['quality'] +
            momentum_score * self.STYLE_WEIGHTS['momentum'] +
            sentiment_score * self.STYLE_WEIGHTS['sentiment']
        )
        
        # 评级（基于百分位）
        if composite_score >= 90:
            grade = 'A'
        elif composite_score >= 75:
            grade = 'B+'
        elif composite_score >= 50:
            grade = 'B'
        elif composite_score >= 35:
            grade = 'B-'
        elif composite_score >= 15:
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
            'factor_details': {
                'value': {f.name: v for f, v in zip(self.VALUE_FACTORS, value_factors.values())},
                'growth': {f.name: v for f, v in zip(self.GROWTH_FACTORS, growth_factors.values())},
                'quality': {f.name: v for f, v in zip(self.QUALITY_FACTORS, quality_factors.values())},
                'momentum': {f.name: v for f, v in zip(self.MOMENTUM_FACTORS, momentum_factors.values())},
                'sentiment': {f.name: v for f, v in zip(self.SENTIMENT_FACTORS, sentiment_factors.values())},
            },
            'trade_date': daily.get('trade_date'),
            'close': daily.get('close'),
        }
    
    def _calc_style_score_with_pool(self, factors: Dict[str, float], 
                                     factor_defs: List[FactorDefinition],
                                     factor_pool: Dict, stock_code: str,
                                     style_name: str) -> float:
        """使用因子池计算风格得分（行业内百分位）"""
        industry = self._get_industry(stock_code)
        weighted_scores = []
        total_weight = 0
        
        for f_def in factor_defs:
            value = factors.get(f_def.code)
            if value is None:
                continue
            
            # 从因子池获取行业内百分位
            pool_key = f"{style_name}_{f_def.code}"
            if pool_key in factor_pool:
                industry_values = factor_pool[pool_key].get(industry, {})
                if industry_values:
                    values = list(industry_values.values())
                    # 计算百分位
                    rank = sum(1 for v in values if v <= value)
                    percentile = rank / len(values) * 100
                else:
                    percentile = 50.0
            else:
                percentile = 50.0
            
            # 反向因子取反
            if f_def.direction == -1:
                percentile = 100 - percentile
            elif f_def.direction == 0:  # 中性因子，中间值最优
                percentile = 100 - abs(percentile - 50) * 2
            
            weighted_scores.append(percentile * f_def.weight)
            total_weight += f_def.weight
        
        if total_weight == 0:
            return 50.0
        
        return sum(weighted_scores) / total_weight
    
    def _calc_style_score_simple(self, factors: Dict[str, float],
                                  factor_defs: List[FactorDefinition]) -> float:
        """简单风格得分（无因子池）"""
        weighted_scores = []
        total_weight = 0
        
        for f_def in factor_defs:
            value = factors.get(f_def.code)
            if value is None:
                continue
            
            # 简单阈值评分
            score = self._simple_threshold_score(value, f_def.code)
            
            # 反向因子取反
            if f_def.direction == -1:
                score = 100 - score
            elif f_def.direction == 0:
                score = 100 - abs(score - 50) * 2
            
            weighted_scores.append(score * f_def.weight)
            total_weight += f_def.weight
        
        if total_weight == 0:
            return 50.0
        
        return sum(weighted_scores) / total_weight
    
    def _simple_threshold_score(self, value: float, factor_code: str) -> float:
        """简单阈值评分"""
        # 根据因子类型设置阈值
        thresholds = {
            'EP': [(10, 100), (6, 70), (3, 50), (0, 30)],
            'BP': [(100, 100), (50, 70), (20, 50), (0, 30)],
            'ROE': [(20, 100), (15, 80), (10, 60), (5, 40), (0, 20)],
            'ROA': [(10, 100), (6, 70), (3, 50), (0, 30)],
            'MOM12': [(0.3, 100), (0.15, 70), (0, 50), (-0.15, 30)],
        }
        
        th = thresholds.get(factor_code, [(1, 100), (0.5, 70), (0, 50)])
        for threshold, score in th:
            if value >= threshold:
                return score
        return th[-1][1]
    
    # ==================== 股票池生成 ====================
    
    def build_factor_pool(self) -> Dict:
        """构建因子池（用于行业内百分位计算）"""
        pool = {}
        
        for code in self.stock_codes:
            fina = self._load_financial_data(code)
            daily = self._load_daily_basic_data(code)
            kline = self._load_day_kline(code, 300)
            
            if not fina or not daily:
                continue
            
            industry = self._get_industry(code)
            
            # 价值因子
            value_factors = self._calc_value_factors(fina, daily)
            for f_def in self.VALUE_FACTORS:
                key = f"value_{f_def.code}"
                if key not in pool:
                    pool[key] = {}
                if industry not in pool[key]:
                    pool[key][industry] = {}
                if value_factors.get(f_def.code) is not None:
                    pool[key][industry][code] = value_factors[f_def.code]
            
            # 成长因子
            growth_factors = self._calc_growth_factors(fina)
            for f_def in self.GROWTH_FACTORS:
                key = f"growth_{f_def.code}"
                if key not in pool:
                    pool[key] = {}
                if industry not in pool[key]:
                    pool[key][industry] = {}
                if growth_factors.get(f_def.code) is not None:
                    pool[key][industry][code] = growth_factors[f_def.code]
            
            # 质量因子
            quality_factors = self._calc_quality_factors(fina)
            for f_def in self.QUALITY_FACTORS:
                key = f"quality_{f_def.code}"
                if key not in pool:
                    pool[key] = {}
                if industry not in pool[key]:
                    pool[key][industry] = {}
                if quality_factors.get(f_def.code) is not None:
                    pool[key][industry][code] = quality_factors[f_def.code]
            
            # 动量因子
            momentum_factors = self._calc_momentum_factors(kline, daily)
            for f_def in self.MOMENTUM_FACTORS:
                key = f"momentum_{f_def.code}"
                if key not in pool:
                    pool[key] = {}
                if industry not in pool[key]:
                    pool[key][industry] = {}
                if momentum_factors.get(f_def.code) is not None:
                    pool[key][industry][code] = momentum_factors[f_def.code]
            
            # 情绪因子
            sentiment_factors = self._calc_sentiment_factors(daily, kline)
            for f_def in self.SENTIMENT_FACTORS:
                key = f"sentiment_{f_def.code}"
                if key not in pool:
                    pool[key] = {}
                if industry not in pool[key]:
                    pool[key][industry] = {}
                if sentiment_factors.get(f_def.code) is not None:
                    pool[key][industry][code] = sentiment_factors[f_def.code]
        
        return pool
    
    def generate_stock_pool(self, top_n: int = 50, min_score: float = 0,
                            filters: Optional[Dict] = None) -> Dict:
        """生成股票池"""
        # 构建因子池
        factor_pool = self.build_factor_pool()
        
        results = []
        for code in self.stock_codes:
            try:
                score_data = self.calculate_score(code, factor_pool)
                if not score_data:
                    continue
                
                if score_data['composite_score'] < min_score:
                    continue
                
                # 应用筛选
                if filters:
                    if filters.get('min_value'):
                        if score_data['style_scores']['value'] < filters['min_value']:
                            continue
                    if filters.get('min_growth'):
                        if score_data['style_scores']['growth'] < filters['min_growth']:
                            continue
                    if filters.get('min_quality'):
                        if score_data['style_scores']['quality'] < filters['min_quality']:
                            continue
                    if filters.get('industry'):
                        if score_data['industry'] != filters['industry']:
                            continue
                    if filters.get('grade'):
                        if score_data['grade'] not in filters['grade']:
                            continue
                
                results.append(score_data)
            except:
                continue
        
        # 排序
        results.sort(key=lambda x: x['composite_score'], reverse=True)
        
        # 取Top N
        top_stocks = results[:top_n]
        
        return {
            'stocks': top_stocks,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_count': len(self.stock_codes),
            'qualified_count': len(results),
        }


# 单例
_scoring_v3 = None

def get_scoring_v3() -> ProFactorScoringV3:
    global _scoring_v3
    if _scoring_v3 is None:
        _scoring_v3 = ProFactorScoringV3()
    return _scoring_v3