"""
大盘指数择时模块

使用沪深300指数判断大盘趋势，动态调整仓位
"""
import pandas as pd
import numpy as np
from typing import Dict, Tuple
import os


class MarketTimingEnhancer:
    """大盘指数择时增强"""
    
    # 沪深300数据缓存
    INDEX_CACHE = {}
    
    @classmethod
    def load_index_data(cls, index_code: str = '000300') -> pd.DataFrame:
        """加载指数数据"""
        if index_code in cls.INDEX_CACHE:
            return cls.INDEX_CACHE[index_code]
        
        # 尝试从缓存加载
        cache_path = f"data_cache/day/{index_code}_day.csv"
        
        if os.path.exists(cache_path):
            df = pd.read_csv(cache_path, encoding='utf-8')
            column_map = {'日期': 'trade_date', '收盘': 'close'}
            df = df.rename(columns=column_map)
            df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d')
            df.set_index('trade_date', inplace=True)
            df = df[['close']]
            df.columns = ['index_close']
            cls.INDEX_CACHE[index_code] = df
            return df
        
        return pd.DataFrame()
    
    @classmethod
    def calculate_market_trend(cls, df: pd.DataFrame, lookback: int = 60) -> pd.DataFrame:
        """计算大盘趋势指标"""
        result = df.copy()
        
        # 趋势强度
        result['index_ma20'] = result['index_close'].rolling(20).mean()
        result['index_ma60'] = result['index_close'].rolling(60).mean()
        result['trend'] = (result['index_ma20'] / result['index_ma60'] - 1) * 100
        
        # 趋势斜率
        result['trend_slope'] = result['index_close'].pct_change(20) * 100
        
        # 波动率
        result['index_vol'] = result['index_close'].pct_change().rolling(20).std() * np.sqrt(252)
        
        # 趋势得分（综合判断）
        result['trend_score'] = 0
        result.loc[result['trend'] > 2, 'trend_score'] = 1   # 明显上涨
        result.loc[result['trend'] > 5, 'trend_score'] = 2   # 强势上涨
        result.loc[result['trend'] < -2, 'trend_score'] = -1  # 明显下跌
        result.loc[result['trend'] < -5, 'trend_score'] = -2  # 强势下跌
        
        return result
    
    @classmethod
    def get_market_adjustment(cls, trend_score: int, volatility: float) -> Tuple[float, str]:
        """
        根据大盘趋势获取仓位调整系数
        
        Returns:
            (adjustment, description)
        """
        if trend_score >= 2:
            # 强势上涨
            return 1.3, "强势上涨+30%"
        elif trend_score >= 1:
            # 明显上涨
            return 1.15, "明显上涨+15%"
        elif trend_score <= -2:
            # 强势下跌
            return 0.3, "强势下跌-70%"
        elif trend_score <= -1:
            # 明显下跌
            return 0.5, "明显下跌-50%"
        else:
            # 震荡
            if volatility > 0.25:
                return 0.7, "高波动震荡-30%"
            else:
                return 1.0, "正常震荡"


# 行业分类配置
INDUSTRY_CONFIG = {
    # 科技行业（TMT）
    'technology': {
        'stocks': ['000063', '000066', '000725', '000977', '002230', '002415', '002475', '300059'],
        'factor_weights': {
            'MOM': 0.35,    # 动量优先
            'KDJ': 0.25,
            'BOLL': 0.15,
            'TURN': 0.15,
            'ROE': 0.10
        },
        'description': '科技行业：动量为主'
    },
    
    # 消费行业
    'consumer': {
        'stocks': ['000568', '000858', '000895', '002304', '002507', '002714', '600519', '600887'],
        'factor_weights': {
            'ROE': 0.30,    # 质量优先
            'LEV': 0.25,
            'EP': 0.20,
            'BP': 0.15,
            'MOM': 0.10
        },
        'description': '消费行业：质量为主'
    },
    
    # 金融行业
    'finance': {
        'stocks': ['000001', '000002', '601318', '601398', '601939', '601988', '600036'],
        'factor_weights': {
            'EP': 0.30,     # 价值优先
            'BP': 0.25,
            'LEV': 0.20,
            'ROE': 0.15,
            'BOLL': 0.10
        },
        'description': '金融行业：价值为主'
    },
    
    # 医药行业
    'healthcare': {
        'stocks': ['000538', '000661', '002007', '002821', '300015', '300347', '600276'],
        'factor_weights': {
            'ROE': 0.30,
            'MOM': 0.25,
            'LEV': 0.20,
            'KDJ': 0.15,
            'EP': 0.10
        },
        'description': '医药行业：质量+动量'
    },
    
    # 制造业
    'manufacturing': {
        'stocks': ['000021', '000157', '000333', '000651', '002050', '002594', '601766'],
        'factor_weights': {
            'MOM': 0.25,
            'ROE': 0.25,
            'LEV': 0.20,
            'KDJ': 0.15,
            'BOLL': 0.15
        },
        'description': '制造业：动量+质量'
    },
    
    # 能源行业
    'energy': {
        'stocks': ['600028', '601857', '601088', '000983', '601225'],
        'factor_weights': {
            'EP': 0.35,
            'BP': 0.30,
            'LEV': 0.20,
            'BOLL': 0.15
        },
        'description': '能源行业：价值为主'
    }
}


def get_stock_industry(stock_code: str) -> str:
    """获取股票所属行业"""
    for industry, config in INDUSTRY_CONFIG.items():
        if stock_code in config['stocks']:
            return industry
    return 'default'


def get_industry_factor_weights(stock_code: str) -> Dict[str, float]:
    """获取股票对应的行业因子权重"""
    industry = get_stock_industry(stock_code)
    
    if industry != 'default':
        return INDUSTRY_CONFIG[industry]['factor_weights']
    
    # 默认权重
    return {
        'MOM': 0.25,
        'KDJ': 0.20,
        'BOLL': 0.15,
        'LEV': 0.15,
        'ROE': 0.15,
        'EP': 0.10
    }


def get_industry_description(stock_code: str) -> str:
    """获取行业描述"""
    industry = get_stock_industry(stock_code)
    
    if industry != 'default':
        return INDUSTRY_CONFIG[industry]['description']
    
    return '默认配置'