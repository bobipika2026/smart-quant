"""
动态因子权重调整系统

根据以下因素动态调整因子权重：
1. 市场环境（牛熊震荡）
2. 经济周期（美林时钟）
3. 板块轮动
4. 因子近期IC表现
5. 风险偏好指标
"""
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum


class MarketRegime(Enum):
    """市场环境"""
    BULL = "bull"           # 牛市
    BEAR = "bear"           # 熊市
    SIDEWAYS = "sideways"   # 震荡市
    RECOVERY = "recovery"   # 反转期


class EconomicCycle(Enum):
    """经济周期（美林时钟）"""
    RECOVERY = "recovery"   # 复苏期
    EXPANSION = "expansion" # 繁荣期
    SLOWDOWN = "slowdown"   # 衰退期
    RECESSION = "recession" # 萧条期


@dataclass
class WeightConfig:
    """权重配置"""
    value: float
    growth: float
    quality: float
    momentum: float
    sentiment: float


# ==================== 市场环境权重矩阵 ====================

MARKET_REGIME_WEIGHTS = {
    # 牛市：动量优先，成长次之
    MarketRegime.BULL: WeightConfig(
        value=0.15,
        growth=0.22,
        quality=0.18,
        momentum=0.28,
        sentiment=0.17
    ),
    # 熊市：质量优先，价值次之
    MarketRegime.BEAR: WeightConfig(
        value=0.28,
        growth=0.12,
        quality=0.32,
        momentum=0.10,
        sentiment=0.18
    ),
    # 震荡市：价值和情绪优先
    MarketRegime.SIDEWAYS: WeightConfig(
        value=0.28,
        growth=0.18,
        quality=0.22,
        momentum=0.12,
        sentiment=0.20
    ),
    # 反转期：成长和动量优先
    MarketRegime.RECOVERY: WeightConfig(
        value=0.18,
        growth=0.28,
        quality=0.20,
        momentum=0.22,
        sentiment=0.12
    ),
}

# ==================== 经济周期权重矩阵 ====================

ECONOMIC_CYCLE_WEIGHTS = {
    # 复苏期：成长优先
    EconomicCycle.RECOVERY: WeightConfig(
        value=0.18,
        growth=0.30,
        quality=0.20,
        momentum=0.22,
        sentiment=0.10
    ),
    # 繁荣期：动量优先
    EconomicCycle.EXPANSION: WeightConfig(
        value=0.15,
        growth=0.22,
        quality=0.18,
        momentum=0.30,
        sentiment=0.15
    ),
    # 衰退期：质量优先
    EconomicCycle.SLOWDOWN: WeightConfig(
        value=0.22,
        growth=0.15,
        quality=0.30,
        momentum=0.15,
        sentiment=0.18
    ),
    # 萧条期：价值优先
    EconomicCycle.RECESSION: WeightConfig(
        value=0.32,
        growth=0.12,
        quality=0.28,
        momentum=0.08,
        sentiment=0.20
    ),
}

# ==================== 基准权重 ====================

BASELINE_WEIGHTS = WeightConfig(
    value=0.22,
    growth=0.18,
    quality=0.28,
    momentum=0.17,
    sentiment=0.15
)


class DynamicWeightSystem:
    """动态权重调整系统"""
    
    def __init__(self, db_path: str = "smart_quant.db"):
        self.db_path = db_path
        self.day_cache_dir = "data_cache/day"
    
    # ==================== 市场环境识别 ====================
    
    def detect_market_regime(self, lookback_days: int = 60) -> MarketRegime:
        """
        识别当前市场环境
        
        方法：
        1. 计算指数收益率和波动率
        2. 判断趋势强度
        3. 结合成交量变化
        """
        try:
            # 使用上证指数作为市场基准
            index_file = os.path.join(self.day_cache_dir, "000001_day.csv")
            
            if not os.path.exists(index_file):
                return MarketRegime.SIDEWAYS
            
            df = pd.read_csv(index_file)
            if df.empty:
                return MarketRegime.SIDEWAYS
            
            df = df.sort_values('trade_date', ascending=False).head(lookback_days)
            df = df.sort_values('trade_date', ascending=True)
            
            close = df['close'].values
            
            if len(close) < 20:
                return MarketRegime.SIDEWAYS
            
            # 计算收益率
            returns = pd.Series(close).pct_change().dropna()
            
            # 计算指标
            total_return = (close[-1] - close[0]) / close[0] * 100
            volatility = returns.std() * np.sqrt(252) * 100
            
            # 均线判断
            ma20 = pd.Series(close).rolling(20).mean().values[-1]
            ma60 = pd.Series(close).rolling(60).mean().values[-1] if len(close) >= 60 else ma20
            
            # 趋势强度
            trend_strength = (close[-1] / ma20 - 1) * 100
            
            # 判断市场环境
            if total_return > 10 and volatility < 25:
                # 收益高、波动低 = 牛市
                return MarketRegime.BULL
            elif total_return < -10:
                # 收益负 = 熊市
                return MarketRegime.BEAR
            elif abs(total_return) < 5 and volatility > 20:
                # 收益平稳、波动高 = 震荡市
                return MarketRegime.SIDEWAYS
            elif trend_strength > 5 and returns[-5:].mean() > 0:
                # 短期走强 = 反转期
                return MarketRegime.RECOVERY
            else:
                return MarketRegime.SIDEWAYS
            
        except Exception as e:
            return MarketRegime.SIDEWAYS
    
    # ==================== 经济周期识别 ====================
    
    def detect_economic_cycle(self) -> EconomicCycle:
        """
        识别经济周期（简化版）
        
        实际应该结合：
        - GDP增速
        - CPI/PPI
        - PMI
        - 利率水平
        
        简化版使用市场指标代理
        """
        try:
            # 获取市场数据
            market_regime = self.detect_market_regime()
            
            # 根据市场环境推断经济周期
            cycle_map = {
                MarketRegime.BULL: EconomicCycle.EXPANSION,
                MarketRegime.BEAR: EconomicCycle.RECESSION,
                MarketRegime.SIDEWAYS: EconomicCycle.SLOWDOWN,
                MarketRegime.RECOVERY: EconomicCycle.RECOVERY,
            }
            
            return cycle_map.get(market_regime, EconomicCycle.SLOWDOWN)
            
        except:
            return EconomicCycle.SLOWDOWN
    
    # ==================== 因子IC计算 ====================
    
    def calc_factor_ic(self, factor_values: pd.Series, 
                        forward_returns: pd.Series) -> float:
        """
        计算因子IC值
        
        IC = Spearman相关系数
        """
        if len(factor_values) != len(forward_returns):
            return 0
        
        # 去除缺失值
        valid = ~(factor_values.isna() | forward_returns.isna())
        if valid.sum() < 10:
            return 0
        
        factor_clean = factor_values[valid]
        return_clean = forward_returns[valid]
        
        # Spearman相关
        from scipy.stats import spearmanr
        corr, _ = spearmanr(factor_clean, return_clean)
        
        return corr if not np.isnan(corr) else 0
    
    # ==================== 风险偏好评估 ====================
    
    def calc_risk_appetite(self) -> float:
        """
        计算市场风险偏好指数
        
        范围：0-100
        - >70: 高风险偏好（激进）
        - 30-70: 中等风险偏好
        - <30: 低风险偏好（保守）
        """
        try:
            index_file = os.path.join(self.day_cache_dir, "000001_day.csv")
            
            if not os.path.exists(index_file):
                return 50.0
            
            df = pd.read_csv(index_file)
            df = df.sort_values('trade_date', ascending=False).head(20)
            
            close = df['close'].values
            volume = df['vol'].values if 'vol' in df.columns else None
            
            # 计算各项指标
            returns = pd.Series(close).pct_change().dropna()
            volatility = returns.std() * np.sqrt(252) * 100
            
            # 波动率评分（低波动 = 高风险偏好）
            vol_score = max(0, min(100, 100 - volatility * 2))
            
            # 趋势评分
            trend = (close[0] / close[-1] - 1) * 100
            trend_score = max(0, min(100, 50 + trend * 5))
            
            # 成交量评分
            if volume is not None and len(volume) >= 10:
                vol_change = volume[0] / volume[-10:].mean() if volume[-10:].mean() > 0 else 1
                volume_score = max(0, min(100, vol_change * 50))
            else:
                volume_score = 50
            
            # 综合风险偏好
            risk_appetite = vol_score * 0.4 + trend_score * 0.3 + volume_score * 0.3
            
            return risk_appetite
            
        except:
            return 50.0
    
    # ==================== 动态权重计算 ====================
    
    def calc_dynamic_weights(self) -> Dict:
        """
        计算动态权重
        
        综合考虑：
        1. 市场环境权重
        2. 经济周期权重
        3. 风险偏好调整
        4. 基准权重约束
        
        Returns:
            {
                'weights': {value: 0.22, growth: 0.18, ...},
                'market_regime': 'bull',
                'economic_cycle': 'expansion',
                'risk_appetite': 65.5,
                'adjustment_factors': {...}
            }
        """
        # 1. 识别市场环境
        market_regime = self.detect_market_regime()
        market_weights = MARKET_REGIME_WEIGHTS[market_regime]
        
        # 2. 识别经济周期
        economic_cycle = self.detect_economic_cycle()
        cycle_weights = ECONOMIC_CYCLE_WEIGHTS[economic_cycle]
        
        # 3. 计算风险偏好
        risk_appetite = self.calc_risk_appetite()
        
        # 4. 权重融合
        # 市场环境权重 40%，经济周期权重 30%，基准权重 30%
        weights = {}
        for factor in ['value', 'growth', 'quality', 'momentum', 'sentiment']:
            market_w = getattr(market_weights, factor)
            cycle_w = getattr(cycle_weights, factor)
            baseline_w = getattr(BASELINE_WEIGHTS, factor)
            
            # 加权融合
            w = market_w * 0.4 + cycle_w * 0.3 + baseline_w * 0.3
            weights[factor] = w
        
        # 5. 风险偏好调整
        if risk_appetite > 70:
            # 高风险偏好：增加成长和动量，减少价值和质量
            adj = (risk_appetite - 70) / 100
            weights['growth'] = min(0.35, weights['growth'] + adj)
            weights['momentum'] = min(0.30, weights['momentum'] + adj)
            weights['value'] = max(0.10, weights['value'] - adj)
            weights['quality'] = max(0.15, weights['quality'] - adj)
        elif risk_appetite < 30:
            # 低风险偏好：增加价值和质量，减少成长和动量
            adj = (30 - risk_appetite) / 100
            weights['value'] = min(0.35, weights['value'] + adj)
            weights['quality'] = min(0.35, weights['quality'] + adj)
            weights['growth'] = max(0.10, weights['growth'] - adj)
            weights['momentum'] = max(0.05, weights['momentum'] - adj)
        
        # 6. 归一化（确保权重和为1）
        total = sum(weights.values())
        for factor in weights:
            weights[factor] = round(weights[factor] / total, 3)
        
        return {
            'weights': weights,
            'market_regime': market_regime.value,
            'economic_cycle': economic_cycle.value,
            'risk_appetite': round(risk_appetite, 1),
            'adjustment_factors': {
                'market_weight_ratio': 0.4,
                'cycle_weight_ratio': 0.3,
                'baseline_weight_ratio': 0.3,
            },
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    # ==================== 权重历史追踪 ====================
    
    def get_weight_history(self, days: int = 30) -> List[Dict]:
        """
        获取权重历史（模拟）
        
        实际应该存储每日权重
        """
        # 这里简化处理，返回当前权重
        current = self.calc_dynamic_weights()
        return [current]
    
    # ==================== 行业轮动权重调整 ====================
    
    def calc_industry_adjustment(self, industry: str) -> Dict[str, float]:
        """
        根据行业特性调整因子权重
        
        不同行业对不同因子的敏感度不同
        """
        # 行业因子敏感度矩阵
        industry_sensitivity = {
            '银行': {'value': 1.2, 'quality': 1.3, 'momentum': 0.8},
            '非银金融': {'value': 1.1, 'momentum': 1.2, 'sentiment': 1.2},
            '食品饮料': {'quality': 1.3, 'growth': 1.1, 'value': 0.9},
            '医药生物': {'growth': 1.2, 'quality': 1.2, 'value': 0.9},
            '电子': {'growth': 1.3, 'momentum': 1.2, 'value': 0.8},
            '计算机': {'growth': 1.3, 'momentum': 1.1, 'sentiment': 1.2},
            '传媒': {'sentiment': 1.4, 'momentum': 1.2, 'value': 0.8},
            '电力设备': {'growth': 1.3, 'momentum': 1.2},
            '有色金属': {'momentum': 1.3, 'value': 0.9},
            '房地产': {'value': 1.2, 'sentiment': 1.2, 'growth': 0.8},
        }
        
        sensitivity = industry_sensitivity.get(industry, {})
        
        # 默认调整系数
        adjustment = {
            'value': 1.0,
            'growth': 1.0,
            'quality': 1.0,
            'momentum': 1.0,
            'sentiment': 1.0,
        }
        
        # 应用行业敏感度
        for factor, coef in sensitivity.items():
            adjustment[factor] = coef
        
        return adjustment


# ==================== 单例 ====================
_dynamic_weight_system = None

def get_dynamic_weight_system() -> DynamicWeightSystem:
    global _dynamic_weight_system
    if _dynamic_weight_system is None:
        _dynamic_weight_system = DynamicWeightSystem()
    return _dynamic_weight_system