"""
最佳因子策略配置 v1.8

v1.8 优化重点：风险控制 + 回撤管理

核心改进：
1. 降低基础仓位：牛市70%（原95%）
2. 动态回撤控制：回撤>15%时仓位减半
3. 分级止损：触发止损时减仓而非清仓
4. 波动率控制：高波动时降低仓位
5. 大盘保护：大盘破位时强制减仓
"""
from typing import Dict

# ==================== 市场择时配置 ====================

MARKET_TIMING_CONFIG = {
    # 趋势判断参数
    'trend_ma_short': 20,      # 短期均线周期
    'trend_ma_long': 60,       # 长期均线周期
    'trend_threshold': 2.0,    # 趋势阈值（%）
    'return_threshold': 5.0,   # 收益阈值（%）
    'return_period': 60,       # 收益计算周期
    
    # 市场环境仓位调整（v1.8优化：整体降低）
    'position_adjustment': {
        'bull': 1.0,           # 牛市不加仓（原1.3）
        'bear': 0.2,           # 熊市减仓80%（原0.3）
        'sideways': 0.6,       # 震荡市适中（原0.8）
    }
}

# ==================== 因子配置 ====================

FACTOR_CONFIG = {
    # 因子定义
    'factors': {
        'KDJ': {'name': 'KDJ_D', 'category': 'technical', 'weight': 0.20},
        'BOLL': {'name': '布林位置', 'category': 'technical', 'weight': 0.18},
        'MOM': {'name': '动量因子', 'category': 'momentum', 'weight': 0.15},
        'LEV': {'name': '质量因子', 'category': 'quality', 'weight': 0.12},
        'EP': {'name': '盈利收益率', 'category': 'value', 'weight': 0.12},
        'BP': {'name': '账面市值比', 'category': 'value', 'weight': 0.10},
        'TURN': {'name': '换手率', 'category': 'sentiment', 'weight': 0.08},
        'ROE': {'name': '净资产收益率', 'category': 'quality', 'weight': 0.05},
    },
    
    # 按市场环境动态权重
    'weights_by_regime': {
        'bull': {  # 牛市：动量为主
            'MOM': 0.35,      # 动量因子（降低）
            'KDJ': 0.20,
            'BOLL': 0.15,
            'TURN': 0.15,
            'ROE': 0.15,      # 增加质量因子
        },
        'bear': {  # 熊市：防守为主
            'LEV': 0.35,      # 质量因子
            'ROE': 0.25,
            'EP': 0.20,
            'BP': 0.20,
        },
        'sideways': {  # 震荡市
            'KDJ': 0.25,
            'BOLL': 0.25,
            'TURN': 0.20,
            'MOM': 0.15,
            'ROE': 0.15,
        }
    }
}

# ==================== 仓位控制配置（v1.8核心优化） ====================

POSITION_CONFIG = {
    'base_position': 0.60,     # 基础仓位60%（原85%，大幅降低）
    'max_position': 0.70,      # 最大仓位70%（原95%）
    'min_position': 0.0,       # 最小仓位（空仓）
    
    # 波动率调整（v1.8优化：更保守）
    'vol_target': 0.15,        # 目标波动率15%（原18%）
    'vol_adjust_range': (0.3, 1.2),  # 波动率调整范围（更窄）
    
    # 信号强度调整（v1.8优化：降低杠杆）
    'signal_adjust': {
        'strong': 1.1,         # 强信号（原1.3）
        'medium': 0.9,         # 中等信号（原1.0）
        'weak': 0.5,           # 弱信号
    },
    
    # 信号阈值（v1.8优化：提高门槛）
    'signal_threshold': {
        'buy': 0.8,            # 买入阈值（原0.7，提高）
        'sell': -0.6,          # 卖出阈值（原-0.7，放宽）
        'vote_min': 4,         # 最少因子确认数（原3，增加）
    },
    
    # 止损止盈（v1.8优化：更严格）
    'stop_loss': 0.05,         # 止损5%（原6%）
    'take_profit': 0.10,       # 止盈10%（原12%）
    
    # ========== v1.8新增：动态回撤控制 ==========
    'drawdown_control': {
        'level_1': 0.10,       # 回撤10%，仓位×0.8
        'level_2': 0.15,       # 回撤15%，仓位×0.5
        'level_3': 0.20,       # 回撤20%，仓位×0.3
        'recovery_threshold': 0.05,  # 回撤恢复到5%以内时恢复正常仓位
    },
    
    # ========== v1.8新增：分级止损 ==========
    'tiered_stop_loss': {
        'enabled': True,
        'tier_1': 0.03,        # 亏损3%，减仓30%
        'tier_2': 0.05,        # 亏损5%，减仓50%
        'tier_3': 0.08,        # 亏损8%，清仓
    },
}

# ==================== 市场环境动态参数（v1.8优化） ====================

REGIME_PARAMS = {
    'bull': {
        'base_position': 0.70,    # 牛市仓位（原95%，大幅降低）
        'signal_threshold': 0.5,  # 买入阈值（原0.2，提高）
        'vote_min': 2,            # 因子确认数（原1，增加）
        'stop_loss': 0.06,        # 止损6%（原10%，更严格）
        'take_profit': 0.15,
        'trend_follow': True,
        'range_trade': False,
    },
    'bear': {
        'base_position': 0.15,    # 熊市仓位（原20%）
        'signal_threshold': 0.9,  # 买入阈值（原0.8，提高）
        'vote_min': 5,            # 因子确认数（原4，增加）
        'stop_loss': 0.03,        # 止损3%
        'take_profit': 0.04,
        'trend_follow': False,
        'range_trade': False,
    },
    'sideways': {
        'base_position': 0.35,    # 震荡市仓位（原50%，降低）
        'signal_threshold': 0.7,  # 买入阈值（原0.6，提高）
        'vote_min': 4,            # 因子确认数
        'stop_loss': 0.04,        # 止损4%
        'take_profit': 0.06,      # 止盈6%
        'trend_follow': False,
        'range_trade': True,      # 启用区间交易
    }
}

# ==================== 回测配置 ====================

BACKTEST_CONFIG = {
    'lookback_period': 252,    # 标准化回看期（1年）
    'min_data_days': 1500,     # 最小数据天数（约6年）
    'holding_period': 20,      # 短期验证持有期（天）
    'validation_periods': 6,   # 短期验证周期数
}

# ==================== 验证标准（v1.8调整） ====================

VALIDATION_CRITERIA = {
    'long_term': {
        'min_annual_return': 2.0,    # 最低年化收益2%
        'min_sharpe': 0.15,           # 最低夏普0.15
        'max_drawdown': -20.0,        # 最大回撤-20%（原-30%，更严格）
    },
    'short_term': {
        'min_win_rate': 50.0,         # 最低胜率50%
        'min_return': 0.0,            # 最低收益0%
    }
}


def get_factor_weights(regime: str) -> Dict[str, float]:
    """获取指定市场环境的因子权重"""
    return FACTOR_CONFIG['weights_by_regime'].get(regime, 
           FACTOR_CONFIG['weights_by_regime']['sideways'])


def get_position_adjustment(regime: str) -> float:
    """获取市场环境仓位调整系数"""
    return MARKET_TIMING_CONFIG['position_adjustment'].get(regime, 1.0)


def get_signal_adjustment(score_z: float) -> float:
    """根据信号强度获取调整系数"""
    abs_z = abs(score_z)
    if abs_z > 1.0:
        return POSITION_CONFIG['signal_adjust']['strong']
    elif abs_z > 0.5:
        return POSITION_CONFIG['signal_adjust']['medium']
    else:
        return POSITION_CONFIG['signal_adjust']['weak']


def get_drawdown_adjustment(current_drawdown: float) -> float:
    """
    根据当前回撤获取仓位调整系数（v1.8新增）
    
    Args:
        current_drawdown: 当前回撤（正数，如0.15表示15%回撤）
    
    Returns:
        仓位调整系数
    """
    dd_config = POSITION_CONFIG.get('drawdown_control', {})
    
    if current_drawdown >= dd_config.get('level_3', 0.20):
        return 0.3  # 回撤超过20%，仓位降至30%
    elif current_drawdown >= dd_config.get('level_2', 0.15):
        return 0.5  # 回撤超过15%，仓位降至50%
    elif current_drawdown >= dd_config.get('level_1', 0.10):
        return 0.8  # 回撤超过10%，仓位降至80%
    else:
        return 1.0  # 回撤小于10%，正常仓位


def print_config():
    """打印配置信息"""
    print("\n" + "="*60)
    print("最佳因子策略配置 v1.8（风险控制版）")
    print("="*60)
    print("\n【v1.8核心优化】")
    print("  1. 基础仓位降低：85% → 60%")
    print("  2. 最大仓位降低：95% → 70%")
    print("  3. 牛市仓位降低：95% → 70%")
    print("  4. 动态回撤控制：回撤>15%时仓位减半")
    print("  5. 分级止损：3%/5%/8%三档止损")
    
    print("\n【市场择时】")
    print(f"  牛市仓位调整: ×{MARKET_TIMING_CONFIG['position_adjustment']['bull']}")
    print(f"  熊市仓位调整: ×{MARKET_TIMING_CONFIG['position_adjustment']['bear']}")
    print(f"  震荡市仓位调整: ×{MARKET_TIMING_CONFIG['position_adjustment']['sideways']}")
    
    print("\n【仓位控制】")
    print(f"  基础仓位: {POSITION_CONFIG['base_position']:.0%}")
    print(f"  最大仓位: {POSITION_CONFIG['max_position']:.0%}")
    print(f"  目标波动率: {POSITION_CONFIG['vol_target']:.0%}")
    print(f"  止损: {POSITION_CONFIG['stop_loss']:.0%}")
    print(f"  止盈: {POSITION_CONFIG['take_profit']:.0%}")
    
    print("\n【动态回撤控制】")
    dd = POSITION_CONFIG.get('drawdown_control', {})
    print(f"  回撤10%: 仓位×{dd.get('level_1', 0.1)/0.1 * 0.8:.1f}")
    print(f"  回撤15%: 仓位×0.5")
    print(f"  回撤20%: 仓位×0.3")
    
    print("\n【验证标准】")
    print(f"  最大回撤限制: {VALIDATION_CRITERIA['long_term']['max_drawdown']}%")
    print("="*60 + "\n")


if __name__ == "__main__":
    print_config()