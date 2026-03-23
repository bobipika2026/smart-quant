"""
最佳因子策略配置

基于2026-03-16综合优化结果
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
    
    # 市场环境仓位调整
    'position_adjustment': {
        'bull': 1.3,           # 牛市加仓30%
        'bear': 0.3,           # 熊市减仓70%
        'sideways': 0.8,       # 震荡市适中
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
    
    # 按市场环境动态权重（v1.4优化：牛市动量为王）
    'weights_by_regime': {
        'bull': {  # 牛市：动量为王，大幅提高动量因子权重
            'MOM': 0.40,      # 动量因子（大幅提高）
            'KDJ': 0.20,      # 技术因子
            'BOLL': 0.15,     # 技术因子
            'TURN': 0.15,     # 情绪因子（市场热度）
            'ROE': 0.10,      # 质量因子
        },
        'bear': {  # 熊市：防守为主，质量+价值
            'LEV': 0.30,      # 质量因子（稳定性）
            'ROE': 0.25,      # 质量因子
            'EP': 0.20,       # 价值因子
            'BP': 0.15,       # 价值因子
            'BOLL': 0.10,     # 技术因子
        },
        'sideways': {  # 震荡市：情绪+技术
            'KDJ': 0.25,
            'BOLL': 0.25,
            'TURN': 0.20,
            'MOM': 0.15,
            'ROE': 0.15,
        }
    }
}

# ==================== 仓位控制配置 ====================

POSITION_CONFIG = {
    'base_position': 0.85,     # 基础仓位85%（进一步提高）
    'max_position': 0.95,      # 最大仓位95%
    'min_position': 0.0,       # 最小仓位（空仓）
    
    # 波动率调整
    'vol_target': 0.18,        # 目标波动率18%（降低，更激进）
    'vol_adjust_range': (0.4, 1.5),  # 波动率调整范围
    
    # 信号强度调整
    'signal_adjust': {
        'strong': 1.3,         # 强信号 |z| > 0.7
        'medium': 1.0,         # 中等信号 0.5 < |z| <= 0.7
        'weak': 0.5,           # 弱信号 |z| <= 0.5
    },
    
    # 信号阈值（深度优化）
    'signal_threshold': {
        'buy': 0.7,            # 买入阈值（提高到0.7）
        'sell': -0.7,          # 卖出阈值（提高到-0.7）
        'vote_min': 3,         # 最少因子确认数
    },
    
    # 止损止盈
    'stop_loss': 0.06,         # 止损6%
    'take_profit': 0.12,       # 止盈12%
}

# ==================== 市场环境动态参数（v1.6） ====================

REGIME_PARAMS = {
    'bull': {
        'base_position': 0.95,    # 牛市激进
        'signal_threshold': 0.2,  
        'vote_min': 1,            
        'stop_loss': 0.10,        
        'take_profit': 0.20,
        'trend_follow': True,
    },
    'bear': {
        'base_position': 0.20,    # 熊市极保守
        'signal_threshold': 0.8,  
        'vote_min': 4,            
        'stop_loss': 0.03,        # 更严格止损
        'take_profit': 0.05,
        'trend_follow': False,
    },
    'sideways': {
        'base_position': 0.50,    # 震荡市降低仓位（关键优化）
        'signal_threshold': 0.6,  # 提高阈值
        'vote_min': 4,            # 提高确认要求
        'stop_loss': 0.04,        # 严格止损
        'take_profit': 0.08,      # 降低止盈预期
        'trend_follow': False,
        'range_trade': True,      # 启用区间交易模式
    }
}

# ==================== 回测配置 ====================

BACKTEST_CONFIG = {
    'lookback_period': 252,    # 标准化回看期（1年）
    'min_data_days': 1500,     # 最小数据天数（约6年）
    'holding_period': 20,      # 短期验证持有期（天）
    'validation_periods': 6,   # 短期验证周期数
}

# ==================== 验证标准 ====================

VALIDATION_CRITERIA = {
    'long_term': {
        'min_annual_return': 2.0,    # 最低年化收益2%
        'min_sharpe': 0.15,           # 最低夏普0.15
        'max_drawdown': -30.0,        # 最大回撤-30%
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


def print_config():
    """打印配置信息"""
    print("\n" + "="*60)
    print("最佳因子策略配置")
    print("="*60)
    print("\n【市场择时】")
    print(f"  趋势判断: {MARKET_TIMING_CONFIG['trend_ma_short']}日MA vs {MARKET_TIMING_CONFIG['trend_ma_long']}日MA")
    print(f"  牛市仓位调整: ×{MARKET_TIMING_CONFIG['position_adjustment']['bull']}")
    print(f"  熊市仓位调整: ×{MARKET_TIMING_CONFIG['position_adjustment']['bear']}")
    print(f"  震荡市仓位调整: ×{MARKET_TIMING_CONFIG['position_adjustment']['sideways']}")
    
    print("\n【因子权重（按市场环境）】")
    for regime, weights in FACTOR_CONFIG['weights_by_regime'].items():
        weight_str = ", ".join([f"{k}:{v:.0%}" for k, v in weights.items()])
        print(f"  {regime:10s}: {weight_str}")
    
    print("\n【仓位控制】")
    print(f"  基础仓位: {POSITION_CONFIG['base_position']:.0%}")
    print(f"  目标波动率: {POSITION_CONFIG['vol_target']:.0%}")
    print(f"  买入信号阈值: z > {POSITION_CONFIG['signal_threshold']['buy']}")
    print(f"  卖出信号阈值: z < {POSITION_CONFIG['signal_threshold']['sell']}")
    
    print("\n【验证标准】")
    print(f"  长期年化收益 ≥ {VALIDATION_CRITERIA['long_term']['min_annual_return']}%")
    print(f"  夏普比率 ≥ {VALIDATION_CRITERIA['long_term']['min_sharpe']}")
    print(f"  短期胜率 ≥ {VALIDATION_CRITERIA['short_term']['min_win_rate']}%")
    print("="*60 + "\n")


if __name__ == "__main__":
    print_config()