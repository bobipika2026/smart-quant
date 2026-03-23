"""
最佳因子策略服务（v1.7：大盘择时 + 行业轮动）
"""
import os
import sys
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime

# 添加路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config.best_factor_config import (
    MARKET_TIMING_CONFIG,
    FACTOR_CONFIG,
    POSITION_CONFIG,
    BACKTEST_CONFIG,
    REGIME_PARAMS,
    get_factor_weights,
    get_position_adjustment,
    get_signal_adjustment
)
from app.services.market_timing import (
    MarketTimingEnhancer,
    get_stock_industry,
    get_industry_factor_weights,
    get_industry_description
)


class BestFactorStrategy:
    """最佳因子策略"""
    
    def __init__(self):
        self.config = {
            'market_timing': MARKET_TIMING_CONFIG,
            'factors': FACTOR_CONFIG,
            'position': POSITION_CONFIG,
            'backtest': BACKTEST_CONFIG
        }
    
    # ==================== 因子计算 ====================
    
    def calculate_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算所有因子"""
        result = df.copy()
        
        # KDJ
        low_14 = result['low'].rolling(14).min()
        high_14 = result['high'].rolling(14).max()
        rsv = (result['close'] - low_14) / (high_14 - low_14 + 0.0001) * 100
        result['KDJ'] = rsv.ewm(alpha=1/3).mean().ewm(alpha=1/3).mean()
        
        # BOLL
        ma20 = result['close'].rolling(20).mean()
        std20 = result['close'].rolling(20).std()
        result['BOLL'] = (result['close'] - ma20) / (2 * std20 + 0.0001)
        
        # MOM
        result['MOM'] = result['close'].pct_change(63)
        
        # LEV
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        
        # ROE (简化)
        result['ROE'] = result['close'].pct_change().rolling(252).mean() / \
                       (result['close'].pct_change().rolling(252).std() + 0.0001)
        
        # TURN
        result['TURN'] = result['volume'].rolling(5).mean() / \
                        (result['volume'].rolling(60).mean() + 0.0001) - 1
        
        # EP, BP
        result['EP'] = 1 / (result['close'] + 0.0001) * 100
        result['BP'] = 1 / (result['close'] + 0.0001) * 50
        
        return result
    
    # ==================== 市场择时 ====================
    
    def detect_market_regime(self, df: pd.DataFrame) -> str:
        """识别市场环境（最新一天）"""
        if len(df) < MARKET_TIMING_CONFIG['trend_ma_long']:
            return 'sideways'
        
        # 趋势
        ma_short = df['close'].rolling(MARKET_TIMING_CONFIG['trend_ma_short']).mean().iloc[-1]
        ma_long = df['close'].rolling(MARKET_TIMING_CONFIG['trend_ma_long']).mean().iloc[-1]
        trend = (ma_short / ma_long - 1) * 100
        
        # 收益
        ret_period = MARKET_TIMING_CONFIG['return_period']
        ret = (df['close'].iloc[-1] / df['close'].iloc[-ret_period] - 1) * 100
        
        # 判断
        if trend > MARKET_TIMING_CONFIG['trend_threshold'] and \
           ret > MARKET_TIMING_CONFIG['return_threshold']:
            return 'bull'
        elif trend < -MARKET_TIMING_CONFIG['trend_threshold'] and \
             ret < -MARKET_TIMING_CONFIG['return_threshold']:
            return 'bear'
        else:
            return 'sideways'
    
    def detect_regime_rolling(self, df: pd.DataFrame) -> pd.DataFrame:
        """滚动识别每一天的市场环境"""
        result = df.copy()
        
        # 计算趋势（滚动）
        result['ma_short'] = result['close'].rolling(MARKET_TIMING_CONFIG['trend_ma_short']).mean()
        result['ma_long'] = result['close'].rolling(MARKET_TIMING_CONFIG['trend_ma_long']).mean()
        result['trend'] = (result['ma_short'] / result['ma_long'] - 1) * 100
        
        # 计算60日收益（滚动）
        result['ret_60'] = result['close'].pct_change(MARKET_TIMING_CONFIG['return_period']) * 100
        
        # 滚动判断市场环境
        trend_th = MARKET_TIMING_CONFIG['trend_threshold']
        return_th = MARKET_TIMING_CONFIG['return_threshold']
        
        result['regime'] = 'sideways'
        result.loc[(result['trend'] > trend_th) & (result['ret_60'] > return_th), 'regime'] = 'bull'
        result.loc[(result['trend'] < -trend_th) & (result['ret_60'] < -return_th), 'regime'] = 'bear'
        
        return result
    
    # ==================== 得分计算 ====================
    
    def calculate_score(self, df: pd.DataFrame, regime: str = None) -> pd.DataFrame:
        """计算综合得分"""
        result = df.copy()
        
        if regime is None:
            regime = self.detect_market_regime(df)
        
        weights = get_factor_weights(regime)
        
        result['score'] = 0
        for factor, weight in weights.items():
            if factor in result.columns:
                factor_std = (result[factor] - result[factor].rolling(252).mean()) / \
                            (result[factor].rolling(252).std() + 0.0001)
                result['score'] += factor_std.fillna(0) * weight
        
        result['score_z'] = (result['score'] - result['score'].rolling(252).mean()) / \
                           (result['score'].rolling(252).std() + 0.0001)
        
        return result
    
    # ==================== 信号生成 ====================
    
    def generate_signal(self, df: pd.DataFrame, regime: str = 'sideways') -> pd.DataFrame:
        """生成交易信号（根据市场环境动态调整）"""
        result = df.copy()
        
        # 获取市场环境特定参数
        regime_params = REGIME_PARAMS.get(regime, REGIME_PARAMS['sideways'])
        buy_threshold = regime_params.get('signal_threshold', 0.7)
        sell_threshold = -buy_threshold
        vote_min = POSITION_CONFIG['signal_threshold'].get('vote_min', 3)
        
        # 多因子投票过滤
        result['vote'] = 0
        for factor in ['KDJ', 'BOLL', 'MOM', 'LEV', 'EP', 'BP', 'ROE']:
            if factor in result.columns:
                factor_std = (result[factor] - result[factor].rolling(252).mean()) / \
                            (result[factor].rolling(252).std() + 0.0001)
                result['vote'] += np.where(factor_std > 0.3, 1, 
                                          np.where(factor_std < -0.3, -1, 0))
        
        # 综合信号
        result['signal'] = 0
        
        # 买入：信号强度足够 + 多因子确认
        buy_cond = (result['score_z'] > buy_threshold) & (result['vote'] >= vote_min)
        
        # 卖出：信号强度足够 + 多因子确认
        sell_cond = (result['score_z'] < sell_threshold) & (result['vote'] <= -vote_min)
        
        result.loc[buy_cond, 'signal'] = 1
        result.loc[sell_cond, 'signal'] = -1
        
        return result
    
    # ==================== 仓位计算 ====================
    
    def calculate_position(self, df: pd.DataFrame, regime: str = None) -> pd.DataFrame:
        """计算动态仓位（根据市场环境动态调整）"""
        result = df.copy()
        
        if regime is None:
            regime = self.detect_market_regime(df)
        
        # 获取市场环境特定参数
        regime_params = REGIME_PARAMS.get(regime, REGIME_PARAMS['sideways'])
        base_position = regime_params.get('base_position', POSITION_CONFIG['base_position'])
        
        # 市场环境调整
        market_adj = get_position_adjustment(regime)
        
        # 波动率调整
        vol = result['close'].pct_change().rolling(20).std() * np.sqrt(252)
        vol_adj = POSITION_CONFIG['vol_target'] / (vol + 0.0001)
        vol_adj = vol_adj.clip(*POSITION_CONFIG['vol_adjust_range'])
        
        # 信号强度调整
        signal_strength = result['score_z'].abs()
        signal_adj = np.where(signal_strength > 0.7, 
                             POSITION_CONFIG['signal_adjust']['strong'],
                             np.where(signal_strength > 0.5, 
                                     POSITION_CONFIG['signal_adjust']['medium'],
                                     POSITION_CONFIG['signal_adjust']['weak']))
        
        # 综合仓位
        result['position'] = base_position * market_adj * vol_adj * signal_adj
        result['position'] = result['position'].clip(
            POSITION_CONFIG['min_position'], 
            POSITION_CONFIG['max_position']
        )
        result['position'] = result['position'] * result['signal'].abs()
        
        return result
    
    # ==================== 止损止盈 ====================
    
    def apply_stop_loss_profit(self, df: pd.DataFrame, regime: str = 'sideways') -> pd.DataFrame:
        """应用止损止盈（根据市场环境动态调整）"""
        result = df.copy()
        
        # 获取市场环境特定参数
        regime_params = REGIME_PARAMS.get(regime, REGIME_PARAMS['sideways'])
        stop_loss = regime_params.get('stop_loss', POSITION_CONFIG.get('stop_loss', 0.06))
        take_profit = regime_params.get('take_profit', POSITION_CONFIG.get('take_profit', 0.12))
        
        # 持仓期间检查盈亏
        result['pnl_pct'] = 0.0
        
        in_position = False
        entry_price = 0.0
        
        for i in range(len(result)):
            current_price = result['close'].iloc[i]
            
            # 买入信号
            if result['signal'].iloc[i] == 1 and not in_position:
                in_position = True
                entry_price = current_price
            
            # 持仓期间
            if in_position and entry_price > 0:
                pnl_pct = (current_price - entry_price) / entry_price
                result.iloc[i, result.columns.get_loc('pnl_pct')] = pnl_pct
                
                # 止损：减仓
                if pnl_pct < -stop_loss:
                    result.iloc[i, result.columns.get_loc('position')] *= 0.5
                
                # 止盈：减仓
                elif pnl_pct > take_profit:
                    result.iloc[i, result.columns.get_loc('position')] *= 0.5
            
            # 卖出信号
            if result['signal'].iloc[i] == -1:
                in_position = False
                entry_price = 0.0
        
        return result
    
    # ==================== 完整策略 ====================
    
    def run_strategy(self, df: pd.DataFrame, stock_code: str = None) -> pd.DataFrame:
        """运行完整策略（v1.7：大盘择时 + 行业轮动）"""
        # 设置股票代码（用于行业轮动）
        if stock_code:
            self.stock_code = stock_code
            self.industry = get_stock_industry(stock_code)
            self.industry_weights = get_industry_factor_weights(stock_code)
            self.industry_desc = get_industry_description(stock_code)
        
        # 计算因子
        df = self.calculate_factors(df)
        
        # 滚动识别市场环境
        df = self.detect_regime_rolling(df)
        
        # 加载大盘指数数据（择时）
        index_df = MarketTimingEnhancer.load_index_data('000300')
        if not index_df.empty:
            index_df = MarketTimingEnhancer.calculate_market_trend(index_df)
            # 合并大盘数据
            df = df.join(index_df[['trend_score', 'index_vol']], how='left')
            df['market_trend_score'] = df['trend_score'].fillna(0)
            df['market_vol'] = df['index_vol'].fillna(0.2)
        
        # 按滚动市场环境计算得分（含行业轮动）
        df = self.calculate_score_with_regime(df)
        
        # 生成信号
        df = self.generate_signal_with_regime(df)
        
        # 计算仓位（含大盘择时调整）
        df = self.calculate_position_with_regime(df)
        
        # 应用止损止盈
        df = self.apply_stop_loss_profit_with_regime(df)
        
        return df
    
    # ==================== 按滚动市场环境计算 ====================
    
    def calculate_score_with_regime(self, df: pd.DataFrame) -> pd.DataFrame:
        """按滚动市场环境计算得分（v1.7：行业轮动）"""
        result = df.copy()
        
        # 获取行业因子权重（如果已设置stock_code）
        industry_weights = getattr(self, 'industry_weights', None)
        
        # 获取各市场环境的因子权重
        weights_bull = get_factor_weights('bull')
        weights_bear = get_factor_weights('bear')
        weights_sideways = get_factor_weights('sideways')
        
        # 初始化得分列
        result['score'] = 0.0
        
        # 计算每个因子在不同环境下的贡献
        for idx in range(len(result)):
            regime = result['regime'].iloc[idx]
            
            # 获取该市场环境的因子权重
            base_weights = weights_bull if regime == 'bull' else (weights_bear if regime == 'bear' else weights_sideways)
            
            # 行业权重调整
            if industry_weights:
                weights = {}
                for factor in set(list(base_weights.keys()) + list(industry_weights.keys())):
                    base_w = base_weights.get(factor, 0)
                    ind_w = industry_weights.get(factor, 0)
                    # 市场权重60% + 行业权重40%
                    weights[factor] = base_w * 0.6 + ind_w * 0.4
            else:
                weights = base_weights
            
            # 计算当天得分
            score = 0.0
            for factor, weight in weights.items():
                if factor in result.columns:
                    # 使用滚动标准化
                    if idx >= 252:
                        factor_val = result[factor].iloc[idx]
                        roll_mean = result[factor].iloc[idx-252:idx].mean()
                        roll_std = result[factor].iloc[idx-252:idx].std()
                        if roll_std > 0:
                            factor_std = (factor_val - roll_mean) / roll_std
                            score += factor_std * weight
            
            result.iloc[idx, result.columns.get_loc('score')] = score
        
        # 标准化得分
        result['score_z'] = result['score'].rolling(252).apply(
            lambda x: (x.iloc[-1] - x.mean()) / (x.std() + 0.0001) if len(x) > 0 else 0,
            raw=False
        )
        
        return result
    
    def generate_signal_with_regime(self, df: pd.DataFrame) -> pd.DataFrame:
        """按滚动市场环境生成信号（v1.6：震荡市区间交易）"""
        result = df.copy()
        
        # 初始化信号列
        result['signal'] = 0
        result['vote'] = 0
        
        # 计算多因子投票
        for factor in ['KDJ', 'BOLL', 'MOM', 'LEV', 'EP', 'BP', 'ROE']:
            if factor in result.columns:
                for idx in range(252, len(result)):
                    factor_val = result[factor].iloc[idx]
                    roll_mean = result[factor].iloc[idx-252:idx].mean()
                    roll_std = result[factor].iloc[idx-252:idx].std()
                    if roll_std > 0:
                        factor_std = (factor_val - roll_mean) / roll_std
                        if factor_std > 0.3:
                            result.iloc[idx, result.columns.get_loc('vote')] += 1
                        elif factor_std < -0.3:
                            result.iloc[idx, result.columns.get_loc('vote')] -= 1
        
        # 持仓状态追踪
        in_position = False
        entry_idx = 0
        entry_price = 0.0
        min_holding_days = 10
        
        # 计算布林带（用于震荡市区间交易）
        result['boll_upper'] = result['close'].rolling(20).mean() + 2 * result['close'].rolling(20).std()
        result['boll_lower'] = result['close'].rolling(20).mean() - 2 * result['close'].rolling(20).std()
        result['boll_mid'] = result['close'].rolling(20).mean()
        
        for idx in range(252, len(result)):
            regime = result['regime'].iloc[idx]
            params = REGIME_PARAMS.get(regime, REGIME_PARAMS['sideways'])
            
            buy_threshold = params.get('signal_threshold', 0.7)
            sell_threshold = -buy_threshold
            vote_min = params.get('vote_min', 3)
            trend_follow = params.get('trend_follow', False)
            range_trade = params.get('range_trade', False)
            
            score_z = result['score_z'].iloc[idx]
            vote = result['vote'].iloc[idx]
            
            days_in_position = idx - entry_idx if in_position else 0
            can_sell = not in_position or days_in_position >= min_holding_days
            
            current_price = result['close'].iloc[idx]
            
            # 牛市趋势跟随
            if trend_follow and regime == 'bull':
                ma_short = result['ma_short'].iloc[idx]
                ma_long = result['ma_long'].iloc[idx]
                trend = (ma_short / ma_long - 1) * 100
                
                if trend > 0 and not in_position:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
                    in_position = True
                    entry_idx = idx
                    entry_price = current_price
                elif trend < -1 and in_position and can_sell:
                    result.iloc[idx, result.columns.get_loc('signal')] = -1
                    in_position = False
                elif in_position:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
            
            # 震荡市区间交易
            elif range_trade and regime == 'sideways':
                boll_upper = result['boll_upper'].iloc[idx]
                boll_lower = result['boll_lower'].iloc[idx]
                boll_mid = result['boll_mid'].iloc[idx]
                
                # 下轨附近买入
                if not in_position and current_price < boll_lower * 1.02 and vote >= 2:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
                    in_position = True
                    entry_idx = idx
                    entry_price = current_price
                
                # 上轨附近卖出
                elif in_position and current_price > boll_upper * 0.98 and can_sell:
                    result.iloc[idx, result.columns.get_loc('signal')] = -1
                    in_position = False
                
                # 持仓
                elif in_position:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
            
            # 普通模式
            else:
                if score_z > buy_threshold and vote >= vote_min and not in_position:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
                    in_position = True
                    entry_idx = idx
                elif score_z < sell_threshold and vote <= -vote_min and in_position and can_sell:
                    result.iloc[idx, result.columns.get_loc('signal')] = -1
                    in_position = False
                elif in_position:
                    result.iloc[idx, result.columns.get_loc('signal')] = 1
        
        return result
    
    def calculate_position_with_regime(self, df: pd.DataFrame) -> pd.DataFrame:
        """按滚动市场环境计算仓位（v1.7：大盘择时调整）"""
        result = df.copy()
        
        # 初始化仓位列
        result['position'] = 0.0
        
        for idx in range(252, len(result)):
            regime = result['regime'].iloc[idx]
            params = REGIME_PARAMS.get(regime, REGIME_PARAMS['sideways'])
            
            # 基础仓位
            base_position = params.get('base_position', POSITION_CONFIG['base_position'])
            
            # 市场环境调整
            market_adj = get_position_adjustment(regime)
            
            # 大盘择时调整（新增）
            market_timing_adj = 1.0
            if 'market_trend_score' in result.columns:
                trend_score = result['market_trend_score'].iloc[idx]
                market_vol = result.get('market_vol', pd.Series([0.2])).iloc[idx] if 'market_vol' in result.columns else 0.2
                market_timing_adj, timing_desc = MarketTimingEnhancer.get_market_adjustment(trend_score, market_vol)
            
            # 波动率调整
            if idx >= 20:
                vol = result['close'].iloc[idx-20:idx].pct_change().std() * np.sqrt(252)
                vol_adj = POSITION_CONFIG['vol_target'] / (vol + 0.0001)
                vol_adj = min(max(vol_adj, POSITION_CONFIG['vol_adjust_range'][0]), POSITION_CONFIG['vol_adjust_range'][1])
            else:
                vol_adj = 1.0
            
            # 信号强度调整
            score_z = abs(result['score_z'].iloc[idx])
            if score_z > 0.7:
                signal_adj = POSITION_CONFIG['signal_adjust']['strong']
            elif score_z > 0.5:
                signal_adj = POSITION_CONFIG['signal_adjust']['medium']
            else:
                signal_adj = POSITION_CONFIG['signal_adjust']['weak']
            
            # 综合仓位（添加大盘择时调整）
            position = base_position * market_adj * market_timing_adj * vol_adj * signal_adj
            position = min(max(position, POSITION_CONFIG['min_position']), POSITION_CONFIG['max_position'])
            position *= abs(result['signal'].iloc[idx])
            
            result.iloc[idx, result.columns.get_loc('position')] = position
        
        return result
    
    def apply_stop_loss_profit_with_regime(self, df: pd.DataFrame) -> pd.DataFrame:
        """按滚动市场环境应用止损止盈"""
        result = df.copy()
        
        result['pnl_pct'] = 0.0
        
        in_position = False
        entry_price = 0.0
        entry_idx = 0
        
        for idx in range(len(result)):
            # 获取当前市场环境的止损止盈参数
            regime = result['regime'].iloc[idx]
            params = REGIME_PARAMS.get(regime, REGIME_PARAMS['sideways'])
            stop_loss = params.get('stop_loss', POSITION_CONFIG.get('stop_loss', 0.06))
            take_profit = params.get('take_profit', POSITION_CONFIG.get('take_profit', 0.12))
            
            current_price = result['close'].iloc[idx]
            
            # 买入信号
            if result['signal'].iloc[idx] == 1 and not in_position:
                in_position = True
                entry_price = current_price
                entry_idx = idx
            
            # 持仓期间
            if in_position and entry_price > 0:
                pnl_pct = (current_price - entry_price) / entry_price
                result.iloc[idx, result.columns.get_loc('pnl_pct')] = pnl_pct
                
                # 止损：减仓
                if pnl_pct < -stop_loss:
                    result.iloc[idx, result.columns.get_loc('position')] *= 0.5
                
                # 止盈：减仓
                elif pnl_pct > take_profit:
                    result.iloc[idx, result.columns.get_loc('position')] *= 0.5
            
            # 卖出信号
            if result['signal'].iloc[idx] == -1:
                in_position = False
                entry_price = 0.0
        
        return result
    
    # ==================== 策略信息 ====================
    
    def get_strategy_info(self, regime: str = None) -> Dict:
        """获取策略信息"""
        if regime is None:
            regime = 'sideways'
        
        weights = get_factor_weights(regime)
        
        return {
            'regime': regime,
            'factor_weights': weights,
            'position_adjustment': get_position_adjustment(regime),
            'config': {
                'base_position': POSITION_CONFIG['base_position'],
                'vol_target': POSITION_CONFIG['vol_target'],
                'signal_threshold': POSITION_CONFIG['signal_threshold']
            }
        }


# 单例
_strategy_instance = None

def get_strategy() -> BestFactorStrategy:
    """获取策略实例"""
    global _strategy_instance
    if _strategy_instance is None:
        _strategy_instance = BestFactorStrategy()
    return _strategy_instance


if __name__ == "__main__":
    # 测试
    from app.config.best_factor_config import print_config
    print_config()
    
    strategy = get_strategy()
    print("\n策略信息（震荡市）:")
    info = strategy.get_strategy_info('sideways')
    print(f"因子权重: {info['factor_weights']}")
    print(f"仓位调整: ×{info['position_adjustment']}")