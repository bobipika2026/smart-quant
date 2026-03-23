"""
因子参数敏感性测试

功能：
1. 对有参数的因子测试不同参数下的IC值
2. 分析参数在不同市场环境下的稳定性
3. 选择最优参数
"""
import os
import json
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.factors.factor_library import get_factor_library, FactorCategory


class ParamSensitivityTester:
    """参数敏感性测试器"""
    
    DAY_CACHE_DIR = "data_cache/day"
    FINANCIAL_DIR = "data_cache/financial"
    OUTPUT_DIR = "data_cache/factor_tests"
    
    # 有参数因子的参数配置
    PARAM_CONFIGS = {
        # 动量因子
        'MOM': {
            'base_code': 'MOM6',
            'name': '动量因子',
            'params': [
                {'code': 'MOM3', 'period': 63, 'desc': '3月动量'},
                {'code': 'MOM6', 'period': 126, 'desc': '6月动量'},
                {'code': 'MOM9', 'period': 189, 'desc': '9月动量'},
                {'code': 'MOM12', 'period': 252, 'desc': '12月动量'},
            ]
        },
        # 成交量动量
        'VOL_M': {
            'base_code': 'VOL_M',
            'name': '成交量动量',
            'params': [
                {'code': 'VOL_M_5_20', 'short': 5, 'long': 20, 'desc': 'VOL_M(5/20)'},
                {'code': 'VOL_M_5_60', 'short': 5, 'long': 60, 'desc': 'VOL_M(5/60)'},
                {'code': 'VOL_M_10_20', 'short': 10, 'long': 20, 'desc': 'VOL_M(10/20)'},
            ]
        },
        # KDJ
        'KDJ': {
            'base_code': 'KDJ_D',
            'name': 'KDJ',
            'params': [
                {'code': 'KDJ_9_3_3', 'n': 9, 'm1': 3, 'm2': 3, 'desc': 'KDJ(9,3,3)'},
                {'code': 'KDJ_14_3_3', 'n': 14, 'm1': 3, 'm2': 3, 'desc': 'KDJ(14,3,3)'},
                {'code': 'KDJ_9_5_5', 'n': 9, 'm1': 5, 'm2': 5, 'desc': 'KDJ(9,5,5)'},
            ]
        },
        # 布林带
        'BOLL': {
            'base_code': 'BOLL_POS',
            'name': '布林带',
            'params': [
                {'code': 'BOLL_20_2', 'period': 20, 'std': 2.0, 'desc': 'BOLL(20,2)'},
                {'code': 'BOLL_20_2.5', 'period': 20, 'std': 2.5, 'desc': 'BOLL(20,2.5)'},
                {'code': 'BOLL_10_2', 'period': 10, 'std': 2.0, 'desc': 'BOLL(10,2)'},
            ]
        },
        # ATR
        'ATR': {
            'base_code': 'ATR_R',
            'name': 'ATR比率',
            'params': [
                {'code': 'ATR_7', 'period': 7, 'desc': 'ATR(7)'},
                {'code': 'ATR_14', 'period': 14, 'desc': 'ATR(14)'},
                {'code': 'ATR_21', 'period': 21, 'desc': 'ATR(21)'},
            ]
        },
        # MA突破
        'BREAK_MA': {
            'base_code': 'BREAK_MA60',
            'name': '突破MA',
            'params': [
                {'code': 'BREAK_MA20', 'period': 20, 'desc': '突破MA20'},
                {'code': 'BREAK_MA60', 'period': 60, 'desc': '突破MA60'},
                {'code': 'BREAK_MA120', 'period': 120, 'desc': '突破MA120'},
            ]
        },
        # 波动率
        'VOLATILITY': {
            'base_code': 'VOL',
            'name': '波动率',
            'params': [
                {'code': 'VOL_20', 'period': 20, 'desc': 'VOL(20)'},
                {'code': 'VOL_60', 'period': 60, 'desc': 'VOL(60)'},
                {'code': 'VOL_120', 'period': 120, 'desc': 'VOL(120)'},
            ]
        },
    }
    
    def __init__(self):
        self.library = get_factor_library()
        self.stock_codes = self._load_stock_codes()
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
    
    def _load_stock_codes(self) -> List[str]:
        """加载股票代码"""
        files = os.listdir(self.DAY_CACHE_DIR)
        codes = [f.replace('_day.csv', '') for f in files if f.endswith('_day.csv')]
        return sorted(codes)
    
    def _load_day_data(self, stock_code: str) -> Optional[pd.DataFrame]:
        """加载日线数据"""
        file_path = os.path.join(self.DAY_CACHE_DIR, f"{stock_code}_day.csv")
        if not os.path.exists(file_path):
            return None
        try:
            df = pd.read_csv(file_path)
            df = df.sort_values('日期', ascending=True)
            return df
        except:
            return None
    
    def calc_momentum(self, df: pd.DataFrame, period: int) -> Optional[float]:
        """计算动量因子"""
        if len(df) < period + 1:
            return None
        close = df['收盘'].values
        return float((close[-1] / close[-period-1]) - 1) if close[-period-1] > 0 else None
    
    def calc_vol_momentum(self, df: pd.DataFrame, short: int, long: int) -> Optional[float]:
        """计算成交量动量"""
        if len(df) < long:
            return None
        vol = df['成交量'].values
        vol_short = vol[-short:].mean()
        vol_long = vol[-long:].mean()
        return float(vol_short / vol_long) if vol_long > 0 else None
    
    def calc_kdj_d(self, df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> Optional[float]:
        """计算KDJ D值"""
        if len(df) < n + m1 + m2:
            return None
        low_min = df['最低'].rolling(n).min()
        high_max = df['最高'].rolling(n).max()
        rsv = (df['收盘'] - low_min) / (high_max - low_min) * 100
        rsv = rsv.fillna(50)
        k = rsv.ewm(com=m1-1).mean()
        d = k.ewm(com=m2-1).mean()
        return float(d.iloc[-1])
    
    def calc_boll_pos(self, df: pd.DataFrame, period: int = 20, std_mult: float = 2.0) -> Optional[float]:
        """计算布林位置"""
        if len(df) < period:
            return None
        close = df['收盘'].values
        ma = np.mean(close[-period:])
        std = np.std(close[-period:])
        if std == 0:
            return 0
        return float((close[-1] - ma) / (std_mult * std))
    
    def calc_atr_ratio(self, df: pd.DataFrame, period: int = 14) -> Optional[float]:
        """计算ATR比率"""
        if len(df) < period + 1:
            return None
        high = df['最高'].values
        low = df['最低'].values
        close = df['收盘'].values
        
        tr = np.maximum(
            high[-period:] - low[-period:],
            np.abs(high[-period:] - np.roll(close, 1)[-period:]),
            np.abs(low[-period:] - np.roll(close, 1)[-period:])
        )
        atr = np.mean(tr[1:])
        return float(atr / close[-1]) if close[-1] > 0 else None
    
    def calc_break_ma(self, df: pd.DataFrame, period: int = 60) -> Optional[float]:
        """计算突破MA信号"""
        if len(df) < period + 1:
            return None
        close = df['收盘'].values
        ma_curr = np.mean(close[-period:])
        ma_prev = np.mean(close[-period-1:-1])
        
        if close[-2] <= ma_prev and close[-1] > ma_curr:
            return 100.0  # 突破
        return 0.0
    
    def calc_volatility(self, df: pd.DataFrame, period: int = 20) -> Optional[float]:
        """计算波动率"""
        if len(df) < period:
            return None
        returns = df['收盘'].pct_change().dropna()
        if len(returns) < period:
            return None
        return float(returns.iloc[-period:].std() * np.sqrt(252) * 100)
    
    def calc_forward_return(self, df: pd.DataFrame, horizon: int = 20) -> Optional[float]:
        """计算未来收益率"""
        if len(df) < horizon + 1:
            return None
        close = df['收盘'].values
        return float((close[-1] / close[-(horizon+1)]) - 1) if close[-(horizon+1)] > 0 else None
    
    def test_single_param(self, factor_type: str, param_config: Dict, 
                           stock_codes: List[str], horizon: int = 20) -> Dict:
        """
        测试单个参数配置
        
        Returns:
            {
                'code': str,
                'desc': str,
                'ic': float,
                'n_stocks': int
            }
        """
        factor_values = []
        forward_returns = []
        
        for stock_code in stock_codes:
            df = self._load_day_data(stock_code)
            if df is None or len(df) < 300:
                continue
            
            # 计算因子值
            if factor_type == 'MOM':
                value = self.calc_momentum(df, param_config['period'])
            elif factor_type == 'VOL_M':
                value = self.calc_vol_momentum(df, param_config['short'], param_config['long'])
            elif factor_type == 'KDJ':
                value = self.calc_kdj_d(df, param_config['n'], param_config['m1'], param_config['m2'])
            elif factor_type == 'BOLL':
                value = self.calc_boll_pos(df, param_config['period'], param_config['std'])
            elif factor_type == 'ATR':
                value = self.calc_atr_ratio(df, param_config['period'])
            elif factor_type == 'BREAK_MA':
                value = self.calc_break_ma(df, param_config['period'])
            elif factor_type == 'VOLATILITY':
                value = self.calc_volatility(df, param_config['period'])
            else:
                continue
            
            # 计算未来收益
            fwd_ret = self.calc_forward_return(df, horizon)
            
            if value is not None and fwd_ret is not None:
                factor_values.append(value)
                forward_returns.append(fwd_ret)
        
        if len(factor_values) < 30:
            return {
                'code': param_config['code'],
                'desc': param_config['desc'],
                'ic': 0,
                'n_stocks': len(factor_values),
                'valid': False
            }
        
        # 计算IC（Spearman相关）
        factor_series = pd.Series(factor_values)
        return_series = pd.Series(forward_returns)
        ic = factor_series.corr(return_series, method='spearman')
        
        return {
            'code': param_config['code'],
            'desc': param_config['desc'],
            'ic': round(float(ic), 4) if not np.isnan(ic) else 0,
            'n_stocks': len(factor_values),
            'valid': True
        }
    
    def test_factor_params(self, factor_type: str, 
                            max_stocks: int = 500,
                            horizon: int = 20) -> Dict:
        """
        测试某个因子类型的不同参数
        
        Returns:
            {
                'factor_type': str,
                'name': str,
                'results': [...],
                'best_param': {...}
            }
        """
        config = self.PARAM_CONFIGS.get(factor_type)
        if not config:
            return None
        
        print(f"\n测试 {config['name']} 参数...")
        print(f"  参数配置数: {len(config['params'])}")
        
        stock_codes = self.stock_codes[:max_stocks]
        results = []
        
        for param_config in config['params']:
            result = self.test_single_param(factor_type, param_config, stock_codes, horizon)
            results.append(result)
            
            status = "✓" if result['valid'] else "✗"
            print(f"  {result['desc']:20s} IC={result['ic']:+.4f} (n={result['n_stocks']}) {status}")
        
        # 找出最优参数（IC绝对值最大）
        valid_results = [r for r in results if r['valid']]
        if valid_results:
            best = max(valid_results, key=lambda x: abs(x['ic']))
        else:
            best = results[0]
        
        return {
            'factor_type': factor_type,
            'name': config['name'],
            'results': results,
            'best_param': best
        }
    
    def test_all_params(self, max_stocks: int = 500, horizon: int = 20) -> Dict:
        """
        测试所有有参数因子的参数敏感性
        
        Returns:
            {
                'summary': {...},
                'factors': [...],
                'best_params': [...]
            }
        """
        print("=" * 60)
        print("因子参数敏感性测试")
        print("=" * 60)
        print(f"测试股票数: {max_stocks}")
        print(f"预测周期: {horizon}天")
        
        results = []
        best_params = []
        
        for factor_type in self.PARAM_CONFIGS.keys():
            result = self.test_factor_params(factor_type, max_stocks, horizon)
            if result:
                results.append(result)
                best_params.append(result['best_param'])
        
        # 汇总
        summary = {
            'test_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_factors': len(results),
            'max_stocks': max_stocks,
            'horizon': horizon
        }
        
        output = {
            'summary': summary,
            'factors': results,
            'best_params': best_params
        }
        
        # 保存结果
        output_file = os.path.join(
            self.OUTPUT_DIR, 
            f"param_sensitivity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print("\n" + "=" * 60)
        print("测试完成")
        print("=" * 60)
        print(f"结果已保存: {output_file}")
        
        # 打印最优参数汇总
        print("\n最优参数汇总:")
        print("-" * 60)
        for r in results:
            best = r['best_param']
            print(f"{r['name']:15s} 最优: {best['desc']:20s} IC={best['ic']:+.4f}")
        
        return output


def run_param_sensitivity_test(max_stocks: int = 500, horizon: int = 20):
    """
    运行参数敏感性测试
    
    Args:
        max_stocks: 测试股票数
        horizon: 预测周期
    """
    tester = ParamSensitivityTester()
    return tester.test_all_params(max_stocks=max_stocks, horizon=horizon)


if __name__ == "__main__":
    result = run_param_sensitivity_test(max_stocks=500, horizon=20)