"""
因子IC检验服务

对因子库中的所有因子进行IC检验
"""
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

from app.factors.factor_library import get_factor_library, FactorCategory


class FactorICTester:
    """因子IC检验器"""
    
    # 数据路径 - 使用绝对路径
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DAY_CACHE_DIR = os.path.join(BASE_DIR, "data_cache/day")
    FINANCIAL_DIR = os.path.join(BASE_DIR, "data_cache/financial")
    DB_PATH = os.path.join(BASE_DIR, "smart_quant.db")
    RESULTS_DIR = os.path.join(BASE_DIR, "data_cache/factor_tests")
    
    def __init__(self):
        self.library = get_factor_library()
        self.stock_codes = self._load_stock_codes()
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
    
    def _load_stock_codes(self) -> List[str]:
        """加载股票代码"""
        # 优先从日线文件获取（更可靠）
        if os.path.exists(self.DAY_CACHE_DIR):
            files = os.listdir(self.DAY_CACHE_DIR)
            codes = [f.replace('_day.csv', '') for f in files if f.endswith('_day.csv')]
            if codes:
                return sorted(codes)
        
        # 备选：从数据库获取
        try:
            conn = sqlite3.connect(self.DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT code FROM stocks ORDER BY code")
            codes = [row[0] for row in cursor.fetchall()]
            conn.close()
            if codes:
                return codes
        except:
            pass
        
        return []
    
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
    
    def calc_forward_returns(self, df: pd.DataFrame, horizon: int = 20) -> pd.DataFrame:
        """计算未来收益率"""
        df = df.copy()
        df['forward_return'] = df['收盘'].pct_change(horizon).shift(-horizon)
        return df
    
    def calc_ic_single_period(self, factor_values: pd.Series, 
                               forward_returns: pd.Series) -> Tuple[float, int]:
        """
        计算单期IC
        
        Returns:
            (IC值, 有效股票数)
        """
        # 对齐数据
        aligned = pd.DataFrame({
            'factor': factor_values,
            'return': forward_returns
        }).dropna()
        
        if len(aligned) < 30:  # 至少30只股票
            return 0.0, 0
        
        # Spearman相关
        try:
            # 使用秩相关
            factor_rank = aligned['factor'].rank()
            return_rank = aligned['return'].rank()
            ic = factor_rank.corr(return_rank, method='pearson')
            return float(ic) if not np.isnan(ic) else 0.0, len(aligned)
        except:
            return 0.0, 0
    
    def test_factor(self, factor_code: str, 
                    start_date: str = None, 
                    end_date: str = None,
                    horizon: int = 20,
                    freq: int = 20) -> Dict:
        """
        测试单个因子
        
        Args:
            factor_code: 因子代码
            start_date: 开始日期
            end_date: 结束日期
            horizon: 预测周期（天）
            freq: 测试频率（天）
        
        Returns:
            {
                'factor_code': str,
                'ic_mean': float,
                'ic_std': float,
                'ir': float,
                'ic_positive_ratio': float,
                'ic_series': [...]
            }
        """
        print(f"  测试因子: {factor_code}")
        
        # 按日期收集因子值和收益率
        factor_data = {}  # {date: {stock: factor_value}}
        return_data = {}  # {date: {stock: forward_return}}
        
        # 加载所有股票的数据
        for stock_code in self.stock_codes[:500]:  # 限制股票数以加速
            try:
                # 加载日线
                df = self._load_day_data(stock_code)
                if df is None or len(df) < 100:
                    continue
                
                # 计算未来收益
                df = self.calc_forward_returns(df, horizon)
                
                # 遍历每个日期
                for idx in range(len(df) - horizon):
                    date = str(df.iloc[idx]['日期'])
                    
                    # 初始化日期
                    if date not in factor_data:
                        factor_data[date] = {}
                        return_data[date] = {}
                    
                    # 计算因子值
                    factor_value = self.library.calc_factor(factor_code, stock_code)
                    forward_return = df.iloc[idx]['forward_return']
                    
                    if factor_value is not None and not np.isnan(forward_return):
                        factor_data[date][stock_code] = factor_value
                        return_data[date][stock_code] = forward_return
                        
            except Exception as e:
                continue
        
        if not factor_data:
            return {
                'factor_code': factor_code,
                'ic_mean': 0,
                'ic_std': 0,
                'ir': 0,
                'ic_positive_ratio': 0,
                'valid_periods': 0
            }
        
        # 按日期计算IC
        dates = sorted(factor_data.keys())
        
        # 按频率采样
        test_dates = dates[::freq]
        
        ic_series = []
        for date in test_dates:
            if date not in return_data:
                continue
            
            factor_values = pd.Series(factor_data[date])
            forward_returns = pd.Series(return_data[date])
            
            ic, n = self.calc_ic_single_period(factor_values, forward_returns)
            if n > 0:
                ic_series.append({
                    'date': date,
                    'ic': ic,
                    'n_stocks': n
                })
        
        if not ic_series:
            return {
                'factor_code': factor_code,
                'ic_mean': 0,
                'ic_std': 0,
                'ir': 0,
                'ic_positive_ratio': 0,
                'valid_periods': 0
            }
        
        # 统计
        ic_values = [x['ic'] for x in ic_series]
        ic_mean = np.mean(ic_values)
        ic_std = np.std(ic_values)
        ir = ic_mean / ic_std if ic_std > 0 else 0
        ic_positive_ratio = sum(1 for ic in ic_values if ic > 0) / len(ic_values)
        
        return {
            'factor_code': factor_code,
            'ic_mean': round(float(ic_mean), 4),
            'ic_std': round(float(ic_std), 4),
            'ir': round(float(ir), 4),
            'ic_positive_ratio': round(float(ic_positive_ratio), 4),
            'valid_periods': len(ic_series),
            'ic_series': ic_series[:10]  # 只保留前10个
        }
    
    def test_all_factors(self, horizon: int = 20, 
                         max_workers: int = 4) -> Dict:
        """
        测试所有因子
        
        Args:
            horizon: 预测周期
            max_workers: 并发数
        
        Returns:
            {
                'factors': [...],
                'summary': {...}
            }
        """
        print(f"\n开始IC检验，共{len(self.library.factor_definitions)}个因子")
        print(f"股票数: {len(self.stock_codes)}")
        print(f"预测周期: {horizon}天")
        print("-" * 50)
        
        results = []
        
        # 按类别测试
        for category in FactorCategory:
            factor_codes = self.library.factors_by_category.get(category, [])
            print(f"\n[{category.value}] {len(factor_codes)}个因子")
            
            for factor_code in factor_codes:
                result = self.test_factor(factor_code, horizon=horizon)
                results.append(result)
                
                # 打印进度
                ic_mean = result['ic_mean']
                ir = result['ir']
                status = "✓" if ir > 0.3 else "✗"
                print(f"  {factor_code}: IC={ic_mean:.3f}, IR={ir:.3f} {status}")
        
        # 排序
        results.sort(key=lambda x: x['ir'], reverse=True)
        
        # 统计
        valid_factors = [r for r in results if r['valid_periods'] > 0]
        good_factors = [r for r in valid_factors if r['ir'] > 0.3]
        
        summary = {
            'total_factors': len(results),
            'valid_factors': len(valid_factors),
            'good_factors': len(good_factors),
            'test_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'horizon': horizon
        }
        
        print("\n" + "=" * 50)
        print("IC检验完成")
        print("=" * 50)
        print(f"总因子数: {summary['total_factors']}")
        print(f"有效因子: {summary['valid_factors']}")
        print(f"优秀因子(IR>0.3): {summary['good_factors']}")
        
        # 保存结果
        result_file = os.path.join(self.RESULTS_DIR, 
                                   f"ic_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(result_file, 'w') as f:
            json.dump({
                'summary': summary,
                'factors': results
            }, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存: {result_file}")
        
        return {
            'factors': results,
            'summary': summary
        }
    
    def get_top_factors(self, top_n: int = 20) -> List[Dict]:
        """获取Top N因子"""
        # 查找最新的测试结果
        files = sorted([f for f in os.listdir(self.RESULTS_DIR) 
                       if f.startswith('ic_test_')], reverse=True)
        
        if not files:
            return []
        
        with open(os.path.join(self.RESULTS_DIR, files[0])) as f:
            data = json.load(f)
        
        return data['factors'][:top_n]


# 单例
_ic_tester = None

def get_ic_tester() -> FactorICTester:
    global _ic_tester
    if _ic_tester is None:
        _ic_tester = FactorICTester()
    return _ic_tester