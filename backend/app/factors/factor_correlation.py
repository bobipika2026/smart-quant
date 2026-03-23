"""
因子相关性分析与降维处理

功能：
1. 计算因子相关性矩阵
2. 识别高相关因子对（相关系数 > 0.8）
3. 基于IC筛选，剔除高相关因子
4. 生成降维后的因子列表
"""
import os
import json
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.factors.factor_library import get_factor_library, FactorCategory


class FactorCorrelationAnalyzer:
    """因子相关性分析器"""
    
    DAY_CACHE_DIR = "data_cache/day"
    RESULTS_DIR = "data_cache/factor_tests"
    OUTPUT_DIR = "data_cache/factor_selection"
    
    def __init__(self):
        self.library = get_factor_library()
        self.stock_codes = self._load_stock_codes()
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
    
    def _load_stock_codes(self) -> List[str]:
        """加载股票代码"""
        files = os.listdir(self.DAY_CACHE_DIR)
        codes = [f.replace('_day.csv', '') for f in files if f.endswith('_day.csv')]
        return sorted(codes)
    
    def calc_factor_values_batch(self, factor_codes: List[str], 
                                  stock_codes: List[str] = None,
                                  max_stocks: int = 500) -> pd.DataFrame:
        """
        批量计算因子值
        
        Returns:
            DataFrame: columns=factors, index=stock_code
        """
        if stock_codes is None:
            stock_codes = self.stock_codes[:max_stocks]
        
        print(f"计算 {len(factor_codes)} 个因子在 {len(stock_codes)} 只股票上的值...")
        
        results = {}
        
        for i, stock_code in enumerate(stock_codes):
            if (i + 1) % 100 == 0:
                print(f"  进度: {i+1}/{len(stock_codes)}")
            
            factor_values = {}
            for factor_code in factor_codes:
                try:
                    value = self.library.calc_factor(factor_code, stock_code)
                    if value is not None and not np.isnan(value) and not np.isinf(value):
                        factor_values[factor_code] = value
                except:
                    continue
            
            if factor_values:
                results[stock_code] = factor_values
        
        df = pd.DataFrame(results).T
        print(f"完成，有效股票数: {len(df)}")
        
        return df
    
    def calc_correlation_matrix(self, factor_values: pd.DataFrame) -> pd.DataFrame:
        """
        计算因子相关性矩阵
        
        Args:
            factor_values: DataFrame, columns=factors, index=stock_code
        
        Returns:
            相关性矩阵
        """
        print("计算相关性矩阵...")
        
        # 去除缺失值过多的因子
        valid_cols = [col for col in factor_values.columns 
                      if factor_values[col].notna().sum() > len(factor_values) * 0.5]
        
        df_clean = factor_values[valid_cols].dropna()
        
        # 计算Spearman相关系数
        corr_matrix = df_clean.corr(method='spearman')
        
        print(f"相关性矩阵: {corr_matrix.shape}")
        
        return corr_matrix
    
    def find_high_correlation_pairs(self, corr_matrix: pd.DataFrame, 
                                     threshold: float = 0.8) -> List[Tuple[str, str, float]]:
        """
        找出高相关因子对
        
        Returns:
            [(factor1, factor2, correlation), ...]
        """
        print(f"查找相关系数 > {threshold} 的因子对...")
        
        high_corr_pairs = []
        n = len(corr_matrix)
        
        for i in range(n):
            for j in range(i + 1, n):
                corr = corr_matrix.iloc[i, j]
                if abs(corr) > threshold:
                    f1 = corr_matrix.index[i]
                    f2 = corr_matrix.columns[j]
                    high_corr_pairs.append((f1, f2, round(corr, 4)))
        
        print(f"发现 {len(high_corr_pairs)} 对高相关因子")
        
        # 按相关性排序
        high_corr_pairs.sort(key=lambda x: abs(x[2]), reverse=True)
        
        return high_corr_pairs
    
    def load_ic_results(self) -> Dict[str, Dict]:
        """加载IC检验结果"""
        files = sorted([f for f in os.listdir(self.RESULTS_DIR) 
                       if f.startswith('ic_test_')], reverse=True)
        
        if not files:
            print("未找到IC检验结果，请先运行IC检验")
            return {}
        
        with open(os.path.join(self.RESULTS_DIR, files[0])) as f:
            data = json.load(f)
        
        # 转换为 {factor_code: ic_info}
        ic_dict = {}
        for factor in data.get('factors', []):
            ic_dict[factor['factor_code']] = factor
        
        return ic_dict
    
    def select_factors_by_correlation(self, 
                                       factor_codes: List[str],
                                       corr_matrix: pd.DataFrame,
                                       ic_dict: Dict[str, Dict],
                                       threshold: float = 0.8) -> Tuple[List[str], List[Dict]]:
        """
        基于相关性筛选因子
        
        规则：
        1. 找出高相关因子对
        2. 保留IC更高的因子
        3. 返回筛选后的因子列表
        
        Returns:
            (selected_factors, removed_factors)
        """
        print(f"\n开始因子降维，阈值: {threshold}")
        print(f"原始因子数: {len(factor_codes)}")
        
        # 初始化
        selected = set(factor_codes)
        removed = []
        
        # 找出高相关因子对
        high_corr_pairs = self.find_high_correlation_pairs(
            corr_matrix.loc[list(selected), list(selected)], 
            threshold
        )
        
        # 处理每一对高相关因子
        for f1, f2, corr in high_corr_pairs:
            if f1 not in selected or f2 not in selected:
                continue
            
            # 获取IC值
            ic1 = ic_dict.get(f1, {}).get('ir', 0)
            ic2 = ic_dict.get(f2, {}).get('ir', 0)
            
            # 保留IC更高的因子
            if ic1 >= ic2:
                keep, drop = f1, f2
                keep_ic, drop_ic = ic1, ic2
            else:
                keep, drop = f2, f1
                keep_ic, drop_ic = ic2, ic1
            
            # 剔除
            if drop in selected:
                selected.remove(drop)
                removed.append({
                    'factor': drop,
                    'correlated_with': keep,
                    'correlation': corr,
                    'removed_ic': drop_ic,
                    'kept_ic': keep_ic
                })
        
        print(f"筛选后因子数: {len(selected)}")
        print(f"剔除因子数: {len(removed)}")
        
        return list(selected), removed
    
    def run_full_analysis(self, 
                          factor_codes: List[str] = None,
                          threshold: float = 0.8,
                          max_stocks: int = 500) -> Dict:
        """
        运行完整的相关性分析和降维
        
        Args:
            factor_codes: 要分析的因子列表（默认使用所有因子）
            threshold: 相关性阈值
            max_stocks: 用于计算的最大股票数
        
        Returns:
            {
                'original_factors': int,
                'selected_factors': int,
                'removed_factors': list,
                'correlation_matrix': DataFrame,
                'high_correlation_pairs': list
            }
        """
        print("=" * 60)
        print("因子相关性分析与降维")
        print("=" * 60)
        
        # 1. 获取因子列表
        if factor_codes is None:
            factor_codes = list(self.library.factor_definitions.keys())
        
        # 过滤有IC数据的因子
        ic_dict = self.load_ic_results()
        valid_factors = [f for f in factor_codes if f in ic_dict]
        print(f"有效因子（有IC数据）: {len(valid_factors)}/{len(factor_codes)}")
        
        # 2. 计算因子值
        factor_values = self.calc_factor_values_batch(valid_factors, max_stocks=max_stocks)
        
        # 3. 计算相关性矩阵
        corr_matrix = self.calc_correlation_matrix(factor_values)
        
        # 4. 找出高相关因子对
        high_corr_pairs = self.find_high_correlation_pairs(corr_matrix, threshold)
        
        # 5. 降维筛选
        selected_factors, removed_factors = self.select_factors_by_correlation(
            valid_factors, corr_matrix, ic_dict, threshold
        )
        
        # 6. 按类别分组
        selected_by_category = {}
        for factor_code in selected_factors:
            factor_meta = self.library.factor_definitions.get(factor_code)
            if factor_meta:
                category = factor_meta.category.value
                if category not in selected_by_category:
                    selected_by_category[category] = []
                selected_by_category[category].append({
                    'code': factor_code,
                    'name': factor_meta.name,
                    'ic': ic_dict.get(factor_code, {}).get('ic_mean', 0),
                    'ir': ic_dict.get(factor_code, {}).get('ir', 0),
                    'weight': factor_meta.weight
                })
        
        # 7. 生成报告
        result = {
            'summary': {
                'original_factors': len(valid_factors),
                'selected_factors': len(selected_factors),
                'removed_factors': len(removed_factors),
                'threshold': threshold,
                'test_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'selected_factors': selected_factors,
            'selected_by_category': selected_by_category,
            'removed_factors': removed_factors,
            'high_correlation_pairs': high_corr_pairs[:50]  # 只保留前50对
        }
        
        # 8. 保存结果
        output_file = os.path.join(
            self.OUTPUT_DIR, 
            f"factor_selection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        # 保存JSON（不包含DataFrame）
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        # 保存相关性矩阵
        corr_file = os.path.join(
            self.OUTPUT_DIR,
            f"correlation_matrix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        corr_matrix.to_csv(corr_file)
        
        print("\n" + "=" * 60)
        print("分析完成")
        print("=" * 60)
        print(f"原始因子数: {result['summary']['original_factors']}")
        print(f"筛选后因子数: {result['summary']['selected_factors']}")
        print(f"剔除因子数: {result['summary']['removed_factors']}")
        print(f"\n结果已保存:")
        print(f"  - {output_file}")
        print(f"  - {corr_file}")
        
        # 打印各类别因子数
        print("\n各类别筛选后因子数:")
        for category, factors in selected_by_category.items():
            print(f"  {category}: {len(factors)}")
        
        return result


def run_factor_selection(threshold: float = 0.8, max_stocks: int = 500):
    """
    运行因子筛选
    
    Args:
        threshold: 相关性阈值（默认0.8）
        max_stocks: 用于计算的最大股票数
    """
    analyzer = FactorCorrelationAnalyzer()
    return analyzer.run_full_analysis(threshold=threshold, max_stocks=max_stocks)


if __name__ == "__main__":
    # 运行因子筛选
    result = run_factor_selection(threshold=0.8, max_stocks=500)
    
    # 打印被剔除的因子
    print("\n被剔除的因子:")
    for item in result['removed_factors'][:20]:
        print(f"  {item['factor']} (corr={item['correlation']:.3f} with {item['correlated_with']})")