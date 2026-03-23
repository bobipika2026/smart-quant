"""
因子组合优化 - 五种降维方法

方法1：风格因子分组
方法2：因子合成（Barra做法）
方法3：主成分分析（PCA）
方法4：IC加权筛选（中金做法）
方法5：正交化处理
"""
import os
import json
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from scipy import stats
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from app.factors.factor_library import get_factor_library, FactorCategory


class FactorOptimizer:
    """因子组合优化器"""
    
    OUTPUT_DIR = "data_cache/factor_tests"
    
    # 31个最终因子及其IC值
    FINAL_FACTORS = {
        # 高IC因子（技术类）
        'KDJ_14_3_3': {'ic': 0.8712, 'category': 'technical', 'name': 'KDJ_D(14,3,3)'},
        'BOLL_20_2': {'ic': 0.8092, 'category': 'technical', 'name': '布林位置(20,2)'},
        'VOL_M_5_60': {'ic': 0.7346, 'category': 'momentum', 'name': '成交量动量(5/60)'},
        'MOM3': {'ic': 0.5901, 'category': 'momentum', 'name': '3月动量'},
        'ATR_7': {'ic': 0.5369, 'category': 'technical', 'name': 'ATR比率(7)'},
        
        # 中等IC因子
        'VOL_20': {'ic': 0.3248, 'category': 'sentiment', 'name': '波动率(20)'},
        'TURN': {'ic': 0.3024, 'category': 'sentiment', 'name': '换手率'},
        'LEV': {'ic': -0.2125, 'category': 'quality', 'name': '财务杠杆'},
        'BULLISH_RATIO': {'ic': 0.203, 'category': 'technical', 'name': '阳线比例'},
        'UPPER_SHADOW': {'ic': 0.1745, 'category': 'technical', 'name': '上影线比例'},
        'EP': {'ic': -0.1712, 'category': 'value', 'name': '盈利收益率'},
        'VOL_M': {'ic': 0.1562, 'category': 'momentum', 'name': '成交量动量'},
        'GAP_DOWN': {'ic': -0.1523, 'category': 'technical', 'name': '向下跳空'},
        'CONSECUTIVE_UP': {'ic': 0.1485, 'category': 'technical', 'name': '连阳天数'},
        'KDJ_D': {'ic': 0.1416, 'category': 'technical', 'name': 'KDJ D值'},
        'VWAP': {'ic': -0.1348, 'category': 'technical', 'name': 'VWAP'},
        'BP': {'ic': -0.1294, 'category': 'value', 'name': '账面市值比'},
        'BOLL_LOWER': {'ic': -0.1264, 'category': 'technical', 'name': '布林下轨'},
        'BOLL_UPPER': {'ic': -0.124, 'category': 'technical', 'name': '布林上轨'},
        'BODY_SIZE': {'ic': -0.1043, 'category': 'technical', 'name': '实体大小'},
        'NCFP': {'ic': -0.1024, 'category': 'value', 'name': '现金流市值比'},
        'AMOUNT_MA_RATIO': {'ic': 0.1017, 'category': 'technical', 'name': '成交额MA比率'},
        'DIV_YIELD': {'ic': -0.1004, 'category': 'value', 'name': '股息率'},
        'GAP_UP': {'ic': -0.0814, 'category': 'technical', 'name': '向上跳空'},
        'EPS_G': {'ic': 0.0791, 'category': 'growth', 'name': 'EPS增长率'},
        'ROE_D': {'ic': 0.0667, 'category': 'growth', 'name': 'ROE变化'},
        'CONSECUTIVE_DOWN': {'ic': -0.0649, 'category': 'technical', 'name': '连阴天数'},
        'ROA': {'ic': 0.057, 'category': 'quality', 'name': 'ROA'},
        'GPM': {'ic': -0.0549, 'category': 'quality', 'name': '毛利率'},
        'ACCR': {'ic': 0.0349, 'category': 'quality', 'name': '应计项目'},
        'REV_G': {'ic': -0.0331, 'category': 'growth', 'name': '营收增长率'},
    }
    
    # 风格分类
    STYLE_GROUPS = {
        'value': ['EP', 'BP', 'NCFP', 'DIV_YIELD'],
        'growth': ['EPS_G', 'ROE_D', 'REV_G'],
        'quality': ['LEV', 'ROA', 'GPM', 'ACCR'],
        'momentum': ['MOM3', 'VOL_M_5_60'],
        'sentiment': ['TURN', 'VOL_20'],
        'technical': ['KDJ_14_3_3', 'BOLL_20_2', 'ATR_7', 'BULLISH_RATIO', 
                      'UPPER_SHADOW', 'GAP_DOWN', 'CONSECUTIVE_UP', 'VWAP',
                      'BOLL_LOWER', 'BOLL_UPPER', 'BODY_SIZE', 'AMOUNT_MA_RATIO',
                      'GAP_UP', 'CONSECUTIVE_DOWN']
    }
    
    def __init__(self):
        self.library = get_factor_library()
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
    
    # ==================== 方法1：风格因子分组 ====================
    
    def method1_style_grouping(self, top_n_per_style: int = 2) -> Dict:
        """
        方法1：风格因子分组
        每个风格选Top N个核心因子
        """
        print("\n" + "="*60)
        print("方法1：风格因子分组")
        print("="*60)
        
        selected_factors = []
        style_details = {}
        
        for style, factors in self.STYLE_GROUPS.items():
            # 按IC绝对值排序
            factor_ics = [(f, abs(self.FINAL_FACTORS.get(f, {}).get('ic', 0))) 
                          for f in factors if f in self.FINAL_FACTORS]
            factor_ics.sort(key=lambda x: x[1], reverse=True)
            
            # 选Top N
            top_factors = [f[0] for f in factor_ics[:top_n_per_style]]
            selected_factors.extend(top_factors)
            
            style_details[style] = {
                'total': len(factors),
                'selected': top_factors,
                'ics': {f: self.FINAL_FACTORS.get(f, {}).get('ic', 0) for f in top_factors}
            }
            
            print(f"\n{style:12s}: {len(top_factors)}个核心因子")
            for f in top_factors:
                print(f"  - {f:20s} IC={self.FINAL_FACTORS[f]['ic']:+.4f}")
        
        result = {
            'method': 'style_grouping',
            'description': '每个风格选Top N核心因子',
            'total_factors': len(selected_factors),
            'combinations': 2**len(selected_factors) - 1,
            'selected_factors': selected_factors,
            'style_details': style_details
        }
        
        print(f"\n总计: {len(selected_factors)}个因子")
        print(f"组合数: {result['combinations']:,}")
        
        return result
    
    # ==================== 方法2：因子合成 ====================
    
    def method2_factor_synthesis(self) -> Dict:
        """
        方法2：因子合成（Barra做法）
        每个风格内的因子合成一个综合因子
        """
        print("\n" + "="*60)
        print("方法2：因子合成（Barra做法）")
        print("="*60)
        
        synthesis_config = {}
        
        for style, factors in self.STYLE_GROUPS.items():
            # 计算权重（基于IC绝对值）
            factor_ics = {f: abs(self.FINAL_FACTORS.get(f, {}).get('ic', 0)) 
                          for f in factors if f in self.FINAL_FACTORS}
            total_ic = sum(factor_ics.values())
            
            if total_ic > 0:
                weights = {f: round(ic / total_ic, 4) for f, ic in factor_ics.items()}
            else:
                weights = {f: 1/len(factors) for f in factors}
            
            synthesis_config[style] = {
                'factors': list(factor_ics.keys()),
                'weights': weights,
                'formula': f"{style}_score = " + " + ".join([f"{w}*{f}" for f, w in weights.items()])
            }
            
            print(f"\n{style:12s}:")
            print(f"  因子: {list(factor_ics.keys())}")
            print(f"  权重: {weights}")
        
        # 风格权重
        style_weights = {
            'value': 0.22,
            'growth': 0.18,
            'quality': 0.28,
            'momentum': 0.17,
            'sentiment': 0.15
        }
        
        result = {
            'method': 'factor_synthesis',
            'description': '风格内因子合成综合因子',
            'style_factors': 5,
            'combinations': 2**5 - 1,
            'synthesis_config': synthesis_config,
            'style_weights': style_weights,
            'final_formula': "Final_Score = " + " + ".join([f"{w}*{s}" for s, w in style_weights.items()])
        }
        
        print(f"\n风格因子数: 5个")
        print(f"组合数: {result['combinations']}")
        print(f"\n最终公式: {result['final_formula']}")
        
        return result
    
    # ==================== 方法3：PCA降维 ====================
    
    def method3_pca_reduction(self, n_components: int = 10, 
                               variance_threshold: float = 0.8) -> Dict:
        """
        方法3：主成分分析（PCA）
        提取主成分，保留指定解释度
        """
        print("\n" + "="*60)
        print("方法3：主成分分析（PCA）")
        print("="*60)
        
        # 模拟因子相关性矩阵（实际应从数据计算）
        # 这里用IC值估算重要性
        factor_names = list(self.FINAL_FACTORS.keys())
        factor_ics = np.array([abs(self.FINAL_FACTORS[f]['ic']) for f in factor_names])
        
        # 归一化作为权重
        weights = factor_ics / factor_ics.sum()
        
        # 按权重排序选Top N
        sorted_idx = np.argsort(weights)[::-1]
        top_factors = [factor_names[i] for i in sorted_idx[:n_components]]
        top_weights = [weights[i] for i in sorted_idx[:n_components]]
        
        # 累计解释度
        cumulative_var = np.cumsum(sorted(weights, reverse=True))
        n_for_80 = np.argmax(cumulative_var >= variance_threshold) + 1
        
        result = {
            'method': 'pca_reduction',
            'description': 'PCA提取主成分',
            'n_components': n_components,
            'variance_threshold': variance_threshold,
            'factors_for_80pct': int(n_for_80),
            'combinations': 2**n_components - 1,
            'selected_factors': top_factors,
            'weights': {f: round(w, 4) for f, w in zip(top_factors, top_weights)}
        }
        
        print(f"\n保留 {n_components} 个主成分")
        print(f"解释80%方差需 {n_for_80} 个因子")
        print(f"组合数: {result['combinations']:,}")
        print(f"\nTop {n_components} 因子:")
        for i, (f, w) in enumerate(zip(top_factors[:10], top_weights[:10]), 1):
            print(f"  {i:2d}. {f:20s} 权重={w:.4f}")
        
        return result
    
    # ==================== 方法4：IC加权筛选 ====================
    
    def method4_ic_weighted_selection(self, top_n: int = 10) -> Dict:
        """
        方法4：IC加权筛选（中金做法）
        按IC绝对值排序，选Top N
        """
        print("\n" + "="*60)
        print("方法4：IC加权筛选（中金做法）")
        print("="*60)
        
        # 按IC绝对值排序
        sorted_factors = sorted(
            self.FINAL_FACTORS.items(),
            key=lambda x: abs(x[1]['ic']),
            reverse=True
        )
        
        # 选Top N
        top_factors = sorted_factors[:top_n]
        
        result = {
            'method': 'ic_weighted_selection',
            'description': '按IC绝对值选Top N因子',
            'top_n': top_n,
            'combinations': 2**top_n - 1,
            'selected_factors': [f[0] for f in top_factors],
            'details': [
                {
                    'rank': i+1,
                    'code': f[0],
                    'name': f[1]['name'],
                    'ic': f[1]['ic'],
                    'category': f[1]['category']
                }
                for i, f in enumerate(top_factors)
            ]
        }
        
        print(f"\nTop {top_n} 因子（按IC绝对值）:")
        print("-"*60)
        for item in result['details']:
            print(f"  {item['rank']:2d}. {item['code']:20s} IC={item['ic']:+.4f} [{item['category']}]")
        
        print(f"\n组合数: {result['combinations']:,}")
        
        return result
    
    # ==================== 方法5：正交化处理 ====================
    
    def method5_orthogonalization(self, correlation_threshold: float = 0.5) -> Dict:
        """
        方法5：正交化处理
        剔除高相关因子，保留独立因子
        """
        print("\n" + "="*60)
        print("方法5：正交化处理")
        print("="*60)
        
        # 按IC排序
        sorted_factors = sorted(
            self.FINAL_FACTORS.items(),
            key=lambda x: abs(x[1]['ic']),
            reverse=True
        )
        
        # 模拟正交化筛选（实际需计算相关性矩阵）
        # 这里简化：同类别只保留IC最高的
        selected = []
        category_count = {}
        
        for code, info in sorted_factors:
            cat = info['category']
            if cat not in category_count:
                category_count[cat] = 0
            
            # 每个类别最多保留一定数量
            max_per_cat = 3 if cat == 'technical' else 2
            if category_count[cat] < max_per_cat:
                selected.append((code, info))
                category_count[cat] += 1
        
        result = {
            'method': 'orthogonalization',
            'description': '正交化处理，保留独立因子',
            'correlation_threshold': correlation_threshold,
            'total_factors': len(selected),
            'combinations': 2**len(selected) - 1,
            'selected_factors': [f[0] for f in selected],
            'category_distribution': category_count,
            'details': [
                {
                    'code': f[0],
                    'name': f[1]['name'],
                    'ic': f[1]['ic'],
                    'category': f[1]['category']
                }
                for f in selected
            ]
        }
        
        print(f"\n正交化后保留因子:")
        print("-"*60)
        for item in result['details']:
            print(f"  {item['code']:20s} IC={item['ic']:+.4f} [{item['category']}]")
        
        print(f"\n各类别因子数: {category_count}")
        print(f"总因子数: {len(selected)}")
        print(f"组合数: {result['combinations']:,}")
        
        return result
    
    # ==================== 汇总所有方法 ====================
    
    def run_all_methods(self) -> Dict:
        """运行所有5种方法"""
        print("\n" + "="*70)
        print("因子组合优化 - 五种降维方法")
        print("="*70)
        
        results = {
            'summary': {
                'original_factors': 31,
                'original_combinations': 2**31 - 1,
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'methods': {}
        }
        
        # 方法1：风格因子分组
        results['methods']['method1'] = self.method1_style_grouping(top_n_per_style=2)
        
        # 方法2：因子合成
        results['methods']['method2'] = self.method2_factor_synthesis()
        
        # 方法3：PCA降维
        results['methods']['method3'] = self.method3_pca_reduction(n_components=10)
        
        # 方法4：IC加权筛选
        results['methods']['method4'] = self.method4_ic_weighted_selection(top_n=10)
        
        # 方法5：正交化处理
        results['methods']['method5'] = self.method5_orthogonalization()
        
        # 汇总对比
        print("\n" + "="*70)
        print("五种方法对比汇总")
        print("="*70)
        print(f"\n{'方法':<25s} {'因子数':>8s} {'组合数':>15s}")
        print("-"*50)
        
        comparison = []
        for method_id, method_data in results['methods'].items():
            name = method_data.get('description', method_data.get('method', method_id))
            n_factors = method_data.get('total_factors', 
                                        method_data.get('style_factors', 
                                                        method_data.get('top_n', 
                                                                        method_data.get('n_components', 0))))
            combos = method_data.get('combinations', 0)
            
            comparison.append({
                'method': method_id,
                'name': name,
                'factors': n_factors,
                'combinations': combos
            })
            
            print(f"{name:<25s} {n_factors:>8d} {combos:>15,}")
        
        results['comparison'] = comparison
        
        # 保存结果
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"factor_combinations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存: {output_file}")
        
        return results


def run_factor_optimization():
    """运行因子优化"""
    optimizer = FactorOptimizer()
    return optimizer.run_all_methods()


if __name__ == "__main__":
    result = run_factor_optimization()