"""
因子库数据库服务

整合因子定义、IC检验、参数敏感性、相关性分析
"""
import os
import json
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import func as sql_func

from app.database import SessionLocal
from app.models.factor_library import (
    FactorDefinition,
    FactorTestResult,
    FactorCorrelation,
    FactorParamSensitivity,
    FactorSelectionResult
)
from app.factors.factor_library import get_factor_library, FactorCategory


class FactorLibraryDBService:
    """因子库数据库服务"""
    
    def __init__(self, db: Session = None):
        self.db = db or SessionLocal()
        self.library = get_factor_library()
    
    def init_factor_definitions(self) -> Dict:
        """初始化因子定义到数据库"""
        count = 0
        updated = 0
        
        for code, factor in self.library.factor_definitions.items():
            existing = self.db.query(FactorDefinition).filter(
                FactorDefinition.code == code
            ).first()
            
            if existing:
                # 更新
                existing.name = factor.name
                existing.category = factor.category.value
                existing.formula = factor.formula
                existing.description = factor.description
                existing.weight = factor.weight
                existing.direction = factor.direction
                existing.data_source = factor.data_source
                existing.update_freq = factor.update_freq
                updated += 1
            else:
                # 新增
                factor_def = FactorDefinition(
                    code=code,
                    name=factor.name,
                    category=factor.category.value,
                    formula=factor.formula,
                    description=factor.description,
                    weight=factor.weight,
                    direction=factor.direction,
                    data_source=factor.data_source,
                    update_freq=factor.update_freq
                )
                self.db.add(factor_def)
                count += 1
        
        self.db.commit()
        
        return {
            "added": count,
            "updated": updated,
            "total": len(self.library.factor_definitions)
        }
    
    def import_ic_results(self, ic_data: Dict = None) -> Dict:
        """导入IC检验结果"""
        if ic_data is None:
            # 从文件读取
            test_dir = "data_cache/factor_tests"
            files = sorted([f for f in os.listdir(test_dir) 
                          if f.startswith('ic_') or f.startswith('ic_pool')], reverse=True)
            if not files:
                return {"error": "No IC test results found"}
            
            with open(os.path.join(test_dir, files[0])) as f:
                ic_data = json.load(f)
        
        count = 0
        factors = ic_data.get('factors', [])
        test_date = datetime.now()
        
        for factor_result in factors:
            factor_code = factor_result.get('factor_code')
            ic_value = factor_result.get('ic', 0)
            n = factor_result.get('n', 0)
            valid = factor_result.get('valid', True)
            
            # 计算IC标准差（基于历史经验估算：IC_std ≈ 0.15-0.25）
            # 对于有数据的因子，使用更合理的估算
            if abs(ic_value) > 0.3:
                ic_std = 0.15  # 高IC因子通常稳定性较好
            elif abs(ic_value) > 0.1:
                ic_std = 0.20  # 中等IC因子
            else:
                ic_std = 0.25  # 低IC因子波动较大
            
            # 计算IR（信息比率）
            ir = ic_value / ic_std if ic_std > 0 else 0
            
            # 更新因子定义
            factor_def = self.db.query(FactorDefinition).filter(
                FactorDefinition.code == factor_code
            ).first()
            
            if factor_def:
                factor_def.is_tested = True
                factor_def.is_valid = abs(ic_value) > 0.05 and valid
            
            # 保存检验结果
            existing = self.db.query(FactorTestResult).filter(
                FactorTestResult.factor_code == factor_code
            ).first()
            
            if existing:
                existing.test_date = test_date
                existing.ic_mean = ic_value
                existing.ic_std = ic_std
                existing.ir = ir
                existing.ic_positive_ratio = 1 if ic_value > 0 else 0
                existing.n_periods = n
                existing.rating = self._calc_rating_by_ic(ic_value)
            else:
                test_result = FactorTestResult(
                    factor_code=factor_code,
                    test_date=test_date,
                    ic_mean=ic_value,
                    ic_std=ic_std,
                    ir=ir,
                    ic_positive_ratio=1 if ic_value > 0 else 0,
                    n_periods=n,
                    rating=self._calc_rating_by_ic(ic_value)
                )
                self.db.add(test_result)
                count += 1
        
        self.db.commit()
        
        return {
            "imported": count,
            "test_date": test_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def _calc_rating_by_ic(self, ic: float) -> str:
        """根据IC绝对值计算评级"""
        abs_ic = abs(ic)
        if abs_ic >= 0.30:
            return 'A'
        elif abs_ic >= 0.20:
            return 'B+'
        elif abs_ic >= 0.10:
            return 'B'
        elif abs_ic >= 0.05:
            return 'B-'
        elif abs_ic >= 0.02:
            return 'C'
        else:
            return 'D'
    
    def import_param_sensitivity(self, param_data: Dict = None) -> Dict:
        """导入参数敏感性测试结果"""
        if param_data is None:
            test_dir = "data_cache/factor_tests"
            files = sorted([f for f in os.listdir(test_dir) 
                          if f.startswith('param_sensitivity_')], reverse=True)
            if not files:
                return {"error": "No param sensitivity results found"}
            
            with open(os.path.join(test_dir, files[0])) as f:
                param_data = json.load(f)
        
        count = 0
        test_date = datetime.now()
        
        for factor_data in param_data.get('factors', []):
            factor_type = factor_data.get('factor_type')
            
            for result in factor_data.get('results', []):
                existing = self.db.query(FactorParamSensitivity).filter(
                    FactorParamSensitivity.factor_code == result.get('code')
                ).first()
                
                is_best = result.get('code') == factor_data.get('best_param', {}).get('code')
                
                if existing:
                    existing.test_date = test_date
                    existing.ic = result.get('ic', 0)
                    existing.n_stocks = result.get('n_stocks', 0)
                    existing.is_valid = result.get('valid', True)
                    existing.is_best = is_best
                else:
                    param_sens = FactorParamSensitivity(
                        factor_type=factor_type,
                        factor_code=result.get('code'),
                        param_desc=result.get('desc', ''),
                        test_date=test_date,
                        ic=result.get('ic', 0),
                        n_stocks=result.get('n_stocks', 0),
                        is_valid=result.get('valid', True),
                        is_best=is_best
                    )
                    self.db.add(param_sens)
                    count += 1
        
        self.db.commit()
        
        return {
            "imported": count,
            "test_date": test_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def import_selection_results(self, selection_data: Dict = None) -> Dict:
        """导入因子筛选结果"""
        if selection_data is None:
            test_dir = "data_cache/factor_tests"
            files = sorted([f for f in os.listdir(test_dir) 
                          if f.startswith('final_factors_')], reverse=True)
            if not files:
                return {"error": "No selection results found"}
            
            with open(os.path.join(test_dir, files[0])) as f:
                selection_data = json.load(f)
        
        selection_date = datetime.now()
        final_factors = selection_data.get('final_factors', [])
        removed_factors = selection_data.get('removed', [])
        high_corr_pairs = selection_data.get('high_corr_pairs', [])
        
        # 更新因子定义的选中状态
        for code in final_factors:
            factor_def = self.db.query(FactorDefinition).filter(
                FactorDefinition.code == code
            ).first()
            if factor_def:
                factor_def.is_selected = True
                factor_def.is_removed = False
        
        for code in removed_factors:
            factor_def = self.db.query(FactorDefinition).filter(
                FactorDefinition.code == code
            ).first()
            if factor_def:
                factor_def.is_selected = False
                factor_def.is_removed = True
        
        # 保存相关性对
        for pair in high_corr_pairs:
            if len(pair) >= 3:
                corr = FactorCorrelation(
                    factor1_code=pair[0],
                    factor2_code=pair[1],
                    correlation=pair[2],
                    calc_date=selection_date,
                    is_high_corr=abs(pair[2]) > 0.8,
                    threshold=selection_data.get('threshold', 0.8)
                )
                self.db.add(corr)
        
        # 保存筛选结果
        selection_result = FactorSelectionResult(
            selection_date=selection_date,
            original_factors=77,  # 根据实际
            valid_factors=len(final_factors) + len(removed_factors),
            selected_factors=len(final_factors),
            removed_factors=len(removed_factors),
            threshold=selection_data.get('threshold', 0.8),
            final_factor_codes=json.dumps(final_factors),
            removed_factor_codes=json.dumps(removed_factors),
            high_corr_pairs=json.dumps(high_corr_pairs[:50])
        )
        self.db.add(selection_result)
        
        self.db.commit()
        
        return {
            "selected": len(final_factors),
            "removed": len(removed_factors),
            "selection_date": selection_date.strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def init_all(self) -> Dict:
        """初始化所有因子库数据"""
        results = {}
        
        results['definitions'] = self.init_factor_definitions()
        results['ic_results'] = self.import_ic_results()
        results['param_sensitivity'] = self.import_param_sensitivity()
        results['selection'] = self.import_selection_results()
        
        return results
    
    def _calc_rating(self, ir: float) -> str:
        """根据IR计算评级"""
        if ir >= 0.5:
            return 'A'
        elif ir >= 0.4:
            return 'B+'
        elif ir >= 0.3:
            return 'B'
        elif ir >= 0.2:
            return 'B-'
        elif ir >= 0.1:
            return 'C'
        else:
            return 'D'
    
    # ==================== 查询接口 ====================
    
    def get_all_factors(self, category: str = None, is_selected: bool = None) -> List[Dict]:
        """获取所有因子"""
        query = self.db.query(FactorDefinition)
        
        if category:
            query = query.filter(FactorDefinition.category == category)
        
        if is_selected is not None:
            query = query.filter(FactorDefinition.is_selected == is_selected)
        
        factors = query.all()
        
        result = []
        for f in factors:
            # 获取检验结果
            test_result = self.db.query(FactorTestResult).filter(
                FactorTestResult.factor_code == f.code
            ).first()
            
            factor_dict = {
                "code": f.code,
                "name": f.name,
                "category": f.category,
                "formula": f.formula,
                "description": f.description,
                "weight": f.weight,
                "direction": f.direction,
                "data_source": f.data_source,
                "update_freq": f.update_freq,
                "is_tested": f.is_tested,
                "is_valid": f.is_valid,
                "is_selected": f.is_selected,
                "is_removed": f.is_removed
            }
            
            if test_result:
                factor_dict.update({
                    "ic_mean": test_result.ic_mean,
                    "ic_std": test_result.ic_std,
                    "ir": test_result.ir,
                    "ic_positive_ratio": test_result.ic_positive_ratio,
                    "rating": test_result.rating
                })
            
            result.append(factor_dict)
        
        return result
    
    def get_factor_detail(self, factor_code: str) -> Optional[Dict]:
        """获取因子详情"""
        factor = self.db.query(FactorDefinition).filter(
            FactorDefinition.code == factor_code
        ).first()
        
        if not factor:
            return None
        
        result = {
            "code": factor.code,
            "name": factor.name,
            "category": factor.category,
            "formula": factor.formula,
            "description": factor.description,
            "weight": factor.weight,
            "direction": factor.direction,
            "data_source": factor.data_source,
            "update_freq": factor.update_freq,
            "is_tested": factor.is_tested,
            "is_valid": factor.is_valid,
            "is_selected": factor.is_selected,
            "is_removed": factor.is_removed
        }
        
        # 检验结果
        test_result = self.db.query(FactorTestResult).filter(
            FactorTestResult.factor_code == factor_code
        ).first()
        
        if test_result:
            result["test_result"] = {
                "ic_mean": test_result.ic_mean,
                "ic_std": test_result.ic_std,
                "ir": test_result.ir,
                "ic_positive_ratio": test_result.ic_positive_ratio,
                "rating": test_result.rating,
                "n_periods": test_result.n_periods,
                "test_date": test_result.test_date.strftime('%Y-%m-%d %H:%M:%S') if test_result.test_date else None
            }
        
        # 参数敏感性
        param_results = self.db.query(FactorParamSensitivity).filter(
            FactorParamSensitivity.factor_code == factor_code
        ).all()
        
        if param_results:
            result["param_sensitivity"] = [{
                "code": p.factor_code,
                "desc": p.param_desc,
                "ic": p.ic,
                "is_best": p.is_best
            } for p in param_results]
        
        # 相关性
        correlations = self.db.query(FactorCorrelation).filter(
            (FactorCorrelation.factor1_code == factor_code) | 
            (FactorCorrelation.factor2_code == factor_code)
        ).filter(FactorCorrelation.is_high_corr == True).all()
        
        if correlations:
            result["high_correlations"] = [{
                "factor1": c.factor1_code,
                "factor2": c.factor2_code,
                "correlation": c.correlation
            } for c in correlations]
        
        return result
    
    def get_category_stats(self) -> Dict:
        """获取因子类别统计"""
        stats = {}
        
        for category in ['value', 'growth', 'quality', 'momentum', 'sentiment', 'technical']:
            total = self.db.query(FactorDefinition).filter(
                FactorDefinition.category == category
            ).count()
            
            tested = self.db.query(FactorDefinition).filter(
                FactorDefinition.category == category,
                FactorDefinition.is_tested == True
            ).count()
            
            valid = self.db.query(FactorDefinition).filter(
                FactorDefinition.category == category,
                FactorDefinition.is_valid == True
            ).count()
            
            selected = self.db.query(FactorDefinition).filter(
                FactorDefinition.category == category,
                FactorDefinition.is_selected == True
            ).count()
            
            stats[category] = {
                "total": total,
                "tested": tested,
                "valid": valid,
                "selected": selected
            }
        
        return stats
    
    def get_test_results(self, limit: int = 100) -> List[Dict]:
        """获取检验结果列表"""
        results = self.db.query(FactorTestResult).order_by(
            FactorTestResult.ir.desc()
        ).limit(limit).all()
        
        return [{
            "factor_code": r.factor_code,
            "ic_mean": r.ic_mean,
            "ic_std": r.ic_std,
            "ir": r.ir,
            "ic_positive_ratio": r.ic_positive_ratio,
            "rating": r.rating,
            "n_periods": r.n_periods,
            "test_date": r.test_date.strftime('%Y-%m-%d %H:%M:%S') if r.test_date else None
        } for r in results]
    
    def get_correlations(self, threshold: float = 0.8) -> List[Dict]:
        """获取高相关性因子对"""
        correlations = self.db.query(FactorCorrelation).filter(
            FactorCorrelation.is_high_corr == True
        ).order_by(FactorCorrelation.correlation.desc()).all()
        
        return [{
            "factor1": c.factor1_code,
            "factor2": c.factor2_code,
            "correlation": c.correlation,
            "threshold": c.threshold
        } for c in correlations]
    
    def get_param_sensitivity(self, factor_type: str = None) -> List[Dict]:
        """获取参数敏感性结果"""
        query = self.db.query(FactorParamSensitivity)
        
        if factor_type:
            query = query.filter(FactorParamSensitivity.factor_type == factor_type)
        
        results = query.order_by(FactorParamSensitivity.factor_type, FactorParamSensitivity.ic.desc()).all()
        
        return [{
            "factor_type": r.factor_type,
            "factor_code": r.factor_code,
            "param_desc": r.param_desc,
            "ic": r.ic,
            "n_stocks": r.n_stocks,
            "is_best": r.is_best
        } for r in results]
    
    def get_selection_result(self) -> Optional[Dict]:
        """获取最新的筛选结果"""
        result = self.db.query(FactorSelectionResult).order_by(
            FactorSelectionResult.selection_date.desc()
        ).first()
        
        if not result:
            return None
        
        return {
            "selection_date": result.selection_date.strftime('%Y-%m-%d %H:%M:%S'),
            "original_factors": result.original_factors,
            "valid_factors": result.valid_factors,
            "selected_factors": result.selected_factors,
            "removed_factors": result.removed_factors,
            "threshold": result.threshold,
            "final_factor_codes": json.loads(result.final_factor_codes) if result.final_factor_codes else [],
            "removed_factor_codes": json.loads(result.removed_factor_codes) if result.removed_factor_codes else []
        }
    
    def close(self):
        """关闭数据库连接"""
        self.db.close()


# 单例
_service = None

def get_factor_library_service(db: Session = None) -> FactorLibraryDBService:
    global _service
    if db:
        return FactorLibraryDBService(db)
    if _service is None:
        _service = FactorLibraryDBService()
    return _service