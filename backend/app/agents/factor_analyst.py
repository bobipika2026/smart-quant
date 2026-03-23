"""
Factor Analyst Agent - 因子分析师智能体

职责：
1. 计算多因子得分
2. 生成因子分析报告
3. 识别关键驱动因子
"""
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime

from app.services.stock_scoring_v3 import get_scoring_v3
from app.services.data import DataService
from app.services.market_timing import get_stock_industry


class FactorAnalystAgent:
    """因子分析师智能体"""
    
    AGENT_NAME = "FactorAnalyst"
    
    # 因子类别权重
    FACTOR_WEIGHTS = {
        'value': 0.22,
        'growth': 0.18,
        'quality': 0.28,
        'momentum': 0.17,
        'sentiment': 0.15,
    }
    
    def __init__(self, llm_client=None, config: Dict = None):
        self.llm_client = llm_client
        self.config = config or {}
        self.scoring_service = get_scoring_v3()
    
    def analyze(self, stock_code: str, trade_date: str = None) -> Dict:
        """
        分析股票因子
        
        Args:
            stock_code: 股票代码
            trade_date: 交易日期（可选）
        
        Returns:
            因子分析结果
        """
        # 1. 获取因子数据
        factor_data = self._get_factor_data(stock_code)
        
        if not factor_data:
            return {
                'status': 'error',
                'message': f'无法获取 {stock_code} 的因子数据'
            }
        
        # 2. 计算各类因子得分
        factor_scores = self._calculate_factor_scores(factor_data)
        
        # 3. 计算综合得分
        composite_score = self._calculate_composite_score(factor_scores)
        
        # 4. 生成评级
        grade = self._get_grade(composite_score)
        
        # 5. 生成报告
        report = self._generate_report(stock_code, factor_scores, composite_score, grade)
        
        return {
            'stock_code': stock_code,
            'trade_date': trade_date or datetime.now().strftime('%Y-%m-%d'),
            'value_factors': factor_scores.get('value', {}),
            'growth_factors': factor_scores.get('growth', {}),
            'quality_factors': factor_scores.get('quality', {}),
            'momentum_factors': factor_scores.get('momentum', {}),
            'sentiment_factors': factor_scores.get('sentiment', {}),
            'composite_score': composite_score,
            'grade': grade,
            'factor_report': report,
        }
    
    def _get_factor_data(self, stock_code: str) -> Dict:
        """获取因子数据"""
        try:
            # 使用v3评分服务 - 生成股票池获取评分
            result = self.scoring_service.generate_stock_pool(top_n=500)
            # 查找该股票的评分
            for stock in result.get('stocks', []):
                if stock.get('stock_code', '').startswith(stock_code):
                    return stock
            return {'stock_code': stock_code, 'composite_score': 50, 'grade': 'B'}
        except Exception as e:
            print(f"[{self.AGENT_NAME}] 获取因子数据失败: {e}")
            # 返回默认值
            return {'stock_code': stock_code, 'composite_score': 50, 'grade': 'B'}
    
    def _calculate_factor_scores(self, factor_data: Dict) -> Dict:
        """计算各类因子得分"""
        scores = {
            'value': {},
            'growth': {},
            'quality': {},
            'momentum': {},
            'sentiment': {},
        }
        
        # 从因子数据中提取
        if 'factor_scores' in factor_data:
            for category, factors in factor_data['factor_scores'].items():
                if category in scores:
                    scores[category] = factors
        
        return scores
    
    def _calculate_composite_score(self, factor_scores: Dict) -> float:
        """计算综合得分"""
        total_score = 0.0
        total_weight = 0.0
        
        for category, weight in self.FACTOR_WEIGHTS.items():
            category_scores = factor_scores.get(category, {})
            if category_scores:
                avg_score = np.mean(list(category_scores.values())) if category_scores else 0
                total_score += avg_score * weight
                total_weight += weight
        
        return total_score / total_weight if total_weight > 0 else 0
    
    def _get_grade(self, composite_score: float) -> str:
        """根据综合得分生成评级"""
        if composite_score >= 80:
            return 'A'
        elif composite_score >= 70:
            return 'B+'
        elif composite_score >= 60:
            return 'B'
        elif composite_score >= 50:
            return 'B-'
        elif composite_score >= 40:
            return 'C'
        else:
            return 'D'
    
    def _generate_report(self, stock_code: str, factor_scores: Dict, 
                         composite_score: float, grade: str) -> str:
        """生成因子分析报告"""
        industry = get_stock_industry(stock_code)
        
        report = f"""
## 因子分析报告 - {stock_code}

**分析日期**: {datetime.now().strftime('%Y-%m-%d')}
**所属行业**: {industry}
**综合评分**: {composite_score:.2f}
**投资评级**: {grade}

### 因子得分明细

| 因子类别 | 得分 | 权重 |
|----------|------|------|
"""
        
        for category, weight in self.FACTOR_WEIGHTS.items():
            scores = factor_scores.get(category, {})
            avg_score = np.mean(list(scores.values())) if scores else 0
            report += f"| {category} | {avg_score:.2f} | {weight*100:.0f}% |\n"
        
        report += f"""
### 关键发现

"""
        
        # 找出最强和最弱因子
        all_factors = []
        for category, scores in factor_scores.items():
            for factor, score in scores.items():
                all_factors.append((factor, score, category))
        
        if all_factors:
            sorted_factors = sorted(all_factors, key=lambda x: x[1], reverse=True)
            top_3 = sorted_factors[:3]
            bottom_3 = sorted_factors[-3:]
            
            report += "**优势因子**:\n"
            for factor, score, category in top_3:
                report += f"- {factor} ({category}): {score:.2f}\n"
            
            report += "\n**劣势因子**:\n"
            for factor, score, category in bottom_3:
                report += f"- {factor} ({category}): {score:.2f}\n"
        
        return report
    
    def analyze_batch(self, stock_codes: list) -> list:
        """批量分析股票"""
        results = []
        for code in stock_codes:
            result = self.analyze(code)
            results.append(result)
        return results


# 导出
__all__ = ['FactorAnalystAgent']