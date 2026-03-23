"""
Portfolio Optimizer Agent - 组合优化智能体

职责：
1. 股票权重分配
2. 风险预算分配
3. 组合风险控制
"""
from typing import Dict, Any, List, Optional
import numpy as np
from datetime import datetime

from app.agents.states import PortfolioState


class PortfolioOptimizerAgent:
    """组合优化智能体"""
    
    AGENT_NAME = "PortfolioOptimizer"
    
    def __init__(self, llm_client=None, config: Dict = None):
        self.llm_client = llm_client
        self.config = config or {}
        
        # 设置默认值
        if 'max_position' not in self.config:
            self.config['max_position'] = 0.15
        if 'max_sector_weight' not in self.config:
            self.config['max_sector_weight'] = 0.30
        if 'target_sharpe' not in self.config:
            self.config['target_sharpe'] = 1.5
        if 'max_drawdown' not in self.config:
            self.config['max_drawdown'] = -0.15
    
    def optimize(self, stock_scores: List[Dict], 
                 risk_preferences: Dict = None) -> PortfolioState:
        """
        优化投资组合
        
        Args:
            stock_scores: 股票评分列表 [{'code': '000001', 'score': 75, 'industry': '银行'}, ...]
            risk_preferences: 风险偏好设置
        
        Returns:
            组合状态
        """
        if not stock_scores:
            return self._empty_portfolio()
        
        # 1. 过滤低分股票
        qualified = [s for s in stock_scores if s.get('score', 0) >= 50]
        
        if not qualified:
            qualified = stock_scores[:10]  # 取前10只
        
        # 2. 计算初始权重（等权或按得分加权）
        weights = self._calculate_weights(qualified)
        
        # 3. 行业约束检查
        weights = self._apply_sector_constraints(qualified, weights)
        
        # 4. 单只股票仓位限制
        weights = self._apply_position_constraints(weights)
        
        # 5. 归一化
        total_weight = sum(weights.values())
        if total_weight > 0:
            weights = {k: v/total_weight for k, v in weights.items()}
        
        # 6. 计算组合指标
        avg_score = np.mean([s['score'] for s in qualified if s['code'] in weights])
        
        # 7. 生成报告
        report = self._generate_report(qualified, weights)
        
        return PortfolioState(
            stocks=list(weights.keys()),
            weights=weights,
            expected_return=avg_score * 0.15 / 100,  # 简化估算
            expected_risk=0.20,  # 假设20%波动率
            sharpe_ratio=avg_score / 100 * 0.75,
            max_position=self.config['max_position'],
            max_sector_weight=self.config['max_sector_weight'],
            portfolio_report=report
        )
    
    def _calculate_weights(self, stocks: List[Dict]) -> Dict[str, float]:
        """计算权重（按得分加权）"""
        total_score = sum(s.get('score', 50) for s in stocks)
        
        if total_score == 0:
            # 等权
            n = len(stocks)
            return {s['code']: 1/n for s in stocks}
        
        # 按得分加权
        return {
            s['code']: s.get('score', 50) / total_score
            for s in stocks
        }
    
    def _apply_sector_constraints(self, stocks: List[Dict], 
                                   weights: Dict[str, float]) -> Dict[str, float]:
        """应用行业约束"""
        # 按行业聚合
        sector_weights = {}
        for s in stocks:
            code = s['code']
            if code not in weights:
                continue
            sector = s.get('industry', '其他')
            if sector not in sector_weights:
                sector_weights[sector] = 0
            sector_weights[sector] += weights[code]
        
        # 检查是否超标
        max_sector = self.config['max_sector_weight']
        adjusted_weights = weights.copy()
        
        for sector, sw in sector_weights.items():
            if sw > max_sector:
                # 按比例削减该行业股票权重
                ratio = max_sector / sw
                for s in stocks:
                    if s.get('industry') == sector and s['code'] in adjusted_weights:
                        adjusted_weights[s['code']] *= ratio
        
        return adjusted_weights
    
    def _apply_position_constraints(self, weights: Dict[str, float]) -> Dict[str, float]:
        """应用单只股票仓位限制"""
        max_pos = self.config['max_position']
        
        adjusted = {}
        for code, weight in weights.items():
            adjusted[code] = min(weight, max_pos)
        
        return adjusted
    
    def _empty_portfolio(self) -> PortfolioState:
        """空组合"""
        return PortfolioState(
            stocks=[],
            weights={},
            expected_return=0,
            expected_risk=0,
            sharpe_ratio=0,
            max_position=self.config['max_position'],
            max_sector_weight=self.config['max_sector_weight'],
            portfolio_report="暂无可投资股票"
        )
    
    def _generate_report(self, stocks: List[Dict], weights: Dict[str, float]) -> str:
        """生成组合报告"""
        report = f"""
## 投资组合报告

**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}
**股票数量**: {len(weights)}

### 权重分配

| 股票代码 | 得分 | 权重 | 行业 |
|----------|------|------|------|
"""
        
        # 按权重排序
        sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
        
        for code, weight in sorted_weights[:15]:
            stock = next((s for s in stocks if s['code'] == code), {})
            score = stock.get('score', 0)
            industry = stock.get('industry', '未知')
            report += f"| {code} | {score:.1f} | {weight*100:.2f}% | {industry} |\n"
        
        # 行业分布
        sector_dist = {}
        for s in stocks:
            if s['code'] in weights:
                sector = s.get('industry', '其他')
                if sector not in sector_dist:
                    sector_dist[sector] = 0
                sector_dist[sector] += weights[s['code']]
        
        report += "\n### 行业分布\n\n"
        for sector, weight in sorted(sector_dist.items(), key=lambda x: x[1], reverse=True):
            report += f"- {sector}: {weight*100:.1f}%\n"
        
        return report
    
    def rebalance(self, current_portfolio: PortfolioState, 
                  new_scores: List[Dict]) -> Dict:
        """
        组合再平衡
        
        Args:
            current_portfolio: 当前组合
            new_scores: 新的评分列表
        
        Returns:
            调仓建议
        """
        new_portfolio = self.optimize(new_scores)
        
        trades = []
        
        # 卖出
        for code in current_portfolio['stocks']:
            if code not in new_portfolio['weights']:
                trades.append({
                    'action': 'sell',
                    'code': code,
                    'weight': current_portfolio['weights'].get(code, 0)
                })
        
        # 买入/调整
        for code, weight in new_portfolio['weights'].items():
            old_weight = current_portfolio['weights'].get(code, 0)
            if old_weight == 0:
                trades.append({
                    'action': 'buy',
                    'code': code,
                    'weight': weight
                })
            elif abs(weight - old_weight) > 0.02:
                trades.append({
                    'action': 'adjust',
                    'code': code,
                    'old_weight': old_weight,
                    'new_weight': weight
                })
        
        return {
            'trades': trades,
            'new_portfolio': new_portfolio
        }


# 导出
__all__ = ['PortfolioOptimizerAgent']