"""
Risk Debater Agent - 风险辩论智能体

职责：
1. 多空观点辩论
2. 风险收益权衡
3. 生成最终风险判断

参考 TradingAgents 的辩论机制
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
import json

from app.agents.states import RiskDebateState


class BullResearcher:
    """多头研究员"""
    
    NAME = "BullResearcher"
    
    def __init__(self, llm_client=None):
        self.llm_client = llm_client
    
    def analyze(self, factor_analysis: Dict, backtest_results: List[Dict]) -> str:
        """多头分析"""
        # 基于因子和回测结果生成看多观点
        points = []
        
        # 因子角度
        composite_score = factor_analysis.get('composite_score', 0)
        if composite_score >= 60:
            points.append(f"综合因子得分{composite_score:.1f}分，基本面表现良好")
        
        grade = factor_analysis.get('grade', 'C')
        if grade in ['A', 'B+', 'B']:
            points.append(f"投资评级{grade}，具备投资价值")
        
        # 回测角度
        for result in backtest_results:
            if result.get('excess_return', 0) > 0:
                points.append(f"{result['strategy_name']}策略超额收益{result['excess_return']*100:.1f}%，跑赢基准")
        
        if not points:
            points.append("当前市场估值偏低，具备安全边际")
        
        return f"""
【多头观点】
{' '.join([f'• {p}' for p in points])}

建议：买入/加仓
"""
    
    def debate(self, bear_point: str, context: Dict) -> str:
        """反驳空头观点"""
        return f"""
【多头反驳】
针对空头观点"{bear_point[:50]}..."，我认为：

1. 短期波动不代表长期趋势
2. 当前估值已充分反映悲观预期
3. 下跌空间有限，上涨概率更大

维持买入建议。
"""


class BearResearcher:
    """空头研究员"""
    
    NAME = "BearResearcher"
    
    def __init__(self, llm_client=None):
        self.llm_client = llm_client
    
    def analyze(self, factor_analysis: Dict, backtest_results: List[Dict]) -> str:
        """空头分析"""
        points = []
        
        # 因子角度
        composite_score = factor_analysis.get('composite_score', 0)
        if composite_score < 50:
            points.append(f"综合因子得分仅{composite_score:.1f}分，基本面偏弱")
        
        grade = factor_analysis.get('grade', 'C')
        if grade in ['D', 'C']:
            points.append(f"投资评级{grade}，投资价值存疑")
        
        # 回测角度
        for result in backtest_results:
            if result.get('max_drawdown', 0) < -0.3:
                points.append(f"{result['strategy_name']}策略最大回撤{result['max_drawdown']*100:.1f}%，风险较大")
        
        if not points:
            points.append("市场环境不确定性增加，建议谨慎")
        
        return f"""
【空头观点】
{' '.join([f'• {p}' for p in points])}

建议：观望/减仓
"""
    
    def debate(self, bull_point: str, context: Dict) -> str:
        """反驳多头观点"""
        return f"""
【空头反驳】
针对多头观点"{bull_point[:50]}..."，我认为：

1. 市场情绪脆弱，利好消化快
2. 宏观环境仍存不确定性
3. 上行空间有限，下行风险未释放

维持谨慎建议。
"""


class RiskJudge:
    """风险裁判"""
    
    NAME = "RiskJudge"
    
    def __init__(self, llm_client=None):
        self.llm_client = llm_client
    
    def decide(self, bull_history: str, bear_history: str, context: Dict) -> Dict:
        """做出最终判断"""
        # 简单规则：基于因子得分
        score = context.get('composite_score', 50)
        
        if score >= 70:
            decision = "BUY"
            bias = "bullish"
            confidence = 0.8
        elif score >= 50:
            decision = "HOLD"
            bias = "neutral"
            confidence = 0.5
        else:
            decision = "SELL"
            bias = "bearish"
            confidence = 0.7
        
        return {
            'decision': decision,
            'bias': bias,
            'confidence': confidence,
            'reasoning': f"基于综合因子得分{score:.1f}分，建议{decision}"
        }


class RiskDebaterAgent:
    """风险辩论智能体"""
    
    AGENT_NAME = "RiskDebater"
    
    def __init__(self, llm_client=None, config: Dict = None):
        self.llm_client = llm_client
        self.config = config or {}
        
        self.bull = BullResearcher(llm_client)
        self.bear = BearResearcher(llm_client)
        self.judge = RiskJudge(llm_client)
        
        self.max_rounds = config.get('max_debate_rounds', 2) if config else 2
    
    def debate(self, factor_analysis: Dict, backtest_results: List[Dict]) -> RiskDebateState:
        """
        执行风险辩论
        
        Args:
            factor_analysis: 因子分析结果
            backtest_results: 回测结果列表
        
        Returns:
            辩论状态
        """
        # 1. 初始观点
        bull_view = self.bull.analyze(factor_analysis, backtest_results)
        bear_view = self.bear.analyze(factor_analysis, backtest_results)
        
        bull_history = [bull_view]
        bear_history = [bear_view]
        debate_history = [f"多头: {bull_view}", f"空头: {bear_view}"]
        
        # 2. 辩论轮次
        context = {
            'composite_score': factor_analysis.get('composite_score', 50),
            'grade': factor_analysis.get('grade', 'C'),
        }
        
        for round_num in range(self.max_rounds):
            # 空头反驳多头
            bear_response = self.bear.debate(bull_history[-1], context)
            bear_history.append(bear_response)
            debate_history.append(f"空头反驳: {bear_response}")
            
            # 多头反驳空头
            bull_response = self.bull.debate(bear_history[-1], context)
            bull_history.append(bull_response)
            debate_history.append(f"多头反驳: {bull_response}")
        
        # 3. 裁判判断
        final_decision = self.judge.decide(
            '\n'.join(bull_history),
            '\n'.join(bear_history),
            context
        )
        
        return RiskDebateState(
            bull_history='\n'.join(bull_history),
            bear_history='\n'.join(bear_history),
            bull_score=final_decision.get('confidence', 0.5) if final_decision['bias'] == 'bullish' else 0.3,
            bear_score=final_decision.get('confidence', 0.5) if final_decision['bias'] == 'bearish' else 0.3,
            history='\n'.join(debate_history),
            count=self.max_rounds,
            judge_decision=final_decision['decision'],
            final_bias=final_decision['bias']
        )
    
    def generate_report(self, debate_state: RiskDebateState) -> str:
        """生成辩论报告"""
        return f"""
## 风险辩论报告

### 多头核心观点
{debate_state['bull_history'][:500]}...

### 空头核心观点
{debate_state['bear_history'][:500]}...

### 最终判断
- **决策**: {debate_state['judge_decision']}
- **倾向**: {debate_state['final_bias']}
- **辩论轮次**: {debate_state['count']}
"""


# 导出
__all__ = ['RiskDebaterAgent', 'BullResearcher', 'BearResearcher', 'RiskJudge']