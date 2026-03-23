"""
Smart Quant Graph - 多智能体协作图

整合所有Agent，实现完整的量化分析流程
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
import json

from app.agents.states import AgentState, FactorAnalysisState, RiskDebateState
from app.agents.factor_analyst import FactorAnalystAgent
from app.agents.backtester import StrategyBacktesterAgent
from app.agents.risk_debater import RiskDebaterAgent
from app.agents.portfolio_optimizer import PortfolioOptimizerAgent


class SmartQuantGraph:
    """
    Smart Quant 多智能体协作图
    
    流程：
    1. FactorAnalyst -> 因子分析
    2. StrategyBacktester -> 策略回测
    3. RiskDebater -> 风险辩论
    4. PortfolioOptimizer -> 组合优化
    5. FinalDecision -> 最终决策
    """
    
    def __init__(self, llm_client=None, config: Dict = None):
        self.llm_client = llm_client
        self.config = config or self._default_config()
        
        # 初始化各Agent
        self.factor_analyst = FactorAnalystAgent(llm_client, self.config)
        self.backtester = StrategyBacktesterAgent(llm_client, self.config)
        self.risk_debater = RiskDebaterAgent(llm_client, self.config)
        self.portfolio_optimizer = PortfolioOptimizerAgent(llm_client, self.config)
        
        # 状态追踪
        self.current_state = None
        self.history = []
    
    def _default_config(self) -> Dict:
        """默认配置"""
        return {
            'max_debate_rounds': 2,
            'strategies': ['ma_cross', 'macd', 'factor_score'],
            'risk_free_rate': 0.03,
            'max_position': 0.15,
            'debug': False,
        }
    
    def propagate(self, stock_code: str, trade_date: str = None) -> tuple:
        """
        执行完整分析流程
        
        Args:
            stock_code: 股票代码
            trade_date: 交易日期
        
        Returns:
            (final_state, decision)
        """
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y-%m-%d')
        
        if self.config.get('debug'):
            print(f"\n{'='*60}")
            print(f"Smart Quant Analysis - {stock_code}")
            print(f"Trade Date: {trade_date}")
            print(f"{'='*60}\n")
        
        # 1. 因子分析
        if self.config.get('debug'):
            print("[1/4] 因子分析中...")
        
        factor_analysis = self.factor_analyst.analyze(stock_code, trade_date)
        
        if self.config.get('debug'):
            print(f"  综合得分: {factor_analysis.get('composite_score', 0):.1f}")
            print(f"  评级: {factor_analysis.get('grade', 'N/A')}")
        
        # 2. 策略回测
        if self.config.get('debug'):
            print("\n[2/4] 策略回测中...")
        
        backtest_results = self.backtester.backtest_multi_strategies(
            stock_code, 
            self.config.get('strategies', ['factor_score'])
        )
        
        if self.config.get('debug'):
            best = backtest_results[0] if backtest_results else {}
            print(f"  最佳策略: {best.get('strategy_name', 'N/A')}")
            print(f"  年化收益: {best.get('annual_return', 0)*100:.2f}%")
        
        # 3. 风险辩论
        if self.config.get('debug'):
            print("\n[3/4] 风险辩论中...")
        
        risk_debate = self.risk_debater.debate(factor_analysis, backtest_results)
        
        if self.config.get('debug'):
            print(f"  最终判断: {risk_debate.get('judge_decision', 'HOLD')}")
            print(f"  倾向: {risk_debate.get('final_bias', 'neutral')}")
        
        # 4. 组合优化
        if self.config.get('debug'):
            print("\n[4/4] 组合优化中...")
        
        portfolio = self.portfolio_optimizer.optimize([
            {'code': stock_code, 'score': factor_analysis.get('composite_score', 50), 'industry': 'default'}
        ])
        
        # 5. 生成最终状态
        final_state = AgentState(
            stock_code=stock_code,
            stock_name=factor_analysis.get('stock_name', stock_code),
            trade_date=trade_date,
            sender='SmartQuantGraph',
            factor_analysis=factor_analysis,
            backtest_results=backtest_results,
            risk_debate=risk_debate,
            portfolio=portfolio,
            market_regime='sideways',  # 可扩展
            market_timing_score=50,
            final_decision=risk_debate.get('judge_decision', 'HOLD'),
            action=risk_debate.get('judge_decision', 'HOLD').lower(),
            position_size=portfolio.get('weights', {}).get(stock_code, 0),
            confidence=self._calculate_confidence(factor_analysis, backtest_results)
        )
        
        self.current_state = final_state
        self.history.append({
            'date': trade_date,
            'stock': stock_code,
            'decision': final_state['final_decision']
        })
        
        # 生成决策摘要
        decision = self._generate_decision(final_state)
        
        if self.config.get('debug'):
            print(f"\n{'='*60}")
            print(f"最终决策: {final_state['final_decision']}")
            print(f"建议仓位: {final_state['position_size']*100:.1f}%")
            print(f"信心度: {final_state['confidence']:.2f}")
            print(f"{'='*60}\n")
        
        return final_state, decision
    
    def _calculate_confidence(self, factor_analysis: Dict, backtest_results: List) -> float:
        """计算信心度"""
        score = factor_analysis.get('composite_score', 50)
        
        # 因子得分贡献
        factor_conf = min(score / 100, 1.0) * 0.5
        
        # 回测结果贡献
        if backtest_results:
            best_sharpe = max(r.get('sharpe_ratio', 0) for r in backtest_results)
            backtest_conf = min(best_sharpe / 2, 0.5)  # 最大0.5
        else:
            backtest_conf = 0.2
        
        return min(factor_conf + backtest_conf, 1.0)
    
    def _generate_decision(self, state: AgentState) -> Dict:
        """生成决策摘要"""
        return {
            'stock_code': state['stock_code'],
            'trade_date': state['trade_date'],
            'decision': state['final_decision'],
            'action': state['action'],
            'position_size': state['position_size'],
            'confidence': state['confidence'],
            'composite_score': state['factor_analysis'].get('composite_score', 0),
            'grade': state['factor_analysis'].get('grade', 'N/A'),
            'risk_bias': state['risk_debate'].get('final_bias', 'neutral'),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    
    def analyze_batch(self, stock_codes: List[str], trade_date: str = None) -> List[Dict]:
        """批量分析"""
        results = []
        for code in stock_codes:
            try:
                _, decision = self.propagate(code, trade_date)
                results.append(decision)
            except Exception as e:
                results.append({
                    'stock_code': code,
                    'error': str(e)
                })
        return results
    
    def generate_full_report(self) -> str:
        """生成完整报告"""
        if not self.current_state:
            return "暂无分析结果"
        
        state = self.current_state
        
        report = f"""
# Smart Quant 分析报告

**股票代码**: {state['stock_code']}
**分析日期**: {state['trade_date']}
**最终决策**: **{state['final_decision']}**
**信心度**: {state['confidence']:.2f}

---

## 1. 因子分析

{state['factor_analysis'].get('factor_report', '暂无')}

---

## 2. 策略回测

"""
        
        for result in state['backtest_results'][:3]:
            report += f"""
### {result.get('strategy_name', '未知策略')}
- 年化收益: {result.get('annual_return', 0)*100:.2f}%
- 夏普比率: {result.get('sharpe_ratio', 0):.3f}
- 最大回撤: {result.get('max_drawdown', 0)*100:.2f}%

"""
        
        report += f"""
---

## 3. 风险辩论

**最终判断**: {state['risk_debate'].get('judge_decision', 'HOLD')}
**倾向**: {state['risk_debate'].get('final_bias', 'neutral')}

---

## 4. 组合建议

{state['portfolio'].get('portfolio_report', '暂无')}

---

## 5. 最终决策

| 项目 | 数值 |
|------|------|
| 决策 | **{state['final_decision']}** |
| 仓位 | {state['position_size']*100:.1f}% |
| 信心度 | {state['confidence']:.2f} |

---

*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
        return report
    
    def save_state(self, filepath: str = None):
        """保存状态"""
        if filepath is None:
            filepath = f"data_cache/agent_states/{self.current_state['stock_code']}_{self.current_state['trade_date']}.json"
        
        import os
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.current_state, f, indent=2, ensure_ascii=False, default=str)
        
        return filepath


# 导出
__all__ = ['SmartQuantGraph']