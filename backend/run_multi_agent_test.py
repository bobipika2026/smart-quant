"""
Smart Quant 多智能体系统测试

测试完整的分析流程
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.agents.graph import SmartQuantGraph


def test_single_stock():
    """测试单只股票分析"""
    print("=" * 70)
    print("Smart Quant 多智能体系统测试")
    print("=" * 70)
    
    # 初始化
    config = {
        'debug': True,
        'max_debate_rounds': 2,
        'strategies': ['ma_cross', 'macd', 'factor_score'],
    }
    
    graph = SmartQuantGraph(config=config)
    
    # 测试股票
    test_stocks = ['000001', '600036', '601398']  # 平安银行、招商银行、工商银行
    
    results = []
    
    for stock_code in test_stocks:
        print(f"\n{'='*70}")
        print(f"分析股票: {stock_code}")
        print(f"{'='*70}")
        
        try:
            state, decision = graph.propagate(stock_code)
            results.append(decision)
        except Exception as e:
            print(f"分析失败: {e}")
            import traceback
            traceback.print_exc()
    
    # 汇总结果
    print(f"\n{'='*70}")
    print("分析结果汇总")
    print(f"{'='*70}")
    
    print(f"\n{'股票代码':<10} {'决策':<8} {'仓位':<10} {'信心度':<8} {'评分':<8}")
    print("-" * 50)
    
    for r in results:
        print(f"{r.get('stock_code', 'N/A'):<10} "
              f"{r.get('decision', 'N/A'):<8} "
              f"{r.get('position_size', 0)*100:.1f}%{'':<5} "
              f"{r.get('confidence', 0):.2f}{'':<4} "
              f"{r.get('composite_score', 0):.1f}")
    
    # 生成报告
    print(f"\n{'='*70}")
    print("完整报告")
    print(f"{'='*70}")
    
    if graph.current_state:
        report = graph.generate_full_report()
        print(report)
        
        # 保存报告
        report_path = "data_cache/agent_states/test_report.md"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"\n报告已保存: {report_path}")


def test_batch_analysis():
    """测试批量分析"""
    print("\n" + "=" * 70)
    print("批量分析测试")
    print("=" * 70)
    
    graph = SmartQuantGraph(config={'debug': False})
    
    # 银行股批量分析
    bank_stocks = ['000001', '600000', '600036', '601166', '601288', '601398', '601818']
    
    results = graph.analyze_batch(bank_stocks)
    
    # 排序
    results.sort(key=lambda x: x.get('composite_score', 0), reverse=True)
    
    print(f"\n{'股票代码':<10} {'评分':<8} {'评级':<6} {'决策':<8} {'信心度':<8}")
    print("-" * 50)
    
    for r in results:
        print(f"{r.get('stock_code', 'N/A'):<10} "
              f"{r.get('composite_score', 0):.1f}{'':<4} "
              f"{r.get('grade', 'N/A'):<6} "
              f"{r.get('decision', 'N/A'):<8} "
              f"{r.get('confidence', 0):.2f}")


if __name__ == '__main__':
    test_single_stock()
    # test_batch_analysis()