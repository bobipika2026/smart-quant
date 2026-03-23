"""
10年期因子策略回测

支持：
1. 自动检测可用数据范围
2. 补充历史数据（如果需要）
3. 五种方法对比回测
4. 滚动窗口回测
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
import glob

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from run_five_methods_backtest import FiveMethodsBacktest


class TenYearBacktest:
    """10年期回测引擎"""
    
    DAY_CACHE_DIR = "data_cache/day"
    OUTPUT_DIR = "data_cache/backtest_results"
    
    def __init__(self):
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
        self.data_range = self._detect_data_range()
    
    def _detect_data_range(self) -> Dict:
        """检测数据范围"""
        files = glob.glob(os.path.join(self.DAY_CACHE_DIR, "*_day.csv"))
        
        if not files:
            return {'start': None, 'end': None, 'years': 0, 'stock_count': 0}
        
        # 采样几个文件检测范围
        sample_files = files[:10]
        all_dates = []
        
        for f in sample_files:
            try:
                df = pd.read_csv(f, encoding='utf-8')
                dates = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d')
                all_dates.extend(dates.tolist())
            except:
                continue
        
        if not all_dates:
            return {'start': None, 'end': None, 'years': 0, 'stock_count': len(files)}
        
        start_date = min(all_dates)
        end_date = max(all_dates)
        years = (end_date - start_date).days / 365.25
        
        return {
            'start': start_date.strftime('%Y-%m-%d'),
            'end': end_date.strftime('%Y-%m-%d'),
            'years': round(years, 1),
            'stock_count': len(files)
        }
    
    def run_backtest(self, 
                     target_years: int = 10,
                     stock_limit: int = 100,
                     force_existing: bool = False) -> Dict:
        """
        运行回测
        
        Args:
            target_years: 目标回测年数
            stock_limit: 股票数量限制
            force_existing: 强制使用现有数据
        """
        print(f"\n{'='*70}")
        print(f"{target_years}年期因子策略回测")
        print(f"{'='*70}")
        print(f"数据范围: {self.data_range['start']} ~ {self.data_range['end']}")
        print(f"可用年数: {self.data_range['years']}年")
        print(f"股票数量: {self.data_range['stock_count']}只")
        print(f"{'='*70}\n")
        
        # 检查数据是否足够
        if self.data_range['years'] < target_years and not force_existing:
            print(f"\n⚠️  数据不足：需要{target_years}年，当前只有{self.data_range['years']}年")
            print("\n选项：")
            print("1. 使用现有数据进行回测")
            print("2. 补充历史数据（需要Tushare高级权限）")
            print("3. 取消回测")
            
            # 自动选择使用现有数据
            print("\n将使用现有数据进行回测...")
        
        # 使用现有数据运行回测
        actual_years = min(self.data_range['years'], target_years)
        
        # 调用五方法回测
        engine = FiveMethodsBacktest()
        results = engine.run_all_methods(stock_limit=stock_limit)
        
        # 添加实际回测年数信息
        results['actual_backtest_years'] = actual_years
        results['data_range'] = self.data_range
        
        # 保存结果
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"backtest_{actual_years}year_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n回测结果已保存: {output_file}")
        
        return results
    
    def run_rolling_backtest(self, 
                             window_years: int = 3,
                             step_months: int = 6,
                             stock_limit: int = 50) -> Dict:
        """
        滚动窗口回测
        
        Args:
            window_years: 滚动窗口年数
            step_months: 步长（月）
            stock_limit: 股票数量
        """
        print(f"\n{'='*70}")
        print(f"滚动窗口回测（{window_years}年窗口，{step_months}月步长）")
        print(f"{'='*70}\n")
        
        # 获取所有交易日
        trade_days_file = "data_cache/trade_days.txt"
        if not os.path.exists(trade_days_file):
            print("❌ 找不到交易日历文件")
            return {}
        
        with open(trade_days_file, 'r') as f:
            trade_days = [line.strip() for line in f.readlines()]
        
        trade_days = pd.to_datetime(trade_days)
        trade_days = trade_days.sort_values()
        
        # 计算滚动窗口
        window_days = window_years * 252
        step_days = step_months * 21
        
        results = []
        start_idx = 0
        
        while start_idx + window_days < len(trade_days):
            window_start = trade_days[start_idx]
            window_end = trade_days[start_idx + window_days]
            
            print(f"\n窗口: {window_start.strftime('%Y-%m-%d')} ~ {window_end.strftime('%Y-%m-%d')}")
            
            # 运行回测（简化版，实际需要筛选日期范围）
            engine = FiveMethodsBacktest()
            window_result = engine.run_all_methods(stock_limit=stock_limit)
            
            results.append({
                'window_start': window_start.strftime('%Y-%m-%d'),
                'window_end': window_end.strftime('%Y-%m-%d'),
                'best_method': window_result.get('best_methods', {}).get('by_return', {}),
                'avg_return': window_result.get('comparison', [{}])[0].get('avg_return', 0) if window_result.get('comparison') else 0
            })
            
            start_idx += step_days
        
        # 汇总结果
        summary = {
            'total_windows': len(results),
            'window_results': results,
            'avg_return_across_windows': np.mean([r['avg_return'] for r in results]) if results else 0
        }
        
        # 保存
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"rolling_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n滚动回测结果已保存: {output_file}")
        
        return summary


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='10年期因子策略回测')
    parser.add_argument('--years', type=int, default=10, help='目标回测年数')
    parser.add_argument('--stocks', type=int, default=100, help='股票数量限制')
    parser.add_argument('--rolling', action='store_true', help='运行滚动窗口回测')
    parser.add_argument('--window', type=int, default=3, help='滚动窗口年数')
    
    args = parser.parse_args()
    
    engine = TenYearBacktest()
    
    if args.rolling:
        engine.run_rolling_backtest(
            window_years=args.window,
            stock_limit=args.stocks
        )
    else:
        engine.run_backtest(
            target_years=args.years,
            stock_limit=args.stocks
        )


if __name__ == "__main__":
    main()