"""
多板块组合回测

测试不同板块组合的表现：
1. 低估值组合（银行、煤炭、建筑装饰等）
2. 高成长组合（新能源、半导体、医药等）
3. 消费组合（食品饮料、家电等）
4. 周期组合（有色、化工、钢铁等）
5. 均衡组合（沪深300等权）
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List
import json
import glob
import tushare as ts

ts.set_token('21cbce2d06540b12e14765850fee73749ccfb0cd7570f466bf7d8e45')
pro = ts.pro_api()

DAY_CACHE_DIR = "data_cache/day"
SECTOR_CACHE_DIR = "data_cache/sector"
OUTPUT_DIR = "data_cache/backtest_results"


class MultiSectorBacktest:
    """多板块组合回测引擎"""
    
    # 板块定义（行业名称 -> Tushare行业代码映射）
    SECTOR_GROUPS = {
        '低估值板块': {
            'industries': ['银行', '证券', '保险', '建筑工程', '煤炭开采'],
            'desc': 'PE<10, 高股息'
        },
        '高成长板块': {
            'industries': ['电气设备', '半导体', '元器件', '软件服务', '医疗保健'],
            'desc': '高增长, 高估值'
        },
        '消费板块': {
            'industries': ['食品', '家用电器', '白酒', '医药生物', '生物制药'],
            'desc': '稳定消费'
        },
        '周期板块': {
            'industries': ['有色金属', '化工原料', '普钢', '石油开采', '水泥'],
            'desc': '周期轮动'
        },
        '科技板块': {
            'industries': ['半导体', '元器件', '通信设备', 'IT设备', '互联网'],
            'desc': '科技创新'
        },
    }
    
    def __init__(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self.industry_map = self._load_industry_map()
        self.stock_basic = self._load_stock_basic()
    
    def _load_industry_map(self) -> pd.DataFrame:
        """加载行业分类"""
        file_path = os.path.join(SECTOR_CACHE_DIR, "stock_industry_map.csv")
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        return pd.DataFrame()
    
    def _load_stock_basic(self) -> pd.DataFrame:
        """加载股票基本信息"""
        try:
            return pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry,market')
        except:
            return pd.DataFrame()
    
    def load_stock_daily(self, code: str) -> pd.DataFrame:
        """加载股票日线数据"""
        possible_files = [
            os.path.join(DAY_CACHE_DIR, f"{code}_day.csv"),
            os.path.join(DAY_CACHE_DIR, f"{code}.SZ_day.csv"),
            os.path.join(DAY_CACHE_DIR, f"{code}.SH_day.csv"),
        ]
        
        for file_path in possible_files:
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                    if '日期' in df.columns:
                        df['trade_date'] = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d', errors='coerce')
                    if '收盘' in df.columns:
                        df['close'] = df['收盘']
                    df = df.dropna(subset=['trade_date', 'close'])
                    df = df.sort_values('trade_date').reset_index(drop=True)
                    return df
                except:
                    continue
        return pd.DataFrame()
    
    def get_sector_stocks(self, industry_name: str, max_stocks: int = 30) -> List[str]:
        """获取行业成分股"""
        if not self.stock_basic.empty:
            stocks = self.stock_basic[self.stock_basic['industry'].str.contains(industry_name[:2], na=False)]
            codes = [c.split('.')[0] for c in stocks['ts_code'].head(max_stocks).tolist()]
            return codes
        
        if not self.industry_map.empty:
            stocks = self.industry_map[self.industry_map['industry'].str.contains(industry_name[:2], na=False)]
            codes = [c.split('.')[0] for c in stocks['ts_code'].head(max_stocks).tolist()]
            return codes
        
        return []
    
    def build_sector_index(self, industry_name: str) -> pd.DataFrame:
        """构建板块等权指数"""
        stock_codes = self.get_sector_stocks(industry_name)
        
        if not stock_codes:
            print(f"  {industry_name}: 无股票数据")
            return pd.DataFrame()
        
        all_returns = []
        
        for code in stock_codes[:30]:
            df = self.load_stock_daily(code)
            if len(df) > 0:
                df = df[['trade_date', 'close']].copy()
                df['return'] = df['close'].pct_change()
                df = df.dropna()
                if len(df) > 100:
                    all_returns.append(df[['trade_date', 'return']])
        
        if not all_returns:
            return pd.DataFrame()
        
        # 合并计算等权收益
        merged = all_returns[0]
        for i, ret_df in enumerate(all_returns[1:], 1):
            merged = merged.merge(ret_df, on='trade_date', how='outer', suffixes=('', f'_{i}'))
        
        return_cols = [c for c in merged.columns if c == 'return' or c.startswith('return_')]
        merged['sector_return'] = merged[return_cols].mean(axis=1)
        
        result = merged[['trade_date', 'sector_return']].copy()
        result = result.sort_values('trade_date').reset_index(drop=True)
        
        print(f"  {industry_name}: {len(result)}条, {len(all_returns)}只股票")
        
        return result
    
    def run_backtest(self, sector_returns: pd.DataFrame) -> Dict:
        """计算业绩指标"""
        sector_returns = sector_returns.copy()
        sector_returns['cumulative_return'] = (1 + sector_returns['portfolio_return'].fillna(0)).cumprod() - 1
        
        total_return = sector_returns['cumulative_return'].iloc[-1] if len(sector_returns) > 0 else 0
        
        days = len(sector_returns)
        years = max(days / 252, 0.01)
        annual_return = (1 + total_return) ** (1 / years) - 1
        
        # 夏普比率
        risk_free_rate = 0.03 / 252
        excess_returns = sector_returns['portfolio_return'].dropna() - risk_free_rate
        sharpe = excess_returns.mean() / excess_returns.std() * np.sqrt(252) if excess_returns.std() > 0 else 0
        
        # 最大回撤
        cum_value = (1 + sector_returns['portfolio_return'].fillna(0)).cumprod()
        cummax = cum_value.cummax()
        drawdown = (cum_value - cummax) / cummax
        max_drawdown = drawdown.min() if len(drawdown) > 0 else 0
        
        # 年度收益
        sector_returns['year'] = sector_returns['trade_date'].dt.year
        yearly_returns = sector_returns.groupby('year').apply(
            lambda x: (1 + x['portfolio_return']).prod() - 1
        ).to_dict()
        
        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'sharpe_ratio': sharpe,
            'max_drawdown': max_drawdown,
            'win_rate': (sector_returns['portfolio_return'] > 0).mean(),
            'trading_days': days,
            'yearly_returns': yearly_returns
        }
    
    def run_all_groups(self) -> Dict:
        """运行所有板块组合回测"""
        print(f"\n{'='*70}")
        print("多板块组合回测")
        print(f"{'='*70}")
        
        results = {}
        
        for group_name, group_info in self.SECTOR_GROUPS.items():
            print(f"\n{'='*50}")
            print(f"📊 {group_name} ({group_info['desc']})")
            print(f"{'='*50}")
            
            industries = group_info['industries']
            sector_indices = {}
            
            for ind in industries:
                idx_df = self.build_sector_index(ind)
                if len(idx_df) > 0:
                    sector_indices[ind] = idx_df
            
            if not sector_indices:
                print(f"  无有效数据")
                continue
            
            # 合并计算组合收益
            merged_df = None
            for ind_name, idx_df in sector_indices.items():
                temp = idx_df[['trade_date', 'sector_return']].copy()
                temp.columns = ['trade_date', f'return_{ind_name}']
                
                if merged_df is None:
                    merged_df = temp
                else:
                    merged_df = merged_df.merge(temp, on='trade_date', how='outer')
            
            # 等权组合
            return_cols = [c for c in merged_df.columns if c.startswith('return_')]
            merged_df['portfolio_return'] = merged_df[return_cols].mean(axis=1)
            merged_df = merged_df.sort_values('trade_date').reset_index(drop=True)
            
            # 回测
            perf = self.run_backtest(merged_df)
            
            results[group_name] = {
                'desc': group_info['desc'],
                'industries': list(sector_indices.keys()),
                'performance': perf,
                'yearly_returns': perf['yearly_returns']
            }
            
            print(f"\n  总收益: {perf['total_return']*100:.2f}%")
            print(f"  年化收益: {perf['annual_return']*100:.2f}%")
            print(f"  夏普比率: {perf['sharpe_ratio']:.3f}")
            print(f"  最大回撤: {perf['max_drawdown']*100:.2f}%")
        
        # 汇总对比
        print(f"\n{'='*70}")
        print("📊 各组合对比")
        print(f"{'='*70}")
        
        print(f"\n{'组合':<12} {'年化收益':>10} {'夏普':>8} {'最大回撤':>10} {'胜率':>8}")
        print("-" * 50)
        
        comparison = []
        for group_name, data in results.items():
            perf = data['performance']
            print(f"{group_name:<12} {perf['annual_return']*100:>9.2f}% {perf['sharpe_ratio']:>8.3f} {perf['max_drawdown']*100:>9.2f}% {perf['win_rate']*100:>7.1f}%")
            comparison.append({
                'group': group_name,
                'annual_return': perf['annual_return'],
                'sharpe': perf['sharpe_ratio'],
                'max_drawdown': perf['max_drawdown']
            })
        
        # 最佳组合
        best_by_return = max(comparison, key=lambda x: x['annual_return'])
        best_by_sharpe = max(comparison, key=lambda x: x['sharpe_ratio'])
        best_by_dd = min(comparison, key=lambda x: x['max_drawdown'])
        
        print(f"\n{'='*70}")
        print("🏆 最佳组合")
        print(f"{'='*70}")
        print(f"  最高收益: {best_by_return['group']} ({best_by_return['annual_return']*100:.2f}%)")
        print(f"  最高夏普: {best_by_sharpe['group']} ({best_by_sharpe['sharpe']:.3f})")
        print(f"  最小回撤: {best_by_dd['group']} ({best_by_dd['max_drawdown']*100:.2f}%)")
        
        # 保存结果
        output = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'results': results,
            'comparison': comparison,
            'best': {
                'by_return': best_by_return,
                'by_sharpe': best_by_sharpe,
                'by_drawdown': best_by_dd
            }
        }
        
        output_file = os.path.join(
            OUTPUT_DIR,
            f"multi_sector_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n结果已保存: {output_file}")
        
        return output


def main():
    engine = MultiSectorBacktest()
    return engine.run_all_groups()


if __name__ == '__main__':
    main()