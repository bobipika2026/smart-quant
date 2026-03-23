"""
低估值板块轮动策略回测（本地数据版）

使用本地缓存数据进行回测:
- 日线数据: data_cache/day/
- 行业分类: data_cache/sector/stock_industry_map.csv

策略逻辑:
1. 选择低估值的N个板块
2. 等权重持有板块成分股
3. 计算组合收益
"""
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import akshare as ak

logger = logging.getLogger(__name__)


class LowValuationBacktest:
    """低估值板块轮动回测"""
    
    # 申万一级行业与Tushare行业分类映射
    SW_TO_TUSHARE_MAP = {
        '银行': '银行',
        '建筑装饰': '建筑工程',
        '非银金融': '证券',  # 主要指证券
        '交通运输': '运输设备',
        '煤炭': '煤炭开采',
        '家用电器': '家用电器',
        '钢铁': '普钢',
        '房地产': '全国地产',
    }
    
    def __init__(self):
        self.day_cache_dir = "data_cache/day"
        self.sector_cache_dir = "data_cache/sector"
    
    def load_industry_map(self) -> pd.DataFrame:
        """加载股票行业分类映射"""
        file_path = os.path.join(self.sector_cache_dir, "stock_industry_map.csv")
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        return pd.DataFrame()
    
    def load_stock_daily(self, code: str) -> pd.DataFrame:
        """加载单只股票日线数据"""
        # 尝试不同的文件名格式
        possible_files = [
            os.path.join(self.day_cache_dir, f"{code}_day.csv"),
            os.path.join(self.day_cache_dir, f"{code}.csv"),
        ]
        
        for file_path in possible_files:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                # 统一列名
                if '日期' in df.columns:
                    df['trade_date'] = df['日期'].astype(str)
                if '收盘' in df.columns:
                    df['close'] = df['收盘']
                return df
        return pd.DataFrame()
    
    def get_current_valuation(self) -> pd.DataFrame:
        """获取当前板块估值数据"""
        try:
            df = ak.sw_index_first_info()
            df = df.rename(columns={
                '行业代码': 'code',
                '行业名称': 'name',
                '成份个数': 'component_count',
                '静态市盈率': 'pe_static',
                'TTM(滚动)市盈率': 'pe_ttm',
                '市净率': 'pb',
                '静态股息率': 'dividend_yield',
            })
            return df
        except Exception as e:
            logger.error(f"[估值数据] 获取失败: {e}")
            return pd.DataFrame()
    
    def calculate_valuation_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算估值综合评分"""
        df = df.copy()
        
        # PE得分（越低越好）
        df['pe_score'] = df['pe_ttm'].apply(
            lambda x: 100 - min(x * 2, 100) if pd.notna(x) else 0
        )
        
        # PB得分（越低越好）
        df['pb_score'] = df['pb'].apply(
            lambda x: 100 - min(x * 20, 100) if pd.notna(x) else 0
        )
        
        # 股息率得分（越高越好）
        df['div_score'] = df['dividend_yield'].apply(
            lambda x: min(x * 10, 100) if pd.notna(x) else 0
        )
        
        # 综合评分
        df['valuation_score'] = (
            df['pe_score'] * 0.4 +
            df['pb_score'] * 0.3 +
            df['div_score'] * 0.3
        )
        
        return df.sort_values('valuation_score', ascending=False).reset_index(drop=True)
    
    def build_sector_index(self, sw_industry_name: str, industry_map: pd.DataFrame) -> pd.DataFrame:
        """
        构建行业指数（等权组合）
        
        使用行业内所有股票的等权平均收益作为行业收益
        """
        # 映射行业名称
        tushare_industry = self.SW_TO_TUSHARE_MAP.get(sw_industry_name, sw_industry_name)
        
        # 获取该行业的所有股票
        stocks = industry_map[industry_map['industry'] == tushare_industry]
        
        if len(stocks) == 0:
            logger.warning(f"[构建指数] {sw_industry_name}({tushare_industry}) 无股票数据")
            return pd.DataFrame()
        
        logger.info(f"[构建指数] {sw_industry_name} -> {tushare_industry}: {len(stocks)}只股票")
        
        all_returns = []
        stock_codes = []
        
        for _, row in stocks.head(50).iterrows():  # 最多取50只
            ts_code = row['ts_code']
            code = ts_code.split('.')[0] if '.' in str(ts_code) else str(ts_code)
            
            df = self.load_stock_daily(code)
            if len(df) > 0:
                if 'close' in df.columns and 'trade_date' in df.columns:
                    df = df[['trade_date', 'close']].copy()
                    df['return'] = df['close'].pct_change()
                    df = df.dropna()
                    
                    if len(df) > 0:
                        all_returns.append(df[['trade_date', 'return']])
                        stock_codes.append(code)
        
        if not all_returns:
            return pd.DataFrame()
        
        # 合并所有股票收益
        merged = all_returns[0]
        for i, ret_df in enumerate(all_returns[1:], 1):
            merged = merged.merge(ret_df, on='trade_date', how='outer', suffixes=('', f'_{i}'))
        
        # 计算等权平均收益
        return_cols = [c for c in merged.columns if c == 'return' or c.startswith('return_')]
        merged['sector_return'] = merged[return_cols].mean(axis=1)
        merged['sector_cumreturn'] = (1 + merged['sector_return']).cumprod() - 1
        
        result = merged[['trade_date', 'sector_return', 'sector_cumreturn']].copy()
        result = result.sort_values('trade_date').reset_index(drop=True)
        
        logger.info(f"[构建指数] {sw_industry_name}: {len(result)}条数据, {len(stock_codes)}只股票")
        
        return result
    
    def backtest(
        self,
        top_n: int = 5,
        start_date: str = '20240101',
        end_date: str = None,
        initial_capital: float = 1000000,
    ) -> Dict:
        """
        低估值板块轮动回测
        
        策略:
        1. 选择估值评分最高的N个板块
        2. 等权重持有各板块
        3. 计算组合收益
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        logger.info(f"[回测] 开始低估值板块轮动回测")
        logger.info(f"[回测] 时间范围: {start_date} - {end_date}")
        logger.info(f"[回测] 持有板块数: {top_n}")
        
        # 1. 获取估值数据
        valuation_df = self.get_current_valuation()
        if len(valuation_df) == 0:
            return {"success": False, "error": "无法获取估值数据"}
        
        # 2. 计算估值评分
        scored_df = self.calculate_valuation_score(valuation_df)
        top_sectors = scored_df.head(top_n)
        sector_names = top_sectors['name'].tolist()
        
        logger.info(f"[回测] 选中的低估值板块: {sector_names}")
        
        # 3. 加载行业分类映射
        industry_map = self.load_industry_map()
        if len(industry_map) == 0:
            return {"success": False, "error": "无法加载行业分类数据"}
        
        # 4. 构建各板块指数
        sector_indices = {}
        for name in sector_names:
            logger.info(f"[回测] 构建{name}指数...")
            idx_df = self.build_sector_index(name, industry_map)
            if len(idx_df) > 0:
                sector_indices[name] = idx_df
                logger.info(f"[回测] {name}: {len(idx_df)}条数据")
        
        if not sector_indices:
            return {"success": False, "error": "无法构建板块指数"}
        
        # 5. 合并板块收益
        merged_df = None
        for name, idx_df in sector_indices.items():
            temp = idx_df[['trade_date', 'sector_return']].copy()
            temp.columns = ['trade_date', f'return_{name}']
            
            if merged_df is None:
                merged_df = temp
            else:
                merged_df = merged_df.merge(temp, on='trade_date', how='outer')
        
        # 6. 计算组合收益（等权）
        return_cols = [c for c in merged_df.columns if c.startswith('return_')]
        merged_df['portfolio_return'] = merged_df[return_cols].mean(axis=1)
        merged_df = merged_df.sort_values('trade_date').reset_index(drop=True)
        
        # 过滤日期范围
        merged_df['trade_date'] = merged_df['trade_date'].astype(str).str.replace('-', '')
        merged_df = merged_df[
            (merged_df['trade_date'] >= start_date) & 
            (merged_df['trade_date'] <= end_date)
        ]
        
        # 7. 计算累计收益
        merged_df['cumulative_return'] = (1 + merged_df['portfolio_return']).cumprod() - 1
        
        # 8. 计算业绩指标
        total_return = merged_df['cumulative_return'].iloc[-1] if len(merged_df) > 0 else 0
        
        days = len(merged_df)
        years = max(days / 252, 0.01)
        annual_return = (1 + total_return) ** (1 / years) - 1
        
        # 夏普比率
        risk_free_rate = 0.03 / 252
        excess_returns = merged_df['portfolio_return'].dropna() - risk_free_rate
        sharpe = excess_returns.mean() / excess_returns.std() * np.sqrt(252) if excess_returns.std() > 0 else 0
        
        # 最大回撤
        cum_value = (1 + merged_df['portfolio_return']).cumprod()
        cummax = cum_value.cummax()
        drawdown = (cum_value - cummax) / cummax
        max_drawdown = drawdown.min() if len(drawdown) > 0 else 0
        
        # 9. 汇总结果
        result = {
            "success": True,
            "strategy": "低估值板块轮动",
            "parameters": {
                "start_date": start_date,
                "end_date": end_date,
                "top_n": top_n,
                "initial_capital": initial_capital,
            },
            "selected_sectors": sector_names,
            "valuation_scores": top_sectors[['name', 'pe_ttm', 'pb', 'dividend_yield', 'valuation_score']].to_dict('records'),
            "performance": {
                "total_return": f"{total_return * 100:.2f}%",
                "annual_return": f"{annual_return * 100:.2f}%",
                "sharpe_ratio": round(sharpe, 3),
                "max_drawdown": f"{max_drawdown * 100:.2f}%",
                "trading_days": days,
            },
            "portfolio_history": merged_df[['trade_date', 'portfolio_return', 'cumulative_return']].tail(30).to_dict('records'),
        }
        
        return result


# 运行回测
if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    strategy = LowValuationBacktest()
    result = strategy.backtest(top_n=5, start_date='20250101')
    
    print("\n" + "=" * 60)
    print("回测结果:")
    print("=" * 60)
    
    import json
    print(json.dumps(result, ensure_ascii=False, indent=2))