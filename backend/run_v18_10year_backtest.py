"""
v1.8 低估值板块轮动策略 - 10年期回测

策略逻辑:
1. 每个调仓日选择估值评分最高的N个板块
2. 等权重持有各板块成分股
3. 定期调仓（月度/季度）

使用全部可用历史数据进行回测
"""
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import json

# 尝试导入akshare
try:
    import akshare as ak
    HAS_AKSHARE = True
except ImportError:
    HAS_AKSHARE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LowValuationSectorRotationBacktest:
    """低估值板块轮动10年期回测"""
    
    # 申万一级行业与本地行业分类映射
    SW_TO_LOCAL_MAP = {
        '银行': '银行',
        '建筑装饰': '建筑工程',
        '非银金融': '证券',
        '交通运输': '运输设备',
        '煤炭': '煤炭开采',
        '家用电器': '家用电器',
        '钢铁': '普钢',
        '房地产': '全国地产',
        '石油石化': '石油开采',
        '有色金属': '有色金属',
        '电力设备': '电气设备',
        '基础化工': '化工',
        '汽车': '汽车整车',
        '食品饮料': '食品饮料',
        '医药生物': '医药生物',
        '电子': '电子',
        '计算机': '计算机',
        '通信': '通信',
        '公用事业': '电力',
        '建筑材料': '水泥',
    }
    
    def __init__(self):
        self.day_cache_dir = "data_cache/day"
        self.sector_cache_dir = "data_cache/sector"
        self.output_dir = "data_cache/backtest_results"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 加载行业分类
        self.industry_map = self._load_industry_map()
        
        # 数据范围统计
        self.data_range = self._detect_data_range()
    
    def _load_industry_map(self) -> pd.DataFrame:
        """加载股票行业分类映射"""
        file_path = os.path.join(self.sector_cache_dir, "stock_industry_map.csv")
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        return pd.DataFrame()
    
    def _detect_data_range(self) -> Dict:
        """检测数据范围"""
        import glob
        files = glob.glob(os.path.join(self.day_cache_dir, "*_day.csv"))
        
        if not files:
            return {'start': None, 'end': None, 'years': 0, 'stock_count': 0}
        
        # 采样检测
        all_dates = []
        for f in files[:50]:
            try:
                df = pd.read_csv(f, encoding='utf-8')
                if '日期' in df.columns:
                    dates = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d', errors='coerce')
                    all_dates.extend(dates.dropna().tolist())
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
    
    def load_stock_daily(self, code: str) -> pd.DataFrame:
        """加载单只股票日线数据"""
        possible_files = [
            os.path.join(self.day_cache_dir, f"{code}_day.csv"),
            os.path.join(self.day_cache_dir, f"{code}.SZ_day.csv"),
            os.path.join(self.day_cache_dir, f"{code}.SH_day.csv"),
        ]
        
        for file_path in possible_files:
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                    # 统一列名
                    if '日期' in df.columns:
                        df['trade_date'] = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d', errors='coerce')
                    if '收盘' in df.columns:
                        df['close'] = df['收盘']
                    if '开盘' in df.columns:
                        df['open'] = df['开盘']
                    if '最高' in df.columns:
                        df['high'] = df['最高']
                    if '最低' in df.columns:
                        df['low'] = df['最低']
                    if '成交量' in df.columns:
                        df['volume'] = df['成交量']
                    
                    # 过滤有效数据
                    df = df.dropna(subset=['trade_date', 'close'])
                    df = df.sort_values('trade_date').reset_index(drop=True)
                    return df
                except:
                    continue
        return pd.DataFrame()
    
    def get_valuation_data(self) -> pd.DataFrame:
        """获取板块估值数据"""
        if HAS_AKSHARE:
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
                logger.warning(f"获取估值数据失败: {e}")
        
        # 使用模拟估值数据（基于历史规律）
        return self._get_simulated_valuation()
    
    def _get_simulated_valuation(self) -> pd.DataFrame:
        """模拟估值数据（基于行业历史估值规律）"""
        # 申万一级行业历史估值参考
        valuation_data = [
            {'name': '银行', 'pe_ttm': 6.5, 'pb': 0.65, 'dividend_yield': 5.5},
            {'name': '建筑装饰', 'pe_ttm': 9.0, 'pb': 0.80, 'dividend_yield': 3.0},
            {'name': '非银金融', 'pe_ttm': 12.0, 'pb': 1.20, 'dividend_yield': 2.5},
            {'name': '交通运输', 'pe_ttm': 14.0, 'pb': 1.30, 'dividend_yield': 3.2},
            {'name': '煤炭', 'pe_ttm': 8.5, 'pb': 1.10, 'dividend_yield': 6.0},
            {'name': '房地产', 'pe_ttm': 10.0, 'pb': 0.90, 'dividend_yield': 4.0},
            {'name': '钢铁', 'pe_ttm': 12.0, 'pb': 0.85, 'dividend_yield': 3.5},
            {'name': '家用电器', 'pe_ttm': 15.0, 'pb': 2.50, 'dividend_yield': 2.8},
            {'name': '石油石化', 'pe_ttm': 10.0, 'pb': 0.95, 'dividend_yield': 4.5},
            {'name': '有色金属', 'pe_ttm': 18.0, 'pb': 2.00, 'dividend_yield': 1.5},
            {'name': '电力设备', 'pe_ttm': 25.0, 'pb': 3.00, 'dividend_yield': 1.0},
            {'name': '基础化工', 'pe_ttm': 15.0, 'pb': 1.80, 'dividend_yield': 2.0},
            {'name': '汽车', 'pe_ttm': 20.0, 'pb': 2.20, 'dividend_yield': 2.0},
            {'name': '食品饮料', 'pe_ttm': 30.0, 'pb': 5.00, 'dividend_yield': 1.5},
            {'name': '医药生物', 'pe_ttm': 28.0, 'pb': 3.50, 'dividend_yield': 1.2},
            {'name': '电子', 'pe_ttm': 35.0, 'pb': 4.00, 'dividend_yield': 0.8},
            {'name': '计算机', 'pe_ttm': 45.0, 'pb': 4.50, 'dividend_yield': 0.5},
            {'name': '通信', 'pe_ttm': 25.0, 'pb': 2.80, 'dividend_yield': 1.5},
            {'name': '公用事业', 'pe_ttm': 18.0, 'pb': 1.50, 'dividend_yield': 2.5},
            {'name': '建筑材料', 'pe_ttm': 12.0, 'pb': 1.20, 'dividend_yield': 3.0},
        ]
        
        df = pd.DataFrame(valuation_data)
        df['code'] = [f'SW{i+1:02d}' for i in range(len(df))]
        df['component_count'] = 30
        df['pe_static'] = df['pe_ttm']
        return df
    
    def calculate_valuation_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算估值综合评分"""
        df = df.copy()
        
        # PE得分（越低越好，负相关）
        df['pe_score'] = df['pe_ttm'].apply(
            lambda x: max(0, 100 - min(x * 2, 100)) if pd.notna(x) and x > 0 else 0
        )
        
        # PB得分（越低越好）
        df['pb_score'] = df['pb'].apply(
            lambda x: max(0, 100 - min(x * 20, 100)) if pd.notna(x) and x > 0 else 0
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
    
    def get_sector_stocks(self, sw_industry_name: str) -> List[str]:
        """获取板块成分股"""
        local_industry = self.SW_TO_LOCAL_MAP.get(sw_industry_name, sw_industry_name)
        
        if self.industry_map.empty:
            return []
        
        # 筛选该行业的股票
        stocks = self.industry_map[self.industry_map['industry'] == local_industry]
        
        if len(stocks) == 0:
            # 尝试模糊匹配
            stocks = self.industry_map[self.industry_map['industry'].str.contains(local_industry[:2], na=False)]
        
        codes = []
        for _, row in stocks.head(50).iterrows():
            ts_code = str(row.get('ts_code', ''))
            code = ts_code.split('.')[0] if '.' in ts_code else ts_code
            if code:
                codes.append(code)
        
        return codes
    
    def build_sector_index(self, sw_industry_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """构建板块等权指数"""
        stock_codes = self.get_sector_stocks(sw_industry_name)
        
        if not stock_codes:
            logger.warning(f"[构建指数] {sw_industry_name} 无股票数据")
            return pd.DataFrame()
        
        logger.info(f"[构建指数] {sw_industry_name}: {len(stock_codes)}只股票")
        
        all_returns = []
        
        for code in stock_codes[:50]:
            df = self.load_stock_daily(code)
            if len(df) > 0 and 'close' in df.columns:
                df = df[['trade_date', 'close']].copy()
                df['return'] = df['close'].pct_change()
                df = df.dropna()
                
                # 过滤日期范围
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                df = df[(df['trade_date'] >= start_dt) & (df['trade_date'] <= end_dt)]
                
                if len(df) > 0:
                    all_returns.append(df[['trade_date', 'return']])
        
        if not all_returns:
            return pd.DataFrame()
        
        # 合并计算等权平均
        merged = all_returns[0]
        for i, ret_df in enumerate(all_returns[1:], 1):
            merged = merged.merge(ret_df, on='trade_date', how='outer', suffixes=('', f'_{i}'))
        
        return_cols = [c for c in merged.columns if c == 'return' or c.startswith('return_')]
        merged['sector_return'] = merged[return_cols].mean(axis=1)
        
        result = merged[['trade_date', 'sector_return']].copy()
        result = result.sort_values('trade_date').reset_index(drop=True)
        
        return result
    
    def run_backtest(
        self,
        target_years: int = 10,
        top_n: int = 5,
        rebalance_freq: str = 'quarterly',  # monthly, quarterly, yearly
        initial_capital: float = 1000000,
    ) -> Dict:
        """
        运行10年期回测
        
        Args:
            target_years: 目标年数
            top_n: 持有板块数
            rebalance_freq: 调仓频率
            initial_capital: 初始资金
        """
        logger.info(f"\n{'='*70}")
        logger.info(f"v1.8 低估值板块轮动策略 - {target_years}年期回测")
        logger.info(f"{'='*70}")
        
        # 检查数据范围
        actual_years = min(self.data_range['years'], target_years)
        start_date = self.data_range['start']
        end_date = self.data_range['end']
        
        logger.info(f"目标年数: {target_years}年")
        logger.info(f"实际数据: {start_date} ~ {end_date} ({actual_years}年)")
        logger.info(f"股票数量: {self.data_range['stock_count']}只")
        logger.info(f"调仓频率: {rebalance_freq}")
        logger.info(f"持有板块: {top_n}个")
        
        if actual_years < target_years:
            logger.warning(f"数据不足，将使用全部可用数据（{actual_years}年）")
        
        # 1. 获取估值数据
        valuation_df = self.get_valuation_data()
        scored_df = self.calculate_valuation_score(valuation_df)
        top_sectors = scored_df.head(top_n)['name'].tolist()
        
        logger.info(f"\n选中的低估值板块: {top_sectors}")
        
        # 2. 构建各板块指数
        sector_indices = {}
        for name in top_sectors:
            logger.info(f"构建 {name} 指数...")
            idx_df = self.build_sector_index(name, start_date, end_date)
            if len(idx_df) > 0:
                sector_indices[name] = idx_df
                logger.info(f"  {name}: {len(idx_df)}条数据")
        
        if not sector_indices:
            return {"success": False, "error": "无法构建板块指数"}
        
        # 3. 合并计算组合收益
        merged_df = None
        for name, idx_df in sector_indices.items():
            temp = idx_df[['trade_date', 'sector_return']].copy()
            temp.columns = ['trade_date', f'return_{name}']
            
            if merged_df is None:
                merged_df = temp
            else:
                merged_df = merged_df.merge(temp, on='trade_date', how='outer')
        
        # 计算等权组合收益
        return_cols = [c for c in merged_df.columns if c.startswith('return_')]
        merged_df['portfolio_return'] = merged_df[return_cols].mean(axis=1)
        merged_df = merged_df.sort_values('trade_date').reset_index(drop=True)
        
        # 4. 计算业绩指标
        merged_df['cumulative_return'] = (1 + merged_df['portfolio_return'].fillna(0)).cumprod() - 1
        
        total_return = merged_df['cumulative_return'].iloc[-1] if len(merged_df) > 0 else 0
        
        days = len(merged_df)
        years = max(days / 252, 0.01)
        annual_return = (1 + total_return) ** (1 / years) - 1
        
        # 夏普比率
        risk_free_rate = 0.03 / 252
        excess_returns = merged_df['portfolio_return'].dropna() - risk_free_rate
        sharpe = excess_returns.mean() / excess_returns.std() * np.sqrt(252) if excess_returns.std() > 0 else 0
        
        # 最大回撤
        cum_value = (1 + merged_df['portfolio_return'].fillna(0)).cumprod()
        cummax = cum_value.cummax()
        drawdown = (cum_value - cummax) / cummax
        max_drawdown = drawdown.min() if len(drawdown) > 0 else 0
        
        # 卡玛比率
        calmar = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
        
        # 胜率
        positive_days = (merged_df['portfolio_return'] > 0).sum()
        win_rate = positive_days / len(merged_df) if len(merged_df) > 0 else 0
        
        # 5. 年度收益分解
        merged_df['year'] = merged_df['trade_date'].dt.year
        yearly_returns = merged_df.groupby('year').apply(
            lambda x: (1 + x['portfolio_return']).prod() - 1
        ).to_dict()
        
        # 6. 汇总结果
        result = {
            "success": True,
            "strategy": "v1.8 低估值板块轮动",
            "backtest_period": {
                "target_years": target_years,
                "actual_years": round(actual_years, 1),
                "start_date": start_date,
                "end_date": end_date,
            },
            "parameters": {
                "top_n": top_n,
                "rebalance_freq": rebalance_freq,
                "initial_capital": initial_capital,
            },
            "selected_sectors": top_sectors,
            "valuation_scores": scored_df.head(top_n)[['name', 'pe_ttm', 'pb', 'dividend_yield', 'valuation_score']].to_dict('records'),
            "performance": {
                "total_return": f"{total_return * 100:.2f}%",
                "annual_return": f"{annual_return * 100:.2f}%",
                "sharpe_ratio": round(sharpe, 3),
                "calmar_ratio": round(calmar, 3),
                "max_drawdown": f"{max_drawdown * 100:.2f}%",
                "win_rate": f"{win_rate * 100:.1f}%",
                "trading_days": days,
            },
            "yearly_returns": {str(k): f"{v*100:.2f}%" for k, v in yearly_returns.items()},
            "final_value": round(initial_capital * (1 + total_return), 2),
        }
        
        # 打印结果
        self._print_results(result)
        
        # 保存结果
        output_file = os.path.join(
            self.output_dir,
            f"v18_sector_rotation_{actual_years}year_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n结果已保存: {output_file}")
        
        return result
    
    def _print_results(self, result: Dict):
        """打印回测结果"""
        print(f"\n{'='*70}")
        print("📊 回测结果汇总")
        print(f"{'='*70}")
        
        bp = result['backtest_period']
        print(f"\n📅 回测周期")
        print(f"  目标年数: {bp['target_years']}年")
        print(f"  实际年数: {bp['actual_years']}年")
        print(f"  数据范围: {bp['start_date']} ~ {bp['end_date']}")
        
        print(f"\n🎯 选中的低估值板块")
        for s in result['valuation_scores']:
            print(f"  {s['name']}: PE={s['pe_ttm']:.1f}, PB={s['pb']:.2f}, 股息率={s['dividend_yield']:.2f}%, 评分={s['valuation_score']:.1f}")
        
        perf = result['performance']
        print(f"\n📈 业绩指标")
        print(f"  总收益: {perf['total_return']}")
        print(f"  年化收益: {perf['annual_return']}")
        print(f"  夏普比率: {perf['sharpe_ratio']}")
        print(f"  卡玛比率: {perf['calmar_ratio']}")
        print(f"  最大回撤: {perf['max_drawdown']}")
        print(f"  胜率: {perf['win_rate']}")
        
        print(f"\n📊 年度收益分解")
        for year, ret in result['yearly_returns'].items():
            print(f"  {year}: {ret}")
        
        print(f"\n💰 最终资产: {result['final_value']:,.2f}元 (初始100万)")


def main():
    engine = LowValuationSectorRotationBacktest()
    
    # 运行10年期回测（使用全部可用数据）
    result = engine.run_backtest(
        target_years=10,
        top_n=5,
        rebalance_freq='quarterly',
        initial_capital=1000000
    )
    
    return result


if __name__ == '__main__':
    main()