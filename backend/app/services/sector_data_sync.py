"""
板块数据同步服务

将板块相关数据同步到本地，包括：
1. 申万一级行业指数列表
2. 股票行业分类映射
3. 个股资金流向（每日）
4. 北向资金数据
5. 概念板块列表

数据缓存目录：data_cache/sector/
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import tushare as ts

from app.config import settings

logger = logging.getLogger(__name__)


class SectorDataSyncService:
    """板块数据同步服务"""
    
    # 缓存目录
    CACHE_DIR = "data_cache/sector"
    
    # 申万一级行业代码（31个核心行业）
    SW_L1_INDUSTRIES = {
        '801010.SI': '农林牧渔',
        '801030.SI': '基础化工',
        '801040.SI': '钢铁',
        '801050.SI': '有色金属',
        '801080.SI': '电子',
        '801880.SI': '汽车',
        '801110.SI': '家用电器',
        '801120.SI': '食品饮料',
        '801130.SI': '纺织服饰',
        '801140.SI': '轻工制造',
        '801150.SI': '医药生物',
        '801160.SI': '公用事业',
        '801170.SI': '交通运输',
        '801180.SI': '房地产',
        '801200.SI': '商贸零售',
        '801210.SI': '社会服务',
        '801780.SI': '银行',
        '801790.SI': '非银金融',
        '801230.SI': '综合',
        '801710.SI': '建筑材料',
        '801720.SI': '建筑装饰',
        '801730.SI': '电力设备',
        '801890.SI': '机械设备',
        '801740.SI': '国防军工',
        '801750.SI': '计算机',
        '801760.SI': '传媒',
        '801770.SI': '通信',
        '801950.SI': '煤炭',
        '801960.SI': '石油石化',
        '801970.SI': '环保',
        '801020.SI': '采掘',  # 可能已退市
    }
    
    def __init__(self):
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        if settings.TUSHARE_TOKEN:
            ts.set_token(settings.TUSHARE_TOKEN)
        self.pro = ts.pro_api()
    
    # ==================== 数据同步方法 ====================
    
    def sync_stock_industry_mapping(self) -> Dict:
        """
        同步股票行业分类映射
        
        缓存文件: stock_industry_map.csv
        字段: ts_code, name, industry, market
        """
        logger.info("[同步] 开始同步股票行业分类...")
        
        try:
            df = self.pro.stock_basic(
                exchange='', 
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market,list_date'
            )
            
            if df is not None and len(df) > 0:
                # 保存CSV
                cache_file = os.path.join(self.CACHE_DIR, "stock_industry_map.csv")
                df.to_csv(cache_file, index=False)
                
                # 统计行业分布
                industry_count = df['industry'].value_counts().to_dict()
                
                logger.info(f"[同步] 股票行业分类完成: {len(df)}只股票, {len(industry_count)}个行业")
                
                return {
                    "success": True,
                    "total_stocks": len(df),
                    "total_industries": len(industry_count),
                    "industry_distribution": industry_count
                }
        except Exception as e:
            logger.error(f"[同步] 股票行业分类失败: {e}")
            return {"success": False, "error": str(e)}
    
    def sync_moneyflow_data(self, trade_date: Optional[str] = None) -> Dict:
        """
        同步个股资金流向数据
        
        缓存文件: moneyflow/moneyflow_{date}.csv
        字段: ts_code, trade_date, buy_sm_vol, sell_lg_vol, net_mf_vol, net_mf_amount...
        
        注意: Tushare对资金流向接口有限频，需要分批获取
        """
        if trade_date is None:
            trade_date = datetime.now().strftime('%Y%m%d')
        
        logger.info(f"[同步] 开始同步资金流向数据: {trade_date}")
        
        try:
            # 获取当日所有股票资金流向
            df = self.pro.moneyflow(trade_date=trade_date)
            
            if df is not None and len(df) > 0:
                # 创建moneyflow子目录
                mf_dir = os.path.join(self.CACHE_DIR, "moneyflow")
                os.makedirs(mf_dir, exist_ok=True)
                
                # 保存CSV
                cache_file = os.path.join(mf_dir, f"moneyflow_{trade_date}.csv")
                df.to_csv(cache_file, index=False)
                
                # 汇总统计
                summary = {
                    "trade_date": trade_date,
                    "total_stocks": len(df),
                    "net_inflow_positive": len(df[df['net_mf_amount'] > 0]),
                    "net_inflow_negative": len(df[df['net_mf_amount'] < 0]),
                    "total_net_amount": round(df['net_mf_amount'].sum() / 1e8, 2),  # 亿元
                }
                
                # 保存汇总
                summary_file = os.path.join(mf_dir, "moneyflow_summary.json")
                summaries = {}
                if os.path.exists(summary_file):
                    with open(summary_file, 'r') as f:
                        summaries = json.load(f)
                summaries[trade_date] = summary
                with open(summary_file, 'w') as f:
                    json.dump(summaries, f, ensure_ascii=False, indent=2)
                
                logger.info(f"[同步] 资金流向完成: {len(df)}只股票, 净流入{summary['total_net_amount']}亿")
                
                return {"success": True, "data": summary}
            else:
                logger.warning(f"[同步] {trade_date} 无资金流向数据")
                return {"success": False, "message": "无数据"}
                
        except Exception as e:
            logger.error(f"[同步] 资金流向失败: {e}")
            return {"success": False, "error": str(e)}
    
    def sync_north_money_data(self, days: int = 30) -> Dict:
        """
        同步北向资金数据
        
        缓存文件: north_money.csv
        字段: trade_date, ggt_ss, ggt_sz, hgt, sgt, north_money, south_money
        """
        logger.info(f"[同步] 开始同步北向资金数据(最近{days}天)...")
        
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
            
            df = self.pro.moneyflow_hsgt(start_date=start_date, end_date=end_date)
            
            if df is not None and len(df) > 0:
                df = df.sort_values('trade_date').reset_index(drop=True)
                
                # 保存CSV
                cache_file = os.path.join(self.CACHE_DIR, "north_money.csv")
                df.to_csv(cache_file, index=False)
                
                # 最新数据
                latest = df.iloc[-1].to_dict() if len(df) > 0 else {}
                
                logger.info(f"[同步] 北向资金完成: {len(df)}条记录")
                
                return {
                    "success": True,
                    "total_records": len(df),
                    "latest": latest
                }
        except Exception as e:
            logger.error(f"[同步] 北向资金失败: {e}")
            return {"success": False, "error": str(e)}
    
    def sync_concept_data(self) -> Dict:
        """
        同步概念板块数据
        
        缓存文件: concept_list.csv
        """
        logger.info("[同步] 开始同步概念板块...")
        
        try:
            df = self.pro.concept(src='ts')
            
            if df is not None and len(df) > 0:
                cache_file = os.path.join(self.CACHE_DIR, "concept_list.csv")
                df.to_csv(cache_file, index=False)
                
                logger.info(f"[同步] 概念板块完成: {len(df)}个概念")
                
                return {"success": True, "total_concepts": len(df)}
        except Exception as e:
            logger.error(f"[同步] 概念板块失败: {e}")
            return {"success": False, "error": str(e)}
    
    def sync_all(self) -> Dict:
        """
        同步所有板块数据
        
        建议: 每个交易日收盘后执行
        """
        logger.info("=" * 60)
        logger.info("[同步] 开始同步所有板块数据")
        logger.info("=" * 60)
        
        results = {}
        
        # 1. 股票行业分类
        logger.info("\n[1/4] 同步股票行业分类...")
        results['stock_industry'] = self.sync_stock_industry_mapping()
        
        # 2. 资金流向
        logger.info("\n[2/4] 同步资金流向...")
        results['moneyflow'] = self.sync_moneyflow_data()
        
        # 3. 北向资金
        logger.info("\n[3/4] 同步北向资金...")
        results['north_money'] = self.sync_north_money_data(days=30)
        
        # 4. 概念板块
        logger.info("\n[4/4] 同步概念板块...")
        results['concept'] = self.sync_concept_data()
        
        logger.info("\n" + "=" * 60)
        logger.info("[同步] 板块数据同步完成")
        logger.info("=" * 60)
        
        return results
    
    # ==================== 数据读取方法 ====================
    
    def get_stock_industry_map(self) -> pd.DataFrame:
        """读取股票行业分类映射"""
        cache_file = os.path.join(self.CACHE_DIR, "stock_industry_map.csv")
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file)
        return pd.DataFrame()
    
    def get_moneyflow_data(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """读取指定日期的资金流向数据（自动查找最近可用日期）"""
        mf_dir = os.path.join(self.CACHE_DIR, "moneyflow")
        
        if trade_date is None:
            # 查找最近可用的资金流向文件
            if os.path.exists(mf_dir):
                files = [f for f in os.listdir(mf_dir) if f.startswith('moneyflow_') and f.endswith('.csv')]
                if files:
                    # 按日期排序，取最新的
                    files.sort(reverse=True)
                    cache_file = os.path.join(mf_dir, files[0])
                    logger.info(f"[资金流向] 使用最新数据: {files[0]}")
                    return pd.read_csv(cache_file)
            return pd.DataFrame()
        
        cache_file = os.path.join(mf_dir, f"moneyflow_{trade_date}.csv")
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file)
        return pd.DataFrame()
    
    def get_north_money_data(self) -> pd.DataFrame:
        """读取北向资金数据"""
        cache_file = os.path.join(self.CACHE_DIR, "north_money.csv")
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file)
        return pd.DataFrame()
    
    def get_industry_moneyflow(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """
        计算行业资金流向（聚合个股数据）
        
        返回: 行业维度的资金流向汇总
        """
        # 读取个股资金流向
        mf_df = self.get_moneyflow_data(trade_date)
        if len(mf_df) == 0:
            return pd.DataFrame()
        
        # 读取股票行业映射
        industry_df = self.get_stock_industry_map()
        if len(industry_df) == 0:
            return pd.DataFrame()
        
        # 合并
        mf_df['ts_code'] = mf_df['ts_code'].astype(str)
        industry_df['ts_code'] = industry_df['ts_code'].astype(str)
        
        merged = mf_df.merge(industry_df[['ts_code', 'industry', 'name']], on='ts_code', how='left')
        
        # 按行业聚合
        industry_flow = merged.groupby('industry').agg({
            'net_mf_amount': 'sum',
            'net_mf_vol': 'sum',
            'buy_lg_amount': 'sum',
            'sell_lg_amount': 'sum',
            'buy_elg_amount': 'sum',
            'sell_elg_amount': 'sum',
            'ts_code': 'count'
        }).reset_index()
        
        industry_flow.columns = [
            'industry', 'net_mf_amount', 'net_mf_vol',
            'buy_lg_amount', 'sell_lg_amount',
            'buy_elg_amount', 'sell_elg_amount', 'stock_count'
        ]
        
        # 计算净流入
        industry_flow['net_mf_amount_yi'] = round(industry_flow['net_mf_amount'] / 1e8, 2)
        industry_flow = industry_flow.sort_values('net_mf_amount', ascending=False).reset_index(drop=True)
        
        return industry_flow
    
    def get_industry_ranking(self, trade_date: Optional[str] = None) -> pd.DataFrame:
        """
        生成行业资金流向排名
        
        返回: 按资金净流入排名的行业列表
        """
        industry_flow = self.get_industry_moneyflow(trade_date)
        
        if len(industry_flow) == 0:
            return pd.DataFrame()
        
        # 添加排名
        industry_flow['rank'] = range(1, len(industry_flow) + 1)
        
        # 添加信号
        def get_signal(row):
            if row['net_mf_amount_yi'] > 5:
                return '强力流入'
            elif row['net_mf_amount_yi'] > 1:
                return '流入'
            elif row['net_mf_amount_yi'] < -5:
                return '强力流出'
            elif row['net_mf_amount_yi'] < -1:
                return '流出'
            else:
                return '平衡'
        
        industry_flow['signal'] = industry_flow.apply(get_signal, axis=1)
        
        return industry_flow


# 命令行入口
def main():
    """命令行运行同步"""
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    service = SectorDataSyncService()
    
    if len(sys.argv) > 1:
        task = sys.argv[1]
        
        if task == 'all':
            result = service.sync_all()
        elif task == 'industry':
            result = service.sync_stock_industry_mapping()
        elif task == 'moneyflow':
            result = service.sync_moneyflow_data()
        elif task == 'north':
            result = service.sync_north_money_data()
        elif task == 'concept':
            result = service.sync_concept_data()
        elif task == 'ranking':
            df = service.get_industry_ranking()
            print(df.head(20))
            return
        else:
            print("用法: python -m app.services.sector_data_sync [all|industry|moneyflow|north|concept|ranking]")
            return
    else:
        result = service.sync_all()
    
    print("\n同步结果:")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()