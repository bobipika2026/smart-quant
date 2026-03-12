"""
数据同步服务 - 从数据源同步A股股票信息
"""
import httpx
import asyncio
import re
import logging
from typing import List, Dict
from datetime import datetime

from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import Stock

logger = logging.getLogger(__name__)


class SyncService:
    """数据同步服务"""
    
    # 新浪财经A股列表API
    SINA_A_STOCK_URL = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
    
    # 交易所节点
    EXCHANGE_NODES = {
        'sh_a': '上交所A股',
        'sz_a': '深交所A股',
        'bj_a': '北交所A股'
    }
    
    async def _fetch_stock_list(self, node: str, page: int = 1, num: int = 80) -> List[Dict]:
        """从新浪财经获取股票列表"""
        params = {
            'page': page,
            'num': num,
            'sort': 'symbol',
            'asc': 1,
            'node': node,
            'symbol': '',
            '_s_r_a': 'page'
        }
        
        headers = {
            'Referer': 'https://vip.stock.finance.sina.com.cn/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(self.SINA_A_STOCK_URL, params=params, headers=headers)
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data and isinstance(data, list):
                        return data
        except Exception as e:
            logger.error(f"[数据同步] 获取{node}第{page}页失败: {e}")
        
        return []
    
    async def _fetch_all_stocks(self) -> List[Dict]:
        """获取所有A股股票列表"""
        all_stocks = []
        
        async with httpx.AsyncClient(timeout=15) as client:
            for node, exchange_name in self.EXCHANGE_NODES.items():
                logger.info(f"[数据同步] 正在获取{exchange_name}股票列表...")
                page = 1
                node_stocks = []
                
                while True:
                    stocks = await self._fetch_stock_list(node, page)
                    if not stocks:
                        break
                    
                    node_stocks.extend(stocks)
                    
                    # 如果返回数量小于请求数量，说明已经是最后一页
                    if len(stocks) < 80:
                        break
                    
                    page += 1
                    await asyncio.sleep(0.3)  # 避免请求过快
                
                logger.info(f"[数据同步] {exchange_name}获取到 {len(node_stocks)} 只股票")
                all_stocks.extend(node_stocks)
        
        return all_stocks
    
    async def sync_all_stocks(self) -> Dict:
        """同步所有A股股票信息到数据库"""
        start_time = datetime.now()
        
        # 获取所有股票数据
        stocks_data = await self._fetch_all_stocks()
        
        if not stocks_data:
            return {
                "success": False,
                "message": "未获取到股票数据",
                "count": 0
            }
        
        # 同步到数据库
        db: Session = SessionLocal()
        try:
            added_count = 0
            updated_count = 0
            
            for stock_info in stocks_data:
                code = stock_info.get('code', '')
                name = stock_info.get('name', '')
                
                if not code or not name:
                    continue
                
                # 判断交易所
                if code.startswith('6'):
                    exchange = 'SH'
                elif code.startswith('0') or code.startswith('3'):
                    exchange = 'SZ'
                elif code.startswith('4') or code.startswith('8'):
                    exchange = 'BJ'
                else:
                    exchange = 'UNKNOWN'
                
                # 查找是否已存在
                existing = db.query(Stock).filter(Stock.code == code).first()
                
                if existing:
                    # 更新
                    existing.name = name
                    existing.exchange = exchange
                    updated_count += 1
                else:
                    # 新增
                    new_stock = Stock(
                        code=code,
                        name=name,
                        exchange=exchange
                    )
                    db.add(new_stock)
                    added_count += 1
            
            db.commit()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "success": True,
                "message": "同步完成",
                "total": len(stocks_data),
                "added": added_count,
                "updated": updated_count,
                "duration_seconds": round(duration, 2)
            }
            
            logger.info(f"[数据同步] {result}")
            return result
            
        except Exception as e:
            db.rollback()
            logger.error(f"[数据同步] 数据库操作失败: {e}")
            return {
                "success": False,
                "message": str(e),
                "count": 0
            }
        finally:
            db.close()
    
    async def sync_stock_prices(self) -> Dict:
        """同步股票实时价格（可选扩展）"""
        # TODO: 实现实时价格同步
        pass