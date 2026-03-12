"""
数据同步相关API
"""
from fastapi import APIRouter, BackgroundTasks
from typing import Dict

from app.services.sync_service import SyncService

router = APIRouter(prefix="/api/sync", tags=["数据同步"])


@router.post("/stocks")
async def sync_stocks(background_tasks: BackgroundTasks) -> Dict:
    """手动触发同步所有A股股票信息"""
    sync_service = SyncService()
    result = await sync_service.sync_all_stocks()
    return result


@router.get("/status")
async def get_sync_status() -> Dict:
    """获取同步状态"""
    from app.database import SessionLocal
    from app.models import Stock
    
    db = SessionLocal()
    try:
        total_stocks = db.query(Stock).count()
        sh_count = db.query(Stock).filter(Stock.exchange == 'SH').count()
        sz_count = db.query(Stock).filter(Stock.exchange == 'SZ').count()
        bj_count = db.query(Stock).filter(Stock.exchange == 'BJ').count()
        
        return {
            "total_stocks": total_stocks,
            "sh_stocks": sh_count,
            "sz_stocks": sz_count,
            "bj_stocks": bj_count
        }
    finally:
        db.close()