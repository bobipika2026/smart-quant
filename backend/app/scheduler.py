"""
定时任务调度器
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import logging

from app.services.sync_service import SyncService
from app.services.factor_service import FactorService
from app.services.data import DataService

logger = logging.getLogger(__name__)

# 创建调度器实例
scheduler = AsyncIOScheduler()


async def sync_all_stocks_job():
    """同步所有A股股票信息的定时任务"""
    logger.info(f"[定时任务] 开始同步A股股票数据 - {datetime.now()}")
    try:
        sync_service = SyncService()
        result = await sync_service.sync_all_stocks()
        logger.info(f"[定时任务] 同步完成: {result}")
    except Exception as e:
        logger.error(f"[定时任务] 同步失败: {e}")


async def update_factors_job():
    """更新因子数据的定时任务"""
    logger.info(f"[定时任务] 开始更新因子数据 - {datetime.now()}")
    try:
        from app.database import SessionLocal
        from app.models import Stock
        
        db = SessionLocal()
        stocks = db.query(Stock).limit(100).all()  # 每次更新100只
        db.close()
        
        if not stocks:
            logger.info("[定时任务] 没有股票数据")
            return
        
        data_service = DataService()
        success_count = 0
        
        for stock in stocks:
            try:
                # 获取历史数据
                hist_data = await data_service.get_stock_history(stock.code)
                
                # 获取因子
                factors = await FactorService.get_all_factors(stock.code, hist_data)
                
                # 保存因子
                if await FactorService.save_factors(factors):
                    success_count += 1
                    
            except Exception as e:
                logger.error(f"[定时任务] {stock.code} 因子更新失败: {e}")
        
        logger.info(f"[定时任务] 因子更新完成: {success_count}/{len(stocks)}")
        
    except Exception as e:
        logger.error(f"[定时任务] 因子更新失败: {e}")


def setup_scheduler():
    """设置定时任务"""
    # 每天08:00同步A股数据
    scheduler.add_job(
        sync_all_stocks_job,
        CronTrigger(hour=8, minute=0),
        id="sync_all_stocks",
        name="同步A股股票数据",
        replace_existing=True
    )
    
    # 每天09:00更新因子数据
    scheduler.add_job(
        update_factors_job,
        CronTrigger(hour=9, minute=0),
        id="update_factors",
        name="更新因子数据",
        replace_existing=True
    )
    
    logger.info("[定时任务] 调度器已初始化:")
    logger.info("[定时任务] - 每天08:00 同步A股数据")
    logger.info("[定时任务] - 每天09:00 更新因子数据")


def start_scheduler():
    """启动调度器"""
    setup_scheduler()
    scheduler.start()
    logger.info("[定时任务] 调度器已启动")


def stop_scheduler():
    """停止调度器"""
    scheduler.shutdown()
    logger.info("[定时任务] 调度器已停止")