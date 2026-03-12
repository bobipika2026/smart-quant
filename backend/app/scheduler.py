"""
定时任务调度器
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import logging

from app.services.sync_service import SyncService

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


def setup_scheduler():
    """设置定时任务"""
    # 每天早上8点同步A股数据
    scheduler.add_job(
        sync_all_stocks_job,
        CronTrigger(hour=8, minute=0),
        id="sync_all_stocks",
        name="同步A股股票数据",
        replace_existing=True
    )
    
    logger.info("[定时任务] 调度器已初始化: 每天08:00同步A股数据")


def start_scheduler():
    """启动调度器"""
    setup_scheduler()
    scheduler.start()
    logger.info("[定时任务] 调度器已启动")


def stop_scheduler():
    """停止调度器"""
    scheduler.shutdown()
    logger.info("[定时任务] 调度器已停止")