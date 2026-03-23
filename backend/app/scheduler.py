"""
定时任务调度器

定时任务列表:
- 任务0: 每天早上6点全量同步所有数据（日线/大盘/财务/板块/宏观/小时线/交易日历）
- 任务1: 每天早上8点同步A股股票信息
- 任务2: 每天早上9点更新因子数据
- 任务3: 每个交易日16:30增量同步板块数据（资金流向/北向资金）
"""
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import logging
import tushare as ts

from app.services.sync_service import SyncService
from app.services.factor_service import FactorService
from app.services.data import DataService
from app.services.scheduled_sync import ScheduledSyncService
from app.config import settings

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()
_sync_service = None


def get_sync_service() -> ScheduledSyncService:
    global _sync_service
    if _sync_service is None:
        _sync_service = ScheduledSyncService()
    return _sync_service


def is_trading_day() -> bool:
    """判断今天是否为交易日"""
    try:
        if settings.TUSHARE_TOKEN:
            ts.set_token(settings.TUSHARE_TOKEN)
        pro = ts.pro_api()
        today = datetime.now().strftime('%Y%m%d')
        df = pro.trade_cal(exchange='SSE', start_date=today, end_date=today)
        if df is not None and len(df) > 0:
            return df.iloc[0]['is_open'] == '1'
    except Exception as e:
        logger.warning(f"[交易日判断] 失败: {e}")
        # 回退到简单判断
        return datetime.now().weekday() < 5
    return False


async def sync_all_stocks_job():
    """同步所有A股股票信息"""
    logger.info(f"[定时任务] 开始同步A股股票数据 - {datetime.now()}")
    try:
        sync_service = SyncService()
        result = await sync_service.sync_all_stocks()
        logger.info(f"[定时任务] 同步完成: {result}")
    except Exception as e:
        logger.error(f"[定时任务] 同步失败: {e}")


async def update_factors_job():
    """更新因子数据"""
    logger.info(f"[定时任务] 开始更新因子数据 - {datetime.now()}")
    try:
        from app.database import SessionLocal
        from app.models import Stock
        
        db = SessionLocal()
        stocks = db.query(Stock).limit(100).all()
        db.close()
        
        if not stocks:
            return
        
        data_service = DataService()
        success_count = 0
        
        for stock in stocks:
            try:
                hist_data = await data_service.get_stock_history(stock.code)
                factors = await FactorService.get_all_factors(stock.code, hist_data)
                if await FactorService.save_factors(factors):
                    success_count += 1
            except:
                pass
        
        logger.info(f"[定时任务] 因子更新完成: {success_count}/{len(stocks)}")
        
    except Exception as e:
        logger.error(f"[定时任务] 因子更新失败: {e}")


async def sync_all_data_job():
    """
    任务0: 每天早上6点全量同步所有数据
    包括：日线、大盘、财务、板块、宏观、小时线、交易日历
    """
    logger.info(f"[定时任务-全量同步] 开始 - {datetime.now()}")
    
    try:
        service = get_sync_service()
        result = await service.sync_all_data()
        logger.info(f"[定时任务-全量同步] 完成: {result}")
    except Exception as e:
        logger.error(f"[定时任务-全量同步] 失败: {e}")


async def sync_sector_data_job():
    """
    任务3: 每个交易日16:30增量同步板块数据
    
    包括：
    - 股票行业分类映射
    - 个股资金流向
    - 北向资金数据
    - 概念板块列表
    """
    # 判断是否为交易日
    if not is_trading_day():
        logger.info(f"[定时任务-板块同步] 今天不是交易日，跳过")
        return
    
    logger.info(f"[定时任务-板块同步] 开始 - {datetime.now()}")
    
    try:
        from app.services.sector_data_sync import SectorDataSyncService
        service = SectorDataSyncService()
        
        # 同步板块数据
        result = service.sync_all()
        
        logger.info(f"[定时任务-板块同步] 完成: {result}")
    except Exception as e:
        logger.error(f"[定时任务-板块同步] 失败: {e}")


def setup_scheduler():
    """设置定时任务"""
    
    # 任务0: 每天早上6点全量同步所有数据
    scheduler.add_job(
        sync_all_data_job,
        CronTrigger(hour=6, minute=0),
        id="sync_all_data",
        name="全量同步所有数据",
        replace_existing=True
    )
    
    # 任务1: 每天08:00同步A股股票信息
    scheduler.add_job(
        sync_all_stocks_job,
        CronTrigger(hour=8, minute=0),
        id="sync_all_stocks",
        name="同步A股股票信息",
        replace_existing=True
    )
    
    # 任务2: 每天09:00更新因子数据
    scheduler.add_job(
        update_factors_job,
        CronTrigger(hour=9, minute=0),
        id="update_factors",
        name="更新因子数据",
        replace_existing=True
    )
    
    # 任务3: 每个交易日16:30增量同步板块数据
    scheduler.add_job(
        sync_sector_data_job,
        CronTrigger(hour=16, minute=30),
        id="sync_sector_data",
        name="增量同步板块数据",
        replace_existing=True
    )
    
    logger.info("[定时任务] 调度器已初始化:")
    logger.info("[定时任务] - 每天06:00 全量同步所有数据")
    logger.info("[定时任务] - 每天08:00 同步A股股票信息")
    logger.info("[定时任务] - 每天09:00 更新因子数据")
    logger.info("[定时任务] - 每天16:30 增量同步板块数据（仅交易日）")


def start_scheduler():
    setup_scheduler()
    scheduler.start()
    logger.info("[定时任务] 调度器已启动")


def stop_scheduler():
    scheduler.shutdown()
    logger.info("[定时任务] 调度器已停止")