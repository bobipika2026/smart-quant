#!/usr/bin/env python
"""
定时数据同步脚本

用法:
    python run_scheduled_sync.py market      # 同步A股行情数据(日线/小时/分钟)
    python run_scheduled_sync.py financial   # 同步A股财务指标数据
    python run_scheduled_sync.py index       # 同步大盘指标数据
    python run_scheduled_sync.py all         # 同步全部数据
    python run_scheduled_sync.py check       # 检查是否为交易日

定时任务配置:
    - 任务1: 每个交易日晚上8点同步A股日线、小时线、1分线
    - 任务2: 每天晚上9点同步A股财务指标数据
    - 任务3: 每天晚上10点同步大盘指标数据

系统级Cron配置示例:
    # 每个交易日20:00同步行情数据
    0 20 * * * cd /path/to/smart-quant/backend && python run_scheduled_sync.py market >> logs/sync_market.log 2>&1
    
    # 每天21:00同步财务数据
    0 21 * * * cd /path/to/smart-quant/backend && python run_scheduled_sync.py financial >> logs/sync_financial.log 2>&1
    
    # 每天22:00同步大盘数据
    0 22 * * * cd /path/to/smart-quant/backend && python run_scheduled_sync.py index >> logs/sync_index.log 2>&1
"""
import sys
import os
import asyncio
import logging
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services.scheduled_sync import ScheduledSyncService

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/scheduled_sync.log', encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)


async def main():
    """主函数"""
    # 确保日志目录存在
    os.makedirs('logs', exist_ok=True)
    
    service = ScheduledSyncService()
    
    if len(sys.argv) > 1:
        task = sys.argv[1].lower()
        
        if task == 'market':
            # 同步行情数据（仅交易日执行）
            if service.is_trading_day():
                logger.info("=" * 60)
                logger.info("开始同步A股行情数据")
                logger.info("=" * 60)
                result = await service.sync_market_data()
                logger.info(f"同步完成: {result}")
            else:
                logger.info("今天不是交易日，跳过行情数据同步")
        
        elif task == 'financial':
            # 同步财务数据
            logger.info("=" * 60)
            logger.info("开始同步A股财务指标数据")
            logger.info("=" * 60)
            result = await service.sync_financial_data()
            logger.info(f"同步完成: {result}")
        
        elif task == 'index':
            # 同步大盘数据
            logger.info("=" * 60)
            logger.info("开始同步大盘指标数据")
            logger.info("=" * 60)
            result = await service.sync_index_data()
            logger.info(f"同步完成: {result}")
        
        elif task == 'all':
            # 同步全部数据
            logger.info("=" * 60)
            logger.info("开始同步全部数据")
            logger.info("=" * 60)
            
            results = {}
            
            # 行情数据（仅交易日）
            if service.is_trading_day():
                results['market'] = await service.sync_market_data()
            else:
                logger.info("今天不是交易日，跳过行情数据同步")
                results['market'] = {"skipped": "非交易日"}
            
            # 财务数据
            results['financial'] = await service.sync_financial_data()
            
            # 大盘数据
            results['index'] = await service.sync_index_data()
            
            logger.info("=" * 60)
            logger.info("全部同步完成:")
            for task_name, result in results.items():
                logger.info(f"  {task_name}: {result.get('success', False)}")
        
        elif task == 'check':
            # 检查是否为交易日
            is_trading = service.is_trading_day()
            today = datetime.now().strftime('%Y-%m-%d')
            logger.info(f"今天 {today} {'是' if is_trading else '不是'}交易日")
        
        else:
            print(__doc__)
    
    else:
        print(__doc__)


if __name__ == '__main__':
    asyncio.run(main())