"""
Smart Quant - 策略交易平台
"""
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import strategy, stock, sync, monitor, picker, factor, factor_matrix
from app.database import init_db
from app.scheduler import start_scheduler, stop_scheduler

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Smart Quant API",
    description="开源策略交易平台 API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(strategy.router)
app.include_router(stock.router)
app.include_router(sync.router)
app.include_router(monitor.router)
app.include_router(picker.router)
app.include_router(factor.router)
app.include_router(factor_matrix.router)


@app.on_event("startup")
async def startup():
    """启动时初始化数据库和定时任务"""
    init_db()
    start_scheduler()
    logger.info("[Smart Quant] 服务启动完成")


@app.on_event("shutdown")
async def shutdown():
    """关闭时停止定时任务"""
    stop_scheduler()
    logger.info("[Smart Quant] 服务已关闭")


@app.get("/")
async def root():
    return {"message": "Smart Quant API", "version": "0.1.0"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)