"""
配置管理
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # 应用配置
    APP_NAME: str = "Smart Quant"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = True
    
    # 数据库配置
    DATABASE_URL: str = "sqlite:///./smart_quant.db"
    
    # 数据源配置
    TUSHARE_TOKEN: str = ""  # TuShare Pro token (可选)
    
    # 通知配置
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    
    class Config:
        env_file = ".env"


settings = Settings()

# 导出最佳因子配置
from app.config.best_factor_config import (
    MARKET_TIMING_CONFIG,
    FACTOR_CONFIG,
    POSITION_CONFIG,
    BACKTEST_CONFIG,
    VALIDATION_CRITERIA,
    get_factor_weights,
    get_position_adjustment,
    get_signal_adjustment,
    print_config
)