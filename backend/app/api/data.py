"""
数据信息API
"""
from fastapi import APIRouter, Query
from app.services.data import DataService
import os
import pandas as pd

router = APIRouter(prefix="/api/data", tags=["数据"])

@router.get("/info")
async def get_data_info(
    code: str = Query(..., description="股票代码"),
    freq: str = Query("day", description="数据频率: day/hour/minute")
):
    """
    获取数据信息（开始日期、结束日期、数据量）
    """
    # 映射频率到缓存目录
    freq_map = {
        "day": ("day", "_day.csv"),
        "60min": ("hour", "_60min.csv"),
        "hour": ("hour", "_60min.csv"),
        "1min": ("minute", "_1min.csv"),
        "minute": ("minute", "_1min.csv"),
    }
    
    freq_key, suffix = freq_map.get(freq, ("day", "_day.csv"))
    cache_file = f"data_cache/{freq_key}/{code}{suffix}"
    
    if not os.path.exists(cache_file):
        return {
            "status": "error",
            "message": f"未找到 {code} 的 {freq} 数据"
        }
    
    try:
        df = pd.read_csv(cache_file)
        
        # 处理日期格式
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'].astype(str))
        elif '时间' in df.columns:
            df['日期'] = pd.to_datetime(df['时间'])
        
        df = df.sort_values('日期').reset_index(drop=True)
        
        return {
            "status": "success",
            "code": code,
            "freq": freq,
            "count": len(df),
            "start_date": df['日期'].iloc[0].strftime('%Y-%m-%d'),
            "end_date": df['日期'].iloc[-1].strftime('%Y-%m-%d'),
            "start_price": float(df['收盘'].iloc[0]),
            "end_price": float(df['收盘'].iloc[-1])
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }