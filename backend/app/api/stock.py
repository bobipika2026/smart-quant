"""
股票相关API
"""
from fastapi import APIRouter, HTTPException
from typing import List, Optional

from app.services.data import DataService

router = APIRouter(prefix="/api/stock", tags=["股票数据"])


@router.get("/search")
async def search_stock(keyword: str):
    """搜索股票"""
    data_service = DataService()
    df = await data_service.search_stocks(keyword)
    
    if df.empty:
        return {"stocks": []}
    
    stocks = df[['代码', '名称', '最新价', '涨跌幅', '成交量']].to_dict('records')
    return {"stocks": stocks}


@router.get("/quote/{code}")
async def get_stock_quote(code: str):
    """获取股票实时行情"""
    data_service = DataService()
    
    # 获取实时行情
    df = await data_service.get_realtime_quotes([code])
    
    if df.empty:
        raise HTTPException(status_code=404, detail="未找到股票")
    
    stock = df[df['代码'] == code].iloc[0].to_dict()
    return {"quote": stock}


@router.get("/history/{code}")
async def get_stock_history(
    code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """获取股票历史数据"""
    data_service = DataService()
    df = await data_service.get_stock_history(
        code=code,
        start_date=start_date,
        end_date=end_date
    )
    
    if df.empty:
        raise HTTPException(status_code=404, detail="未找到股票数据")
    
    # 转换为列表返回
    records = df.to_dict('records')
    return {
        "code": code,
        "count": len(records),
        "data": records[:500]  # 限制返回数量
    }


@router.get("/info/{code}")
async def get_stock_info(code: str):
    """获取股票基本信息"""
    data_service = DataService()
    info = await data_service.get_stock_info(code)
    
    if not info:
        raise HTTPException(status_code=404, detail="未找到股票信息")
    
    return {"info": info}


@router.get("/quotes")
async def get_realtime_quotes():
    """获取全部A股实时行情"""
    data_service = DataService()
    df = await data_service.get_realtime_quotes()
    
    if df.empty:
        return {"stocks": [], "count": 0}
    
    return {
        "count": len(df),
        "stocks": df.head(100).to_dict('records')  # 只返回前100条
    }