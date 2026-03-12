"""
选股相关API
"""
from fastapi import APIRouter, HTTPException
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import Stock
from app.services.data import DataService

router = APIRouter(prefix="/api/picker", tags=["智能选股"])


class PickerCondition(BaseModel):
    """选股条件"""
    # 价格条件
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    # 涨跌幅条件
    change_min: Optional[float] = None
    change_max: Optional[float] = None
    # 成交量条件（万手）
    volume_min: Optional[float] = None
    volume_max: Optional[float] = None
    # 行业
    industry: Optional[str] = None
    # 交易所
    exchange: Optional[str] = None  # SH, SZ, BJ
    # 策略选股
    strategy_id: Optional[str] = None
    signal_type: Optional[str] = None  # buy, sell


class PickResult(BaseModel):
    """选股结果"""
    code: str
    name: str
    price: float
    change: float
    volume: float
    exchange: str
    signal: Optional[str] = None


@router.post("/condition")
async def pick_by_condition(condition: PickerCondition):
    """条件选股"""
    from app.services.data import DataService
    
    db: Session = SessionLocal()
    try:
        # 1. 确定要查询的股票代码范围
        codes_to_query = None
        
        # 如果选择了行业，使用预定义的行业股票列表
        if condition.industry:
            industry_stocks = DataService.INDUSTRY_STOCKS.get(condition.industry, [])
            if not industry_stocks:
                return {"results": [], "count": 0, "message": f"未找到行业 [{condition.industry}] 的股票数据"}
            codes_to_query = industry_stocks
        
        # 2. 从数据库获取股票列表
        query = db.query(Stock)
        
        if condition.exchange:
            query = query.filter(Stock.exchange == condition.exchange)
        
        if codes_to_query:
            # 过滤出在指定行业内的股票
            query = query.filter(Stock.code.in_(codes_to_query))
        
        stocks = query.limit(500).all()
        
        if not stocks:
            return {"results": [], "count": 0, "message": "没有符合条件的股票"}
        
        # 3. 获取实时行情
        codes = [s.code for s in stocks]
        data_service = DataService()
        realtime_data = await data_service._fetch_sina_data(codes)
        
        # 4. 筛选符合条件的股票
        results = []
        for stock in stocks:
            info = realtime_data.get(stock.code)
            if not info:
                continue
            
            price = info.get('最新价', 0)
            change = info.get('涨跌幅', 0)
            volume = info.get('成交量', 0) / 10000  # 转换为万手
            
            # 价格筛选
            if condition.price_min and price < condition.price_min:
                continue
            if condition.price_max and price > condition.price_max:
                continue
            
            # 涨跌幅筛选
            if condition.change_min is not None and change < condition.change_min:
                continue
            if condition.change_max is not None and change > condition.change_max:
                continue
            
            # 成交量筛选
            if condition.volume_min and volume < condition.volume_min:
                continue
            if condition.volume_max and volume > condition.volume_max:
                continue
            
            results.append({
                "code": stock.code,
                "name": stock.name or info.get('名称', stock.code),
                "price": price,
                "change": change,
                "volume": round(volume, 2),
                "exchange": stock.exchange,
                "industry": condition.industry or stock.industry
            })
        
        # 4. 按涨跌幅排序
        results.sort(key=lambda x: x['change'], reverse=True)
        
        return {
            "results": results[:100],  # 限制返回100条
            "count": len(results),
            "message": f"找到 {len(results)} 只符合条件的股票"
        }
    finally:
        db.close()


@router.post("/strategy")
async def pick_by_strategy(strategy_id: str, signal_type: str = "buy", limit: int = 50):
    """策略选股 - 用策略信号筛选股票"""
    from app.services.strategy import get_strategy
    
    db: Session = SessionLocal()
    try:
        # 1. 获取所有股票
        stocks = db.query(Stock).limit(300).all()  # 限制300只避免超时
        
        if not stocks:
            return {"results": [], "count": 0, "message": "没有股票数据"}
        
        # 2. 获取策略
        try:
            strategy = get_strategy(strategy_id)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"策略不存在: {strategy_id}")
        
        # 3. 遍历股票检测信号
        data_service = DataService()
        results = []
        
        for stock in stocks:
            try:
                # 获取历史数据
                df = await data_service.get_stock_history(stock.code)
                if df.empty:
                    continue
                
                # 生成信号
                df_with_signals = strategy.generate_signals(df)
                
                # 获取最新信号
                last_signal = df_with_signals['signal'].iloc[-1]
                last_price = df_with_signals['收盘' if '收盘' in df_with_signals.columns else 'close'].iloc[-1]
                
                # 筛选符合条件的信号
                if signal_type == "buy" and last_signal == 1:
                    results.append({
                        "code": stock.code,
                        "name": stock.name,
                        "price": round(last_price, 2),
                        "signal": "buy",
                        "strategy": strategy.name
                    })
                elif signal_type == "sell" and last_signal == -1:
                    results.append({
                        "code": stock.code,
                        "name": stock.name,
                        "price": round(last_price, 2),
                        "signal": "sell",
                        "strategy": strategy.name
                    })
            except Exception as e:
                print(f"策略选股失败 {stock.code}: {e}")
                continue
        
        return {
            "results": results[:limit],
            "count": len(results),
            "message": f"找到 {len(results)} 只发出{signal_type}信号的股票"
        }
    finally:
        db.close()


@router.get("/industries")
async def get_industries():
    """获取行业列表"""
    return {
        "industries": [
            "银行", "证券", "保险", "医药", "科技", 
            "地产", "新能源", "消费", "汽车", "军工",
            "化工", "钢铁", "煤炭", "有色", "电力"
        ]
    }


@router.post("/batch-monitor")
async def batch_add_monitor(codes: List[str]):
    """批量添加到监控"""
    db: Session = SessionLocal()
    try:
        added = 0
        for code in codes:
            stock = db.query(Stock).filter(Stock.code == code).first()
            if stock and not stock.is_monitoring:
                stock.is_monitoring = True
                added += 1
        
        db.commit()
        
        return {
            "success": True,
            "added": added,
            "message": f"已添加 {added} 只股票到监控列表"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()