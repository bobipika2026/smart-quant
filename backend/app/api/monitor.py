"""
监控相关API
"""
from fastapi import APIRouter, HTTPException
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import Stock, Signal
from app.services.data import DataService

router = APIRouter(prefix="/api/monitor", tags=["股票监控"])


class MonitorStock(BaseModel):
    """监控股票请求"""
    code: str


class SignalResponse(BaseModel):
    """信号响应"""
    id: int
    stock_code: str
    stock_name: str
    signal_type: str
    price: float
    reason: str
    created_at: str


@router.get("/list")
async def get_monitor_list():
    """获取监控列表"""
    db: Session = SessionLocal()
    try:
        # 查询所有正在监控的股票
        stocks = db.query(Stock).filter(Stock.is_monitoring == True).all()
        
        if not stocks:
            return {"monitors": []}
        
        # 获取实时行情
        codes = [s.code for s in stocks]
        data_service = DataService()
        realtime_data = await data_service._fetch_sina_data(codes)
        
        monitors = []
        for stock in stocks:
            info = realtime_data.get(stock.code, {})
            monitors.append({
                "code": stock.code,
                "name": stock.name or info.get("名称", "未知"),
                "exchange": stock.exchange,
                "price": info.get("最新价", 0),
                "change": info.get("涨跌幅", 0),
                "volume": info.get("成交量", 0),
                "is_monitoring": True
            })
        
        return {"monitors": monitors}
    finally:
        db.close()


@router.post("/add")
async def add_monitor(request: MonitorStock):
    """添加股票到监控列表"""
    db: Session = SessionLocal()
    try:
        # 查找股票
        stock = db.query(Stock).filter(Stock.code == request.code).first()
        
        if not stock:
            # 如果数据库没有，创建新记录
            data_service = DataService()
            info = await data_service.get_stock_info(request.code)
            
            stock = Stock(
                code=request.code,
                name=info.get("名称", request.code),
                exchange="SH" if request.code.startswith("6") else "SZ",
                is_monitoring=True
            )
            db.add(stock)
        else:
            stock.is_monitoring = True
        
        db.commit()
        
        return {
            "success": True,
            "message": f"已添加 {stock.name} 到监控列表",
            "stock": {
                "code": stock.code,
                "name": stock.name
            }
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/remove")
async def remove_monitor(request: MonitorStock):
    """从监控列表移除股票"""
    db: Session = SessionLocal()
    try:
        stock = db.query(Stock).filter(Stock.code == request.code).first()
        
        if not stock:
            raise HTTPException(status_code=404, detail="股票不存在")
        
        stock.is_monitoring = False
        db.commit()
        
        return {
            "success": True,
            "message": f"已从监控列表移除 {stock.name}"
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.get("/quotes")
async def get_monitor_quotes():
    """获取监控股票实时行情"""
    db: Session = SessionLocal()
    try:
        stocks = db.query(Stock).filter(Stock.is_monitoring == True).all()
        
        if not stocks:
            return {"quotes": []}
        
        codes = [s.code for s in stocks]
        data_service = DataService()
        df = await data_service.get_realtime_quotes(codes)
        
        if df.empty:
            return {"quotes": []}
        
        quotes = df.to_dict('records')
        return {"quotes": quotes}
    finally:
        db.close()


@router.get("/signals")
async def get_signals(limit: int = 20):
    """获取最近的交易信号"""
    db: Session = SessionLocal()
    try:
        signals = db.query(Signal).order_by(Signal.created_at.desc()).limit(limit).all()
        
        result = []
        for signal in signals:
            # 获取股票名称
            stock = db.query(Stock).filter(Stock.code == signal.stock_code).first()
            stock_name = stock.name if stock else signal.stock_code
            
            result.append({
                "id": signal.id,
                "stock_code": signal.stock_code,
                "stock_name": stock_name,
                "signal_type": signal.signal_type,
                "price": signal.price,
                "reason": signal.reason,
                "created_at": signal.created_at.strftime("%Y-%m-%d %H:%M:%S") if signal.created_at else ""
            })
        
        return {"signals": result}
    finally:
        db.close()


@router.post("/check-signals")
async def check_signals():
    """检查监控股票的信号（手动触发）"""
    from app.services.strategy import list_strategies, get_strategy
    
    db: Session = SessionLocal()
    try:
        # 获取监控中的股票
        stocks = db.query(Stock).filter(Stock.is_monitoring == True).all()
        
        if not stocks:
            return {"signals": [], "message": "没有监控中的股票"}
        
        data_service = DataService()
        strategies = list_strategies()
        new_signals = []
        
        for stock in stocks:
            # 获取最近历史数据
            df = await data_service.get_stock_history(stock.code)
            if df.empty:
                continue
            
            # 对每个策略检测信号
            for strategy_info in strategies[:3]:  # 只用前3个策略
                try:
                    strategy = get_strategy(strategy_info['id'])
                    df_with_signals = strategy.generate_signals(df)
                    
                    # 获取最新信号
                    last_signal = df_with_signals['signal'].iloc[-1]
                    last_price = df_with_signals['收盘' if '收盘' in df_with_signals.columns else 'close'].iloc[-1]
                    
                    signal_type = None
                    reason = None
                    
                    if last_signal == 1:
                        signal_type = "buy"
                        reason = f"{strategy.name}发出买入信号"
                    elif last_signal == -1:
                        signal_type = "sell"
                        reason = f"{strategy.name}发出卖出信号"
                    
                    if signal_type:
                        # 保存信号
                        signal = Signal(
                            stock_code=stock.code,
                            signal_type=signal_type,
                            price=last_price,
                            reason=reason
                        )
                        db.add(signal)
                        new_signals.append({
                            "stock_code": stock.code,
                            "stock_name": stock.name,
                            "signal_type": signal_type,
                            "price": last_price,
                            "reason": reason
                        })
                except Exception as e:
                    print(f"检测信号失败: {stock.code} - {e}")
        
        db.commit()
        
        return {
            "signals": new_signals,
            "message": f"检测完成，发现 {len(new_signals)} 个新信号"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()