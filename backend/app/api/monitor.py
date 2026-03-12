"""
监控相关API
"""
from fastapi import APIRouter, HTTPException
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

from sqlalchemy.orm import Session
from app.database import SessionLocal, Base, engine
from app.models import Stock, Signal, MonitorConfig
from app.services.data import DataService

router = APIRouter(prefix="/api/monitor", tags=["股票监控"])


class AddMonitorRequest(BaseModel):
    """添加监控请求"""
    stock_code: str
    strategy_id: Optional[str] = None  # 如果为空，需要从回测页面添加


class MonitorConfigRequest(BaseModel):
    """监控配置请求"""
    stock_code: str
    stock_name: str
    strategy_id: str
    strategy_name: str


# 确保新表创建
Base.metadata.create_all(bind=engine)


@router.get("/list")
async def get_monitor_list():
    """获取监控列表（按股票+策略组合）"""
    db: Session = SessionLocal()
    try:
        # 查询所有活跃的监控配置
        configs = db.query(MonitorConfig).filter(MonitorConfig.is_active == True).all()
        
        if not configs:
            return {"monitors": []}
        
        # 获取所有股票代码
        codes = list(set([c.stock_code for c in configs]))
        data_service = DataService()
        realtime_data = await data_service._fetch_sina_data(codes)
        
        monitors = []
        for config in configs:
            info = realtime_data.get(config.stock_code, {})
            monitors.append({
                "id": config.id,
                "stock_code": config.stock_code,
                "stock_name": config.stock_name or info.get("名称", config.stock_code),
                "strategy_id": config.strategy_id,
                "strategy_name": config.strategy_name,
                "price": info.get("最新价", 0),
                "change": info.get("涨跌幅", 0),
                "volume": info.get("成交量", 0),
                "is_active": config.is_active
            })
        
        return {"monitors": monitors}
    finally:
        db.close()


@router.post("/add")
async def add_monitor(request: AddMonitorRequest):
    """添加股票到监控（简单模式，不指定策略）"""
    db: Session = SessionLocal()
    try:
        stock = db.query(Stock).filter(Stock.code == request.stock_code).first()
        
        if not stock:
            data_service = DataService()
            info = await data_service.get_stock_info(request.stock_code)
            stock = Stock(
                code=request.stock_code,
                name=info.get("名称", request.stock_code),
                exchange="SH" if request.stock_code.startswith("6") else "SZ",
                is_monitoring=True
            )
            db.add(stock)
        else:
            stock.is_monitoring = True
        
        db.commit()
        
        return {
            "success": True,
            "message": f"已添加 {stock.name} 到监控列表",
            "stock": {"code": stock.code, "name": stock.name}
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/add-config")
async def add_monitor_config(request: MonitorConfigRequest):
    """添加监控配置（股票+策略组合）"""
    db: Session = SessionLocal()
    try:
        # 检查是否已存在
        existing = db.query(MonitorConfig).filter(
            MonitorConfig.stock_code == request.stock_code,
            MonitorConfig.strategy_id == request.strategy_id,
            MonitorConfig.is_active == True
        ).first()
        
        if existing:
            return {
                "success": True,
                "message": f"{request.stock_name} + {request.strategy_name} 已在监控中",
                "config_id": existing.id
            }
        
        # 创建新配置
        config = MonitorConfig(
            stock_code=request.stock_code,
            stock_name=request.stock_name,
            strategy_id=request.strategy_id,
            strategy_name=request.strategy_name,
            is_active=True
        )
        db.add(config)
        db.commit()
        
        return {
            "success": True,
            "message": f"已添加 {request.stock_name} + {request.strategy_name} 到监控",
            "config_id": config.id
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/remove-config/{config_id}")
async def remove_monitor_config(config_id: int):
    """移除监控配置"""
    db: Session = SessionLocal()
    try:
        config = db.query(MonitorConfig).filter(MonitorConfig.id == config_id).first()
        
        if not config:
            raise HTTPException(status_code=404, detail="监控配置不存在")
        
        config.is_active = False
        db.commit()
        
        return {"success": True, "message": f"已移除 {config.stock_name} 的监控"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/remove")
async def remove_monitor(request: AddMonitorRequest):
    """从监控列表移除股票（旧接口，保留兼容）"""
    db: Session = SessionLocal()
    try:
        stock = db.query(Stock).filter(Stock.code == request.stock_code).first()
        if stock:
            stock.is_monitoring = False
        
        # 同时移除该股票的所有监控配置
        configs = db.query(MonitorConfig).filter(
            MonitorConfig.stock_code == request.stock_code,
            MonitorConfig.is_active == True
        ).all()
        for config in configs:
            config.is_active = False
        
        db.commit()
        
        return {"success": True, "message": f"已从监控列表移除"}
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
        configs = db.query(MonitorConfig).filter(MonitorConfig.is_active == True).all()
        
        if not configs:
            return {"quotes": []}
        
        codes = list(set([c.stock_code for c in configs]))
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
    """检查监控配置的信号（只检测已配置的策略）"""
    from app.services.strategy import get_strategy
    
    db: Session = SessionLocal()
    try:
        # 获取所有活跃的监控配置
        configs = db.query(MonitorConfig).filter(MonitorConfig.is_active == True).all()
        
        if not configs:
            return {"signals": [], "message": "没有监控配置"}
        
        data_service = DataService()
        new_signals = []
        
        for config in configs:
            try:
                # 获取历史数据
                df = await data_service.get_stock_history(config.stock_code)
                if df.empty:
                    continue
                
                # 使用配置的策略检测信号
                strategy = get_strategy(config.strategy_id)
                df_with_signals = strategy.generate_signals(df)
                
                last_signal = df_with_signals['signal'].iloc[-1]
                last_price = df_with_signals['收盘' if '收盘' in df_with_signals.columns else 'close'].iloc[-1]
                
                signal_type = None
                reason = None
                
                if last_signal == 1:
                    signal_type = "buy"
                    reason = f"{config.strategy_name}发出买入信号"
                elif last_signal == -1:
                    signal_type = "sell"
                    reason = f"{config.strategy_name}发出卖出信号"
                
                if signal_type:
                    signal = Signal(
                        stock_code=config.stock_code,
                        signal_type=signal_type,
                        price=last_price,
                        reason=reason
                    )
                    db.add(signal)
                    new_signals.append({
                        "stock_code": config.stock_code,
                        "stock_name": config.stock_name,
                        "strategy": config.strategy_name,
                        "signal_type": signal_type,
                        "price": last_price,
                        "reason": reason
                    })
            except Exception as e:
                print(f"检测信号失败: {config.stock_code} - {e}")
        
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