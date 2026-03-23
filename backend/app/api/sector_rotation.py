"""
板块轮动监控 API v1.8

接口列表:
- GET /api/sector-rotation/report          # 综合轮动报告
- GET /api/sector-rotation/signals         # 轮动信号列表
- GET /api/sector-rotation/strength        # 板块强度排名
- GET /api/sector-rotation/fund-flow       # 板块资金流向
- GET /api/sector-rotation/valuation       # 板块估值数据
- GET /api/sector-rotation/sector/{name}   # 单板块详情
- GET /api/sector-rotation/weights/{name}  # 板块因子权重
- GET /api/sector-rotation/position/{name}/{signal}  # 仓位建议
"""
from fastapi import APIRouter, HTTPException, Path, Query
from typing import Optional
import logging

from app.services.sector_rotation import get_sector_rotation_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sector-rotation", tags=["板块轮动"])


@router.get("/report")
async def get_rotation_report():
    """
    获取板块轮动综合报告
    
    包括:
    - 轮动信号（强势启动/底部反转/持续强势/高位风险）
    - 板块强度排名
    - 板块资金流向
    - 板块估值数据
    """
    try:
        service = get_sector_rotation_service()
        report = await service.get_rotation_report()
        return {"success": True, "data": report}
    except Exception as e:
        logger.error(f"[API] 获取轮动报告失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/report/local")
async def get_local_data_report():
    """
    基于本地缓存数据的报告
    
    数据源: data_cache/sector/
    - 行业资金流向排名
    - 北向资金趋势
    - 行业股票分布
    
    需要先执行数据同步: python -m app.services.sector_data_sync all
    """
    try:
        service = get_sector_rotation_service()
        report = await service.get_local_data_report()
        return {"success": True, "data": report}
    except Exception as e:
        logger.error(f"[API] 获取本地数据报告失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync")
async def sync_sector_data(task: str = "all"):
    """
    同步板块数据到本地缓存
    
    任务类型:
    - all: 同步所有数据
    - industry: 同步股票行业分类
    - moneyflow: 同步资金流向
    - north: 同步北向资金
    - concept: 同步概念板块
    """
    try:
        from app.services.sector_data_sync import SectorDataSyncService
        service = SectorDataSyncService()
        
        if task == 'all':
            result = service.sync_all()
        elif task == 'industry':
            result = service.sync_stock_industry_mapping()
        elif task == 'moneyflow':
            result = service.sync_moneyflow_data()
        elif task == 'north':
            result = service.sync_north_money_data()
        elif task == 'concept':
            result = service.sync_concept_data()
        else:
            raise HTTPException(status_code=400, detail=f"未知任务: {task}")
        
        return {"success": True, "data": result}
    except Exception as e:
        logger.error(f"[API] 数据同步失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/signals")
async def get_rotation_signals():
    """
    获取板块轮动信号
    
    信号类型:
    - strong_start: 强势启动（排名快速上升）
    - bottom_reversal: 底部反转（长期低位开始回升）
    - sustained_strong: 持续强势（排名持续靠前）
    - high_risk: 高位风险（涨幅过大+资金流出）
    """
    try:
        service = get_sector_rotation_service()
        signals = await service.detect_rotation_signals()
        return {"success": True, "data": signals}
    except Exception as e:
        logger.error(f"[API] 获取轮动信号失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strength")
async def get_sector_strength(days: int = Query(20, description="计算周期（天）")):
    """
    获取板块相对强度排名
    
    参数:
    - days: 计算周期，默认20天
    
    返回:
    - 板块排名、涨幅、强度指标
    """
    try:
        service = get_sector_rotation_service()
        df = await service.calculate_sector_strength(days)
        return {
            "success": True,
            "data": df.to_dict('records'),
            "total": len(df)
        }
    except Exception as e:
        logger.error(f"[API] 获取板块强度失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fund-flow")
async def get_sector_fund_flow():
    """
    获取板块资金流向
    
    包括:
    - 主力净流入
    - 超大单/大单/中单/小单流向
    """
    try:
        service = get_sector_rotation_service()
        df = await service.get_sector_fund_flow()
        return {
            "success": True,
            "data": df.to_dict('records'),
            "total": len(df)
        }
    except Exception as e:
        logger.error(f"[API] 获取资金流向失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/valuation")
async def get_sector_valuation():
    """
    获取板块估值数据（申万一级行业）
    
    包括:
    - 市盈率（静态/TTM）
    - 市净率
    - 股息率
    - 成分股数量
    """
    try:
        service = get_sector_rotation_service()
        df = await service.get_sw_index_realtime()
        return {
            "success": True,
            "data": df.to_dict('records'),
            "total": len(df)
        }
    except Exception as e:
        logger.error(f"[API] 获取估值数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sector/{sector_name}")
async def get_sector_detail(sector_name: str = Path(..., description="板块名称，如：电子、银行")):
    """
    获取单个板块详细信息
    
    包括:
    - 因子权重配置
    - 估值数据
    - 技术指标
    - 历史行情
    """
    try:
        service = get_sector_rotation_service()
        detail = await service.get_sector_detail(sector_name)
        return {"success": True, "data": detail}
    except Exception as e:
        logger.error(f"[API] 获取板块详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/weights/{sector_name}")
async def get_sector_factor_weights(sector_name: str = Path(..., description="板块名称")):
    """
    获取板块因子权重配置
    
    基于v1.7行业轮动策略:
    - 科技: 动量40%
    - 消费: ROE 35%
    - 金融: EP 35%
    - 周期: BP 35%
    - 制造: MOM+ROE各25%
    """
    try:
        service = get_sector_rotation_service()
        weights = service.get_industry_factor_weights(sector_name)
        return {"success": True, "data": weights}
    except Exception as e:
        logger.error(f"[API] 获取因子权重失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/position/{sector_name}/{signal_type}")
async def get_position_suggestion(
    sector_name: str = Path(..., description="板块名称"),
    signal_type: str = Path(..., description="信号类型: bottom_reversal/strong_start/sustained_strong/high_risk")
):
    """
    获取仓位建议
    
    信号类型:
    - bottom_reversal: 底部反转，底仓20%+突破加仓30%
    - strong_start: 强势启动，追仓40%+回调加仓20%
    - sustained_strong: 持续强势，轻仓20%
    - high_risk: 高位风险，建议清仓
    """
    valid_signals = ['bottom_reversal', 'strong_start', 'sustained_strong', 'high_risk']
    if signal_type not in valid_signals:
        raise HTTPException(status_code=400, detail=f"无效信号类型，可选: {valid_signals}")
    
    try:
        service = get_sector_rotation_service()
        suggestion = service.get_position_suggestion(sector_name, signal_type)
        return {"success": True, "data": suggestion}
    except Exception as e:
        logger.error(f"[API] 获取仓位建议失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check():
    """健康检查"""
    return {
        "status": "healthy",
        "service": "sector_rotation",
        "version": "v1.8"
    }


@router.get("/industries")
async def get_industry_list():
    """
    获取申万一级行业列表
    
    返回31个一级行业代码和名称
    """
    from app.services.sector_rotation import SectorRotationService
    return {
        "success": True,
        "data": SectorRotationService.SW_INDUSTRIES,
        "total": len(SectorRotationService.SW_INDUSTRIES)
    }


@router.get("/styles")
async def get_industry_styles():
    """
    获取行业风格分类
    
    8大风格:
    - 科技、消费、金融、周期、制造、医药、公用、其他
    """
    from app.services.sector_rotation import SectorRotationService
    return {
        "success": True,
        "data": SectorRotationService.INDUSTRY_STYLE,
        "total": len(SectorRotationService.INDUSTRY_STYLE)
    }