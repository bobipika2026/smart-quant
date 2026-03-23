"""
股票评分API接口
"""
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from datetime import datetime
import os

from app.services.stock_scoring import get_scoring_service
from app.services.stock_scoring_pro import get_pro_scoring_service
from app.services.stock_scoring_v2 import get_pro_scoring_v2
from app.services.stock_scoring_v3 import get_scoring_v3

router = APIRouter(prefix="/api/stock-scoring", tags=["股票评分"])


# ==================== 专业评分接口 v3 (严格量化标准) ====================

@router.get("/v3/pool")
async def get_v3_stock_pool(
    top_n: int = Query(50, ge=10, le=100, description="股票池大小"),
    min_score: float = Query(0, ge=0, le=100, description="最低综合评分"),
    min_value: Optional[float] = Query(None, ge=0, le=100, description="价值得分下限"),
    min_growth: Optional[float] = Query(None, ge=0, le=100, description="成长得分下限"),
    min_quality: Optional[float] = Query(None, ge=0, le=100, description="质量得分下限"),
    industry: Optional[str] = Query(None, description="行业筛选"),
    grade: Optional[str] = Query(None, description="评级筛选(多个用逗号分隔)")
):
    """
    专业因子评分 v3 - 严格量化标准
    
    特性：
    - 三层级因子结构（风格 -> 细分因子）
    - 行业内百分位评分
    - MAD去极值 + Z-Score标准化
    - 基于IC优化的权重配置
    - 申万一级行业分类
    
    权重配置：
    - 价值 22% (基于IC ~0.04)
    - 成长 18% (基于IC ~0.035)
    - 质量 28% (基于IC ~0.05)
    - 动量 17% (基于IC ~0.045)
    - 情绪 15% (基于IC ~0.035)
    """
    service = get_scoring_v3()
    
    filters = {}
    if min_value is not None:
        filters['min_value'] = min_value
    if min_growth is not None:
        filters['min_growth'] = min_growth
    if min_quality is not None:
        filters['min_quality'] = min_quality
    if industry:
        filters['industry'] = industry
    if grade:
        filters['grade'] = grade.split(',')
    
    result = service.generate_stock_pool(top_n=top_n, min_score=min_score, filters=filters if filters else None)
    
    stocks = []
    for i, stock in enumerate(result['stocks'], 1):
        stocks.append({
            "rank": i,
            "stock_code": stock['stock_code'],
            "stock_name": stock['stock_name'],
            "industry": stock.get('industry', ''),
            "composite_score": stock['composite_score'],
            "grade": stock['grade'],
            "value_score": stock['style_scores']['value'],
            "growth_score": stock['style_scores']['growth'],
            "quality_score": stock['style_scores']['quality'],
            "momentum_score": stock['style_scores']['momentum'],
            "sentiment_score": stock['style_scores']['sentiment'],
            "trade_date": stock.get('trade_date'),
            "close": stock.get('close'),
        })
    
    # 行业分布
    industry_dist = {}
    for s in result['stocks']:
        ind = s.get('industry', '其他')
        industry_dist[ind] = industry_dist.get(ind, 0) + 1
    
    return {
        "code": 0,
        "data": {
            "stocks": stocks,
            "generated_at": result['generated_at'],
            "total_analyzed": result['total_count'],
            "qualified_count": result['qualified_count'],
            "pool_size": len(stocks),
            "industry_distribution": industry_dist,
            "filters_applied": filters if filters else None,
            "factor_model": {
                "value": 0.22,
                "growth": 0.18,
                "quality": 0.28,
                "momentum": 0.17,
                "sentiment": 0.15
            },
            "factor_count": {
                "value": 6,
                "growth": 6,
                "quality": 9,
                "momentum": 6,
                "sentiment": 6
            },
            "standard": "Barra/MSCI量化标准"
        },
        "message": "success"
    }


@router.get("/v3/score/{stock_code}")
async def get_v3_stock_score(stock_code: str):
    """
    获取单只股票的专业评分详情 v3
    
    包含完整因子明细
    """
    service = get_scoring_v3()
    factor_pool = service.build_factor_pool()
    result = service.calculate_score(stock_code, factor_pool)
    
    if result is None:
        raise HTTPException(status_code=404, detail=f"股票 {stock_code} 数据不足")
    
    return {
        "code": 0,
        "data": result,
        "message": "success"
    }


@router.get("/v3/industries")
async def get_v3_industries():
    """获取行业列表（申万一级行业）"""
    service = get_scoring_v3()
    industries = list(service.INDUSTRY_CODES.keys())
    return {
        "code": 0,
        "data": {
            "industries": sorted(industries),
            "count": len(industries)
        },
        "message": "success"
    }


@router.get("/v3/factors")
async def get_factor_definitions():
    """获取因子定义"""
    service = get_scoring_v3()
    
    def format_factors(factors):
        return [{
            "name": f.name,
            "code": f.code,
            "weight": f.weight,
            "ic_expected": f.ic_expected,
            "direction": "正向" if f.direction == 1 else "反向" if f.direction == -1 else "中性"
        } for f in factors]
    
    return {
        "code": 0,
        "data": {
            "value_factors": format_factors(service.VALUE_FACTORS),
            "growth_factors": format_factors(service.GROWTH_FACTORS),
            "quality_factors": format_factors(service.QUALITY_FACTORS),
            "momentum_factors": format_factors(service.MOMENTUM_FACTORS),
            "sentiment_factors": format_factors(service.SENTIMENT_FACTORS),
            "style_weights": service.STYLE_WEIGHTS
        },
        "message": "success"
    }


@router.get("/v3/dynamic-weights")
async def get_dynamic_weights():
    """
    获取动态权重配置
    
    根据市场环境、经济周期、风险偏好动态调整因子权重
    
    返回：
    - 当前市场环境（牛/熊/震荡/反转）
    - 经济周期（复苏/繁荣/衰退/萧条）
    - 风险偏好指数
    - 动态调整后的权重
    """
    from app.services.dynamic_weights import get_dynamic_weight_system
    
    system = get_dynamic_weight_system()
    result = system.calc_dynamic_weights()
    
    return {
        "code": 0,
        "data": result,
        "message": "success"
    }


@router.get("/v3/pool-with-dynamic-weights")
async def get_pool_with_dynamic_weights(
    top_n: int = Query(50, ge=10, le=100),
    use_dynamic: bool = Query(True, description="是否使用动态权重")
):
    """
    使用动态权重生成股票池
    
    - **use_dynamic**: 是否使用动态权重（False则使用基准权重）
    """
    from app.services.dynamic_weights import get_dynamic_weight_system
    
    service = get_scoring_v3()
    weight_system = get_dynamic_weight_system()
    
    # 获取动态权重
    dynamic_config = weight_system.calc_dynamic_weights()
    dynamic_weights = dynamic_config['weights']
    
    # 构建因子池
    factor_pool = service.build_factor_pool()
    
    # 临时更新权重
    if use_dynamic:
        original_weights = service.STYLE_WEIGHTS.copy()
        service.STYLE_WEIGHTS.update(dynamic_weights)
    
    results = []
    for code in service.stock_codes:
        try:
            score_data = service.calculate_score(code, factor_pool)
            if score_data:
                results.append(score_data)
        except:
            continue
    
    # 恢复权重
    if use_dynamic:
        service.STYLE_WEIGHTS.update(original_weights)
    
    # 排序
    results.sort(key=lambda x: x['composite_score'], reverse=True)
    top_stocks = results[:top_n]
    
    # 行业分布
    industry_dist = {}
    for s in top_stocks:
        ind = s.get('industry', '其他')
        industry_dist[ind] = industry_dist.get(ind, 0) + 1
    
    stocks = []
    for i, stock in enumerate(top_stocks, 1):
        stocks.append({
            "rank": i,
            "stock_code": stock['stock_code'],
            "stock_name": stock['stock_name'],
            "industry": stock.get('industry', ''),
            "composite_score": stock['composite_score'],
            "grade": stock['grade'],
            "style_scores": stock['style_scores'],
            "trade_date": stock.get('trade_date'),
            "close": stock.get('close'),
        })
    
    return {
        "code": 0,
        "data": {
            "stocks": stocks,
            "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "pool_size": len(stocks),
            "industry_distribution": industry_dist,
            "weight_config": dynamic_config,
            "weight_mode": "dynamic" if use_dynamic else "baseline"
        },
        "message": "success"
    }


@router.get("/v3/export")
async def export_v3_stock_pool(
    top_n: int = Query(50, ge=10, le=100),
    min_score: float = Query(0, ge=0, le=100)
):
    """导出股票池CSV"""
    import pandas as pd
    
    service = get_scoring_v3()
    result = service.generate_stock_pool(top_n=top_n, min_score=min_score)
    
    rows = []
    for i, s in enumerate(result['stocks'], 1):
        rows.append({
            '排名': i,
            '代码': s['stock_code'],
            '名称': s['stock_name'],
            '行业': s.get('industry', ''),
            '综合评分': s['composite_score'],
            '评级': s['grade'],
            '价值得分': s['style_scores']['value'],
            '成长得分': s['style_scores']['growth'],
            '质量得分': s['style_scores']['quality'],
            '动量得分': s['style_scores']['momentum'],
            '情绪得分': s['style_scores']['sentiment'],
            '最新价': s.get('close', ''),
            '交易日期': s.get('trade_date', ''),
        })
    
    df = pd.DataFrame(rows)
    export_dir = "data_cache/exports"
    os.makedirs(export_dir, exist_ok=True)
    
    file_name = f"stock_pool_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(export_dir, file_name)
    df.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    return {
        "code": 0,
        "data": {
            "file_name": file_name,
            "file_path": file_path,
            "row_count": len(rows)
        },
        "message": "导出成功"
    }


# ==================== v2 接口（保留兼容） ====================

@router.get("/v2/pool")
async def get_v2_stock_pool(
    top_n: int = Query(50, ge=10, le=100, description="股票池大小"),
    min_score: float = Query(0, ge=0, le=100, description="最低综合评分"),
    min_value: Optional[float] = Query(None, ge=0, le=100, description="价值得分下限"),
    min_growth: Optional[float] = Query(None, ge=0, le=100, description="成长得分下限"),
    min_quality: Optional[float] = Query(None, ge=0, le=100, description="质量得分下限"),
    industry: Optional[str] = Query(None, description="行业筛选"),
    grade: Optional[str] = Query(None, description="评级筛选(多个用逗号分隔)")
):
    """
    获取专业股票池 v2（行业中性版）
    
    特性：
    - 行业内百分位评分
    - 多维度筛选
    - 行业分布统计
    
    - **top_n**: 股票池大小
    - **min_score**: 最低综合评分
    - **min_value**: 价值得分下限
    - **min_growth**: 成长得分下限  
    - **min_quality**: 质量得分下限
    - **industry**: 行业筛选
    - **grade**: 评级筛选(A,B+,B,B-,C,D)
    """
    service = get_pro_scoring_v2()
    
    # 构建筛选条件
    filters = {}
    if min_value is not None:
        filters['min_value'] = min_value
    if min_growth is not None:
        filters['min_growth'] = min_growth
    if min_quality is not None:
        filters['min_quality'] = min_quality
    if industry:
        filters['industry'] = industry
    if grade:
        filters['grade'] = grade.split(',')
    
    result = service.generate_stock_pool(top_n=top_n, min_score=min_score, filters=filters if filters else None)
    
    # 添加排名
    stocks = []
    for i, stock in enumerate(result['stocks'], 1):
        stocks.append({
            "rank": i,
            "stock_code": stock['stock_code'],
            "stock_name": stock['stock_name'],
            "industry": stock.get('industry', ''),
            "composite_score": stock['composite_score'],
            "grade": stock['grade'],
            "value_score": stock['style_scores']['value'],
            "growth_score": stock['style_scores']['growth'],
            "quality_score": stock['style_scores']['quality'],
            "momentum_score": stock['style_scores']['momentum'],
            "sentiment_score": stock['style_scores']['sentiment'],
            "trade_date": stock.get('trade_date'),
            "close": stock.get('close'),
        })
    
    # 行业分布
    industry_dist = service.get_industry_distribution(result['stocks'])
    
    return {
        "code": 0,
        "data": {
            "stocks": stocks,
            "generated_at": result['generated_at'],
            "total_analyzed": result['total_count'],
            "qualified_count": result['qualified_count'],
            "pool_size": len(stocks),
            "industry_distribution": industry_dist,
            "filters_applied": filters if filters else None,
            "factor_model": {
                "value": 0.25,
                "growth": 0.20,
                "quality": 0.25,
                "momentum": 0.15,
                "sentiment": 0.15
            }
        },
        "message": "success"
    }


@router.get("/v2/score/{stock_code}")
async def get_v2_stock_score(stock_code: str):
    """
    获取单只股票的专业评分详情 v2（行业中性版）
    """
    service = get_pro_scoring_v2()
    
    # 先构建因子池
    factor_pool = service._build_factor_pool()
    result = service.calculate_score(stock_code, factor_pool)
    
    if result is None:
        raise HTTPException(status_code=404, detail=f"股票 {stock_code} 数据不足，无法评分")
    
    return {
        "code": 0,
        "data": result,
        "message": "success"
    }


@router.get("/v2/industries")
async def get_industries():
    """获取行业列表"""
    service = get_pro_scoring_v2()
    industries = service.get_industry_list()
    return {
        "code": 0,
        "data": {
            "industries": sorted(industries)
        },
        "message": "success"
    }


@router.get("/v2/export")
async def export_stock_pool(
    top_n: int = Query(50, ge=10, le=100),
    min_score: float = Query(0, ge=0, le=100),
    format: str = Query("csv", description="导出格式: csv")
):
    """
    导出股票池
    
    返回CSV文件下载链接
    """
    service = get_pro_scoring_v2()
    result = service.generate_stock_pool(top_n=top_n, min_score=min_score)
    
    # 添加排名
    for i, stock in enumerate(result['stocks'], 1):
        stock['rank'] = i
    
    # 导出
    export_dir = "data_cache/exports"
    os.makedirs(export_dir, exist_ok=True)
    
    file_name = f"stock_pool_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    file_path = os.path.join(export_dir, file_name)
    
    saved_path = service.export_to_csv(result['stocks'], file_path)
    
    return {
        "code": 0,
        "data": {
            "file_name": file_name,
            "file_path": saved_path,
            "row_count": len(result['stocks']),
            "generated_at": result['generated_at']
        },
        "message": "导出成功"
    }


# ==================== 专业评分接口 ====================

@router.get("/pro/score/{stock_code}")
async def get_pro_stock_score(stock_code: str):
    """
    获取单只股票的专业评分详情
    
    使用五大类因子模型：价值、成长、质量、动量、情绪
    
    - **stock_code**: 股票代码（如 000001）
    """
    service = get_pro_scoring_service()
    result = service.calculate_score(stock_code)
    
    if result is None:
        raise HTTPException(status_code=404, detail=f"股票 {stock_code} 数据不足，无法评分")
    
    return {
        "code": 0,
        "data": result,
        "message": "success"
    }


@router.get("/pro/pool")
async def get_pro_stock_pool(
    top_n: int = Query(50, ge=10, le=100, description="股票池大小"),
    min_score: float = Query(0, ge=0, le=100, description="最低评分门槛")
):
    """
    获取专业股票池（Top N评分股票）
    
    基于五大类因子：价值(25%) + 成长(20%) + 质量(25%) + 动量(15%) + 情绪(15%)
    """
    service = get_pro_scoring_service()
    result = service.generate_stock_pool(top_n=top_n, min_score=min_score)
    
    # 简化返回
    stocks = []
    for i, stock in enumerate(result['stocks'], 1):
        stocks.append({
            "rank": i,
            "stock_code": stock['stock_code'],
            "stock_name": stock['stock_name'],
            "composite_score": stock['composite_score'],
            "grade": stock['grade'],
            "value_score": stock['style_scores']['value'],
            "growth_score": stock['style_scores']['growth'],
            "quality_score": stock['style_scores']['quality'],
            "momentum_score": stock['style_scores']['momentum'],
            "sentiment_score": stock['style_scores']['sentiment'],
            "trade_date": stock.get('trade_date'),
            "close": stock.get('close'),
        })
    
    return {
        "code": 0,
        "data": {
            "stocks": stocks,
            "generated_at": result['generated_at'],
            "total_analyzed": result['total_count'],
            "pool_size": len(stocks),
            "factor_model": {
                "value": 0.25,
                "growth": 0.20,
                "quality": 0.25,
                "momentum": 0.15,
                "sentiment": 0.15
            }
        },
        "message": "success"
    }


@router.get("/pro/style/{stock_code}")
async def get_stock_style(stock_code: str):
    """
    分析股票风格特征
    
    返回股票的主导风格和风格标签
    """
    service = get_pro_scoring_service()
    result = service.analyze_style(stock_code)
    
    if result is None:
        raise HTTPException(status_code=404, detail=f"股票 {stock_code} 数据不足")
    
    return {
        "code": 0,
        "data": result,
        "message": "success"
    }


# ==================== 基础评分接口 ====================


@router.get("/score/{stock_code}")
async def get_stock_score(stock_code: str):
    """
    获取单只股票的评分详情
    
    - **stock_code**: 股票代码（如 000001）
    """
    service = get_scoring_service()
    result = service.calculate_score(stock_code)
    
    if result is None:
        raise HTTPException(status_code=404, detail=f"股票 {stock_code} 数据不足，无法评分")
    
    return {
        "code": 0,
        "data": result,
        "message": "success"
    }


@router.get("/recommend")
async def get_recommend_stocks(
    limit: int = Query(50, ge=10, le=200, description="返回股票数量"),
    min_score: float = Query(50.0, ge=0, le=100, description="最低评分门槛"),
    fast_mode: bool = Query(True, description="快速模式（跳过技术因子）")
):
    """
    获取推荐股票列表（股票池）
    
    - **limit**: 返回前N只股票
    - **min_score**: 最低评分门槛
    - **fast_mode**: 快速模式（仅财务+市场因子，速度更快）
    """
    service = get_scoring_service()
    
    # 生成股票池
    result = service.generate_stock_pool(top_n=limit, min_score=min_score)
    
    # 如果是快速模式，使用简化版评分
    if fast_mode:
        stock_codes = service._get_all_stock_codes()
        results = []
        
        for code in stock_codes:
            score_data = service.calculate_score_fast(code)
            if score_data and score_data['total_score'] >= min_score:
                results.append(score_data)
        
        results.sort(key=lambda x: x['total_score'], reverse=True)
        top_stocks = results[:limit]
        
        return {
            "code": 0,
            "data": {
                "stocks": top_stocks,
                "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "total_count": len(stock_codes),
                "qualified_count": len(results),
                "mode": "fast"
            },
            "message": "success"
        }
    
    return {
        "code": 0,
        "data": result,
        "message": "success"
    }


@router.get("/pool")
async def get_stock_pool(
    top_n: int = Query(50, ge=10, le=100, description="股票池大小")
):
    """
    获取股票池（Top N评分股票）
    
    这是选股系统的核心接口，返回评分最高的N只股票作为股票池
    """
    service = get_scoring_service()
    result = service.generate_stock_pool(top_n=top_n, min_score=0)
    
    # 简化返回，只保留核心字段
    stocks = []
    for i, stock in enumerate(result['stocks'], 1):
        stocks.append({
            "rank": i,
            "stock_code": stock['stock_code'],
            "stock_name": stock['stock_name'],
            "total_score": stock['total_score'],
            "grade": stock['grade'],
            "financial_score": stock['scores']['financial'],
            "market_score": stock['scores']['market'],
            "technical_score": stock['scores']['technical'],
            "trade_date": stock.get('trade_date'),
            "close": stock.get('close'),
        })
    
    return {
        "code": 0,
        "data": {
            "stocks": stocks,
            "generated_at": result['generated_at'],
            "total_analyzed": result['total_count'],
            "pool_size": len(stocks)
        },
        "message": "success"
    }


@router.get("/factor-analysis/{factor_name}")
async def get_factor_analysis(
    factor_name: str,
    limit: int = Query(20, ge=5, le=50)
):
    """
    因子分析 - 查看某因子的分布情况
    
    - **factor_name**: 因子名称（roe, pe, pb, revenue_growth等）
    """
    service = get_scoring_service()
    stock_codes = service._get_all_stock_codes()
    
    factor_values = []
    
    for code in stock_codes:
        # 根据因子类型加载数据
        if factor_name in ['pe', 'pb', 'ps', 'turnover', 'market_cap', 'volume_ratio']:
            daily_data = service._load_daily_basic_data(code)
            if daily_data:
                value = daily_data.get(factor_name)
                if value is not None:
                    factor_values.append({
                        'stock_code': code,
                        'stock_name': service._get_stock_name(code),
                        'value': value
                    })
        else:
            fina_data = service._load_financial_data(code)
            if fina_data:
                value = fina_data.get(factor_name)
                if value is not None:
                    factor_values.append({
                        'stock_code': code,
                        'stock_name': service._get_stock_name(code),
                        'value': value
                    })
    
    # 排序
    factor_values.sort(key=lambda x: x['value'], reverse=True)
    
    # 分位数统计
    values = [f['value'] for f in factor_values if f['value'] is not None]
    if values:
        import numpy as np
        stats = {
            'count': len(values),
            'mean': round(float(np.mean(values)), 2),
            'median': round(float(np.median(values)), 2),
            'std': round(float(np.std(values)), 2),
            'min': round(float(min(values)), 2),
            'max': round(float(max(values)), 2),
            'p25': round(float(np.percentile(values, 25)), 2),
            'p75': round(float(np.percentile(values, 75)), 2),
        }
    else:
        stats = {}
    
    return {
        "code": 0,
        "data": {
            "factor_name": factor_name,
            "stats": stats,
            "top_stocks": factor_values[:limit],
            "bottom_stocks": factor_values[-limit:][::-1] if len(factor_values) >= limit else []
        },
        "message": "success"
    }


@router.post("/refresh-pool")
async def refresh_stock_pool(top_n: int = 50):
    """
    刷新股票池（重新计算所有股票评分）
    
    这个接口会重新计算所有股票的评分，适合每日定时调用
    """
    service = get_scoring_service()
    result = service.generate_stock_pool(top_n=top_n, min_score=0)
    
    return {
        "code": 0,
        "data": {
            "generated_at": result['generated_at'],
            "total_analyzed": result['total_count'],
            "pool_size": len(result['stocks']),
            "top_5": [
                {
                    "rank": i + 1,
                    "stock_code": s['stock_code'],
                    "stock_name": s['stock_name'],
                    "total_score": s['total_score'],
                    "grade": s['grade']
                }
                for i, s in enumerate(result['stocks'][:5])
            ]
        },
        "message": "股票池刷新成功"
    }