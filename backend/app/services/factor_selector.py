"""
因子选股服务 - 基于因子筛选股票
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc

from app.database import SessionLocal
from app.models.factor import FactorValue
from app.models import Stock


class FactorSelector:
    """因子选股服务"""
    
    # ==================== 筛选条件预设 ====================
    
    # 优质股票筛选条件
    QUALITY_STOCK_FILTER = {
        'roe_min': 10,           # ROE > 10%
        'debt_ratio_max': 70,    # 资产负债率 < 70%
        'market_cap_min': 50,    # 市值 > 50亿
        'north_holdings_min': 1, # 北向持股 > 1%
    }
    
    # 价值股票筛选条件
    VALUE_STOCK_FILTER = {
        'pe_max': 20,            # PE < 20
        'pb_max': 3,             # PB < 3
        'roe_min': 8,            # ROE > 8%
        'market_cap_min': 100,   # 市值 > 100亿
    }
    
    # 成长股票筛选条件
    GROWTH_STOCK_FILTER = {
        'roe_min': 15,           # ROE > 15%
        'revenue_growth_min': 10, # 营收增长 > 10%
        'market_cap_min': 50,    # 市值 > 50亿
        'north_holdings_min': 2, # 北向持股 > 2%
    }
    
    # ==================== 因子筛选 ====================
    
    @staticmethod
    async def select_stocks(
        filters: Dict = None,
        sort_by: str = 'roe',
        limit: int = 100,
        use_existing_matrix: bool = True
    ) -> Dict:
        """
        基于因子筛选股票
        
        Args:
            filters: 筛选条件字典
            sort_by: 排序字段
            limit: 返回数量
            use_existing_matrix: 是否使用已有因子矩阵
        
        Returns:
            筛选结果
        """
        from app.services.factor_service import FactorService
        from app.services.data import DataService
        
        db: Session = SessionLocal()
        
        try:
            # 默认筛选条件
            if filters is None:
                filters = FactorSelector.QUALITY_STOCK_FILTER
            
            print(f"[因子选股] 筛选条件: {filters}")
            
            # 获取股票列表
            stocks = db.query(Stock.code, Stock.name).all()
            stock_dict = {s.code: s.name for s in stocks}
            
            results = []
            
            if use_existing_matrix:
                # 使用已有因子矩阵数据
                query = db.query(FactorValue)
                
                # 获取最新日期的数据
                latest_date = db.query(FactorValue.trade_date).order_by(
                    desc(FactorValue.trade_date)
                ).first()
                
                if latest_date:
                    query = query.filter(FactorValue.trade_date == latest_date[0])
                
                records = query.all()
                print(f"[因子选股] 从因子矩阵获取 {len(records)} 条记录")
                
                for r in records:
                    stock_data = FactorSelector._apply_filters(r, filters)
                    if stock_data:
                        stock_data['stock_name'] = stock_dict.get(r.stock_code, r.stock_code)
                        results.append(stock_data)
            
            else:
                # 实时获取因子数据
                data_service = DataService()
                print(f"[因子选股] 实时获取 {len(stocks)} 只股票的因子数据")
                
                for i, (code, name) in enumerate(stocks[:500]):  # 限制500只
                    try:
                        hist_data = await data_service.get_stock_history(code)
                        factors = await FactorService.get_all_factors(code, hist_data)
                        
                        stock_data = FactorSelector._apply_filters_dict(factors, filters)
                        if stock_data:
                            stock_data['stock_name'] = name
                            results.append(stock_data)
                        
                        if (i + 1) % 50 == 0:
                            print(f"[因子选股] 进度: {i+1}/{min(len(stocks), 500)}")
                    
                    except Exception as e:
                        continue
            
            # 排序
            sort_field = sort_by
            if sort_field in ['roe', 'market_cap', 'north_holdings_ratio']:
                results.sort(key=lambda x: x.get(sort_field, 0) or 0, reverse=True)
            elif sort_field in ['pe', 'pb', 'debt_ratio']:
                results.sort(key=lambda x: x.get(sort_field, 999) or 999)
            
            # 限制数量
            results = results[:limit]
            
            return {
                'success': True,
                'total': len(results),
                'filters': filters,
                'sort_by': sort_by,
                'data': results
            }
            
        except Exception as e:
            return {'error': str(e)}
        finally:
            db.close()
    
    @staticmethod
    def _apply_filters(record: FactorValue, filters: Dict) -> Optional[Dict]:
        """对单条记录应用筛选条件"""
        data = {
            'stock_code': record.stock_code,
            'trade_date': str(record.trade_date),
            'pe': record.pe,
            'pb': record.pb,
            'roe': record.roe,
            'debt_ratio': record.debt_ratio,
            'market_cap': record.market_cap,
            'north_holdings_ratio': record.north_holdings_ratio,
            'rsi_14': record.rsi_14,
            'macd': record.macd,
            'kdj_k': record.kdj_k,
        }
        
        return FactorSelector._apply_filters_dict(data, filters)
    
    @staticmethod
    def _apply_filters_dict(data: Dict, filters: Dict) -> Optional[Dict]:
        """对字典数据应用筛选条件"""
        # 检查所有条件
        for key, value in filters.items():
            if key.endswith('_min'):
                field = key[:-4]
                field_value = data.get(field)
                # 如果字段为空，跳过该条件（允许缺失数据）
                if field_value is not None and field_value < value:
                    return None
            elif key.endswith('_max'):
                field = key[:-4]
                field_value = data.get(field)
                # 如果字段为空，跳过该条件（允许缺失数据）
                if field_value is not None and field_value > value:
                    return None
        
        return data
    
    # ==================== 多因子打分 ====================
    
    @staticmethod
    def score_stocks(
        stocks_data: List[Dict],
        weights: Dict = None
    ) -> List[Dict]:
        """
        多因子打分
        
        Args:
            stocks_data: 股票数据列表
            weights: 因子权重 {'roe': 0.3, 'market_cap': 0.2, ...}
        """
        if weights is None:
            weights = {
                'roe': 0.25,
                'market_cap': 0.15,
                'north_holdings_ratio': 0.15,
                'debt_ratio': -0.15,  # 负相关
                'rsi_14': 0.10,
            }
        
        df = pd.DataFrame(stocks_data)
        
        # 标准化
        for col in weights.keys():
            if col in df.columns:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    mean = col_data.mean()
                    std = col_data.std()
                    if std > 0:
                        df[f'{col}_score'] = (df[col] - mean) / std
                    else:
                        df[f'{col}_score'] = 0
        
        # 计算综合得分
        df['total_score'] = 0
        for col, weight in weights.items():
            score_col = f'{col}_score'
            if score_col in df.columns:
                df['total_score'] += df[score_col].fillna(0) * weight
        
        # 排序
        df = df.sort_values('total_score', ascending=False)
        
        return df.to_dict('records')
    
    # ==================== 筛选方案 ====================
    
    @staticmethod
    async def get_filter_presets() -> List[Dict]:
        """获取预设筛选方案"""
        return [
            {
                'name': 'quality',
                'description': '优质股票（高ROE、低负债、大市值）',
                'filters': FactorSelector.QUALITY_STOCK_FILTER,
                'sort_by': 'roe'
            },
            {
                'name': 'value',
                'description': '价值股票（低PE、低PB、稳健增长）',
                'filters': FactorSelector.VALUE_STOCK_FILTER,
                'sort_by': 'pe'
            },
            {
                'name': 'growth',
                'description': '成长股票（高ROE、高增长、北向青睐）',
                'filters': FactorSelector.GROWTH_STOCK_FILTER,
                'sort_by': 'roe'
            }
        ]