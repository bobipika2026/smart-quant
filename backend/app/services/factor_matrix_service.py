"""
因子矩阵服务 - 因子矩阵生成与分析
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from app.database import SessionLocal
from app.models.factor import FactorBacktest, FactorPerformance


class FactorMatrixService:
    """因子矩阵服务"""
    
    # ==================== 因子矩阵生成 ====================
    
    @staticmethod
    async def save_backtest_factors(
        backtest_result: Dict,
        strategy_id: str,
        strategy_name: str,
        stock_code: str,
        stock_name: str,
        params: Dict = None
    ) -> bool:
        """
        保存回测结果的因子组合
        
        Args:
            backtest_result: 回测结果
            strategy_id: 策略ID
            strategy_name: 策略名称
            stock_code: 股票代码
            stock_name: 股票名称
            params: 策略参数
        """
        db: Session = SessionLocal()
        try:
            # 获取股票的额外信息（行业、市值等级）
            industry = await FactorMatrixService._get_industry(stock_code)
            market_cap_level = await FactorMatrixService._get_market_cap_level(stock_code)
            
            # 解析参数
            param_short = params.get('short_period') or params.get('fast_period') if params else None
            param_long = params.get('long_period') or params.get('slow_period') if params else None
            param_threshold = params.get('threshold') if params else None
            
            # 计算持仓天数
            start_date = backtest_result.get('start_date')
            end_date = backtest_result.get('end_date')
            holding_days = None
            if start_date and end_date:
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')
                holding_days = (end_date - start_date).days
            
            # 创建因子回测记录
            factor_backtest = FactorBacktest(
                # 策略因子
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                
                # 参数因子
                param_short_period=param_short,
                param_long_period=param_long,
                param_threshold=param_threshold,
                params_json=str(params) if params else None,
                
                # 股票因子
                stock_code=stock_code,
                stock_name=stock_name,
                industry=industry,
                market_cap_level=market_cap_level,
                
                # 时间因子
                start_date=start_date,
                end_date=end_date,
                holding_days=holding_days,
                trade_count=len(backtest_result.get('trades', [])),
                
                # 结果
                total_return=backtest_result.get('total_return'),
                annual_return=backtest_result.get('annual_return'),
                max_drawdown=backtest_result.get('max_drawdown'),
                sharpe_ratio=backtest_result.get('sharpe_ratio'),
                win_rate=backtest_result.get('win_rate'),
            )
            
            db.add(factor_backtest)
            db.commit()
            print(f"[因子矩阵] 保存回测因子成功: {strategy_id} - {stock_code}")
            return True
            
        except Exception as e:
            db.rollback()
            print(f"[因子矩阵] 保存回测因子失败: {e}")
            return False
        finally:
            db.close()
    
    @staticmethod
    async def _get_industry(stock_code: str) -> Optional[str]:
        """获取股票行业"""
        try:
            import akshare as ak
            df = await asyncio.to_thread(ak.stock_individual_info_em, symbol=stock_code)
            if df is not None and len(df) > 0:
                info = dict(zip(df['item'], df['value']))
                return info.get('行业')
        except:
            pass
        return None
    
    @staticmethod
    async def _get_market_cap_level(stock_code: str) -> Optional[str]:
        """获取市值等级"""
        try:
            import akshare as ak
            df = await asyncio.to_thread(ak.stock_individual_info_em, symbol=stock_code)
            if df is not None and len(df) > 0:
                info = dict(zip(df['item'], df['value']))
                market_cap_str = info.get('总市值', '0')
                if isinstance(market_cap_str, str):
                    market_cap_str = market_cap_str.replace('亿', '').strip()
                market_cap = float(market_cap_str) if market_cap_str else 0
                
                if market_cap >= 1000:
                    return '大盘'
                elif market_cap >= 200:
                    return '中盘'
                else:
                    return '小盘'
        except:
            pass
        return None
    
    # ==================== 因子矩阵查询 ====================
    
    @staticmethod
    def get_factor_matrix(
        strategy_id: str = None,
        stock_code: str = None,
        industry: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        查询因子矩阵
        
        Args:
            strategy_id: 策略ID过滤
            stock_code: 股票代码过滤
            industry: 行业过滤
            limit: 返回条数限制
        """
        db: Session = SessionLocal()
        try:
            query = db.query(FactorBacktest)
            
            if strategy_id:
                query = query.filter(FactorBacktest.strategy_id == strategy_id)
            if stock_code:
                query = query.filter(FactorBacktest.stock_code == stock_code)
            if industry:
                query = query.filter(FactorBacktest.industry == industry)
            
            query = query.order_by(desc(FactorBacktest.created_at)).limit(limit)
            records = query.all()
            
            # 转换为字典列表
            result = []
            for r in records:
                result.append({
                    'id': r.id,
                    # 策略因子
                    'strategy_id': r.strategy_id,
                    'strategy_name': r.strategy_name,
                    # 参数因子
                    'param_short': r.param_short_period,
                    'param_long': r.param_long_period,
                    'param_threshold': r.param_threshold,
                    # 股票因子
                    'stock_code': r.stock_code,
                    'stock_name': r.stock_name,
                    'industry': r.industry,
                    'market_cap_level': r.market_cap_level,
                    # 时间因子
                    'start_date': r.start_date.strftime('%Y-%m-%d') if r.start_date else None,
                    'end_date': r.end_date.strftime('%Y-%m-%d') if r.end_date else None,
                    'holding_days': r.holding_days,
                    'trade_count': r.trade_count,
                    # 结果
                    'total_return': r.total_return,
                    'annual_return': r.annual_return,
                    'max_drawdown': r.max_drawdown,
                    'sharpe_ratio': r.sharpe_ratio,
                    'win_rate': r.win_rate,
                    'created_at': r.created_at.strftime('%Y-%m-%d %H:%M:%S') if r.created_at else None,
                })
            
            return result
            
        except Exception as e:
            print(f"[因子矩阵] 查询失败: {e}")
            return []
        finally:
            db.close()
    
    # ==================== 因子收益分析 ====================
    
    @staticmethod
    def analyze_factor_performance(factor_name: str, top_n: int = 10) -> Dict:
        """
        分析单个因子的收益表现
        
        Args:
            factor_name: 因子名称（如 'strategy_id', 'industry', 'param_short' 等）
            top_n: 返回前N个表现最好的因子值
        """
        db: Session = SessionLocal()
        try:
            # 映射因子名到数据库字段
            factor_map = {
                'strategy_id': FactorBacktest.strategy_id,
                'strategy_name': FactorBacktest.strategy_name,
                'industry': FactorBacktest.industry,
                'market_cap_level': FactorBacktest.market_cap_level,
                'param_short': FactorBacktest.param_short_period,
                'param_long': FactorBacktest.param_long_period,
            }
            
            if factor_name not in factor_map:
                return {'error': f'不支持的因子: {factor_name}'}
            
            factor_col = factor_map[factor_name]
            
            # 按因子值分组统计
            query = db.query(
                factor_col.label('factor_value'),
                func.count(FactorBacktest.id).label('sample_count'),
                func.avg(FactorBacktest.total_return).label('avg_return'),
                func.avg(FactorBacktest.sharpe_ratio).label('avg_sharpe'),
                func.avg(FactorBacktest.win_rate).label('avg_win_rate'),
                func.avg(FactorBacktest.max_drawdown).label('avg_drawdown'),
            ).filter(
                factor_col.isnot(None),
                FactorBacktest.total_return.isnot(None)
            ).group_by(factor_col).all()
            
            # 转换为列表并排序
            results = []
            for row in query:
                results.append({
                    'factor_value': row.factor_value,
                    'sample_count': row.sample_count,
                    'avg_return': round(row.avg_return, 2) if row.avg_return else None,
                    'avg_sharpe': round(row.avg_sharpe, 2) if row.avg_sharpe else None,
                    'avg_win_rate': round(row.avg_win_rate * 100, 1) if row.avg_win_rate else None,
                    'avg_drawdown': round(row.avg_drawdown, 2) if row.avg_drawdown else None,
                })
            
            # 按收益排序
            results.sort(key=lambda x: x['avg_return'] or 0, reverse=True)
            
            return {
                'factor_name': factor_name,
                'total_values': len(results),
                'top_performers': results[:top_n],
                'bottom_performers': results[-top_n:] if len(results) > top_n else [],
            }
            
        except Exception as e:
            print(f"[因子矩阵] 分析失败: {e}")
            return {'error': str(e)}
        finally:
            db.close()
    
    @staticmethod
    def get_correlation_matrix() -> Dict:
        """
        获取因子与收益的相关性矩阵
        """
        db: Session = SessionLocal()
        try:
            # 获取所有回测记录
            records = db.query(FactorBacktest).filter(
                FactorBacktest.total_return.isnot(None)
            ).all()
            
            if len(records) < 5:
                return {'error': '样本数量不足，至少需要5条记录'}
            
            # 转换为DataFrame
            data = []
            for r in records:
                data.append({
                    'param_short': r.param_short_period,
                    'param_long': r.param_long_period,
                    'holding_days': r.holding_days,
                    'trade_count': r.trade_count,
                    'total_return': r.total_return,
                    'sharpe_ratio': r.sharpe_ratio,
                    'max_drawdown': r.max_drawdown,
                    'win_rate': r.win_rate,
                })
            
            df = pd.DataFrame(data)
            
            # 计算数值型因子的相关性
            numeric_cols = ['param_short', 'param_long', 'holding_days', 'trade_count']
            result_cols = ['total_return', 'sharpe_ratio', 'max_drawdown', 'win_rate']
            
            correlations = {}
            for factor_col in numeric_cols:
                if factor_col in df.columns and df[factor_col].notna().sum() > 0:
                    correlations[factor_col] = {}
                    for result_col in result_cols:
                        if result_col in df.columns and df[result_col].notna().sum() > 0:
                            valid_data = df[[factor_col, result_col]].dropna()
                            if len(valid_data) >= 3:
                                corr = valid_data[factor_col].corr(valid_data[result_col])
                                correlations[factor_col][result_col] = round(corr, 3) if not pd.isna(corr) else None
            
            return {
                'sample_count': len(records),
                'correlations': correlations,
            }
            
        except Exception as e:
            print(f"[因子矩阵] 相关性分析失败: {e}")
            return {'error': str(e)}
        finally:
            db.close()
    
    # ==================== 统计信息 ====================
    
    @staticmethod
    def get_statistics() -> Dict:
        """获取因子矩阵统计信息"""
        db: Session = SessionLocal()
        try:
            total_count = db.query(FactorBacktest).count()
            
            # 策略分布
            strategy_dist = db.query(
                FactorBacktest.strategy_name,
                func.count(FactorBacktest.id).label('count')
            ).group_by(FactorBacktest.strategy_name).all()
            
            # 行业分布
            industry_dist = db.query(
                FactorBacktest.industry,
                func.count(FactorBacktest.id).label('count')
            ).group_by(FactorBacktest.industry).all()
            
            # 平均收益
            avg_return = db.query(func.avg(FactorBacktest.total_return)).scalar()
            avg_sharpe = db.query(func.avg(FactorBacktest.sharpe_ratio)).scalar()
            
            return {
                'total_records': total_count,
                'strategy_distribution': [{'name': r[0], 'count': r[1]} for r in strategy_dist],
                'industry_distribution': [{'name': r[0], 'count': r[1]} for r in industry_dist if r[0]],
                'avg_return': round(avg_return, 2) if avg_return else None,
                'avg_sharpe': round(avg_sharpe, 2) if avg_sharpe else None,
            }
            
        except Exception as e:
            print(f"[因子矩阵] 统计失败: {e}")
            return {'error': str(e)}
        finally:
            db.close()


import asyncio