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
from app.models.factor import FactorBacktest, FactorPerformance, FactorValue
from app.services.factor_service import FactorService
from app.models import Stock


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
            # 获取股票的完整因子数据
            from app.services.data import DataService
            data_service = DataService()
            
            # 获取历史数据用于计算技术因子
            start_date_str = backtest_result.get('start_date')
            hist_data = None
            if start_date_str:
                try:
                    hist_data = await data_service.get_stock_history(stock_code, start_date=start_date_str)
                except:
                    pass
            
            # 获取所有因子
            all_factors = await FactorService.get_all_factors(stock_code, hist_data)
            
            # 提取各类因子
            industry = all_factors.get('industry') or await FactorMatrixService._get_industry(stock_code)
            market_cap_level = FactorMatrixService._get_market_cap_level_from_value(all_factors.get('market_cap'))
            
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
                
                # 基本面因子
                pe=all_factors.get('pe'),
                pb=all_factors.get('pb'),
                roe=all_factors.get('roe'),
                debt_ratio=all_factors.get('debt_ratio'),
                nav_per_share=all_factors.get('nav_per_share'),
                net_profit_margin=all_factors.get('net_profit_margin'),
                revenue_growth=all_factors.get('revenue_growth'),
                profit_growth=all_factors.get('profit_growth'),
                dividend_yield=all_factors.get('dividend_yield'),
                
                # 市场因子
                market_cap=all_factors.get('market_cap'),
                turnover_rate=all_factors.get('turnover_rate'),
                volatility_20=all_factors.get('volatility_20'),
                
                # 情绪因子
                north_holdings_ratio=all_factors.get('north_holdings_ratio'),
                north_5d_change=all_factors.get('north_5d_change'),
                margin_balance=all_factors.get('margin_balance'),
                
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
                
                return FactorMatrixService._get_market_cap_level_from_value(market_cap)
        except:
            pass
        return None
    
    @staticmethod
    def _get_market_cap_level_from_value(market_cap: float) -> Optional[str]:
        """根据市值数值返回市值等级"""
        if market_cap is None:
            return None
        if market_cap >= 1000:
            return '大盘'
        elif market_cap >= 200:
            return '中盘'
        else:
            return '小盘'
    
    # ==================== 因子矩阵生成（全量股票） ====================
    
    @staticmethod
    async def generate_factor_matrix(stock_codes: List[str] = None, limit: int = 100) -> Dict:
        """
        生成因子矩阵
        
        Args:
            stock_codes: 股票列表，None则使用全市场
            limit: 股票数量限制
        
        Returns:
            因子矩阵统计信息
        """
        from app.services.data import DataService
        from app.models import Stock
        
        db: Session = SessionLocal()
        try:
            # 获取股票列表
            if stock_codes is None:
                stocks = db.query(Stock.code).limit(limit).all()
                stock_codes = [s.code for s in stocks]
            
            print(f"[因子矩阵] 开始生成，股票数: {len(stock_codes)}")
            
            data_service = DataService()
            success_count = 0
            fail_count = 0
            
            for i, code in enumerate(stock_codes):
                try:
                    # 获取历史数据
                    hist_data = await data_service.get_stock_history(code)
                    
                    # 获取所有因子
                    factors = await FactorService.get_all_factors(code, hist_data)
                    
                    # 保存到 factor_values 表
                    await FactorService.save_factors(factors)
                    
                    success_count += 1
                    if (i + 1) % 50 == 0:
                        print(f"[因子矩阵] 进度: {i+1}/{len(stock_codes)}")
                    
                except Exception as e:
                    fail_count += 1
                    print(f"[因子矩阵] {code} 失败: {e}")
            
            return {
                'success': True,
                'total': len(stock_codes),
                'success_count': success_count,
                'fail_count': fail_count
            }
            
        except Exception as e:
            return {'error': str(e)}
        finally:
            db.close()
    
    @staticmethod
    def get_factor_matrix_data(
        trade_date: str = None,
        limit: int = 100
    ) -> Dict:
        """
        获取因子矩阵数据
        
        Args:
            trade_date: 交易日期
            limit: 返回条数
        """
        db: Session = SessionLocal()
        try:
            query = db.query(FactorValue)
            
            if trade_date:
                query = query.filter(FactorValue.trade_date == trade_date)
            
            query = query.limit(limit)
            records = query.all()
            
            # 定义因子列
            factor_columns = {
                '基本面': ['pe', 'pb', 'roe', 'debt_ratio', 'nav_per_share', 'market_cap', 'float_market_cap'],
                '市场': ['turnover_rate', 'volume_ratio', 'volatility_20'],
                '情绪': ['north_holdings_ratio', 'north_5d_change', 'margin_balance'],
                '技术-均线': ['ma_5', 'ma_10', 'ma_20', 'ma_60'],
                '技术-趋势': ['rsi_14', 'macd', 'macd_hist', 'atr_14'],
                '技术-布林带': ['boll_upper', 'boll_lower', 'boll_mid', 'boll_width'],
                '技术-KDJ': ['kdj_k', 'kdj_d', 'kdj_j'],
                '技术-DMI': ['dmi_plus', 'dmi_minus', 'dmi_adx'],
                '技术-其他': ['cci_14', 'wr_14', 'obv', 'bias_6', 'bias_12', 'vwap', 'aroon_osc', 'mom_10', 'roc_10'],
            }
            
            # 转换为矩阵格式
            matrix = []
            for r in records:
                row = {
                    'stock_code': r.stock_code,
                    'trade_date': str(r.trade_date),
                }
                
                # 添加各类因子
                for cat, cols in factor_columns.items():
                    for col in cols:
                        row[col] = getattr(r, col, None)
                
                matrix.append(row)
            
            # 统计因子可用性
            factor_stats = {}
            all_cols = [col for cols in factor_columns.values() for col in cols]
            for col in all_cols:
                available = sum(1 for r in records if getattr(r, col, None) is not None)
                factor_stats[col] = {
                    'available': available,
                    'total': len(records),
                    'rate': round(available / len(records) * 100, 1) if records else 0
                }
            
            return {
                'count': len(matrix),
                'data': matrix,
                'factor_stats': factor_stats,
                'factor_columns': factor_columns
            }
            
        except Exception as e:
            return {'error': str(e)}
        finally:
            db.close()

    # ==================== 因子矩阵查询（旧） ====================
    
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
                    # 基本面因子
                    'pe': r.pe,
                    'pb': r.pb,
                    'roe': r.roe,
                    'debt_ratio': r.debt_ratio,
                    'nav_per_share': r.nav_per_share,
                    'net_profit_margin': r.net_profit_margin,
                    'revenue_growth': r.revenue_growth,
                    'profit_growth': r.profit_growth,
                    'dividend_yield': r.dividend_yield,
                    # 市场因子
                    'market_cap': r.market_cap,
                    'turnover_rate': r.turnover_rate,
                    'volatility_20': r.volatility_20,
                    # 情绪因子
                    'north_holdings_ratio': r.north_holdings_ratio,
                    'north_5d_change': r.north_5d_change,
                    'margin_balance': r.margin_balance,
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
                # 策略因子
                'strategy_id': FactorBacktest.strategy_id,
                'strategy_name': FactorBacktest.strategy_name,
                # 股票因子
                'industry': FactorBacktest.industry,
                'market_cap_level': FactorBacktest.market_cap_level,
                # 参数因子
                'param_short': FactorBacktest.param_short_period,
                'param_long': FactorBacktest.param_long_period,
                # 基本面因子（分段分析）
                'pe_range': None,  # 需要特殊处理
                'pb_range': None,
                'roe_range': None,
                # 市场因子（分段分析）
                'market_cap_range': None,
                # 情绪因子（分段分析）
                'north_holdings_ratio_range': None,
            }
            
            # 基本面因子分段分析
            if factor_name in ['pe_range', 'pb_range', 'roe_range']:
                return FactorMatrixService._analyze_numeric_factor_range(
                    factor_name.replace('_range', ''), top_n
                )
            
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
    
    # ==================== 数值型因子分段分析 ====================
    
    @staticmethod
    def _analyze_numeric_factor_range(factor_name: str, top_n: int = 10) -> Dict:
        """
        分析数值型因子的分段收益表现
        将因子值分成几段，分析每段的收益表现
        """
        db: Session = SessionLocal()
        try:
            # 映射因子名到数据库字段
            factor_col_map = {
                'pe': FactorBacktest.pe,
                'pb': FactorBacktest.pb,
                'roe': FactorBacktest.roe,
                'market_cap': FactorBacktest.market_cap,
                'north_holdings_ratio': FactorBacktest.north_holdings_ratio,
            }
            
            if factor_name not in factor_col_map:
                return {'error': f'不支持的因子: {factor_name}'}
            
            factor_col = factor_col_map[factor_name]
            
            # 获取所有记录
            records = db.query(
                factor_col,
                FactorBacktest.total_return,
                FactorBacktest.sharpe_ratio
            ).filter(
                factor_col.isnot(None),
                FactorBacktest.total_return.isnot(None)
            ).all()
            
            if len(records) < 5:
                return {'error': '样本数量不足'}
            
            # 转换为DataFrame
            df = pd.DataFrame([(r[0], r[1], r[2]) for r in records], 
                            columns=['factor_value', 'total_return', 'sharpe_ratio'])
            
            # 分段（分4段）
            try:
                df['range'] = pd.qcut(df['factor_value'], q=4, labels=['低', '中低', '中高', '高'], duplicates='drop')
            except:
                # 如果值太少，分2段
                df['range'] = pd.cut(df['factor_value'], bins=2, labels=['低', '高'])
            
            # 按段统计
            results = []
            for range_name in df['range'].unique():
                if pd.isna(range_name):
                    continue
                subset = df[df['range'] == range_name]
                results.append({
                    'factor_value': f'{factor_name}_{range_name}',
                    'range': range_name,
                    'sample_count': len(subset),
                    'avg_return': round(subset['total_return'].mean(), 2),
                    'avg_sharpe': round(subset['sharpe_ratio'].mean(), 2) if subset['sharpe_ratio'].notna().any() else None,
                })
            
            # 按收益排序
            results.sort(key=lambda x: x['avg_return'] or 0, reverse=True)
            
            return {
                'factor_name': factor_name,
                'total_values': len(results),
                'ranges': results,
            }
            
        except Exception as e:
            print(f"[因子矩阵] 分段分析失败: {e}")
            return {'error': str(e)}
        finally:
            db.close()


import asyncio