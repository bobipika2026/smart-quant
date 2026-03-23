"""
板块轮动监控服务 v1.8

功能:
1. 板块强度排名 - 申万一级行业实时行情
2. 板块资金流向 - 主力资金/北向资金（优先本地缓存）
3. 轮动信号识别 - 从弱转强的板块
4. 分批建仓策略 - 左侧埋伏 + 右侧加仓

数据源:
- 本地缓存: data_cache/sector/ (优先)
- AkShare: 申万行业估值
- Tushare: 资金流向（已同步到本地）
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import akshare as ak

from app.config import settings

logger = logging.getLogger(__name__)


class SectorRotationService:
    """板块轮动监控服务"""
    
    # 缓存目录
    CACHE_DIR = "data_cache/sector_rotation"
    SECTOR_CACHE_DIR = "data_cache/sector"  # 数据同步缓存目录
    
    # 申万一级行业代码映射
    SW_INDUSTRIES = {
        '801010.SI': '农林牧渔',
        '801030.SI': '基础化工',
        '801040.SI': '钢铁',
        '801050.SI': '有色金属',
        '801080.SI': '电子',
        '801880.SI': '汽车',
        '801110.SI': '家用电器',
        '801120.SI': '食品饮料',
        '801130.SI': '纺织服饰',
        '801140.SI': '轻工制造',
        '801150.SI': '医药生物',
        '801160.SI': '公用事业',
        '801170.SI': '交通运输',
        '801180.SI': '房地产',
        '801200.SI': '商贸零售',
        '801210.SI': '社会服务',
        '801780.SI': '银行',
        '801790.SI': '非银金融',
        '801230.SI': '综合',
        '801710.SI': '建筑材料',
        '801720.SI': '建筑装饰',
        '801730.SI': '电力设备',
        '801890.SI': '机械设备',
        '801740.SI': '国防军工',
        '801750.SI': '计算机',
        '801760.SI': '传媒',
        '801770.SI': '通信',
        '801950.SI': '煤炭',
        '801960.SI': '石油石化',
        '801970.SI': '环保',
    }
    
    # 行业风格分类（用于因子权重配置）
    INDUSTRY_STYLE = {
        '科技': ['电子', '计算机', '通信', '传媒'],
        '消费': ['食品饮料', '家用电器', '纺织服饰', '社会服务', '商贸零售'],
        '金融': ['银行', '非银金融'],
        '周期': ['钢铁', '有色金属', '煤炭', '石油石化', '基础化工', '建筑材料', '建筑装饰'],
        '制造': ['汽车', '电力设备', '机械设备', '国防军工', '轻工制造'],
        '医药': ['医药生物'],
        '公用': ['公用事业', '交通运输', '环保'],
        '其他': ['农林牧渔', '房地产', '综合'],
    }
    
    def __init__(self):
        os.makedirs(self.CACHE_DIR, exist_ok=True)
    
    # ==================== 数据获取 ====================
    
    async def get_sw_index_realtime(self) -> pd.DataFrame:
        """获取申万一级行业实时行情"""
        try:
            df = ak.sw_index_first_info()
            df = df.rename(columns={
                '行业代码': 'code',
                '行业名称': 'name',
                '成份个数': 'component_count',
                '静态市盈率': 'pe_static',
                'TTM(滚动)市盈率': 'pe_ttm',
                '市净率': 'pb',
                '静态股息率': 'dividend_yield',
            })
            df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            return df
        except Exception as e:
            logger.error(f"[申万行业数据] 获取失败: {e}")
            return pd.DataFrame()
    
    async def get_sw_index_history(self, code: str, days: int = 60) -> pd.DataFrame:
        """获取申万行业指数历史数据"""
        try:
            import tushare as ts
            pro = ts.pro_api()
            
            start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
            end_date = datetime.now().strftime('%Y%m%d')
            
            df = pro.index_daily(ts_code=code, start_date=start_date, end_date=end_date)
            if df is not None and len(df) > 0:
                df = df.rename(columns={
                    'trade_date': 'trade_date',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'vol': 'volume',
                    'amount': 'amount',
                    'pct_chg': 'pct_chg',
                })
                df = df.sort_values('trade_date').reset_index(drop=True)
                return df.tail(days)
        except Exception as e:
            logger.warning(f"[申万历史数据] {code} 获取失败: {e}")
        return pd.DataFrame()
    
    async def get_sector_fund_flow(self) -> pd.DataFrame:
        """获取行业资金流向（优先本地缓存）"""
        # 优先读取本地缓存
        try:
            from app.services.sector_data_sync import SectorDataSyncService
            sync_service = SectorDataSyncService()
            
            # 获取最近交易日的行业资金流向
            df = sync_service.get_industry_ranking()
            if len(df) > 0:
                df = df.rename(columns={
                    'industry': 'name',
                    'net_mf_amount_yi': 'net_inflow_yi',
                })
                df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                logger.info(f"[资金流向] 使用本地缓存数据: {len(df)}个行业")
                return df
        except Exception as e:
            logger.warning(f"[资金流向] 本地缓存读取失败: {e}")
        
        # 回退到AkShare
        try:
            df = ak.stock_sector_fund_flow_rank(indicator='今日')
            df = df.rename(columns={
                '名称': 'name',
                '今日涨跌幅': 'change_pct',
                '今日主力净流入-净额': 'main_net_inflow',
                '今日主力净流入-净占比': 'main_net_inflow_pct',
                '今日超大单净流入-净额': 'super_large_net_inflow',
                '今日大单净流入-净额': 'large_net_inflow',
                '今日中单净流入-净额': 'medium_net_inflow',
                '今日小单净流入-净额': 'small_net_inflow',
            })
            df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            return df
        except Exception as e:
            logger.error(f"[行业资金流向] 获取失败: {e}")
            return pd.DataFrame()
    
    # ==================== 板块强度计算 ====================
    
    async def calculate_sector_strength(self, days: int = 20) -> pd.DataFrame:
        """
        计算板块相对强度
        基于估值数据和资金流向综合计算
        
        强度指标 = 估值变化 + 资金流向 + 涨跌幅
        """
        try:
            # 1. 获取申万行业估值数据
            realtime_df = await self.get_sw_index_realtime()
            
            if len(realtime_df) == 0:
                return pd.DataFrame()
            
            # 2. 获取资金流向数据
            flow_df = await self.get_sector_fund_flow()
            
            # 3. 构建综合强度指标
            results = []
            for _, row in realtime_df.iterrows():
                name = row['name']
                code = row['code']
                
                # 基础指标
                pe_ttm = row.get('pe_ttm', 0)
                pb = row.get('pb', 0)
                dividend_yield = row.get('dividend_yield', 0)
                
                # 查找资金流向
                main_flow = 0
                main_flow_pct = 0
                if len(flow_df) > 0:
                    flow_match = flow_df[flow_df['name'] == name]
                    if len(flow_match) > 0:
                        main_flow = flow_match.iloc[0].get('main_net_inflow', 0)
                        main_flow_pct = flow_match.iloc[0].get('main_net_inflow_pct', 0)
                
                # 计算强度评分（简化版）
                # PE越低越好（负向）
                # PB越低越好（负向）
                # 股息率越高越好（正向）
                # 资金流入越大越好（正向）
                
                pe_score = -pe_ttm / 100 if pe_ttm > 0 else 0  # PE得分
                pb_score = -pb / 10 if pb > 0 else 0  # PB得分
                div_score = dividend_yield / 10  # 股息率得分
                flow_score = main_flow_pct / 100 if main_flow_pct else 0  # 资金流向得分
                
                # 综合强度
                strength = (pe_score + pb_score + div_score + flow_score) * 100
                
                results.append({
                    'code': code,
                    'name': name,
                    'pe_ttm': round(pe_ttm, 2),
                    'pb': round(pb, 2),
                    'dividend_yield': round(dividend_yield, 2),
                    'main_flow': round(main_flow, 2) if main_flow else 0,
                    'main_flow_pct': round(main_flow_pct, 2) if main_flow_pct else 0,
                    'strength': round(strength, 2),
                })
            
            df = pd.DataFrame(results)
            if len(df) > 0:
                df = df.sort_values('strength', ascending=False).reset_index(drop=True)
                df['rank'] = range(1, len(df) + 1)
            
            return df
            
        except Exception as e:
            logger.error(f"[板块强度计算] 失败: {e}")
            return pd.DataFrame()
    
    async def calculate_sector_rank_change(self, days: int = 5) -> pd.DataFrame:
        """
        计算板块排名变化
        识别"从弱转强"的板块
        """
        cache_file = os.path.join(self.CACHE_DIR, f"sector_rank_history.json")
        
        # 读取历史排名
        history = {}
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                history = json.load(f)
        
        # 获取当前排名
        current_rank = await self.calculate_sector_strength(days)
        
        if len(current_rank) == 0:
            return pd.DataFrame()
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        current_data = {row['name']: row['rank'] for _, row in current_rank.iterrows()}
        
        # 保存当前排名
        history[current_date] = current_data
        # 只保留最近10天
        if len(history) > 10:
            dates = sorted(history.keys(), reverse=True)[:10]
            history = {d: history[d] for d in dates}
        
        with open(cache_file, 'w') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        
        # 计算排名变化
        results = []
        prev_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        for _, row in current_rank.iterrows():
            name = row['name']
            curr_rank = row['rank']
            
            # 查找历史排名
            prev_rank = None
            for date in sorted(history.keys(), reverse=True):
                if date < current_date and name in history[date]:
                    prev_rank = history[date][name]
                    break
            
            if prev_rank is not None:
                rank_change = prev_rank - curr_rank  # 正数表示排名上升
            else:
                rank_change = 0
            
            results.append({
                'name': name,
                'current_rank': curr_rank,
                'prev_rank': prev_rank,
                'rank_change': rank_change,
                'strength': row['strength'],
                'change_pct': row['change_pct'],
            })
        
        df = pd.DataFrame(results)
        if len(df) > 0:
            df = df.sort_values('rank_change', ascending=False).reset_index(drop=True)
        
        return df
    
    # ==================== 轮动信号识别 ====================
    
    async def detect_rotation_signals(self) -> Dict:
        """
        识别板块轮动信号
        
        信号类型:
        1. 强势启动 - 排名快速上升 + 资金流入
        2. 底部反转 - 长期低位 + 开始放量
        3. 持续强势 - 排名持续靠前
        4. 高位风险 - 涨幅过大 + 资金流出
        """
        signals = {
            'strong_start': [],      # 强势启动
            'bottom_reversal': [],   # 底部反转
            'sustained_strong': [],  # 持续强势
            'high_risk': [],         # 高位风险
        }
        
        try:
            # 获取板块排名变化
            rank_df = await self.calculate_sector_rank_change(days=5)
            
            if len(rank_df) == 0:
                return signals
            
            # 获取资金流向
            flow_df = await self.get_sector_fund_flow()
            
            # 合并数据
            for _, row in rank_df.iterrows():
                name = row['name']
                rank = row['current_rank']
                rank_change = row['rank_change']
                change_pct = row['change_pct']
                
                # 查找资金流向
                main_flow = 0
                if len(flow_df) > 0:
                    flow_match = flow_df[flow_df['name'] == name]
                    if len(flow_match) > 0:
                        main_flow = flow_match.iloc[0].get('main_net_inflow', 0)
                
                signal_data = {
                    'name': name,
                    'rank': rank,
                    'rank_change': rank_change,
                    'change_pct': change_pct,
                    'main_flow': main_flow,
                }
                
                # 强势启动：排名上升超过5位 + 涨幅为正
                if rank_change >= 5 and change_pct > 0:
                    signals['strong_start'].append(signal_data)
                
                # 底部反转：排名在后30%但开始上升
                if rank >= len(rank_df) * 0.7 and rank_change > 0:
                    signals['bottom_reversal'].append(signal_data)
                
                # 持续强势：排名持续前5
                if rank <= 5:
                    signals['sustained_strong'].append(signal_data)
                
                # 高位风险：涨幅过大（超过10%）+ 资金流出
                if change_pct > 10 and main_flow < 0:
                    signals['high_risk'].append(signal_data)
            
            # 按排名变化排序
            for key in signals:
                signals[key] = sorted(signals[key], key=lambda x: x['rank_change'], reverse=True)
            
            return signals
            
        except Exception as e:
            logger.error(f"[轮动信号识别] 失败: {e}")
            return signals
    
    # ==================== 分批建仓策略 ====================
    
    def get_position_suggestion(self, sector_name: str, signal_type: str) -> Dict:
        """
        根据信号类型给出仓位建议
        
        策略:
        - 底部反转: 底仓20% + 突破加仓30%
        - 强势启动: 追仓40% + 回调加仓20%
        - 持续强势: 观望/轻仓20%
        - 高位风险: 减仓/清仓
        """
        suggestions = {
            'bottom_reversal': {
                'base_position': 20,      # 底仓比例
                'add_position': 30,       # 加仓比例（突破后）
                'trigger': '放量突破20日均线',
                'stop_loss': -8,          # 止损比例
                'take_profit': 20,        # 止盈比例
                'description': '左侧埋伏，等待启动信号',
            },
            'strong_start': {
                'base_position': 40,
                'add_position': 20,
                'trigger': '回调企稳后',
                'stop_loss': -5,
                'take_profit': 15,
                'description': '右侧追击，顺势而为',
            },
            'sustained_strong': {
                'base_position': 20,
                'add_position': 10,
                'trigger': '回调5%后',
                'stop_loss': -5,
                'take_profit': 10,
                'description': '轻仓观望，防止追高',
            },
            'high_risk': {
                'base_position': 0,
                'add_position': 0,
                'trigger': '无',
                'stop_loss': 0,
                'take_profit': 0,
                'description': '高位风险，建议减仓或清仓',
            },
        }
        
        result = suggestions.get(signal_type, suggestions['sustained_strong'])
        result['sector_name'] = sector_name
        result['signal_type'] = signal_type
        return result
    
    # ==================== 行业因子权重配置 ====================
    
    def get_industry_factor_weights(self, industry_name: str) -> Dict:
        """
        根据行业获取推荐的因子权重
        
        基于v1.7的行业轮动配置
        """
        # 找到行业所属风格
        style = '其他'
        for s, industries in self.INDUSTRY_STYLE.items():
            if industry_name in industries:
                style = s
                break
        
        # 各风格的因子权重配置
        weights = {
            '科技': {
                'MOM': 0.40,    # 动量为主
                'MA_DEV': 0.20,
                'RSI': 0.15,
                'ROE': 0.15,
                'TURN': 0.10,
                'description': '科技行业注重动量和技术因子',
            },
            '消费': {
                'ROE': 0.35,    # 质量为主
                'NPM': 0.20,
                'MOM': 0.20,
                'BP': 0.15,
                'TURN': 0.10,
                'description': '消费行业注重质量和稳定',
            },
            '金融': {
                'EP': 0.35,     # 价值为主
                'BP': 0.25,
                'ROE': 0.20,
                'LEV': 0.10,
                'MOM': 0.10,
                'description': '金融行业注重价值因子',
            },
            '周期': {
                'BP': 0.35,     # 价值为主
                'EP': 0.25,
                'MOM': 0.20,
                'AT': 0.10,
                'VOL_M': 0.10,
                'description': '周期行业注重价值和动量',
            },
            '制造': {
                'MOM': 0.25,
                'ROE': 0.25,    # 均衡配置
                'AT': 0.20,
                'BP': 0.15,
                'VOL_M': 0.15,
                'description': '制造业均衡配置',
            },
            '医药': {
                'ROE': 0.30,
                'NPM': 0.25,
                'MOM': 0.20,
                'BP': 0.15,
                'TURN': 0.10,
                'description': '医药行业注重质量',
            },
            '公用': {
                'DIV_YIELD': 0.30,  # 股息率
                'BP': 0.25,
                'ROE': 0.20,
                'LEV': 0.15,
                'MOM': 0.10,
                'description': '公用事业注重股息和价值',
            },
            '其他': {
                'MOM': 0.20,
                'ROE': 0.20,
                'BP': 0.20,
                'AT': 0.15,
                'TURN': 0.15,
                'VOL_M': 0.10,
                'description': '均衡配置',
            },
        }
        
        result = weights.get(style, weights['其他'])
        result['industry_name'] = industry_name
        result['style'] = style
        return result
    
    # ==================== 综合报告 ====================
    
    async def get_rotation_report(self) -> Dict:
        """
        生成板块轮动综合报告
        """
        try:
            # 获取轮动信号
            signals = await self.detect_rotation_signals()
            
            # 获取板块强度排名
            strength_df = await self.calculate_sector_strength(days=20)
            
            # 获取资金流向
            flow_df = await self.get_sector_fund_flow()
            
            # 获取申万行业估值数据
            realtime_df = await self.get_sw_index_realtime()
            
            # 汇总报告
            report = {
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'signals': signals,
                'strength_ranking': strength_df.to_dict('records') if len(strength_df) > 0 else [],
                'fund_flow': flow_df.to_dict('records') if len(flow_df) > 0 else [],
                'valuation': realtime_df.to_dict('records') if len(realtime_df) > 0 else [],
                'summary': {
                    'strong_sectors': [s['name'] for s in signals.get('sustained_strong', [])[:5]],
                    'reversal_sectors': [s['name'] for s in signals.get('bottom_reversal', [])[:5]],
                    'risk_sectors': [s['name'] for s in signals.get('high_risk', [])],
                }
            }
            
            return report
            
        except Exception as e:
            logger.error(f"[轮动报告] 生成失败: {e}")
            return {'error': str(e)}
    
    async def get_local_data_report(self) -> Dict:
        """
        基于本地缓存数据生成报告
        
        数据源:
        - data_cache/sector/stock_industry_map.csv
        - data_cache/sector/moneyflow/
        - data_cache/sector/north_money.csv
        """
        try:
            from app.services.sector_data_sync import SectorDataSyncService
            sync_service = SectorDataSyncService()
            
            # 1. 行业资金流向排名
            industry_flow = sync_service.get_industry_ranking()
            
            # 2. 北向资金
            north_money = sync_service.get_north_money_data()
            
            # 3. 股票行业分布
            stock_map = sync_service.get_stock_industry_map()
            
            # 构建报告
            report = {
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_source': 'local_cache',
                'industry_flow': industry_flow.to_dict('records') if len(industry_flow) > 0 else [],
                'north_money': {
                    'latest': north_money.iloc[-1].to_dict() if len(north_money) > 0 else {},
                    'trend_5d': north_money.tail(5)['north_money'].tolist() if len(north_money) >= 5 else [],
                },
                'industry_distribution': stock_map['industry'].value_counts().head(20).to_dict() if len(stock_map) > 0 else {},
                'signals': {
                    'strong_inflow': [],   # 资金强力流入
                    'strong_outflow': [],  # 资金强力流出
                    'north_positive': [],  # 北向资金连续流入
                }
            }
            
            # 生成信号
            if len(industry_flow) > 0:
                report['signals']['strong_inflow'] = industry_flow[
                    industry_flow['net_mf_amount_yi'] > 1
                ][['industry', 'net_mf_amount_yi', 'signal']].head(10).to_dict('records')
                
                report['signals']['strong_outflow'] = industry_flow[
                    industry_flow['net_mf_amount_yi'] < -1
                ][['industry', 'net_mf_amount_yi', 'signal']].head(10).to_dict('records')
            
            if len(north_money) >= 3:
                # 北向资金连续3天流入
                recent_3d = north_money.tail(3)['north_money']
                if all(recent_3d.astype(float) > 0):
                    report['signals']['north_positive'] = {
                        'trend': '连续3天流入',
                        'values': recent_3d.tolist()
                    }
            
            return report
            
        except Exception as e:
            logger.error(f"[本地数据报告] 生成失败: {e}")
            return {'error': str(e)}
    
    async def get_sector_detail(self, sector_name: str) -> Dict:
        """
        获取单个板块详细信息
        """
        try:
            # 获取因子权重
            factor_weights = self.get_industry_factor_weights(sector_name)
            
            # 获取实时数据
            realtime_df = await self.get_sw_index_realtime()
            sector_data = realtime_df[realtime_df['name'] == sector_name]
            
            valuation = {}
            if len(sector_data) > 0:
                valuation = sector_data.iloc[0].to_dict()
            
            # 获取历史数据
            code = [k for k, v in self.SW_INDUSTRIES.items() if v == sector_name]
            hist_df = pd.DataFrame()
            if code:
                hist_df = await self.get_sw_index_history(code[0], days=60)
            
            history = hist_df.to_dict('records') if len(hist_df) > 0 else []
            
            # 计算技术指标
            tech_indicators = {}
            if len(hist_df) >= 20:
                close = hist_df['close'].values
                tech_indicators = {
                    'ma5': round(close[-5:].mean(), 2),
                    'ma10': round(close[-10:].mean(), 2),
                    'ma20': round(close[-20:].mean(), 2),
                    'latest_close': round(close[-1], 2),
                    'high_20d': round(close[-20:].max(), 2),
                    'low_20d': round(close[-20:].min(), 2),
                }
            
            return {
                'sector_name': sector_name,
                'factor_weights': factor_weights,
                'valuation': valuation,
                'tech_indicators': tech_indicators,
                'history': history[-20:],  # 最近20天
            }
            
        except Exception as e:
            logger.error(f"[板块详情] {sector_name} 获取失败: {e}")
            return {'error': str(e)}


# 单例
_service = None

def get_sector_rotation_service() -> SectorRotationService:
    global _service
    if _service is None:
        _service = SectorRotationService()
    return _service