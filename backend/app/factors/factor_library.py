"""
专业量化因子库

基于10年A股日线+财务数据构建
包含价值、成长、质量、动量、情绪五大类因子
"""
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import sqlite3
from dataclasses import dataclass
from enum import Enum


class FactorCategory(Enum):
    """因子类别"""
    VALUE = "value"           # 价值因子
    GROWTH = "growth"         # 成长因子
    QUALITY = "quality"       # 质量因子
    MOMENTUM = "momentum"     # 动量因子
    SENTIMENT = "sentiment"   # 情绪因子
    TECHNICAL = "technical"   # 技术因子


@dataclass
class FactorMeta:
    """因子元数据"""
    name: str                 # 因子名称
    code: str                 # 因子代码
    category: FactorCategory  # 因子类别
    formula: str              # 计算公式
    description: str          # 描述
    weight: float             # 默认权重
    ic_expected: float        # 预期IC
    direction: int            # 方向: 1=正向, -1=反向, 0=中性
    data_source: str          # 数据来源
    update_freq: str          # 更新频率


class FactorLibrary:
    """
    专业因子库
    
    功能：
    1. 因子定义与元数据管理
    2. 因子计算
    3. 因子数据存储与查询
    """
    
    # 数据路径
    DAY_CACHE_DIR = "data_cache/day"
    FINANCIAL_DIR = "data_cache/financial"
    DB_PATH = "smart_quant.db"
    FACTOR_CACHE_DIR = "data_cache/factors"
    
    def __init__(self):
        self._init_factor_definitions()
        self._ensure_cache_dir()
    
    def _ensure_cache_dir(self):
        """确保缓存目录存在"""
        os.makedirs(self.FACTOR_CACHE_DIR, exist_ok=True)
    
    # ==================== 因子定义 ====================
    
    def _init_factor_definitions(self):
        """初始化因子定义"""
        
        self.factor_definitions = {}
        
        # ==================== 价值因子 ====================
        value_factors = [
            FactorMeta(
                name="盈利收益率",
                code="EP",
                category=FactorCategory.VALUE,
                formula="E_ttm / MarketCap",
                description="Earnings to Price，盈利收益率",
                weight=0.25,
                ic_expected=0.04,
                direction=1,
                data_source="daily_basic",
                update_freq="daily"
            ),
            FactorMeta(
                name="账面市值比",
                code="BP",
                category=FactorCategory.VALUE,
                formula="B / MarketCap",
                description="Book to Price，账面市值比",
                weight=0.20,
                ic_expected=0.03,
                direction=1,
                data_source="daily_basic",
                update_freq="daily"
            ),
            FactorMeta(
                name="销售市值比",
                code="SP",
                category=FactorCategory.VALUE,
                formula="Revenue_ttm / MarketCap",
                description="Sales to Price，销售市值比",
                weight=0.15,
                ic_expected=0.02,
                direction=1,
                data_source="daily_basic",
                update_freq="daily"
            ),
            FactorMeta(
                name="现金流市值比",
                code="NCFP",
                category=FactorCategory.VALUE,
                formula="OCF_ttm / MarketCap",
                description="Operating Cash Flow to Price",
                weight=0.20,
                ic_expected=0.04,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="股息率",
                code="DIV_YIELD",
                category=FactorCategory.VALUE,
                formula="DPS / Price",
                description="Dividend Yield",
                weight=0.10,
                ic_expected=0.03,
                direction=1,
                data_source="daily_basic",
                update_freq="daily"
            ),
            FactorMeta(
                name="企业价值比",
                code="EV_EBITDA",
                category=FactorCategory.VALUE,
                formula="EBITDA / EV",
                description="EV to EBITDA",
                weight=0.10,
                ic_expected=0.03,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
        ]
        
        # ==================== 成长因子 ====================
        growth_factors = [
            FactorMeta(
                name="营收增长率",
                code="REV_G",
                category=FactorCategory.GROWTH,
                formula="(Rev_t - Rev_t-1) / |Rev_t-1|",
                description="Revenue Growth YoY",
                weight=0.20,
                ic_expected=0.03,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="净利润增长率",
                code="NP_G",
                category=FactorCategory.GROWTH,
                formula="(NP_t - NP_t-1) / |NP_t-1|",
                description="Net Profit Growth YoY",
                weight=0.25,
                ic_expected=0.04,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="EPS增长率",
                code="EPS_G",
                category=FactorCategory.GROWTH,
                formula="(EPS_t - EPS_t-1) / |EPS_t-1|",
                description="EPS Growth YoY",
                weight=0.15,
                ic_expected=0.03,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="ROE变化",
                code="ROE_D",
                category=FactorCategory.GROWTH,
                formula="ROE_t - ROE_t-1",
                description="ROE Change YoY",
                weight=0.20,
                ic_expected=0.03,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="营收增长加速度",
                code="REV_ACC",
                category=FactorCategory.GROWTH,
                formula="ΔREV_G",
                description="Revenue Growth Acceleration",
                weight=0.10,
                ic_expected=0.02,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="净利率变化",
                code="NPM_D",
                category=FactorCategory.GROWTH,
                formula="NPM_t - NPM_t-1",
                description="Net Profit Margin Change",
                weight=0.10,
                ic_expected=0.02,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
        ]
        
        # ==================== 质量因子 ====================
        quality_factors = [
            FactorMeta(
                name="ROE",
                code="ROE",
                category=FactorCategory.QUALITY,
                formula="NetIncome / Equity",
                description="Return on Equity",
                weight=0.20,
                ic_expected=0.06,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="ROA",
                code="ROA",
                category=FactorCategory.QUALITY,
                formula="NetIncome / TotalAssets",
                description="Return on Assets",
                weight=0.10,
                ic_expected=0.04,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="毛利率",
                code="GPM",
                category=FactorCategory.QUALITY,
                formula="(Rev - COGS) / Rev",
                description="Gross Profit Margin",
                weight=0.10,
                ic_expected=0.03,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="净利率",
                code="NPM",
                category=FactorCategory.QUALITY,
                formula="NetIncome / Revenue",
                description="Net Profit Margin",
                weight=0.10,
                ic_expected=0.03,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="资产周转率",
                code="AT",
                category=FactorCategory.QUALITY,
                formula="Revenue / TotalAssets",
                description="Asset Turnover",
                weight=0.10,
                ic_expected=0.02,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="财务杠杆",
                code="LEV",
                category=FactorCategory.QUALITY,
                formula="Assets / Equity",
                description="Financial Leverage",
                weight=0.10,
                ic_expected=0.02,
                direction=-1,  # 反向
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="流动比率",
                code="CR",
                category=FactorCategory.QUALITY,
                formula="CurrentAssets / CurrentLiabilities",
                description="Current Ratio",
                weight=0.05,
                ic_expected=0.01,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="应计项目",
                code="ACCR",
                category=FactorCategory.QUALITY,
                formula="(NI - OCF) / TotalAssets",
                description="Accruals",
                weight=0.15,
                ic_expected=0.04,
                direction=-1,  # 反向
                data_source="financial",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="现金流质量",
                code="OCF_NI",
                category=FactorCategory.QUALITY,
                formula="OCF / NI",
                description="Operating Cash Flow to Net Income",
                weight=0.10,
                ic_expected=0.03,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
        ]
        
        # ==================== 动量因子 ====================
        momentum_factors = [
            FactorMeta(
                name="12月动量",
                code="MOM12",
                category=FactorCategory.MOMENTUM,
                formula="Price_t / Price_t-12m - 1",
                description="12-Month Momentum",
                weight=0.30,
                ic_expected=0.05,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="6月动量",
                code="MOM6",
                category=FactorCategory.MOMENTUM,
                formula="Price_t / Price_t-6m - 1",
                description="6-Month Momentum",
                weight=0.20,
                ic_expected=0.04,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="相对强度",
                code="RS",
                category=FactorCategory.MOMENTUM,
                formula="Stock / Industry_Index",
                description="Relative Strength",
                weight=0.15,
                ic_expected=0.03,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="成交量动量",
                code="VOL_M",
                category=FactorCategory.MOMENTUM,
                formula="Vol_5d / Vol_20d",
                description="Volume Momentum",
                weight=0.10,
                ic_expected=0.02,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="均线偏离",
                code="MA_DEV",
                category=FactorCategory.MOMENTUM,
                formula="(Price - MA20) / MA20",
                description="MA Deviation",
                weight=0.10,
                ic_expected=0.02,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="盈利修正",
                code="ER",
                category=FactorCategory.MOMENTUM,
                formula="EPS_Revision",
                description="Earnings Revision",
                weight=0.15,
                ic_expected=0.04,
                direction=1,
                data_source="financial",
                update_freq="quarterly"
            ),
        ]
        
        # ==================== 情绪因子 ====================
        sentiment_factors = [
            FactorMeta(
                name="北向持股变化",
                code="NORTH_C",
                category=FactorCategory.SENTIMENT,
                formula="ΔNorthHolding / FloatShares",
                description="Northbound Holding Change",
                weight=0.25,
                ic_expected=0.05,
                direction=1,
                data_source="external",
                update_freq="daily"
            ),
            FactorMeta(
                name="机构持股比例",
                code="INST",
                category=FactorCategory.SENTIMENT,
                formula="InstHolding / TotalShares",
                description="Institutional Holding",
                weight=0.20,
                ic_expected=0.04,
                direction=1,
                data_source="external",
                update_freq="quarterly"
            ),
            FactorMeta(
                name="分析师评级",
                code="ANALYST",
                category=FactorCategory.SENTIMENT,
                formula="Rating_Mean",
                description="Analyst Rating",
                weight=0.15,
                ic_expected=0.03,
                direction=1,
                data_source="external",
                update_freq="daily"
            ),
            FactorMeta(
                name="换手率",
                code="TURN",
                category=FactorCategory.SENTIMENT,
                formula="Volume / FloatShares",
                description="Turnover Rate",
                weight=0.10,
                ic_expected=0.02,
                direction=0,  # 中性
                data_source="daily_basic",
                update_freq="daily"
            ),
            FactorMeta(
                name="融资余额变化",
                code="MTGN_C",
                category=FactorCategory.SENTIMENT,
                formula="ΔMarginBalance / MarketCap",
                description="Margin Balance Change",
                weight=0.15,
                ic_expected=0.03,
                direction=1,
                data_source="external",
                update_freq="daily"
            ),
            FactorMeta(
                name="波动率",
                code="VOL",
                category=FactorCategory.SENTIMENT,
                formula="Std(Return) * sqrt(252)",
                description="Volatility",
                weight=0.15,
                ic_expected=0.02,
                direction=-1,  # 反向
                data_source="day",
                update_freq="daily"
            ),
        ]
        
        # ==================== 技术因子（补充） ====================
        technical_factors = [
            # ========== 趋势类 ==========
            FactorMeta(
                name="均线多头排列",
                code="MA_BULL",
                category=FactorCategory.TECHNICAL,
                formula="MA5>MA10>MA20>MA60",
                description="均线多头排列强度",
                weight=0.04,
                ic_expected=0.025,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="均线偏离MA5",
                code="MA_DEV_5",
                category=FactorCategory.TECHNICAL,
                formula="(Price - MA5) / MA5",
                description="价格与MA5偏离度",
                weight=0.02,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="均线偏离MA20",
                code="MA_DEV_20",
                category=FactorCategory.TECHNICAL,
                formula="(Price - MA20) / MA20",
                description="价格与MA20偏离度",
                weight=0.03,
                ic_expected=0.02,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="均线偏离MA60",
                code="MA_DEV_60",
                category=FactorCategory.TECHNICAL,
                formula="(Price - MA60) / MA60",
                description="价格与MA60偏离度",
                weight=0.02,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="MA金叉",
                code="MA_CROSS",
                category=FactorCategory.TECHNICAL,
                formula="MA5上穿MA20",
                description="MA金叉信号",
                weight=0.03,
                ic_expected=0.025,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            
            # ========== 动量类 ==========
            FactorMeta(
                name="RSI",
                code="RSI",
                category=FactorCategory.TECHNICAL,
                formula="RSI(14)",
                description="Relative Strength Index",
                weight=0.02,
                ic_expected=0.02,
                direction=0,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="RSI_6",
                code="RSI_6",
                category=FactorCategory.TECHNICAL,
                formula="RSI(6)",
                description="6日RSI",
                weight=0.015,
                ic_expected=0.015,
                direction=0,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="KDJ_K",
                code="KDJ_K",
                category=FactorCategory.TECHNICAL,
                formula="K值",
                description="KDJ K值",
                weight=0.015,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="KDJ_D",
                code="KDJ_D",
                category=FactorCategory.TECHNICAL,
                formula="D值",
                description="KDJ D值",
                weight=0.01,
                ic_expected=0.012,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="KDJ_J",
                code="KDJ_J",
                category=FactorCategory.TECHNICAL,
                formula="3K - 2D",
                description="KDJ J值",
                weight=0.02,
                ic_expected=0.018,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="MACD_DIF",
                code="MACD_DIF",
                category=FactorCategory.TECHNICAL,
                formula="EMA12 - EMA26",
                description="MACD DIF",
                weight=0.025,
                ic_expected=0.022,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="MACD_DEA",
                code="MACD_DEA",
                category=FactorCategory.TECHNICAL,
                formula="DIF的EMA9",
                description="MACD DEA",
                weight=0.015,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="MACD_HIST",
                code="MACD_HIST",
                category=FactorCategory.TECHNICAL,
                formula="(DIF - DEA) * 2",
                description="MACD柱状线",
                weight=0.03,
                ic_expected=0.025,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="CCI",
                code="CCI",
                category=FactorCategory.TECHNICAL,
                formula="(TP-MA)/(0.015*MD)",
                description="Commodity Channel Index",
                weight=0.02,
                ic_expected=0.018,
                direction=0,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="WR",
                code="WR",
                category=FactorCategory.TECHNICAL,
                formula="(HIGH_n - CLOSE)/(HIGH_n - LOW_n)*100",
                description="Williams %R",
                weight=0.02,
                ic_expected=0.02,
                direction=-1,
                data_source="day",
                update_freq="daily"
            ),
            
            # ========== 波动类 ==========
            FactorMeta(
                name="布林上轨",
                code="BOLL_UPPER",
                category=FactorCategory.TECHNICAL,
                formula="MA20 + 2*STD20",
                description="Bollinger Upper Band",
                weight=0.01,
                ic_expected=0.01,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="布林下轨",
                code="BOLL_LOWER",
                category=FactorCategory.TECHNICAL,
                formula="MA20 - 2*STD20",
                description="Bollinger Lower Band",
                weight=0.01,
                ic_expected=0.01,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="布林带宽",
                code="BOLL_WIDTH",
                category=FactorCategory.TECHNICAL,
                formula="(UPPER - LOWER) / MA20",
                description="Bollinger Bandwidth",
                weight=0.015,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="布林位置",
                code="BOLL_POS",
                category=FactorCategory.TECHNICAL,
                formula="(Price - MA20) / (2 * Std)",
                description="Bollinger Position",
                weight=0.025,
                ic_expected=0.02,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="ATR",
                code="ATR",
                category=FactorCategory.TECHNICAL,
                formula="Average True Range",
                description="Average True Range",
                weight=0.02,
                ic_expected=0.018,
                direction=-1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="ATR比率",
                code="ATR_R",
                category=FactorCategory.TECHNICAL,
                formula="ATR / Price",
                description="ATR Ratio",
                weight=0.02,
                ic_expected=0.02,
                direction=-1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="波动率20日",
                code="VOLATILITY_20",
                category=FactorCategory.TECHNICAL,
                formula="Std(Return,20)*sqrt(252)",
                description="20日年化波动率",
                weight=0.025,
                ic_expected=0.022,
                direction=-1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="波动率60日",
                code="VOLATILITY_60",
                category=FactorCategory.TECHNICAL,
                formula="Std(Return,60)*sqrt(252)",
                description="60日年化波动率",
                weight=0.02,
                ic_expected=0.02,
                direction=-1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="波动率变化",
                code="VOL_RATIO",
                category=FactorCategory.TECHNICAL,
                formula="VOL_20 / VOL_60",
                description="波动率变化",
                weight=0.015,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            
            # ========== 成交量类 ==========
            FactorMeta(
                name="OBV",
                code="OBV",
                category=FactorCategory.TECHNICAL,
                formula="On Balance Volume",
                description="OBV能量潮",
                weight=0.02,
                ic_expected=0.018,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="OBV_MA",
                code="OBV_MA",
                category=FactorCategory.TECHNICAL,
                formula="MA(OBV,20)",
                description="OBV均线",
                weight=0.015,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="VWAP",
                code="VWAP",
                category=FactorCategory.TECHNICAL,
                formula="Sum(Vol*Price)/Sum(Vol)",
                description="成交量加权均价",
                weight=0.02,
                ic_expected=0.018,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="量价相关",
                code="VOL_PRICE_CORR",
                category=FactorCategory.TECHNICAL,
                formula="Corr(Vol, Price, 20)",
                description="量价相关性",
                weight=0.018,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="成交量MA比率",
                code="VOL_MA_RATIO",
                category=FactorCategory.TECHNICAL,
                formula="Vol / MA(Vol,20)",
                description="成交量与均量比",
                weight=0.02,
                ic_expected=0.018,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="成交额MA比率",
                code="AMOUNT_MA_RATIO",
                category=FactorCategory.TECHNICAL,
                formula="Amount / MA(Amount,20)",
                description="成交额与均额比",
                weight=0.018,
                ic_expected=0.016,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="换手率MA比率",
                code="TURN_MA_RATIO",
                category=FactorCategory.TECHNICAL,
                formula="Turn / MA(Turn,20)",
                description="换手率与均换手比",
                weight=0.018,
                ic_expected=0.016,
                direction=1,
                data_source="daily_basic",
                update_freq="daily"
            ),
            
            # ========== 价格形态类 ==========
            FactorMeta(
                name="高点距离",
                code="HIGH_DIST",
                category=FactorCategory.TECHNICAL,
                formula="(High_20d - Close) / Close",
                description="距20日高点距离",
                weight=0.02,
                ic_expected=0.018,
                direction=-1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="低点距离",
                code="LOW_DIST",
                category=FactorCategory.TECHNICAL,
                formula="(Close - Low_20d) / Close",
                description="距20日低点距离",
                weight=0.02,
                ic_expected=0.018,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="振幅",
                code="AMPLITUDE",
                category=FactorCategory.TECHNICAL,
                formula="(High - Low) / Open",
                description="日内振幅",
                weight=0.015,
                ic_expected=0.015,
                direction=0,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="实体大小",
                code="BODY_SIZE",
                category=FactorCategory.TECHNICAL,
                formula="|Close - Open| / Open",
                description="K线实体大小",
                weight=0.012,
                ic_expected=0.012,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="上影线比例",
                code="UPPER_SHADOW",
                category=FactorCategory.TECHNICAL,
                formula="(High - Max(O,C)) / (High - Low)",
                description="上影线占比",
                weight=0.01,
                ic_expected=0.01,
                direction=-1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="下影线比例",
                code="LOWER_SHADOW",
                category=FactorCategory.TECHNICAL,
                formula="(Min(O,C) - Low) / (High - Low)",
                description="下影线占比",
                weight=0.01,
                ic_expected=0.01,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="阳线比例",
                code="BULLISH_RATIO",
                category=FactorCategory.TECHNICAL,
                formula="Count(Close>Open,20)/20",
                description="20日阳线比例",
                weight=0.015,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="连阳天数",
                code="CONSECUTIVE_UP",
                category=FactorCategory.TECHNICAL,
                formula="连续阳线天数",
                description="连续上涨天数",
                weight=0.015,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="连阴天数",
                code="CONSECUTIVE_DOWN",
                category=FactorCategory.TECHNICAL,
                formula="连续阴线天数",
                description="连续下跌天数",
                weight=0.012,
                ic_expected=0.012,
                direction=-1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="突破MA20",
                code="BREAK_MA20",
                category=FactorCategory.TECHNICAL,
                formula="Close突破MA20",
                description="突破MA20信号",
                weight=0.025,
                ic_expected=0.022,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="突破MA60",
                code="BREAK_MA60",
                category=FactorCategory.TECHNICAL,
                formula="Close突破MA60",
                description="突破MA60信号",
                weight=0.025,
                ic_expected=0.022,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            
            # ========== 缺口类 ==========
            FactorMeta(
                name="向上跳空",
                code="GAP_UP",
                category=FactorCategory.TECHNICAL,
                formula="Open > High_t-1",
                description="向上跳空缺口",
                weight=0.015,
                ic_expected=0.015,
                direction=1,
                data_source="day",
                update_freq="daily"
            ),
            FactorMeta(
                name="向下跳空",
                code="GAP_DOWN",
                category=FactorCategory.TECHNICAL,
                formula="Open < Low_t-1",
                description="向下跳空缺口",
                weight=0.012,
                ic_expected=0.012,
                direction=-1,
                data_source="day",
                update_freq="daily"
            ),
        ]
        
        # 合并所有因子
        all_factors = (
            value_factors + 
            growth_factors + 
            quality_factors + 
            momentum_factors + 
            sentiment_factors +
            technical_factors
        )
        
        for factor in all_factors:
            self.factor_definitions[factor.code] = factor
        
        # 按类别分组
        self.factors_by_category = {
            FactorCategory.VALUE: [f.code for f in value_factors],
            FactorCategory.GROWTH: [f.code for f in growth_factors],
            FactorCategory.QUALITY: [f.code for f in quality_factors],
            FactorCategory.MOMENTUM: [f.code for f in momentum_factors],
            FactorCategory.SENTIMENT: [f.code for f in sentiment_factors],
            FactorCategory.TECHNICAL: [f.code for f in technical_factors],
        }
    
    # ==================== 因子计算 ====================
    
    def calc_factor(self, factor_code: str, stock_code: str, 
                    date: str = None) -> Optional[float]:
        """
        计算单个因子值
        
        Args:
            factor_code: 因子代码
            stock_code: 股票代码
            date: 日期 (YYYYMMDD)
        """
        factor_meta = self.factor_definitions.get(factor_code)
        if not factor_meta:
            return None
        
        # 根据数据源加载不同数据
        if factor_meta.data_source == "day":
            return self._calc_day_factor(factor_code, stock_code, date)
        elif factor_meta.data_source == "daily_basic":
            return self._calc_daily_basic_factor(factor_code, stock_code, date)
        elif factor_meta.data_source == "financial":
            return self._calc_financial_factor(factor_code, stock_code, date)
        else:
            return None
    
    def _calc_day_factor(self, factor_code: str, stock_code: str, 
                         date: str = None) -> Optional[float]:
        """计算行情类因子"""
        file_path = os.path.join(self.DAY_CACHE_DIR, f"{stock_code}_day.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            
            df = df.sort_values('日期', ascending=False)
            
            if factor_code == "MOM12":
                # 12月动量
                if len(df) < 252:
                    return None
                close = df['收盘'].values
                return float((close[0] / close[252]) - 1) if close[252] > 0 else None
            
            elif factor_code == "MOM6":
                # 6月动量
                if len(df) < 126:
                    return None
                close = df['收盘'].values
                return float((close[0] / close[126]) - 1) if close[126] > 0 else None
            
            elif factor_code == "VOL_M":
                # 成交量动量
                if len(df) < 20:
                    return None
                vol = df['成交量'].values
                vol_5 = vol[:5].mean()
                vol_20 = vol[:20].mean()
                return float(vol_5 / vol_20) if vol_20 > 0 else None
            
            elif factor_code == "MA_DEV":
                # 均线偏离
                if len(df) < 20:
                    return None
                close = df['收盘'].values
                ma20 = close[:20].mean()
                return float((close[0] - ma20) / ma20) if ma20 > 0 else None
            
            elif factor_code == "VOL":
                # 波动率
                if len(df) < 20:
                    return None
                returns = pd.Series(df['收盘'].values).pct_change().dropna()
                return float(returns.std() * np.sqrt(252) * 100)
            
            elif factor_code == "RSI":
                # RSI
                return self._calc_rsi(df['收盘'].values)
            
            elif factor_code == "MACD":
                # MACD
                return self._calc_macd(df['收盘'].values)
            
            elif factor_code == "KDJ_J":
                # KDJ J值
                return self._calc_kdj_j(df)
            
            elif factor_code == "BOLL_POS":
                # 布林位置
                return self._calc_boll_pos(df['收盘'].values)
            
            elif factor_code == "OBV":
                # OBV
                return self._calc_obv(df)
            
            elif factor_code == "ATR_R":
                # ATR比率
                return self._calc_atr_ratio(df)
            
            # ========== 新增技术因子 ==========
            
            # 趋势类
            elif factor_code == "MA_BULL":
                return self._calc_ma_bull(df)
            elif factor_code == "MA_DEV_5":
                return self._calc_ma_dev(df, 5)
            elif factor_code == "MA_DEV_60":
                return self._calc_ma_dev(df, 60)
            elif factor_code == "MA_CROSS":
                return self._calc_ma_cross(df)
            
            # 动量类
            elif factor_code == "RSI_6":
                return self._calc_rsi(df['收盘'].values, 6)
            elif factor_code == "KDJ_K":
                return self._calc_kdj_k(df)
            elif factor_code == "KDJ_D":
                return self._calc_kdj_d(df)
            elif factor_code == "MACD_DIF":
                return self._calc_macd_dif(df['收盘'].values)
            elif factor_code == "MACD_DEA":
                return self._calc_macd_dea(df['收盘'].values)
            elif factor_code == "MACD_HIST":
                return self._calc_macd_hist(df['收盘'].values)
            elif factor_code == "CCI":
                return self._calc_cci(df)
            elif factor_code == "WR":
                return self._calc_wr(df)
            
            # 波动类
            elif factor_code == "BOLL_UPPER":
                return self._calc_boll_upper(df['收盘'].values)
            elif factor_code == "BOLL_LOWER":
                return self._calc_boll_lower(df['收盘'].values)
            elif factor_code == "BOLL_WIDTH":
                return self._calc_boll_width(df['收盘'].values)
            elif factor_code == "ATR":
                return self._calc_atr(df)
            elif factor_code == "VOLATILITY_20":
                return self._calc_volatility(df, 20)
            elif factor_code == "VOLATILITY_60":
                return self._calc_volatility(df, 60)
            elif factor_code == "VOL_RATIO":
                return self._calc_vol_ratio(df)
            
            # 成交量类
            elif factor_code == "OBV_MA":
                return self._calc_obv_ma(df)
            elif factor_code == "VWAP":
                return self._calc_vwap(df)
            elif factor_code == "VOL_PRICE_CORR":
                return self._calc_vol_price_corr(df)
            elif factor_code == "VOL_MA_RATIO":
                return self._calc_vol_ma_ratio(df)
            elif factor_code == "AMOUNT_MA_RATIO":
                return self._calc_amount_ma_ratio(df)
            elif factor_code == "TURN_MA_RATIO":
                return self._calc_turn_ma_ratio(df)
            
            # 价格形态类
            elif factor_code == "HIGH_DIST":
                return self._calc_high_dist(df)
            elif factor_code == "LOW_DIST":
                return self._calc_low_dist(df)
            elif factor_code == "AMPLITUDE":
                return self._calc_amplitude(df)
            elif factor_code == "BODY_SIZE":
                return self._calc_body_size(df)
            elif factor_code == "UPPER_SHADOW":
                return self._calc_upper_shadow(df)
            elif factor_code == "LOWER_SHADOW":
                return self._calc_lower_shadow(df)
            elif factor_code == "BULLISH_RATIO":
                return self._calc_bullish_ratio(df)
            elif factor_code == "CONSECUTIVE_UP":
                return self._calc_consecutive_up(df)
            elif factor_code == "CONSECUTIVE_DOWN":
                return self._calc_consecutive_down(df)
            elif factor_code == "BREAK_MA20":
                return self._calc_break_ma(df, 20)
            elif factor_code == "BREAK_MA60":
                return self._calc_break_ma(df, 60)
            
            # 缺口类
            elif factor_code == "GAP_UP":
                return self._calc_gap_up(df)
            elif factor_code == "GAP_DOWN":
                return self._calc_gap_down(df)
            
            return None
            
        except Exception as e:
            return None
    
    def _calc_daily_basic_factor(self, factor_code: str, stock_code: str, 
                                  date: str = None) -> Optional[float]:
        """计算每日基本面因子"""
        file_path = os.path.join(self.FINANCIAL_DIR, f"{stock_code}_daily_basic.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            
            df = df.sort_values('trade_date', ascending=False)
            latest = df.iloc[0]
            
            if factor_code == "EP":
                pe = latest.get('pe_ttm')
                return float(100 / pe) if pe and pe > 0 else None
            
            elif factor_code == "BP":
                pb = latest.get('pb')
                return float(100 / pb) if pb and pb > 0 else None
            
            elif factor_code == "SP":
                ps = latest.get('ps_ttm')
                return float(100 / ps) if ps and ps > 0 else None
            
            elif factor_code == "DIV_YIELD":
                return float(latest.get('dv_ratio')) if latest.get('dv_ratio') else None
            
            elif factor_code == "TURN":
                return float(latest.get('turnover_rate'))
            
            return None
            
        except Exception as e:
            return None
    
    def _calc_financial_factor(self, factor_code: str, stock_code: str, 
                                date: str = None) -> Optional[float]:
        """计算财务因子"""
        file_path = os.path.join(self.FINANCIAL_DIR, f"{stock_code}_fina_indicator.csv")
        if not os.path.exists(file_path):
            return None
        
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                return None
            
            df = df.sort_values('end_date', ascending=False)
            latest = df.iloc[0]
            prev = df.iloc[3] if len(df) > 3 else latest
            
            if factor_code == "ROE":
                return float(latest.get('roe')) if latest.get('roe') else None
            
            elif factor_code == "ROA":
                return float(latest.get('roa')) if latest.get('roa') else None
            
            elif factor_code == "GPM":
                return float(latest.get('grossprofit_margin')) if latest.get('grossprofit_margin') else None
            
            elif factor_code == "NPM":
                return float(latest.get('netprofit_margin')) if latest.get('netprofit_margin') else None
            
            elif factor_code == "REV_G":
                return float(latest.get('tr_yoy')) if latest.get('tr_yoy') else None
            
            elif factor_code == "NP_G":
                return float(latest.get('netprofit_yoy')) if latest.get('netprofit_yoy') else None
            
            elif factor_code == "EPS_G":
                return float(latest.get('dt_eps_yoy')) if latest.get('dt_eps_yoy') else None
            
            elif factor_code == "ROE_D":
                roe_t = latest.get('roe')
                roe_prev = prev.get('roe')
                if roe_t and roe_prev:
                    return float(roe_t - roe_prev)
                return None
            
            elif factor_code == "LEV":
                debt = latest.get('debt_to_assets')
                return float(debt) if debt else None
            
            elif factor_code == "CR":
                return float(latest.get('current_ratio')) if latest.get('current_ratio') else None
            
            elif factor_code == "ACCR":
                ocfps = latest.get('ocfps')
                eps = latest.get('eps')
                if ocfps and eps:
                    return float((eps - ocfps) / eps) if eps != 0 else None
                return None
            
            elif factor_code == "OCF_NI":
                ocfps = latest.get('ocfps')
                eps = latest.get('eps')
                if ocfps and eps and eps != 0:
                    return float(ocfps / eps)
                return None
            
            elif factor_code == "NCFP":
                ocfps = latest.get('ocfps')
                # 需要配合股价
                day_file = os.path.join(self.DAY_CACHE_DIR, f"{stock_code}_day.csv")
                if os.path.exists(day_file):
                    day_df = pd.read_csv(day_file)
                    if not day_df.empty:
                        close = day_df.iloc[-1]['收盘']
                        if ocfps and close > 0:
                            return float(ocfps / close * 100)
                return None
            
            return None
            
        except Exception as e:
            return None
    
    # ==================== 技术指标计算 ====================
    
    def _calc_rsi(self, close: np.ndarray, period: int = 14) -> Optional[float]:
        """计算RSI"""
        if len(close) < period + 1:
            return None
        
        deltas = np.diff(close)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)
    
    def _calc_macd(self, close: np.ndarray) -> Optional[float]:
        """计算MACD"""
        if len(close) < 35:
            return None
        
        ema12 = pd.Series(close).ewm(span=12).mean()
        ema26 = pd.Series(close).ewm(span=26).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9).mean()
        macd = (dif - dea).iloc[-1]
        return float(macd)
    
    def _calc_kdj_j(self, df: pd.DataFrame) -> Optional[float]:
        """计算KDJ J值"""
        if len(df) < 9:
            return None
        
        low_min = df['最低'].rolling(9).min()
        high_max = df['最高'].rolling(9).max()
        
        rsv = (df['收盘'] - low_min) / (high_max - low_min) * 100
        rsv = rsv.fillna(50)
        
        k = rsv.ewm(com=2).mean()
        d = k.ewm(com=2).mean()
        j = 3 * k - 2 * d
        
        return float(j.iloc[-1])
    
    def _calc_boll_pos(self, close: np.ndarray) -> Optional[float]:
        """计算布林位置"""
        if len(close) < 20:
            return None
        
        ma20 = np.mean(close[-20:])
        std20 = np.std(close[-20:])
        
        if std20 == 0:
            return 0
        
        return float((close[-1] - ma20) / (2 * std20))
    
    def _calc_obv(self, df: pd.DataFrame) -> Optional[float]:
        """计算OBV"""
        if '成交量' not in df.columns or len(df) < 2:
            return None
        
        obv = 0
        close = df['收盘'].values
        volume = df['成交量'].values
        
        for i in range(1, len(close)):
            if close[i] > close[i-1]:
                obv += volume[i]
            elif close[i] < close[i-1]:
                obv -= volume[i]
        
        return float(obv)
    
    def _calc_atr_ratio(self, df: pd.DataFrame) -> Optional[float]:
        """计算ATR比率"""
        if len(df) < 14:
            return None
        
        high = df['最高'].values
        low = df['最低'].values
        close = df['收盘'].values
        
        tr = np.maximum(
            high[-14:] - low[-14:],
            np.abs(high[-14:] - np.roll(close, 1)[-14:]),
            np.abs(low[-14:] - np.roll(close, 1)[-14:])
        )
        
        atr = np.mean(tr[1:])  # 跳过第一个
        return float(atr / close[-1]) if close[-1] > 0 else None
    
    # ==================== 新增技术因子计算方法 ====================
    
    def _calc_ma_bull(self, df: pd.DataFrame) -> Optional[float]:
        """均线多头排列强度"""
        if len(df) < 60:
            return None
        close = df['收盘'].values
        ma5 = np.mean(close[:5])
        ma10 = np.mean(close[:10])
        ma20 = np.mean(close[:20])
        ma60 = np.mean(close[:60])
        
        # 多头排列得分
        score = 0
        if ma5 > ma10: score += 25
        if ma10 > ma20: score += 25
        if ma20 > ma60: score += 25
        if ma5 > ma20: score += 15
        if ma5 > ma60: score += 10
        return float(score)
    
    def _calc_ma_dev(self, df: pd.DataFrame, period: int) -> Optional[float]:
        """均线偏离"""
        if len(df) < period:
            return None
        close = df['收盘'].values
        ma = np.mean(close[:period])
        return float((close[0] - ma) / ma) if ma > 0 else None
    
    def _calc_ma_cross(self, df: pd.DataFrame) -> Optional[float]:
        """MA金叉信号"""
        if len(df) < 20:
            return None
        close = df['收盘'].values
        ma5_prev = np.mean(close[1:6])
        ma20_prev = np.mean(close[1:21])
        ma5_curr = np.mean(close[:5])
        ma20_curr = np.mean(close[:20])
        
        if ma5_prev <= ma20_prev and ma5_curr > ma20_curr:
            return 100.0  # 金叉
        return 0.0
    
    def _calc_kdj_k(self, df: pd.DataFrame) -> Optional[float]:
        """KDJ K值"""
        if len(df) < 9:
            return None
        low_min = df['最低'].rolling(9).min()
        high_max = df['最高'].rolling(9).max()
        rsv = (df['收盘'] - low_min) / (high_max - low_min) * 100
        rsv = rsv.fillna(50)
        k = rsv.ewm(com=2).mean()
        return float(k.iloc[-1])
    
    def _calc_kdj_d(self, df: pd.DataFrame) -> Optional[float]:
        """KDJ D值"""
        if len(df) < 12:
            return None
        low_min = df['最低'].rolling(9).min()
        high_max = df['最高'].rolling(9).max()
        rsv = (df['收盘'] - low_min) / (high_max - low_min) * 100
        rsv = rsv.fillna(50)
        k = rsv.ewm(com=2).mean()
        d = k.ewm(com=2).mean()
        return float(d.iloc[-1])
    
    def _calc_macd_dif(self, close: np.ndarray) -> Optional[float]:
        """MACD DIF"""
        if len(close) < 26:
            return None
        ema12 = pd.Series(close).ewm(span=12).mean()
        ema26 = pd.Series(close).ewm(span=26).mean()
        return float((ema12 - ema26).iloc[-1])
    
    def _calc_macd_dea(self, close: np.ndarray) -> Optional[float]:
        """MACD DEA"""
        if len(close) < 35:
            return None
        ema12 = pd.Series(close).ewm(span=12).mean()
        ema26 = pd.Series(close).ewm(span=26).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9).mean()
        return float(dea.iloc[-1])
    
    def _calc_macd_hist(self, close: np.ndarray) -> Optional[float]:
        """MACD柱状线"""
        if len(close) < 35:
            return None
        ema12 = pd.Series(close).ewm(span=12).mean()
        ema26 = pd.Series(close).ewm(span=26).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9).mean()
        return float(2 * (dif - dea).iloc[-1])
    
    def _calc_cci(self, df: pd.DataFrame) -> Optional[float]:
        """CCI"""
        if len(df) < 14:
            return None
        tp = (df['最高'] + df['最低'] + df['收盘']) / 3
        ma = tp.rolling(14).mean()
        md = tp.rolling(14).apply(lambda x: np.abs(x - x.mean()).mean())
        cci = (tp - ma) / (0.015 * md)
        return float(cci.iloc[-1]) if not pd.isna(cci.iloc[-1]) else None
    
    def _calc_wr(self, df: pd.DataFrame) -> Optional[float]:
        """Williams %R"""
        if len(df) < 14:
            return None
        high_14 = df['最高'].rolling(14).max().iloc[-1]
        low_14 = df['最低'].rolling(14).min().iloc[-1]
        close = df['收盘'].iloc[-1]
        if high_14 == low_14:
            return 50.0
        return float((high_14 - close) / (high_14 - low_14) * 100)
    
    def _calc_boll_upper(self, close: np.ndarray) -> Optional[float]:
        """布林上轨"""
        if len(close) < 20:
            return None
        ma20 = np.mean(close[-20:])
        std20 = np.std(close[-20:])
        return float(ma20 + 2 * std20)
    
    def _calc_boll_lower(self, close: np.ndarray) -> Optional[float]:
        """布林下轨"""
        if len(close) < 20:
            return None
        ma20 = np.mean(close[-20:])
        std20 = np.std(close[-20:])
        return float(ma20 - 2 * std20)
    
    def _calc_boll_width(self, close: np.ndarray) -> Optional[float]:
        """布林带宽"""
        if len(close) < 20:
            return None
        ma20 = np.mean(close[-20:])
        std20 = np.std(close[-20:])
        return float(4 * std20 / ma20) if ma20 > 0 else None
    
    def _calc_atr(self, df: pd.DataFrame) -> Optional[float]:
        """ATR"""
        if len(df) < 14:
            return None
        high = df['最高'].values
        low = df['最低'].values
        close = df['收盘'].values
        
        tr = np.maximum(
            high[-14:] - low[-14:],
            np.abs(high[-14:] - np.roll(close, 1)[-14:]),
            np.abs(low[-14:] - np.roll(close, 1)[-14:])
        )
        return float(np.mean(tr[1:]))
    
    def _calc_volatility(self, df: pd.DataFrame, period: int) -> Optional[float]:
        """波动率"""
        if len(df) < period:
            return None
        returns = df['收盘'].pct_change().dropna()
        if len(returns) < period:
            return None
        return float(returns.iloc[-period:].std() * np.sqrt(252) * 100)
    
    def _calc_vol_ratio(self, df: pd.DataFrame) -> Optional[float]:
        """波动率变化"""
        vol_20 = self._calc_volatility(df, 20)
        vol_60 = self._calc_volatility(df, 60)
        if vol_20 and vol_60 and vol_60 > 0:
            return float(vol_20 / vol_60)
        return None
    
    def _calc_obv_ma(self, df: pd.DataFrame) -> Optional[float]:
        """OBV均线"""
        if '成交量' not in df.columns or len(df) < 20:
            return None
        obv = self._calc_obv(df)
        if obv is None:
            return None
        # 简化：返回OBV本身
        return float(obv)
    
    def _calc_vwap(self, df: pd.DataFrame) -> Optional[float]:
        """VWAP"""
        if '成交额' not in df.columns or len(df) < 1:
            return None
        amount = df['成交额'].iloc[-1]
        volume = df['成交量'].iloc[-1]
        if volume > 0:
            return float(amount / volume)
        return None
    
    def _calc_vol_price_corr(self, df: pd.DataFrame) -> Optional[float]:
        """量价相关性"""
        if '成交量' not in df.columns or len(df) < 20:
            return None
        close = df['收盘'].iloc[-20:]
        vol = df['成交量'].iloc[-20:]
        return float(close.corr(vol))
    
    def _calc_vol_ma_ratio(self, df: pd.DataFrame) -> Optional[float]:
        """成交量MA比率"""
        if '成交量' not in df.columns or len(df) < 20:
            return None
        vol = df['成交量'].values
        vol_curr = vol[0]
        vol_ma = np.mean(vol[:20])
        return float(vol_curr / vol_ma) if vol_ma > 0 else None
    
    def _calc_amount_ma_ratio(self, df: pd.DataFrame) -> Optional[float]:
        """成交额MA比率"""
        if '成交额' not in df.columns or len(df) < 20:
            return None
        amount = df['成交额'].values
        amt_curr = amount[0]
        amt_ma = np.mean(amount[:20])
        return float(amt_curr / amt_ma) if amt_ma > 0 else None
    
    def _calc_turn_ma_ratio(self, df: pd.DataFrame) -> Optional[float]:
        """换手率MA比率"""
        # 需要从daily_basic获取
        return None
    
    def _calc_high_dist(self, df: pd.DataFrame) -> Optional[float]:
        """距高点距离"""
        if len(df) < 20:
            return None
        high_20 = df['最高'].iloc[-20:].max()
        close = df['收盘'].iloc[-1]
        return float((high_20 - close) / close) if close > 0 else None
    
    def _calc_low_dist(self, df: pd.DataFrame) -> Optional[float]:
        """距低点距离"""
        if len(df) < 20:
            return None
        low_20 = df['最低'].iloc[-20:].min()
        close = df['收盘'].iloc[-1]
        return float((close - low_20) / close) if close > 0 else None
    
    def _calc_amplitude(self, df: pd.DataFrame) -> Optional[float]:
        """振幅"""
        if len(df) < 1:
            return None
        high = df['最高'].iloc[-1]
        low = df['最低'].iloc[-1]
        open_ = df['开盘'].iloc[-1]
        return float((high - low) / open_) if open_ > 0 else None
    
    def _calc_body_size(self, df: pd.DataFrame) -> Optional[float]:
        """实体大小"""
        if len(df) < 1:
            return None
        close = df['收盘'].iloc[-1]
        open_ = df['开盘'].iloc[-1]
        return float(abs(close - open_) / open_) if open_ > 0 else None
    
    def _calc_upper_shadow(self, df: pd.DataFrame) -> Optional[float]:
        """上影线比例"""
        if len(df) < 1:
            return None
        high = df['最高'].iloc[-1]
        low = df['最低'].iloc[-1]
        close = df['收盘'].iloc[-1]
        open_ = df['开盘'].iloc[-1]
        upper = high - max(close, open_)
        total = high - low
        return float(upper / total) if total > 0 else None
    
    def _calc_lower_shadow(self, df: pd.DataFrame) -> Optional[float]:
        """下影线比例"""
        if len(df) < 1:
            return None
        high = df['最高'].iloc[-1]
        low = df['最低'].iloc[-1]
        close = df['收盘'].iloc[-1]
        open_ = df['开盘'].iloc[-1]
        lower = min(close, open_) - low
        total = high - low
        return float(lower / total) if total > 0 else None
    
    def _calc_bullish_ratio(self, df: pd.DataFrame) -> Optional[float]:
        """阳线比例"""
        if len(df) < 20:
            return None
        close = df['收盘'].iloc[-20:]
        open_ = df['开盘'].iloc[-20:]
        bullish = (close > open_).sum()
        return float(bullish / 20)
    
    def _calc_consecutive_up(self, df: pd.DataFrame) -> Optional[float]:
        """连阳天数"""
        if len(df) < 5:
            return None
        close = df['收盘'].values
        open_ = df['开盘'].values
        
        count = 0
        for i in range(len(close)):
            if close[i] > open_[i]:
                count += 1
            else:
                break
        return float(min(count, 10))  # 最多10分
    
    def _calc_consecutive_down(self, df: pd.DataFrame) -> Optional[float]:
        """连阴天数"""
        if len(df) < 5:
            return None
        close = df['收盘'].values
        open_ = df['开盘'].values
        
        count = 0
        for i in range(len(close)):
            if close[i] < open_[i]:
                count += 1
            else:
                break
        return float(min(count, 10))
    
    def _calc_break_ma(self, df: pd.DataFrame, period: int) -> Optional[float]:
        """突破MA"""
        if len(df) < period + 1:
            return None
        close = df['收盘'].values
        ma_prev = np.mean(close[1:period+1])
        ma_curr = np.mean(close[:period])
        
        if close[1] <= ma_prev and close[0] > ma_curr:
            return 100.0
        return 0.0
    
    def _calc_gap_up(self, df: pd.DataFrame) -> Optional[float]:
        """向上跳空"""
        if len(df) < 2:
            return None
        open_ = df['开盘'].iloc[-1]
        high_prev = df['最高'].iloc[-2]
        return 100.0 if open_ > high_prev else 0.0
    
    def _calc_gap_down(self, df: pd.DataFrame) -> Optional[float]:
        """向下跳空"""
        if len(df) < 2:
            return None
        open_ = df['开盘'].iloc[-1]
        low_prev = df['最低'].iloc[-2]
        return 100.0 if open_ < low_prev else 0.0
    
    # ==================== 批量计算 ====================
    
    def calc_all_factors(self, stock_code: str, date: str = None) -> Dict[str, float]:
        """
        计算所有因子值
        
        Returns:
            {factor_code: value}
        """
        result = {}
        for factor_code in self.factor_definitions:
            try:
                value = self.calc_factor(factor_code, stock_code, date)
                if value is not None and not np.isnan(value) and not np.isinf(value):
                    result[factor_code] = float(value)
            except:
                continue
        return result
    
    def calc_category_factors(self, category: FactorCategory, 
                              stock_code: str, date: str = None) -> Dict[str, float]:
        """计算某类别的因子"""
        result = {}
        for factor_code in self.factors_by_category.get(category, []):
            value = self.calc_factor(factor_code, stock_code, date)
            if value is not None:
                result[factor_code] = value
        return result
    
    # ==================== 因子信息查询 ====================
    
    def get_factor_info(self, factor_code: str) -> Optional[FactorMeta]:
        """获取因子信息"""
        return self.factor_definitions.get(factor_code)
    
    def list_factors(self, category: FactorCategory = None) -> List[FactorMeta]:
        """列出因子"""
        if category:
            codes = self.factors_by_category.get(category, [])
            return [self.factor_definitions[c] for c in codes]
        return list(self.factor_definitions.values())
    
    def get_factor_weights(self, category: FactorCategory) -> Dict[str, float]:
        """获取某类别的因子权重"""
        codes = self.factors_by_category.get(category, [])
        return {code: self.factor_definitions[code].weight for code in codes}


# ==================== 单例 ====================

_factor_library = None

def get_factor_library() -> FactorLibrary:
    global _factor_library
    if _factor_library is None:
        _factor_library = FactorLibrary()
    return _factor_library