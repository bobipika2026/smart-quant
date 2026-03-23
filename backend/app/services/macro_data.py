"""
宏观经济数据同步服务

获取过去10年的宏观经济环境数据：
1. GDP（季度）
2. CPI、PPI（月度）
3. 货币供应量 M0/M1/M2（月度）
4. 社会融资规模（月度）
5. PMI指数（月度）
6. 利率（LPR、SHIBOR）
7. 汇率
8. 工业增加值
9. 固定资产投资
10. 社会消费品零售总额
11. 进出口数据
"""
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import tushare as ts

from app.config import settings

logger = logging.getLogger(__name__)


class MacroDataService:
    """宏观经济数据服务"""
    
    # 缓存目录
    CACHE_DIR = "data_cache/macro"
    
    # 宏观指标配置
    MACRO_INDICATORS = {
        # === 经济增长 ===
        'gdp': {
            'name': '国内生产总值',
            'api': 'cn_gdp',
            'freq': '季度',
            'source': 'tushare'
        },
        'gdp_yoy': {
            'name': 'GDP同比增速',
            'api': 'cn_gdp_yoy',
            'freq': '季度',
            'source': 'akshare'
        },
        
        # === 通胀指标 ===
        'cpi': {
            'name': 'CPI同比',
            'api': 'cn_cpi_yearly',
            'freq': '月度',
            'source': 'akshare'
        },
        'cpi_m': {
            'name': 'CPI月度数据',
            'api': 'shibor',  # tushare接口
            'freq': '月度',
            'source': 'tushare'
        },
        'ppi': {
            'name': 'PPI同比',
            'api': 'cn_ppi_yearly',
            'freq': '月度',
            'source': 'akshare'
        },
        
        # === 货币供应 ===
        'money_supply': {
            'name': '货币供应量M0/M1/M2',
            'api': 'cn_m2_yearly',
            'freq': '月度',
            'source': 'tushare'
        },
        
        # === 社会融资 ===
        'social_financing': {
            'name': '社会融资规模',
            'api': 'cn_sf',
            'freq': '月度',
            'source': 'akshare'
        },
        
        # === PMI ===
        'pmi_manufacturing': {
            'name': '制造业PMI',
            'api': 'cn_pmi',
            'freq': '月度',
            'source': 'akshare'
        },
        'pmi_non_manufacturing': {
            'name': '非制造业PMI',
            'api': 'cn_nonmanufact_pmi',
            'freq': '月度',
            'source': 'akshare'
        },
        
        # === 利率 ===
        'lpr': {
            'name': '贷款市场报价利率LPR',
            'api': 'lpr_data',
            'freq': '月度',
            'source': 'tushare'
        },
        'shibor': {
            'name': '上海银行间同业拆放利率',
            'api': 'shibor',
            'freq': '日度',
            'source': 'tushare'
        },
        
        # === 汇率 ===
        'usd_cny': {
            'name': '美元兑人民币汇率',
            'api': 'fx_spot',
            'freq': '日度',
            'source': 'tushare'
        },
        
        # === 工业 ===
        'industrial_value': {
            'name': '工业增加值',
            'api': 'cn_iv',
            'freq': '月度',
            'source': 'akshare'
        },
        
        # === 投资 ===
        'fixed_investment': {
            'name': '固定资产投资',
            'api': 'cn_fa',
            'freq': '月度',
            'source': 'akshare'
        },
        
        # === 消费 ===
        'retail_sales': {
            'name': '社会消费品零售总额',
            'api': 'cn_rsc',
            'freq': '月度',
            'source': 'akshare'
        },
        
        # === 进出口 ===
        'trade_data': {
            'name': '进出口数据',
            'api': 'cn_trade',
            'freq': '月度',
            'source': 'akshare'
        },
    }
    
    def __init__(self):
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        
        if settings.TUSHARE_TOKEN:
            ts.set_token(settings.TUSHARE_TOKEN)
    
    def sync_all_macro_data(self, years: int = 10) -> Dict:
        """
        同步所有宏观经济数据
        
        Args:
            years: 历史年数
        
        Returns:
            同步结果
        """
        import akshare as ak
        
        logger.info("=" * 60)
        logger.info(f"开始同步宏观经济数据（过去{years}年）")
        logger.info("=" * 60)
        
        results = {}
        pro = ts.pro_api() if settings.TUSHARE_TOKEN else None
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years * 365)
        
        # 1. GDP数据（季度）
        logger.info("\n[1] 同步GDP数据...")
        try:
            if pro:
                df = pro.cn_gdp(start_date=start_date.strftime('%Y%m%d'))
                if df is not None and len(df) > 0:
                    self._save_data('gdp', df)
                    results['gdp'] = {'success': True, 'count': len(df)}
                    logger.info(f"  ✓ GDP: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ GDP: {e}")
            results['gdp'] = {'success': False, 'error': str(e)}
        
        # 2. CPI同比（月度）
        logger.info("\n[2] 同步CPI数据...")
        try:
            df = ak.macro_china_cpiny()
            if df is not None and len(df) > 0:
                self._save_data('cpi', df)
                results['cpi'] = {'success': True, 'count': len(df)}
                logger.info(f"  ✓ CPI: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ CPI: {e}")
            results['cpi'] = {'success': False, 'error': str(e)}
        
        # 3. PPI同比（月度）
        logger.info("\n[3] 同步PPI数据...")
        try:
            df = ak.macro_china_ppi()
            if df is not None and len(df) > 0:
                self._save_data('ppi', df)
                results['ppi'] = {'success': True, 'count': len(df)}
                logger.info(f"  ✓ PPI: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ PPI: {e}")
            results['ppi'] = {'success': False, 'error': str(e)}
        
        # 4. 货币供应量 M0/M1/M2（月度）
        logger.info("\n[4] 同步货币供应量数据...")
        try:
            df = ak.macro_china_m2_yearly()
            if df is not None and len(df) > 0:
                self._save_data('money_supply', df)
                results['money_supply'] = {'success': True, 'count': len(df)}
                logger.info(f"  ✓ 货币供应量: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ 货币供应量: {e}")
            results['money_supply'] = {'success': False, 'error': str(e)}
        
        # 5. 社会融资规模（月度）
        logger.info("\n[5] 同步社会融资规模数据...")
        try:
            df = ak.macro_china_shrzgm()
            if df is not None and len(df) > 0:
                self._save_data('social_financing', df)
                results['social_financing'] = {'success': True, 'count': len(df)}
                logger.info(f"  ✓ 社会融资规模: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ 社会融资规模: {e}")
            results['social_financing'] = {'success': False, 'error': str(e)}
        
        # 6. 制造业PMI（月度）
        logger.info("\n[6] 同步制造业PMI数据...")
        try:
            df = ak.macro_china_pmi_yearly()
            if df is not None and len(df) > 0:
                self._save_data('pmi_manufacturing', df)
                results['pmi_manufacturing'] = {'success': True, 'count': len(df)}
                logger.info(f"  ✓ 制造业PMI: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ 制造业PMI: {e}")
            results['pmi_manufacturing'] = {'success': False, 'error': str(e)}
        
        # 7. 非制造业PMI（月度）
        logger.info("\n[7] 同步非制造业PMI数据...")
        try:
            df = ak.macro_china_nonmanufact_pmi()
            if df is not None and len(df) > 0:
                self._save_data('pmi_non_manufacturing', df)
                results['pmi_non_manufacturing'] = {'success': True, 'count': len(df)}
                logger.info(f"  ✓ 非制造业PMI: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ 非制造业PMI: {e}")
            results['pmi_non_manufacturing'] = {'success': False, 'error': str(e)}
        
        # 8. LPR利率（月度）
        logger.info("\n[8] 同步LPR利率数据...")
        try:
            if pro:
                df = pro.lpr_data(start_date=start_date.strftime('%Y%m%d'))
                if df is not None and len(df) > 0:
                    self._save_data('lpr', df)
                    results['lpr'] = {'success': True, 'count': len(df)}
                    logger.info(f"  ✓ LPR: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ LPR: {e}")
            results['lpr'] = {'success': False, 'error': str(e)}
        
        # 9. SHIBOR利率（日度）
        logger.info("\n[9] 同步SHIBOR利率数据...")
        try:
            if pro:
                df = pro.shibor(start_date=start_date.strftime('%Y%m%d'))
                if df is not None and len(df) > 0:
                    self._save_data('shibor', df)
                    results['shibor'] = {'success': True, 'count': len(df)}
                    logger.info(f"  ✓ SHIBOR: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ SHIBOR: {e}")
            results['shibor'] = {'success': False, 'error': str(e)}
        
        # 10. 工业增加值（月度）
        logger.info("\n[10] 同步工业增加值数据...")
        try:
            df = ak.macro_china_gyzjz()
            if df is not None and len(df) > 0:
                self._save_data('industrial_value', df)
                results['industrial_value'] = {'success': True, 'count': len(df)}
                logger.info(f"  ✓ 工业增加值: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ 工业增加值: {e}")
            results['industrial_value'] = {'success': False, 'error': str(e)}
        
        # 11. 固定资产投资（月度）
        logger.info("\n[11] 同步固定资产投资数据...")
        try:
            df = ak.macro_china_gdtzc()
            if df is not None and len(df) > 0:
                self._save_data('fixed_investment', df)
                results['fixed_investment'] = {'success': True, 'count': len(df)}
                logger.info(f"  ✓ 固定资产投资: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ 固定资产投资: {e}")
            results['fixed_investment'] = {'success': False, 'error': str(e)}
        
        # 12. 社会消费品零售总额（月度）
        logger.info("\n[12] 同步社会消费品零售总额数据...")
        try:
            df = ak.macro_china_xfzgx()
            if df is not None and len(df) > 0:
                self._save_data('retail_sales', df)
                results['retail_sales'] = {'success': True, 'count': len(df)}
                logger.info(f"  ✓ 社会消费品零售总额: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ 社会消费品零售总额: {e}")
            results['retail_sales'] = {'success': False, 'error': str(e)}
        
        # 13. 进出口数据（月度）
        logger.info("\n[13] 同步进出口数据...")
        try:
            df = ak.macro_china_jck()
            if df is not None and len(df) > 0:
                self._save_data('trade_data', df)
                results['trade_data'] = {'success': True, 'count': len(df)}
                logger.info(f"  ✓ 进出口数据: {len(df)}条")
        except Exception as e:
            logger.warning(f"  ✗ 进出口数据: {e}")
            results['trade_data'] = {'success': False, 'error': str(e)}
        
        # 汇总
        success_count = sum(1 for r in results.values() if r.get('success'))
        total_count = len(results)
        
        logger.info("\n" + "=" * 60)
        logger.info(f"宏观经济数据同步完成: {success_count}/{total_count}")
        logger.info("=" * 60)
        
        return {
            'success': True,
            'total': total_count,
            'success_count': success_count,
            'results': results
        }
    
    def _save_data(self, name: str, df: pd.DataFrame):
        """保存数据到缓存"""
        cache_file = os.path.join(self.CACHE_DIR, f"{name}.csv")
        df.to_csv(cache_file, index=False, encoding='utf-8-sig')
        logger.debug(f"已保存: {cache_file}")
    
    def get_macro_data(self, name: str) -> Optional[pd.DataFrame]:
        """获取缓存的宏观经济数据"""
        cache_file = os.path.join(self.CACHE_DIR, f"{name}.csv")
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file)
        return None
    
    def list_macro_data(self) -> Dict[str, dict]:
        """列出所有可用的宏观经济数据"""
        result = {}
        for name, config in self.MACRO_INDICATORS.items():
            cache_file = os.path.join(self.CACHE_DIR, f"{name}.csv")
            if os.path.exists(cache_file):
                df = pd.read_csv(cache_file)
                result[name] = {
                    'name': config['name'],
                    'freq': config['freq'],
                    'count': len(df),
                    'available': True
                }
            else:
                result[name] = {
                    'name': config['name'],
                    'freq': config['freq'],
                    'count': 0,
                    'available': False
                }
        return result


# 命令行入口
if __name__ == '__main__':
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    service = MacroDataService()
    
    if len(sys.argv) > 1 and sys.argv[1] == 'list':
        # 列出已有数据
        data = service.list_macro_data()
        print("\n宏观经济数据列表:")
        print("-" * 50)
        for name, info in data.items():
            status = "✓" if info['available'] else "✗"
            print(f"  {status} {info['name']} ({name}): {info['count']}条")
    else:
        # 同步所有数据
        service.sync_all_macro_data(years=10)