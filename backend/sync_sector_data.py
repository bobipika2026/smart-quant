#!/usr/bin/env python
"""
板块行情数据同步

同步内容:
1. 概念板块列表 (879个)
2. 概念板块成分股
3. 行业板块数据
"""
import os
import sys
import time
import pandas as pd
import tushare as ts

sys.path.insert(0, '.')
from app.config import settings

CACHE_DIR = "data_cache/sector"
os.makedirs(CACHE_DIR, exist_ok=True)

if settings.TUSHARE_TOKEN:
    ts.set_token(settings.TUSHARE_TOKEN)
    pro = ts.pro_api()

print("=" * 60)
print("板块行情数据同步")
print("=" * 60)

# 1. 概念板块列表
print("\n[1] 概念板块列表...")
try:
    df = pro.concept(src='ts')
    if df is not None and len(df) > 0:
        df.to_csv(f"{CACHE_DIR}/concept_list.csv", index=False)
        print(f"  ✓ 概念板块: {len(df)}个")
        concept_list = df
except Exception as e:
    print(f"  ✗ 失败: {e}")
    concept_list = None

# 2. 所有概念板块成分
if concept_list is not None:
    print("\n[2] 概念板块成分股...")
    all_constituents = []
    success = 0
    
    for idx, row in concept_list.iterrows():
        try:
            cons = pro.concept_detail(id=row['code'], src='ts')
            if cons is not None and len(cons) > 0:
                cons['concept_code'] = row['code']
                cons['concept_name'] = row['name']
                all_constituents.append(cons)
                success += 1
                
            # 每10个汇报一次
            if (idx + 1) % 50 == 0:
                print(f"    进度: {idx+1}/{len(concept_list)}, 成功: {success}")
                
            # 限频
            time.sleep(0.5)
            
        except Exception as e:
            pass
    
    if all_constituents:
        df_all = pd.concat(all_constituents, ignore_index=True)
        df_all.to_csv(f"{CACHE_DIR}/concept_constituents_all.csv", index=False)
        print(f"  ✓ 概念成分: {len(df_all)}条, 涵盖 {success} 个概念")

# 3. 申万行业分类
print("\n[3] 申万行业分类...")
try:
    df = pro.index_classify(level='L1', src='SW')
    if df is not None and len(df) > 0:
        df.to_csv(f"{CACHE_DIR}/sw_industry_l1.csv", index=False)
        print(f"  ✓ 申万一级行业: {len(df)}个")
except Exception as e:
    print(f"  ✗ 失败: {e}")

# 4. 行业股票映射
print("\n[4] 行业股票映射...")
try:
    # 使用ths源获取行业分类
    df = pro.ths_index()
    if df is not None and len(df) > 0:
        df.to_csv(f"{CACHE_DIR}/ths_index.csv", index=False)
        print(f"  ✓ 同花顺指数: {len(df)}个")
except Exception as e:
    print(f"  ✗ 失败: {e}")

# 5. 汇总统计
print("\n" + "=" * 60)
print("板块数据汇总:")
print("=" * 60)

for f in os.listdir(CACHE_DIR):
    if f.endswith('.csv'):
        df = pd.read_csv(os.path.join(CACHE_DIR, f))
        print(f"  {f}: {len(df)}条")

print("=" * 60)