#!/usr/bin/env python3
"""
筛选符合条件的股票：
1. 剔除ST股票（名称包含ST）
2. 剔除市值低于50亿的股票
"""
import akshare as ak
import pandas as pd

print("=" * 60)
print("股票筛选")
print("=" * 60)

# 1. 获取A股实时行情（包含市值）
print("\n获取A股行情数据...")
df = ak.stock_zh_a_spot_em()
print(f"总股票数: {len(df)}")

# 2. 剔除ST股票
print("\n剔除ST股票...")
df['is_st'] = df['名称'].str.contains('ST', case=False, na=False)
non_st = df[~df['is_st']].copy()
print(f"非ST股票: {len(non_st)}")

# 3. 剔除市值<50亿
print("\n筛选市值>=50亿...")
# 总市值单位是元，转换为亿元
non_st['总市值_亿'] = non_st['总市值'] / 100000000
filtered = non_st[non_st['总市值_亿'] >= 50].copy()
print(f"市值>=50亿: {len(filtered)}")

# 4. 保存结果
result_codes = filtered['代码'].tolist()
print(f"\n符合条件的股票: {len(result_codes)}")

with open('filtered_stocks.txt', 'w') as f:
    for code in result_codes:
        f.write(code + '\n')

print(f"已保存到 filtered_stocks.txt")

# 5. 显示市值分布
print("\n市值分布:")
print(f"  50-100亿: {len(filtered[(filtered['总市值_亿'] >= 50) & (filtered['总市值_亿'] < 100)])}")
print(f"  100-200亿: {len(filtered[(filtered['总市值_亿'] >= 100) & (filtered['总市值_亿'] < 200)])}")
print(f"  200-500亿: {len(filtered[(filtered['总市值_亿'] >= 200) & (filtered['总市值_亿'] < 500)])}")
print(f"  500-1000亿: {len(filtered[(filtered['总市值_亿'] >= 500) & (filtered['总市值_亿'] < 1000)])}")
print(f"  1000亿以上: {len(filtered[filtered['总市值_亿'] >= 1000])}")

# 6. 显示Top 20市值
print("\n市值Top 20:")
top20 = filtered.nlargest(20, '总市值_亿')[['代码', '名称', '总市值_亿']]
for i, row in top20.iterrows():
    print(f"  {row['代码']} {row['名称']}: {row['总市值_亿']:.0f}亿")

print("\n" + "=" * 60)
print(f"最终筛选结果: {len(result_codes)} 只股票")
print("=" * 60)