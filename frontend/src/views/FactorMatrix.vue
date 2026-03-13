<template>
  <div class="factor-matrix">
    <!-- 最佳因子组合卡片 -->
    <n-card title="🏆 最佳因子组合" class="mb-4">
      <template #header-extra>
        <n-input v-model:value="searchStock" placeholder="搜索股票" style="width: 150px" clearable />
      </template>

      <!-- 统计 -->
      <n-grid :cols="4" :x-gap="16" class="mb-4">
        <n-gi>
          <n-statistic label="股票数量" :value="statistics.stock_count || 0" />
        </n-gi>
        <n-gi>
          <n-statistic label="总组合数" :value="statistics.total_combinations || 0" />
        </n-gi>
        <n-gi>
          <n-statistic label="平均收益" :value="statistics.avg_return?.toFixed(1) || 0">
            <template #suffix>%</template>
          </n-statistic>
        </n-gi>
        <n-gi>
          <n-statistic label="最高夏普" :value="statistics.max_sharpe?.toFixed(2) || 0" />
        </n-gi>
      </n-grid>

      <!-- 最佳组合表格 -->
      <n-data-table
        :columns="columns"
        :data="filteredData"
        :pagination="pagination"
        :row-class-name="rowClass"
        striped
      />
    </n-card>

    <!-- 收益分布图 -->
    <n-card title="收益分布" class="mb-4">
      <div ref="chartRef" style="height: 300px"></div>
    </n-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, nextTick } from 'vue'
import { NCard, NDataTable, NStatistic, NGrid, NGi, NInput, NTag } from 'naive-ui'
import * as echarts from 'echarts'

// 数据
const searchStock = ref('')
const bestCombinations = ref([])
const statistics = ref({})

// 图表
const chartRef = ref(null)
let chart = null

// 表格列
const columns = [
  {
    title: '排名',
    key: 'rank_in_stock',
    width: 60,
    render(row) {
      const colors = { 1: 'success', 2: 'warning', 3: 'default' }
      return h(NTag, { type: colors[row.rank_in_stock] || 'default', size: 'small' }, () => `#${row.rank_in_stock}`)
    }
  },
  {
    title: '股票',
    key: 'stock_code',
    width: 100,
    render(row) {
      return `${row.stock_code} ${row.stock_name || ''}`
    }
  },
  {
    title: '策略组合',
    key: 'strategy_desc',
    ellipsis: { tooltip: true }
  },
  {
    title: '收益',
    key: 'total_return',
    width: 100,
    sorter: (a, b) => (a.total_return || 0) - (b.total_return || 0),
    render(row) {
      const val = row.total_return || 0
      const color = val >= 0 ? '#18a058' : '#d03050'
      return h('span', { style: { color, fontWeight: 'bold' } }, `${val.toFixed(2)}%`)
    }
  },
  {
    title: '夏普',
    key: 'sharpe_ratio',
    width: 80,
    sorter: (a, b) => (a.sharpe_ratio || 0) - (b.sharpe_ratio || 0),
    render(row) {
      return (row.sharpe_ratio || 0).toFixed(2)
    }
  },
  {
    title: '综合得分',
    key: 'composite_score',
    width: 100,
    defaultSortOrder: 'descend',
    sorter: (a, b) => (a.composite_score || 0) - (b.composite_score || 0),
    render(row) {
      return (row.composite_score || 0).toFixed(1)
    }
  },
  {
    title: '持仓周期',
    key: 'holding_period',
    width: 100
  }
]

// 筛选后的数据
const filteredData = computed(() => {
  if (!searchStock.value) return bestCombinations.value
  const keyword = searchStock.value.toLowerCase()
  return bestCombinations.value.filter(r => 
    r.stock_code?.toLowerCase().includes(keyword) ||
    r.stock_name?.toLowerCase().includes(keyword)
  )
})

// 分页
const pagination = ref({
  pageSize: 20,
  showSizePicker: true,
  pageSizes: [10, 20, 50, 100]
})

// 行样式
const rowClass = (row) => {
  return row.rank_in_stock === 1 ? 'best-row' : ''
}

// 获取数据
async function fetchData() {
  try {
    const res = await fetch('/api/factor-matrix-v2/best-combinations?limit=500')
    const data = await res.json()
    bestCombinations.value = data.data || []
    
    // 计算统计
    if (bestCombinations.value.length > 0) {
      const stocks = new Set(bestCombinations.value.map(r => r.stock_code))
      statistics.value = {
        stock_count: stocks.size,
        total_combinations: bestCombinations.value.length,
        avg_return: bestCombinations.value.reduce((s, r) => s + (r.total_return || 0), 0) / bestCombinations.value.length,
        max_sharpe: Math.max(...bestCombinations.value.map(r => r.sharpe_ratio || 0))
      }
    }
    
    await nextTick()
    renderChart()
    
  } catch (e) {
    console.error('获取数据失败:', e)
  }
}

// 渲染图表
function renderChart() {
  if (!chartRef.value) return
  
  if (!chart) {
    chart = echarts.init(chartRef.value)
  }
  
  // 按股票分组，取每个股票的最佳收益
  const stockReturns = {}
  bestCombinations.value.forEach(r => {
    if (r.rank_in_stock === 1) {
      stockReturns[r.stock_code] = r.total_return || 0
    }
  })
  
  const sorted = Object.entries(stockReturns).sort((a, b) => b[1] - a[1]).slice(0, 20)
  
  chart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
    xAxis: {
      type: 'category',
      data: sorted.map(s => s[0]),
      axisLabel: { rotate: 45 }
    },
    yAxis: {
      type: 'value',
      name: '收益率(%)'
    },
    series: [{
      name: '最佳收益',
      type: 'bar',
      data: sorted.map(s => ({
        value: s[1].toFixed(2),
        itemStyle: { color: s[1] >= 0 ? '#18a058' : '#d03050' }
      }))
    }]
  })
}

// 初始化
onMounted(fetchData)

// h 函数
import { h } from 'vue'
</script>

<style scoped>
.factor-matrix {
  padding: 16px;
}

.mb-4 {
  margin-bottom: 16px;
}

:deep(.best-row) {
  background-color: rgba(24, 160, 88, 0.1);
}

:deep(.best-row:hover td) {
  background-color: rgba(24, 160, 88, 0.15) !important;
}
</style>