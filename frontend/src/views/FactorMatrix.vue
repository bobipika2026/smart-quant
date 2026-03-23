<template>
  <div class="factor-matrix">
    <!-- 最佳因子组合卡片 -->
    <n-card title="🏆 最佳因子组合" class="mb-4">
      <template #header-extra>
        <n-space>
          <n-input v-model:value="searchStock" placeholder="搜索股票" style="width: 150px" clearable />
          <n-button @click="fetchData" :loading="loading">刷新</n-button>
        </n-space>
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
import { ref, computed, onMounted, nextTick, h } from 'vue'
import { useRouter } from 'vue-router'
import { NCard, NDataTable, NStatistic, NGrid, NGi, NInput, NTag, NButton, NSpace, useMessage } from 'naive-ui'
import * as echarts from 'echarts'

const router = useRouter()
const message = useMessage()

// 数据
const loading = ref(false)
const searchStock = ref('')
const bestCombinations = ref([])
const statistics = ref({})

// 图表
const chartRef = ref(null)
let chart = null

// 策略映射：因子代码 -> { strategyId, params }
const factorConfigMap = {
  // MA金叉
  'ma_5_20': { strategyId: 'ma_cross', params: { short_period: 5, long_period: 20 } },
  'ma_5_30': { strategyId: 'ma_cross', params: { short_period: 5, long_period: 30 } },
  'ma_10_20': { strategyId: 'ma_cross', params: { short_period: 10, long_period: 20 } },
  'ma_10_30': { strategyId: 'ma_cross', params: { short_period: 10, long_period: 30 } },
  // MACD
  'macd_default': { strategyId: 'macd', params: { fast: 12, slow: 26, signal: 9 } },
  'macd_fast': { strategyId: 'macd', params: { fast: 8, slow: 17, signal: 9 } },
  // RSI
  'rsi_14_70': { strategyId: 'rsi', params: { period: 14, overbought: 70, oversold: 30 } },
  'rsi_14_80': { strategyId: 'rsi', params: { period: 14, overbought: 80, oversold: 20 } },
  // KDJ
  'kdj_default': { strategyId: 'kdj', params: { n: 9, m1: 3, m2: 3 } },
  // 布林带
  'boll_20_2': { strategyId: 'boll', params: { period: 20, std: 2 } },
  // 其他
  'cci_14': { strategyId: 'cci', params: { period: 14 } },
  'wr_14': { strategyId: 'wr', params: { period: 14 } },
  'dmi_14': { strategyId: 'dmi', params: { period: 14 } },
}

// 时间因子映射（使用交易日，与后端一致）
const timeFactorMap = {
  'period_3m': 63,
  'period_6m': 126,
  'period_1y': 252,
  'period_2y': 504,
}

// 持仓周期到数据频率的映射
const holdingPeriodToFreq = {
  'day_5y': 'day',
  'hour_2y': '60min',
  'minute_1m': '1min',
  'period_3m': 'day',
  'period_6m': 'day',
  'period_1y': 'day',
  'period_2y': 'day',
}

// 解析因子组合，获取策略配置和时间范围
function parseFactorCombination(row) {
  const combo = row.factor_combination || {}
  const strategyDesc = row.strategy_desc || ''
  
  // 解析组合模式
  const modeMatch = strategyDesc.match(/\((OR|AND)\)/)
  const mode = modeMatch ? modeMatch[1] : 'OR'
  
  // 获取激活的因子
  const activeFactors = Object.keys(combo).filter(k => combo[k] === 1 && !k.startsWith('period_'))
  
  // 获取时间因子
  const timeFactor = Object.keys(combo).find(k => combo[k] === 1 && k.startsWith('period_'))
  const days = timeFactorMap[timeFactor] || 180
  
  // 构建策略配置
  const strategies = activeFactors.map(factorCode => {
    const config = factorConfigMap[factorCode]
    if (config) {
      return {
        strategyId: config.strategyId,
        params: config.params
      }
    }
    return null
  }).filter(Boolean)
  
  console.log('[因子矩阵] 解析因子组合:', { strategies, mode, days })
  return { strategies, mode, days }
}

// 去回测
function goToBacktest(row) {
  const { strategies, mode, days } = parseFactorCombination(row)
  
  if (strategies.length === 0) {
    message.error('无法解析策略配置')
    return
  }
  
  // 根据持仓周期确定数据频率
  const dataFreq = holdingPeriodToFreq[row.holding_period] || 'day'
  
  const query = {
    stockCode: row.stock_code,
    strategyIds: strategies.map(s => s.strategyId).join(','),
    comboMode: mode,
    days: days,
    dataFreq: dataFreq,
    useDataStart: true,  // 新增：使用数据开始日期
    paramsJson: JSON.stringify(strategies.map(s => s.params))
  }
  
  console.log('[因子矩阵] 跳转回测，参数:', query)
  router.push({ path: '/backtest', query })
}

// 去监控
async function goToMonitor(row) {
  const strategyIds = parseStrategyIds(row.strategy_desc)
  try {
    const res = await fetch('/api/monitor/add-config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_code: row.stock_code,
        stock_name: row.stock_name,
        strategy_id: strategyIds[0] || 'ma_cross',
        strategy_name: row.strategy_desc
      })
    })
    const data = await res.json()
    if (data.success) {
      message.success(data.message || '已添加到监控')
    } else {
      message.error(data.message || '添加失败')
    }
  } catch (e) {
    message.error('添加监控失败')
  }
}

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
    width: 120,
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
      // 中国股市习惯：红涨绿跌
      const color = val >= 0 ? '#d03050' : '#18a058'
      return h('span', { style: { color, fontWeight: 'bold' } }, `${val.toFixed(2)}%`)
    }
  },
  {
    title: '年化',
    key: 'annual_return',
    width: 80,
    sorter: (a, b) => (a.annual_return || 0) - (b.annual_return || 0),
    render(row) {
      const val = row.annual_return || 0
      const color = val >= 0 ? '#d03050' : '#18a058'
      return h('span', { style: { color } }, `${val.toFixed(2)}%`)
    }
  },
  {
    title: '基准',
    key: 'benchmark_return',
    width: 80,
    render(row) {
      const val = row.benchmark_return || 0
      const color = val >= 0 ? '#d03050' : '#18a058'
      return h('span', { style: { color } }, `${val.toFixed(2)}%`)
    }
  },
  {
    title: '夏普',
    key: 'sharpe_ratio',
    width: 70,
    sorter: (a, b) => (a.sharpe_ratio || 0) - (b.sharpe_ratio || 0),
    render(row) {
      return (row.sharpe_ratio || 0).toFixed(2)
    }
  },
  {
    title: '盈亏比',
    key: 'profit_loss_ratio',
    width: 70,
    sorter: (a, b) => (a.profit_loss_ratio || 0) - (b.profit_loss_ratio || 0),
    render(row) {
      const val = row.profit_loss_ratio || 0
      const color = val >= 1.5 ? '#d03050' : val >= 1 ? '#18a058' : '#999'
      return h('span', { style: { color } }, val.toFixed(2))
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
  },
  {
    title: '操作',
    key: 'actions',
    width: 160,
    render(row) {
      return h(NSpace, { size: 'small' }, () => [
        h(NButton, { 
          size: 'small', 
          type: 'primary',
          onClick: () => goToBacktest(row)
        }, () => '去回测'),
        h(NButton, { 
          size: 'small', 
          type: 'success',
          onClick: () => goToMonitor(row)
        }, () => '去监控')
      ])
    }
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

// 分页配置 - 每页100条
const pagination = ref({
  pageSize: 100,
  showSizePicker: true,
  pageSizes: [50, 100, 200, 500],
  showQuickJumper: true
})

// 行样式
const rowClass = (row) => {
  return row.rank_in_stock === 1 ? 'best-row' : ''
}

// 获取数据
async function fetchData() {
  loading.value = true
  try {
    const res = await fetch('/api/factor-matrix-v2/best-combinations?limit=50000')
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
    
    message.success(`加载 ${bestCombinations.value.length} 条数据`)
  } catch (e) {
    console.error('获取数据失败:', e)
    message.error('获取数据失败')
  } finally {
    loading.value = false
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