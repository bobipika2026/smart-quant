<template>
  <div class="factor-matrix">
    <n-card title="因子矩阵" class="mb-4">
      <template #header-extra>
        <n-space>
          <n-select
            v-model:value="selectedStrategy"
            :options="strategyOptions"
            placeholder="策略筛选"
            clearable
            style="width: 150px"
          />
          <n-select
            v-model:value="selectedIndustry"
            :options="industryOptions"
            placeholder="行业筛选"
            clearable
            style="width: 150px"
          />
        </n-space>
      </template>

      <!-- 统计卡片 -->
      <n-grid :cols="4" :x-gap="16" class="mb-4">
        <n-gi>
          <n-statistic label="总回测次数" :value="statistics.total_records" />
        </n-gi>
        <n-gi>
          <n-statistic label="平均收益" :value="statistics.avg_return || 0">
            <template #suffix>%</template>
          </n-statistic>
        </n-gi>
        <n-gi>
          <n-statistic label="平均夏普" :value="statistics.avg_sharpe || 0" />
        </n-gi>
        <n-gi>
          <n-statistic label="策略数量" :value="statistics.strategy_distribution?.length || 0" />
        </n-gi>
      </n-grid>
    </n-card>

    <!-- 因子矩阵表格 -->
    <n-card title="回测记录" class="mb-4">
      <n-data-table
        :columns="columns"
        :data="matrixData"
        :pagination="pagination"
        :loading="loading"
        striped
      />
    </n-card>

    <!-- 因子分析 -->
    <n-grid :cols="2" :x-gap="16">
      <!-- 策略因子分析 -->
      <n-gi>
        <n-card title="策略收益对比">
          <div ref="strategyChartRef" style="height: 300px"></div>
        </n-card>
      </n-gi>

      <!-- 行业因子分析 -->
      <n-gi>
        <n-card title="行业收益对比">
          <div ref="industryChartRef" style="height: 300px"></div>
        </n-card>
      </n-gi>
    </n-grid>
  </div>
</template>

<script setup>
import { ref, onMounted, watch, nextTick } from 'vue'
import { NCard, NDataTable, NStatistic, NGrid, NGi, NSpace, NSelect, NTag, useMessage } from 'naive-ui'
import * as echarts from 'echarts'

const message = useMessage()

// 数据
const loading = ref(false)
const statistics = ref({})
const matrixData = ref([])
const selectedStrategy = ref(null)
const selectedIndustry = ref(null)
const strategyAnalysis = ref({})
const industryAnalysis = ref({})

// 图表引用
const strategyChartRef = ref(null)
const industryChartRef = ref(null)
let strategyChart = null
let industryChart = null

// 分页
const pagination = ref({
  pageSize: 20
})

// 筛选选项
const strategyOptions = ref([])
const industryOptions = ref([])

// 表格列
const columns = [
  {
    title: '策略',
    key: 'strategy_name',
    width: 100,
    fixed: 'left'
  },
  {
    title: '股票',
    key: 'stock_code',
    width: 80
  },
  {
    title: '行业',
    key: 'industry',
    width: 80,
    render(row) {
      return row.industry || '-'
    }
  },
  {
    title: '市值',
    key: 'market_cap_level',
    width: 60,
    render(row) {
      const levelMap = { '大盘': 'success', '中盘': 'warning', '小盘': 'error' }
      return row.market_cap_level ? 
        h(NTag, { type: levelMap[row.market_cap_level] || 'default', size: 'small' }, () => row.market_cap_level) : '-'
    }
  },
  {
    title: '参数',
    key: 'params',
    width: 100,
    render(row) {
      if (row.param_short && row.param_long) {
        return `${row.param_short}/${row.param_long}`
      }
      return '-'
    }
  },
  {
    title: '收益',
    key: 'total_return',
    width: 80,
    render(row) {
      const val = row.total_return
      const color = val >= 0 ? '#18a058' : '#d03050'
      return h('span', { style: { color, fontWeight: 'bold' } }, `${val?.toFixed(2)}%`)
    }
  },
  {
    title: '夏普',
    key: 'sharpe_ratio',
    width: 70,
    render(row) {
      return row.sharpe_ratio?.toFixed(2) || '-'
    }
  },
  {
    title: '最大回撤',
    key: 'max_drawdown',
    width: 90,
    render(row) {
      return `${row.max_drawdown?.toFixed(2)}%`
    }
  },
  {
    title: '胜率',
    key: 'win_rate',
    width: 70,
    render(row) {
      return `${row.win_rate?.toFixed(1)}%`
    }
  },
  {
    title: '交易次数',
    key: 'trade_count',
    width: 80
  },
  {
    title: '时间',
    key: 'created_at',
    width: 150
  }
]

// 获取统计信息
async function fetchStatistics() {
  try {
    const res = await fetch('/api/factor-matrix/statistics')
    statistics.value = await res.json()
    
    // 更新筛选选项
    strategyOptions.value = (statistics.value.strategy_distribution || []).map(s => ({
      label: s.name,
      value: s.name
    }))
    industryOptions.value = (statistics.value.industry_distribution || []).map(s => ({
      label: s.name,
      value: s.name
    }))
  } catch (e) {
    console.error('获取统计失败:', e)
  }
}

// 获取因子矩阵
async function fetchMatrix() {
  loading.value = true
  try {
    const params = new URLSearchParams({ limit: 100 })
    if (selectedStrategy.value) {
      params.append('strategy_id', selectedStrategy.value)
    }
    if (selectedIndustry.value) {
      params.append('industry', selectedIndustry.value)
    }
    
    const res = await fetch(`/api/factor-matrix/matrix?${params}`)
    const data = await res.json()
    matrixData.value = data.data || []
  } catch (e) {
    console.error('获取矩阵失败:', e)
  } finally {
    loading.value = false
  }
}

// 获取因子分析
async function fetchAnalysis() {
  try {
    const [strategyRes, industryRes] = await Promise.all([
      fetch('/api/factor-matrix/analyze/strategy_name'),
      fetch('/api/factor-matrix/analyze/industry')
    ])
    strategyAnalysis.value = await strategyRes.json()
    industryAnalysis.value = await industryRes.json()
    
    await nextTick()
    renderCharts()
  } catch (e) {
    console.error('获取分析失败:', e)
  }
}

// 渲染图表
function renderCharts() {
  // 策略收益图表
  if (strategyChartRef.value) {
    if (!strategyChart) {
      strategyChart = echarts.init(strategyChartRef.value)
    }
    
    const data = strategyAnalysis.value.top_performers || []
    strategyChart.setOption({
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' }
      },
      grid: {
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true
      },
      xAxis: {
        type: 'category',
        data: data.map(d => d.factor_value),
        axisLabel: { rotate: 30 }
      },
      yAxis: {
        type: 'value',
        name: '收益(%)'
      },
      series: [{
        name: '平均收益',
        type: 'bar',
        data: data.map(d => ({
          value: d.avg_return,
          itemStyle: { color: d.avg_return >= 0 ? '#18a058' : '#d03050' }
        })),
        label: {
          show: true,
          position: 'top',
          formatter: '{c}%'
        }
      }]
    })
  }
  
  // 行业收益图表
  if (industryChartRef.value) {
    if (!industryChart) {
      industryChart = echarts.init(industryChartRef.value)
    }
    
    const data = industryAnalysis.value.top_performers || []
    industryChart.setOption({
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' }
      },
      grid: {
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true
      },
      xAxis: {
        type: 'category',
        data: data.map(d => d.factor_value),
        axisLabel: { rotate: 30 }
      },
      yAxis: {
        type: 'value',
        name: '收益(%)'
      },
      series: [{
        name: '平均收益',
        type: 'bar',
        data: data.map(d => ({
          value: d.avg_return,
          itemStyle: { color: d.avg_return >= 0 ? '#18a058' : '#d03050' }
        })),
        label: {
          show: true,
          position: 'top',
          formatter: '{c}%'
        }
      }]
    })
  }
}

// 监听筛选变化
watch([selectedStrategy, selectedIndustry], () => {
  fetchMatrix()
})

// 初始化
onMounted(async () => {
  await fetchStatistics()
  await fetchMatrix()
  await fetchAnalysis()
})

// 窗口大小变化时重新渲染图表
window.addEventListener('resize', () => {
  strategyChart?.resize()
  industryChart?.resize()
})

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
</style>