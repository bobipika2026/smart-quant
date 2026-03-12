<template>
  <div class="backtest">
    <n-card title="回测配置">
      <n-form :model="form" label-placement="left" label-width="100">
        <n-grid :cols="2" :x-gap="24">
          <n-gi>
            <n-form-item label="股票代码">
              <n-input v-model:value="form.stockCode" placeholder="输入股票代码，如：000001" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="选择策略">
              <n-select v-model:value="form.strategyId" :options="strategyOptions" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="开始日期">
              <n-date-picker v-model:value="form.startDate" type="date" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="结束日期">
              <n-date-picker v-model:value="form.endDate" type="date" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="初始资金">
              <n-input-number v-model:value="form.initialCapital" :min="10000" :step="10000">
                <template #prefix>¥</template>
              </n-input-number>
            </n-form-item>
          </n-gi>
        </n-grid>
        <n-space style="margin-top: 16px">
          <n-button type="primary" :loading="running" @click="runBacktest">
            🚀 开始回测
          </n-button>
        </n-space>
      </n-form>
    </n-card>

    <n-card v-if="result" title="回测结果" style="margin-top: 16px">
      <template #header-extra>
        <n-button type="success" @click="addToMonitor" :loading="addingMonitor">
          📡 添加到监控
        </n-button>
      </template>
      
      <n-descriptions :column="4" bordered>
        <n-descriptions-item label="初始资金">
          ¥{{ result.initial_capital?.toLocaleString() }}
        </n-descriptions-item>
        <n-descriptions-item label="最终市值">
          ¥{{ result.final_value?.toLocaleString() }}
        </n-descriptions-item>
        <n-descriptions-item label="策略收益">
          <n-tag :type="result.total_return >= 0 ? 'success' : 'error'">
            {{ result.total_return }}%
          </n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="基准收益">
          <n-tag :type="result.benchmark_return >= 0 ? 'success' : 'error'">
            {{ result.benchmark_return }}%
          </n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="年化收益">
          <n-tag :type="result.annual_return >= 0 ? 'success' : 'error'">
            {{ result.annual_return }}%
          </n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="最大回撤">
          <n-tag type="warning">{{ result.max_drawdown }}%</n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="夏普比率">
          {{ result.sharpe_ratio }}
        </n-descriptions-item>
        <n-descriptions-item label="胜率">
          {{ result.win_rate }}%
        </n-descriptions-item>
      </n-descriptions>
    </n-card>

    <n-card v-if="result?.equity_curve" title="收益曲线" style="margin-top: 16px">
      <v-chart :option="chartOption" style="height: 400px" autoresize />
    </n-card>

    <n-card v-if="result?.trades?.length" title="交易记录" style="margin-top: 16px">
      <n-data-table
        :columns="tradeColumns"
        :data="result.trades"
        :pagination="{ pageSize: 10 }"
        size="small"
      />
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import axios from 'axios'

use([CanvasRenderer, LineChart, GridComponent, TooltipComponent, LegendComponent])

const running = ref(false)
const addingMonitor = ref(false)
const result = ref<any>(null)
const strategies = ref<any[]>([])

const form = ref({
  stockCode: '000001',
  strategyId: 'ma_cross',
  startDate: Date.now() - 365 * 24 * 60 * 60 * 1000,
  endDate: Date.now(),
  initialCapital: 100000
})

const strategyOptions = computed(() =>
  strategies.value.map(s => ({ label: s.name, value: s.id }))
)

const chartOption = computed(() => {
  if (!result.value?.equity_curve) return {}
  
  const data = result.value.equity_curve
  const dates = data.map((d: any) => d.date)
  const equity = data.map((d: any) => d.equity)
  
  const initialEquity = equity[0]
  const benchmark = data.map((d: any, i: number) => {
    if (i === 0) return initialEquity
    const priceChange = d.price / data[0].price
    return Math.round(initialEquity * priceChange * 100) / 100
  })
  
  return {
    tooltip: { trigger: 'axis' },
    legend: { data: ['策略市值', '基准市值'] },
    grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
    xAxis: { type: 'category', data: dates },
    yAxis: { type: 'value', name: '市值(元)' },
    series: [
      {
        name: '策略市值',
        type: 'line',
        data: equity,
        smooth: true,
        lineStyle: { width: 2 },
        areaStyle: { opacity: 0.1 }
      },
      {
        name: '基准市值',
        type: 'line',
        data: benchmark,
        smooth: true,
        lineStyle: { width: 2, type: 'dashed' },
        itemStyle: { opacity: 0.5 }
      }
    ]
  }
})

const tradeColumns = [
  { title: '日期', key: 'date' },
  { 
    title: '类型', 
    key: 'type',
    render: (row: any) => row.type === 'buy' ? '买入' : '卖出'
  },
  { title: '价格', key: 'price' },
  { title: '数量', key: 'shares' },
  { title: '金额', key: 'value' }
]

const runBacktest = async () => {
  running.value = true
  try {
    const res = await axios.post('/api/strategy/backtest', {
      stock_code: form.value.stockCode,
      strategy_id: form.value.strategyId,
      start_date: new Date(form.value.startDate).toISOString().slice(0, 10).replace(/-/g, ''),
      end_date: new Date(form.value.endDate).toISOString().slice(0, 10).replace(/-/g, ''),
      initial_capital: form.value.initialCapital
    })
    result.value = res.data
    result.value.stockCode = form.value.stockCode
    result.value.strategyId = form.value.strategyId
  } catch (e: any) {
    console.error('回测失败', e)
    alert(e.response?.data?.detail || '回测失败')
  } finally {
    running.value = false
  }
}

const addToMonitor = async () => {
  if (!result.value) return
  
  // 获取股票名称
  let stockName = form.value.stockCode
  try {
    const res = await axios.get(`/api/stock/info/${form.value.stockCode}`)
    stockName = res.data.info?.名称 || form.value.stockCode
  } catch (e) {
    // ignore
  }
  
  // 获取策略名称
  const strategy = strategies.value.find(s => s.id === form.value.strategyId)
  const strategyName = strategy?.name || form.value.strategyId
  
  addingMonitor.value = true
  try {
    const res = await axios.post('/api/monitor/add-config', {
      stock_code: form.value.stockCode,
      stock_name: stockName,
      strategy_id: form.value.strategyId,
      strategy_name: strategyName
    })
    alert(res.data.message)
  } catch (e: any) {
    alert(e.response?.data?.detail || '添加失败')
  } finally {
    addingMonitor.value = false
  }
}

onMounted(async () => {
  try {
    const res = await axios.get('/api/strategy/list')
    strategies.value = res.data.strategies || []
  } catch (e) {
    console.error('获取策略列表失败')
  }
})
</script>