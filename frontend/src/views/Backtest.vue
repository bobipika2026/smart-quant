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
          <n-gi :span="2">
            <n-form-item label="时间范围">
              <n-space>
                <n-button size="small" @click="setTimeRange(1)">1年</n-button>
                <n-button size="small" @click="setTimeRange(3)">3年</n-button>
                <n-button size="small" type="primary" @click="setTimeRange(5)">5年</n-button>
                <n-button size="small" @click="setTimeRange(10)">10年</n-button>
                <n-text depth="3" style="margin-left: 8px">
                  ({{ yearsText }}年)
                </n-text>
              </n-space>
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

    <!-- 策略评估建议 -->
    <n-card v-if="result" title="📊 策略评估" style="margin-top: 16px">
      <n-space vertical size="large">
        <!-- 综合评分 -->
        <n-card size="small" :bordered="false" style="background: #f5f7f9">
          <n-space align="center">
            <span style="font-size: 16px; font-weight: bold">综合评分：</span>
            <n-rate :value="overallRating" readonly :count="5" />
            <n-tag :type="overallRating >= 4 ? 'success' : overallRating >= 3 ? 'warning' : 'error'" size="large">
              {{ ratingText }}
            </n-tag>
          </n-space>
        </n-card>

        <!-- 指标解读 -->
        <n-grid :cols="3" :x-gap="16">
          <n-gi>
            <n-card size="small" title="💰 收益评估">
              <n-space vertical>
                <n-text>策略收益：<n-tag :type="returnTag.type" size="small">{{ returnTag.text }}</n-tag></n-text>
                <n-text depth="3" style="font-size: 12px">{{ returnTag.advice }}</n-text>
              </n-space>
            </n-card>
          </n-gi>
          <n-gi>
            <n-card size="small" title="📉 风险评估">
              <n-space vertical>
                <n-text>最大回撤：<n-tag :type="drawdownTag.type" size="small">{{ drawdownTag.text }}</n-tag></n-text>
                <n-text depth="3" style="font-size: 12px">{{ drawdownTag.advice }}</n-text>
              </n-space>
            </n-card>
          </n-gi>
          <n-gi>
            <n-card size="small" title="⚖️ 风险收益比">
              <n-space vertical>
                <n-text>夏普比率：<n-tag :type="sharpeTag.type" size="small">{{ sharpeTag.text }}</n-tag></n-text>
                <n-text depth="3" style="font-size: 12px">{{ sharpeTag.advice }}</n-text>
              </n-space>
            </n-card>
          </n-gi>
        </n-grid>

        <!-- 是否跑赢基准 -->
        <n-alert :type="result.total_return > result.benchmark_return ? 'success' : 'warning'">
          <template #header>
            {{ result.total_return > result.benchmark_return ? '✅ 跑赢基准' : '⚠️ 未跑赢基准' }}
          </template>
          策略收益 {{ result.total_return }}% 
          {{ result.total_return > result.benchmark_return ? '>' : '<' }} 
          基准收益 {{ result.benchmark_return }}%
          （{{ result.total_return > result.benchmark_return ? '超越' : '落后' }} {{ Math.abs(result.total_return - result.benchmark_return).toFixed(2) }}%）
        </n-alert>

        <!-- 投资建议 -->
        <n-card size="small" title="💡 投资建议">
          <n-text>{{ investmentAdvice }}</n-text>
        </n-card>
      </n-space>
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
import { LineChart, ScatterChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import axios from 'axios'
import { NRate } from 'naive-ui'

use([CanvasRenderer, LineChart, ScatterChart, GridComponent, TooltipComponent, LegendComponent])

const running = ref(false)
const addingMonitor = ref(false)
const result = ref<any>(null)
const strategies = ref<any[]>([])

const form = ref({
  stockCode: '000001',
  strategyId: 'ma_cross',
  startDate: Date.now() - 365 * 5 * 24 * 60 * 60 * 1000, // 默认5年
  endDate: Date.now(),
  initialCapital: 100000
})

// 计算当前选择的时间范围（年数）
const yearsText = computed(() => {
  const days = (form.value.endDate - form.value.startDate) / (24 * 60 * 60 * 1000)
  return (days / 365).toFixed(1)
})

// 设置时间范围快捷按钮
const setTimeRange = (years: number) => {
  form.value.endDate = Date.now()
  form.value.startDate = Date.now() - years * 365 * 24 * 60 * 60 * 1000
}

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
  
  // 提取买卖点
  const trades = result.value.trades || []
  const buyPoints: any[] = []
  const sellPoints: any[] = []
  
  trades.forEach((trade: any) => {
    const tradeDate = trade.date
    const dateIndex = dates.findIndex((d: string) => d === tradeDate || d.startsWith(tradeDate))
    if (dateIndex >= 0) {
      if (trade.type === 'buy') {
        buyPoints.push({
          name: '买入',
          value: [tradeDate, equity[dateIndex]],
          itemStyle: { color: '#18a058' }
        })
      } else {
        sellPoints.push({
          name: '卖出', 
          value: [tradeDate, equity[dateIndex]],
          itemStyle: { color: '#d03050' }
        })
      }
    }
  })
  
  // 计算纵轴范围（基于市值数据）
  const allValues = [...equity, ...benchmark].filter(v => v > 0)
  const minValue = Math.min(...allValues)
  const maxValue = Math.max(...allValues)
  
  // 计算合适的范围（上下留10%空间）
  const range = maxValue - minValue
  const yMin = Math.max(0, Math.floor((minValue - range * 0.1) / 1000) * 1000)
  const yMax = Math.ceil((maxValue + range * 0.1) / 1000) * 1000
  
  return {
    tooltip: { 
      trigger: 'axis',
      formatter: (params: any) => {
        let result = params[0].axisValue + '<br/>'
        params.forEach((item: any) => {
          if (item.seriesName === '买入点') {
            result += `<span style="color:#18a058">● 买入点</span><br/>`
          } else if (item.seriesName === '卖出点') {
            result += `<span style="color:#d03050">● 卖出点</span><br/>`
          } else {
            result += `${item.marker} ${item.seriesName}: ¥${item.value?.toLocaleString()}<br/>`
          }
        })
        return result
      }
    },
    legend: { 
      data: ['策略市值', '基准市值', '买入点', '卖出点']
    },
    grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
    xAxis: { type: 'category', data: dates },
    yAxis: { 
      type: 'value', 
      name: '市值(元)',
      min: yMin,
      max: yMax,
      splitNumber: 5,
      axisLabel: {
        formatter: (value: number) => {
          if (value >= 10000) {
            return (value / 10000).toFixed(1) + '万'
          }
          return value.toString()
        }
      }
    },
    series: [
      {
        name: '策略市值',
        type: 'line',
        data: equity,
        smooth: true,
        lineStyle: { width: 2 },
        areaStyle: { opacity: 0.1 },
        z: 1
      },
      {
        name: '基准市值',
        type: 'line',
        data: benchmark,
        smooth: true,
        lineStyle: { width: 2, type: 'dashed' },
        itemStyle: { opacity: 0.5 },
        z: 1
      },
      // 买入点标记
      {
        name: '买入点',
        type: 'scatter',
        data: buyPoints,
        symbol: 'triangle',
        symbolSize: 12,
        symbolRotate: 0,
        itemStyle: { color: '#18a058' },
        z: 10
      },
      // 卖出点标记
      {
        name: '卖出点',
        type: 'scatter',
        data: sellPoints,
        symbol: 'triangle',
        symbolSize: 12,
        symbolRotate: 180,
        itemStyle: { color: '#d03050' },
        z: 10
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

// 策略评估计算
const returnTag = computed(() => {
  const r = result.value?.total_return || 0
  if (r >= 20) return { type: 'success', text: '优秀', advice: '收益表现优异，建议关注' }
  if (r >= 10) return { type: 'success', text: '良好', advice: '收益表现良好，可考虑使用' }
  if (r >= 0) return { type: 'warning', text: '一般', advice: '收益表现一般，谨慎使用' }
  if (r >= -10) return { type: 'warning', text: '亏损', advice: '策略亏损，建议优化参数' }
  return { type: 'error', text: '严重亏损', advice: '策略效果不佳，不建议使用' }
})

const drawdownTag = computed(() => {
  const d = Math.abs(result.value?.max_drawdown || 0)
  if (d <= 10) return { type: 'success', text: '低风险', advice: '回撤控制良好，风险较低' }
  if (d <= 20) return { type: 'success', text: '中等风险', advice: '回撤适中，可接受范围' }
  if (d <= 30) return { type: 'warning', text: '较高风险', advice: '回撤较大，需关注风险' }
  return { type: 'error', text: '高风险', advice: '回撤过大，风险控制不佳' }
})

const sharpeTag = computed(() => {
  const s = result.value?.sharpe_ratio || 0
  if (s >= 2) return { type: 'success', text: '优秀', advice: '风险调整后收益极佳' }
  if (s >= 1) return { type: 'success', text: '良好', advice: '风险调整后收益良好' }
  if (s >= 0.5) return { type: 'warning', text: '一般', advice: '风险收益比一般' }
  return { type: 'error', text: '较差', advice: '风险收益比不佳' }
})

const overallRating = computed(() => {
  if (!result.value) return 0
  let score = 0
  
  // 收益评分 (0-2分)
  const r = result.value.total_return || 0
  if (r >= 20) score += 2
  else if (r >= 5) score += 1.5
  else if (r >= 0) score += 1
  else score += 0
  
  // 回撤评分 (0-1.5分)
  const d = Math.abs(result.value.max_drawdown || 0)
  if (d <= 10) score += 1.5
  else if (d <= 20) score += 1
  else if (d <= 30) score += 0.5
  
  // 夏普评分 (0-1.5分)
  const s = result.value.sharpe_ratio || 0
  if (s >= 1.5) score += 1.5
  else if (s >= 1) score += 1
  else if (s >= 0.5) score += 0.5
  
  return Math.min(5, Math.round(score))
})

const ratingText = computed(() => {
  const r = overallRating.value
  if (r >= 4) return '强烈推荐'
  if (r >= 3) return '建议使用'
  if (r >= 2) return '谨慎使用'
  return '不建议使用'
})

const investmentAdvice = computed(() => {
  if (!result.value) return ''
  
  const r = result.value.total_return || 0
  const d = Math.abs(result.value.max_drawdown || 0)
  const beat = result.value.total_return > result.value.benchmark_return
  
  if (r >= 15 && d <= 20 && beat) {
    return '✅ 该策略表现优秀：收益可观、风险可控、跑赢基准。建议添加到监控，可在实盘中参考使用。'
  }
  if (r >= 5 && d <= 25 && beat) {
    return '👍 该策略表现良好：收益为正、风险适中、跑赢基准。可以考虑添加监控，但建议进一步优化参数。'
  }
  if (r > 0 && beat) {
    return '⚠️ 该策略表现一般：虽然跑赢基准，但收益较低。建议优化策略参数后再使用。'
  }
  if (r > 0 && !beat) {
    return '⚠️ 该策略未跑赢基准：收益为正但不如简单持有。建议考虑其他策略或直接持有。'
  }
  if (d > 30) {
    return '❌ 该策略风险过高：最大回撤超过30%，可能导致较大亏损。不建议使用。'
  }
  return '❌ 该策略效果不佳：收益为负。建议调整策略参数或选择其他策略。'
})

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