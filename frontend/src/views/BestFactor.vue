<template>
  <div class="best-factor-page">
    <!-- 策略概述 -->
    <n-card title="🏆 最佳因子策略 v1.7" :bordered="false">
      <template #header-extra>
        <n-tag type="info">大盘择时 + 行业轮动</n-tag>
      </template>
      
      <n-descriptions label-placement="left" :column="3" bordered>
        <n-descriptions-item label="策略版本">v1.7</n-descriptions-item>
        <n-descriptions-item label="回测周期">10年</n-descriptions-item>
        <n-descriptions-item label="股票池">50只</n-descriptions-item>
        <n-descriptions-item label="平均年化收益">
          <n-text type="error">{{ summary.avg_return }}%</n-text>
        </n-descriptions-item>
        <n-descriptions-item label="平均夏普">
          <n-text type="success">{{ summary.avg_sharpe }}</n-text>
        </n-descriptions-item>
        <n-descriptions-item label="正收益占比">
          <n-text type="success">{{ summary.positive_return_pct }}%</n-text>
        </n-descriptions-item>
      </n-descriptions>
    </n-card>

    <!-- 核心指标 -->
    <n-grid :cols="4" :x-gap="16" :y-gap="16" class="mt-16">
      <n-gi>
        <n-card size="small">
          <n-statistic label="平均年化收益" :value="summary.avg_return">
            <template #suffix>%</template>
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card size="small">
          <n-statistic label="平均夏普比率" :value="summary.avg_sharpe" :precision="3" />
        </n-card>
      </n-gi>
      <n-gi>
        <n-card size="small">
          <n-statistic label="正收益占比" :value="summary.positive_return_pct">
            <template #suffix>%</template>
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card size="small">
          <n-statistic label="跑赢基准占比" :value="summary.beat_benchmark_pct">
            <template #suffix>%</template>
          </n-statistic>
        </n-card>
      </n-gi>
    </n-grid>

    <!-- 优化内容 -->
    <n-card title="优化内容（v1.7 大盘择时 + 行业轮动）" :bordered="false" class="mt-16">
      <n-grid :cols="3" :x-gap="16">
        <n-gi>
          <n-card size="small" title="1. 大盘择时" :bordered="true">
            <n-list>
              <n-list-item>
                <n-text>沪深300 MA20/MA60判断趋势</n-text>
              </n-list-item>
              <n-list-item>
                <n-text type="success">上涨加仓15-30%</n-text>
              </n-list-item>
              <n-list-item>
                <n-text type="error">下跌减仓50-70%</n-text>
              </n-list-item>
            </n-list>
          </n-card>
        </n-gi>
        <n-gi>
          <n-card size="small" title="2. 行业轮动" :bordered="true">
            <n-list>
              <n-list-item>
                <n-text type="success">科技：MOM 40%</n-text>
              </n-list-item>
              <n-list-item>
                <n-text>消费：ROE 35%</n-text>
              </n-list-item>
              <n-list-item>
                <n-text>金融：EP 35%</n-text>
              </n-list-item>
            </n-list>
          </n-card>
        </n-gi>
        <n-gi>
          <n-card size="small" title="3. 效果对比" :bordered="true">
            <n-list>
              <n-list-item>
                <n-text type="success">科技股年化: 5.39% → 9.12%</n-text>
              </n-list-item>
              <n-list-item>
                <n-text type="success">金融股年化: 6.86% → 6.14%</n-text>
              </n-list-item>
              <n-list-item>
                <n-text>行业适配提升显著</n-text>
              </n-list-item>
            </n-list>
          </n-card>
        </n-gi>
      </n-grid>
    </n-card>

    <!-- 因子权重配置 -->
    <n-card title="因子权重配置（按市场环境）" :bordered="false" class="mt-16">
      <n-tabs type="line">
        <n-tab-pane name="bull" tab="牛市">
          <n-data-table :columns="weightColumns" :data="bullWeights" :bordered="false" />
        </n-tab-pane>
        <n-tab-pane name="bear" tab="熊市">
          <n-data-table :columns="weightColumns" :data="bearWeights" :bordered="false" />
        </n-tab-pane>
        <n-tab-pane name="sideways" tab="震荡市">
          <n-data-table :columns="weightColumns" :data="sidewaysWeights" :bordered="false" />
        </n-tab-pane>
      </n-tabs>
    </n-card>

    <!-- ========== 新增：股票回测详情列表 ========== -->
    <n-card title="📊 股票回测详情" :bordered="false" class="mt-16">
      <template #header-extra>
        <n-space>
          <n-input v-model:value="searchKey" placeholder="搜索股票代码" clearable style="width: 150px" />
          <n-button @click="exportResults" type="primary" size="small">导出CSV</n-button>
        </n-space>
      </template>
      
      <n-data-table 
        :columns="stockColumns" 
        :data="filteredStocks" 
        :bordered="false"
        :row-key="(row: any) => row.stock_code"
        :pagination="{ pageSize: 10 }"
      />
    </n-card>

    <!-- 单只股票详情弹窗 -->
    <n-modal v-model:show="showDetail" preset="card" style="width: 1000px" title="回测详情">
      <template v-if="selectedStock">
        <n-spin :show="detailLoading">
          <!-- 基本信息 -->
          <n-descriptions :column="4" bordered>
            <n-descriptions-item label="股票代码">{{ selectedStock.stock_code }}</n-descriptions-item>
            <n-descriptions-item label="年化收益">
              <n-text :type="selectedStock.annual_return >= 0 ? 'error' : 'success'">
                {{ selectedStock.annual_return }}%
              </n-text>
            </n-descriptions-item>
            <n-descriptions-item label="夏普比率">{{ selectedStock.sharpe_ratio }}</n-descriptions-item>
            <n-descriptions-item label="最大回撤">
              <n-text type="success">{{ selectedStock.max_drawdown }}%</n-text>
            </n-descriptions-item>
          </n-descriptions>
          
          <!-- 详细数据（加载后） -->
          <template v-if="stockDetail">
            <n-card title="📈 资金曲线" size="small" class="mt-16">
              <div style="margin-bottom: 8px; font-size: 12px; color: #666;">
                <span style="background: rgba(33, 150, 243, 0.15); padding: 2px 8px; border-radius: 3px;">蓝色区域 = 验证区间（最近2年）</span>
              </div>
              <v-chart :option="chartOption" style="height: 350px" autoresize />
            </n-card>
            
            <!-- 回测统计 vs 验证统计 -->
            <n-grid :cols="2" :x-gap="16" class="mt-16">
              <n-gi>
                <n-card title="📊 全周期回测" size="small">
                  <n-descriptions :column="2" size="small">
                    <n-descriptions-item label="年化收益">
                      <n-text :type="stockDetail.summary?.annual_return >= 0 ? 'error' : 'success'">
                        {{ stockDetail.summary?.annual_return }}%
                      </n-text>
                    </n-descriptions-item>
                    <n-descriptions-item label="夏普比率">{{ stockDetail.summary?.sharpe_ratio }}</n-descriptions-item>
                    <n-descriptions-item label="最大回撤">
                      <n-text type="success">{{ stockDetail.summary?.max_drawdown }}%</n-text>
                    </n-descriptions-item>
                    <n-descriptions-item label="总交易">{{ stockDetail.summary?.total_trades }}次</n-descriptions-item>
                    <n-descriptions-item label="数据天数">{{ stockDetail.summary?.total_days }}天</n-descriptions-item>
                    <n-descriptions-item label="胜率">{{ stockDetail.summary?.win_rate }}%</n-descriptions-item>
                  </n-descriptions>
                </n-card>
              </n-gi>
              <n-gi>
                <n-card title="✅ 验证区间（最近2年）" size="small" v-if="stockDetail.validation">
                  <n-descriptions :column="2" size="small">
                    <n-descriptions-item label="年化收益">
                      <n-text :type="stockDetail.validation?.annual_return >= 0 ? 'error' : 'success'">
                        {{ stockDetail.validation?.annual_return }}%
                      </n-text>
                    </n-descriptions-item>
                    <n-descriptions-item label="夏普比率">{{ stockDetail.validation?.sharpe_ratio }}</n-descriptions-item>
                    <n-descriptions-item label="基准收益">
                      <n-text :type="stockDetail.validation?.benchmark_return >= 0 ? 'error' : 'success'">
                        {{ stockDetail.validation?.benchmark_return }}%
                      </n-text>
                    </n-descriptions-item>
                    <n-descriptions-item label="验证天数">{{ stockDetail.validation?.days }}天</n-descriptions-item>
                    <n-descriptions-item label="起始日期">{{ stockDetail.validation?.start_date }}</n-descriptions-item>
                    <n-descriptions-item label="当前市场">{{ stockDetail.regime === 'bull' ? '牛市' : stockDetail.regime === 'bear' ? '熊市' : '震荡' }}</n-descriptions-item>
                  </n-descriptions>
                </n-card>
              </n-gi>
            </n-grid>
            
            <!-- 交易记录 -->
            <n-card title="📋 最近交易记录" size="small" class="mt-16" v-if="stockDetail.trades?.length">
              <n-data-table 
                :columns="[
                  { title: '日期', key: 'date', width: 120 },
                  { title: '类型', key: 'type', width: 80, render: (row: any) => h(NTag, { type: row.type === 'buy' ? 'success' : 'error', size: 'small' }, { default: () => row.type === 'buy' ? '买入' : '卖出' }) },
                  { title: '价格', key: 'price', width: 100 },
                  { title: '仓位', key: 'position', width: 100, render: (row: any) => `${(row.position * 100).toFixed(0)}%` },
                  { title: '信号强度', key: 'score_z', width: 100 }
                ]" 
                :data="stockDetail.trades.slice(-20)" 
                size="small"
                :max-height="200"
              />
            </n-card>
          </template>
          
          <!-- 因子权重 -->
          <n-card title="该股票最优因子权重" size="small" class="mt-16" v-if="selectedStock.optimal_weights">
            <n-space>
              <n-tag v-for="(w, f) in selectedStock.optimal_weights" :key="f" type="info">
                {{ f }}: {{ (w*100).toFixed(0) }}%
              </n-tag>
            </n-space>
          </n-card>
        </n-spin>

        <!-- 操作按钮 -->
        <n-space class="mt-16">
          <n-button type="primary" @click="viewInBacktest">在回测中心查看</n-button>
          <n-button @click="showDetail = false">关闭</n-button>
        </n-space>
      </template>
    </n-modal>

    <!-- 方案对比 -->
    <n-card title="方案对比" :bordered="false" class="mt-16">
      <n-data-table :columns="comparisonColumns" :data="comparisonData" :bordered="false" />
    </n-card>

    <!-- API接口 -->
    <n-card title="API接口" :bordered="false" class="mt-16">
      <n-code language="bash" :code="apiCode" />
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, h, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart, ScatterChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import { 
  NCard, NDescriptions, NDescriptionsItem, NGrid, NGi, NStatistic, NTag, NText, 
  NList, NListItem, NTabs, NTabPane, NDataTable, NCode, NSpace, NButton, NInput, 
  NModal, NSpin
} from 'naive-ui'
import type { DataTableColumns } from 'naive-ui'
import axios from 'axios'

// 注册 ECharts
use([CanvasRenderer, LineChart, ScatterChart, GridComponent, TooltipComponent, LegendComponent])

const router = useRouter()

// ========== 原有数据 ==========
const weightColumns = [
  { title: '因子', key: 'factor' },
  { title: '权重', key: 'weight' },
  { title: '类别', key: 'category' },
]

const bullWeights = ref([
  { factor: 'MOM', weight: '40%', category: '动量' },
  { factor: 'KDJ', weight: '20%', category: '技术' },
  { factor: 'BOLL', weight: '15%', category: '技术' },
  { factor: 'TURN', weight: '15%', category: '情绪' },
  { factor: 'ROE', weight: '10%', category: '质量' },
])

const bearWeights = ref([
  { factor: 'LEV', weight: '30%', category: '质量' },
  { factor: 'ROE', weight: '25%', category: '质量' },
  { factor: 'EP', weight: '20%', category: '价值' },
  { factor: 'BP', weight: '15%', category: '价值' },
  { factor: 'BOLL', weight: '10%', category: '技术' },
])

const sidewaysWeights = ref([
  { factor: 'KDJ', weight: '25%', category: '技术' },
  { factor: 'BOLL', weight: '25%', category: '技术' },
  { factor: 'TURN', weight: '20%', category: '情绪' },
  { factor: 'MOM', weight: '15%', category: '动量' },
  { factor: 'ROE', weight: '15%', category: '质量' },
])

const comparisonColumns = [
  { title: '方案', key: 'name' },
  { title: '年化收益', key: 'return' },
  { title: '夏普比率', key: 'sharpe' },
  { title: '最大回撤', key: 'drawdown' },
  { title: '短期胜率', key: 'winRate' },
  { title: '跑赢基准', key: 'beatBenchmark' },
]

const comparisonData = ref([
  { name: '⭐ v1.7（大盘择时+行业轮动）', return: '4.05%', sharpe: '0.12', drawdown: '-48%', winRate: '-', beatBenchmark: '18.0%' },
  { name: 'v1.6（震荡市优化）', return: '6.86%', sharpe: '0.557', drawdown: '-20%', winRate: '-', beatBenchmark: '-' },
  { name: '综合优化v1.5', return: '2.11%', sharpe: '0.171', drawdown: '-26%', winRate: '58.2%', beatBenchmark: '59.2%' },
  { name: 'IC加权筛选', return: '1.75%', sharpe: '0.063', drawdown: '-', winRate: '50%', beatBenchmark: '50%' },
])

const apiCode = `# 获取策略配置
GET /api/best-factor/config

# 获取因子权重（bull/bear/sideways）
GET /api/best-factor/factor-weights/bull

# 获取回测结果
GET /api/best-factor/backtest-results

# 计算交易信号
GET /api/best-factor/signal?close_prices=10.1,10.2,10.3...`

// ========== 新增：股票详情数据 ==========
const summary = ref({
  avg_return: 4.05,
  avg_sharpe: 0.12,
  avg_drawdown: 47.61,
  positive_return_pct: 68.0,
  beat_benchmark_pct: 18.0,
  total_stocks: 50
})
const stockResults = ref<any[]>([])
const searchKey = ref('')
const showDetail = ref(false)
const selectedStock = ref<any>(null)
const detailLoading = ref(false)
const stockDetail = ref<any>(null)

// 图表配置
const chartOption = computed(() => {
  if (!stockDetail.value?.equity_curve) return {}
  
  const data = stockDetail.value.equity_curve
  const dates = data.map((d: any) => d.date)
  const equity = data.map((d: any) => d.equity)
  const benchmark = data.map((d: any) => d.benchmark)
  
  // 计算纵轴范围
  const allValues = [...equity, ...benchmark]
  const minValue = Math.min(...allValues)
  const maxValue = Math.max(...allValues)
  
  // 验证区间起始位置
  const validationStartIdx = data.findIndex((d: any) => d.is_validation)
  
  // 提取买卖点
  const trades = stockDetail.value.trades || []
  const buyPoints: any[] = []
  const sellPoints: any[] = []
  
  trades.forEach((trade: any) => {
    const dateIndex = dates.findIndex((d: string) => d === trade.date || d.startsWith(trade.date))
    if (dateIndex >= 0) {
      if (trade.type === 'buy') {
        buyPoints.push([trade.date, equity[dateIndex]])
      } else {
        sellPoints.push([trade.date, equity[dateIndex]])
      }
    }
  })
  
  // 标记区域（验证区间）
  const markArea = validationStartIdx >= 0 ? [
    {
      name: '验证区间',
      xAxis: dates[validationStartIdx]
    },
    {
      xAxis: dates[dates.length - 1]
    }
  ] : []
  
  return {
    tooltip: { 
      trigger: 'axis',
      formatter: (params: any) => {
        let result = params[0].axisValue + '<br/>'
        params.forEach((item: any) => {
          if (item.seriesName === '买入') {
            result += `<span style="color:#18a058">▲ 买入</span><br/>`
          } else if (item.seriesName === '卖出') {
            result += `<span style="color:#d03050">▼ 卖出</span><br/>`
          } else if (item.seriesName === '验证区间') {
            result += `<span style="background:#e8f4ff;padding:2px 6px;border-radius:3px">验证区间</span><br/>`
          } else {
            result += `${item.marker} ${item.seriesName}: ¥${item.value?.toLocaleString()}<br/>`
          }
        })
        return result
      }
    },
    legend: { data: ['策略市值', '基准市值', '买入', '卖出'] },
    grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
    xAxis: { 
      type: 'category', 
      data: dates, 
      axisLabel: { interval: 60, rotate: 45 },
      axisPointer: { show: true }
    },
    yAxis: { 
      type: 'value', 
      name: '市值(元)',
      min: Math.floor(minValue * 0.95),
      max: Math.ceil(maxValue * 1.05),
      axisLabel: { 
        formatter: (v: number) => v >= 10000 ? (v/10000).toFixed(1)+'万' : v 
      }
    },
    series: [
      {
        name: '策略市值',
        type: 'line',
        data: equity,
        smooth: true,
        lineStyle: { width: 2, color: '#2080f0' },
        areaStyle: { opacity: 0.1 },
        markArea: validationStartIdx >= 0 ? {
          silent: true,
          itemStyle: { color: 'rgba(33, 150, 243, 0.08)' },
          data: [[
            { xAxis: dates[validationStartIdx] },
            { xAxis: dates[dates.length - 1] }
          ]]
        } : undefined
      },
      {
        name: '基准市值',
        type: 'line',
        data: benchmark,
        smooth: true,
        lineStyle: { width: 2, type: 'dashed', color: '#909399' }
      },
      {
        name: '买入',
        type: 'scatter',
        data: buyPoints,
        symbol: 'triangle',
        symbolSize: 10,
        itemStyle: { color: '#18a058' }
      },
      {
        name: '卖出',
        type: 'scatter',
        data: sellPoints,
        symbol: 'triangle',
        symbolSize: 10,
        symbolRotate: 180,
        itemStyle: { color: '#d03050' }
      }
    ]
  }
})

const filteredStocks = computed(() => {
  if (!searchKey.value) return stockResults.value
  return stockResults.value.filter(s => s.stock_code?.includes(searchKey.value))
})

const stockColumns: DataTableColumns = [
  {
    title: '股票代码',
    key: 'stock_code',
    width: 100,
    fixed: 'left',
    render: (row: any) => h(NButton, {
      text: true,
      type: 'primary',
      onClick: () => showStockDetail(row)
    }, { default: () => row.stock_code })
  },
  {
    title: '基准收益',
    key: 'benchmark_return',
    width: 100,
    sorter: (a: any, b: any) => a.benchmark_return - b.benchmark_return,
    render: (row: any) => h(NText, {
      type: row.benchmark_return >= 0 ? 'error' : 'success'
    }, { default: () => `${row.benchmark_return}%` })
  },
  {
    title: '年化收益',
    key: 'annual_return',
    width: 100,
    sorter: (a: any, b: any) => a.annual_return - b.annual_return,
    render: (row: any) => h(NText, {
      type: row.annual_return >= 0 ? 'error' : 'success'
    }, { default: () => `${row.annual_return}%` })
  },
  {
    title: '超额收益',
    key: 'excess_return',
    width: 100,
    sorter: (a: any, b: any) => a.excess_return - b.excess_return,
    render: (row: any) => h(NText, {
      type: row.excess_return >= 0 ? 'error' : 'success'
    }, { default: () => `${row.excess_return}%` })
  },
  {
    title: '夏普比率',
    key: 'sharpe_ratio',
    width: 90,
    sorter: (a: any, b: any) => a.sharpe_ratio - b.sharpe_ratio,
    render: (row: any) => row.sharpe_ratio?.toFixed(2) || '-'
  },
  {
    title: '最大回撤',
    key: 'max_drawdown',
    width: 100,
    sorter: (a: any, b: any) => a.max_drawdown - b.max_drawdown,
    render: (row: any) => h(NText, { type: 'success' }, { default: () => `${row.max_drawdown}%` })
  },
  {
    title: '胜率',
    key: 'win_rate',
    width: 80,
    render: (row: any) => `${row.win_rate}%`
  },
  {
    title: '交易次数',
    key: 'trade_count',
    width: 80,
    render: (row: any) => row.trade_count || '-'
  },
  {
    title: '操作',
    key: 'actions',
    width: 80,
    fixed: 'right',
    render: (row: any) => h(NButton, {
      size: 'small',
      onClick: () => showStockDetail(row)
    }, { default: () => '详情' })
  }
]

const showStockDetail = async (stock: any) => {
  selectedStock.value = stock
  showDetail.value = true
  detailLoading.value = true
  stockDetail.value = null
  
  try {
    const res = await axios.get(`/api/best-factor/stock-detail/${stock.stock_code}`)
    if (res.data.status === 'success') {
      stockDetail.value = res.data
    }
  } catch (e) {
    console.error('加载详情失败', e)
  } finally {
    detailLoading.value = false
  }
}

const viewInBacktest = () => {
  if (selectedStock.value) {
    router.push({ path: '/backtest', query: { stock: selectedStock.value.stock_code } })
  }
}

const exportResults = () => {
  const csv = [
    ['股票代码', '年化收益', '夏普比率', '最大回撤', '跑赢基准', '胜率', '短期收益', '短期胜率'].join(','),
    ...filteredStocks.value.map(s => [s.stock_code, s.annual_return, s.sharpe_ratio, s.max_drawdown, s.excess_return, s.win_rate, s.short_term_return, s.short_term_winrate].join(','))
  ].join('\n')
  
  const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `best_factor_backtest_${new Date().toISOString().slice(0,10)}.csv`
  a.click()
}

// 加载数据
onMounted(async () => {
  try {
    const res = await axios.get('/api/best-factor/backtest-results')
    if (res.data.status === 'success' && res.data.all_results) {
      stockResults.value = res.data.all_results
      // 更新汇总数据
      if (res.data.summary) {
        summary.value = res.data.summary
      }
    }
  } catch (e) {
    console.error('加载回测结果失败', e)
  }
})
</script>

<style scoped>
.best-factor-page {
  padding: 16px;
}
.mt-16 {
  margin-top: 16px;
}
</style>