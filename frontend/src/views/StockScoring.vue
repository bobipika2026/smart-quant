<template>
  <div class="stock-scoring">
    <!-- 股票池概览 -->
    <n-card title="📊 专业量化因子评分 v3" class="mb-4">
      <template #header-extra>
        <n-space>
          <n-button @click="showFactorInfo = true" quaternary>
            因子说明
          </n-button>
          <n-button @click="showFilters = !showFilters" :type="hasFilters ? 'primary' : 'default'">
            {{ hasFilters ? '筛选中' : '筛选' }}
          </n-button>
          <n-button @click="exportPool" :loading="exporting">
            导出CSV
          </n-button>
          <n-button @click="fetchStockPool" :loading="loading" type="primary">
            刷新
          </n-button>
        </n-space>
      </template>

      <!-- 因子模型说明 -->
      <n-alert type="info" class="mb-4">
        <template #header>Barra/MSCI量化标准 - 33个细分因子</template>
        <n-space :size="12">
          <n-tag type="success">价值 22% (6因子)</n-tag>
          <n-tag type="warning">成长 18% (6因子)</n-tag>
          <n-tag type="info">质量 28% (9因子)</n-tag>
          <n-tag type="error">动量 17% (6因子)</n-tag>
          <n-tag type="default">情绪 15% (6因子)</n-tag>
        </n-space>
      </n-alert>

      <!-- 动态权重状态 -->
      <n-card size="small" class="mb-4" v-if="dynamicConfig">
        <n-grid :cols="6" :x-gap="12">
          <n-gi>
            <n-statistic label="市场环境">
              <template #default>
                <n-tag :type="getRegimeType(dynamicConfig.market_regime)">
                  {{ getRegimeName(dynamicConfig.market_regime) }}
                </n-tag>
              </template>
            </n-statistic>
          </n-gi>
          <n-gi>
            <n-statistic label="经济周期">
              <template #default>
                <n-tag :type="getCycleType(dynamicConfig.economic_cycle)">
                  {{ getCycleName(dynamicConfig.economic_cycle) }}
                </n-tag>
              </template>
            </n-statistic>
          </n-gi>
          <n-gi>
            <n-statistic label="风险偏好" :value="dynamicConfig.risk_appetite">
              <template #suffix>
                <n-tag size="small" :type="dynamicConfig.risk_appetite > 60 ? 'success' : dynamicConfig.risk_appetite < 40 ? 'warning' : 'info'">
                  {{ dynamicConfig.risk_appetite > 60 ? '激进' : dynamicConfig.risk_appetite < 40 ? '保守' : '中等' }}
                </n-tag>
              </template>
            </n-statistic>
          </n-gi>
          <n-gi v-for="(w, factor) in dynamicConfig.weights" :key="factor">
            <n-statistic :label="styleNames[factor]">
              <template #default>
                <span :style="{color: getWeightColor(w)}">{{ (w * 100).toFixed(1) }}%</span>
              </template>
            </n-statistic>
          </n-gi>
        </n-grid>
        <n-space class="mt-2">
          <n-button size="small" @click="fetchDynamicWeights">刷新权重</n-button>
          <n-button size="small" type="primary" @click="useDynamicWeights = !useDynamicWeights">
            {{ useDynamicWeights ? '使用动态权重' : '使用基准权重' }}
          </n-button>
        </n-space>
      </n-card>

      <!-- 筛选面板 -->
      <n-collapse-transition :show="showFilters">
        <n-card size="small" class="mb-4" :bordered="false" style="background: #fafafa">
          <n-grid :cols="4" :x-gap="16" :y-gap="12">
            <n-gi>
              <n-form-item label="最低综合分" size="small">
                <n-slider v-model:value="filters.min_score" :min="0" :max="100" :step="5" />
              </n-form-item>
            </n-gi>
            <n-gi>
              <n-form-item label="价值得分 ≥" size="small">
                <n-slider v-model:value="filters.min_value" :min="0" :max="100" :step="5" />
              </n-form-item>
            </n-gi>
            <n-gi>
              <n-form-item label="成长得分 ≥" size="small">
                <n-slider v-model:value="filters.min_growth" :min="0" :max="100" :step="5" />
              </n-form-item>
            </n-gi>
            <n-gi>
              <n-form-item label="质量得分 ≥" size="small">
                <n-slider v-model:value="filters.min_quality" :min="0" :max="100" :step="5" />
              </n-form-item>
            </n-gi>
            <n-gi>
              <n-form-item label="行业筛选" size="small">
                <n-select v-model:value="filters.industry" :options="industryOptions" clearable placeholder="全部行业" />
              </n-form-item>
            </n-gi>
            <n-gi>
              <n-form-item label="评级筛选" size="small">
                <n-select v-model:value="filters.grade" :options="gradeOptions" clearable placeholder="全部评级" />
              </n-form-item>
            </n-gi>
            <n-gi>
              <n-form-item label="股票池大小" size="small">
                <n-input-number v-model:value="topN" :min="10" :max="100" :step="10" />
              </n-form-item>
            </n-gi>
            <n-gi>
              <n-space align="center" style="margin-top: 22px">
                <n-button size="small" @click="applyFilters" type="primary">应用筛选</n-button>
                <n-button size="small" @click="resetFilters">重置</n-button>
              </n-space>
            </n-gi>
          </n-grid>
        </n-card>
      </n-collapse-transition>

      <!-- 统计信息 -->
      <n-grid :cols="5" :x-gap="16" class="mb-4">
        <n-gi>
          <n-statistic label="分析股票数" :value="statistics.total_analyzed || 0" />
        </n-gi>
        <n-gi>
          <n-statistic label="符合条件" :value="statistics.qualified_count || 0" />
        </n-gi>
        <n-gi>
          <n-statistic label="股票池大小" :value="statistics.pool_size || 0" />
        </n-gi>
        <n-gi>
          <n-statistic label="平均评分" :value="avgScore?.toFixed(1) || 0" />
        </n-gi>
        <n-gi>
          <n-statistic label="更新时间" :value="statistics.generated_at?.split(' ')[1] || '-'" />
        </n-gi>
      </n-grid>

      <!-- 股票池表格 -->
      <n-data-table
        :columns="columns"
        :data="stockPool"
        :pagination="pagination"
        :row-class-name="rowClass"
        :scroll-x="1400"
        striped
      />
    </n-card>

    <!-- 行业分布 -->
    <n-card title="行业分布" class="mb-4" v-if="industryDist">
      <n-grid :cols="3" :x-gap="12">
        <n-gi v-for="(count, industry) in industryDist" :key="industry">
          <n-tag type="info" size="large" style="width: 100%; justify-content: space-between">
            <span>{{ industry }}</span>
            <span>{{ count }}只</span>
          </n-tag>
        </n-gi>
      </n-grid>
    </n-card>

    <!-- 单股评分详情弹窗 -->
    <n-modal v-model:show="showDetail" preset="card" style="width: 900px" title="股票评分详情">
      <div v-if="selectedStock">
        <n-descriptions :column="3" bordered>
          <n-descriptions-item label="股票代码">{{ selectedStock.stock_code }}</n-descriptions-item>
          <n-descriptions-item label="股票名称">{{ selectedStock.stock_name }}</n-descriptions-item>
          <n-descriptions-item label="所属行业">{{ selectedStock.industry || '-' }}</n-descriptions-item>
          <n-descriptions-item label="综合评分">
            <n-tag :type="getGradeType(selectedStock.grade)" size="large">
              {{ selectedStock.composite_score }}分 ({{ selectedStock.grade }})
            </n-tag>
          </n-descriptions-item>
          <n-descriptions-item label="最新价">{{ selectedStock.close }}元</n-descriptions-item>
          <n-descriptions-item label="交易日期">{{ selectedStock.trade_date }}</n-descriptions-item>
        </n-descriptions>

        <!-- 五维雷达图 -->
        <div class="mt-4">
          <n-card title="风格因子雷达图" size="small">
            <div ref="radarChartRef" style="height: 280px"></div>
          </n-card>
        </div>

        <!-- 风格因子详情 -->
        <n-card title="风格因子得分（行业百分位）" class="mt-4" size="small">
          <n-grid :cols="5" :x-gap="12">
            <n-gi v-for="(score, key) in selectedStock.style_scores" :key="key">
              <n-statistic :label="styleNames[key]" :value="score">
                <template #suffix>分</template>
              </n-statistic>
              <n-progress
                type="line"
                :percentage="score"
                :status="score >= 70 ? 'success' : score >= 50 ? 'info' : 'warning'"
                :show-indicator="false"
                class="mt-2"
              />
            </n-gi>
          </n-grid>
        </n-card>
      </div>
    </n-modal>

    <!-- 因子说明弹窗 -->
    <n-modal v-model:show="showFactorInfo" preset="card" style="width: 800px" title="因子体系说明">
      <n-alert type="info" class="mb-4">
        参考 Barra CNE5、MSCI China、中金量化等专业因子框架
      </n-alert>
      
      <n-tabs type="line">
        <n-tab-pane name="value" tab="价值因子 (22%)">
          <n-table :bordered="false" size="small">
            <thead><tr><th>因子</th><th>权重</th><th>公式</th></tr></thead>
            <tbody>
              <tr><td>盈利收益率 EP</td><td>25%</td><td>E_ttm / 市值</td></tr>
              <tr><td>账面市值比 BP</td><td>20%</td><td>B / 市值</td></tr>
              <tr><td>销售市值比 SP</td><td>15%</td><td>Revenue / 市值</td></tr>
              <tr><td>现金流市值比 NCFP</td><td>20%</td><td>OCF / 市值</td></tr>
              <tr><td>股息率 DIV</td><td>10%</td><td>DPS / Price</td></tr>
              <tr><td>企业价值比 EV_EBITDA</td><td>10%</td><td>EBITDA / EV</td></tr>
            </tbody>
          </n-table>
        </n-tab-pane>
        <n-tab-pane name="growth" tab="成长因子 (18%)">
          <n-table :bordered="false" size="small">
            <thead><tr><th>因子</th><th>权重</th><th>公式</th></tr></thead>
            <tbody>
              <tr><td>营收增长率</td><td>20%</td><td>(Rev_t - Rev_t-1) / |Rev_t-1|</td></tr>
              <tr><td>净利润增长率</td><td>25%</td><td>(NP_t - NP_t-1) / |NP_t-1|</td></tr>
              <tr><td>EPS增长率</td><td>15%</td><td>(EPS_t - EPS_t-1) / |EPS_t-1|</td></tr>
              <tr><td>ROE变化</td><td>20%</td><td>ROE_t - ROE_t-1</td></tr>
              <tr><td>营收增长加速度</td><td>10%</td><td>ΔREV_G</td></tr>
              <tr><td>净利率变化</td><td>10%</td><td>NPM_t - NPM_t-1</td></tr>
            </tbody>
          </n-table>
        </n-tab-pane>
        <n-tab-pane name="quality" tab="质量因子 (28%)">
          <n-table :bordered="false" size="small">
            <thead><tr><th>因子</th><th>权重</th><th>公式</th></tr></thead>
            <tbody>
              <tr><td>ROE</td><td>20%</td><td>净利润 / 净资产</td></tr>
              <tr><td>ROA</td><td>10%</td><td>净利润 / 总资产</td></tr>
              <tr><td>毛利率</td><td>10%</td><td>(营收 - 成本) / 营收</td></tr>
              <tr><td>净利率</td><td>10%</td><td>净利润 / 营收</td></tr>
              <tr><td>资产周转率</td><td>10%</td><td>营收 / 总资产</td></tr>
              <tr><td>财务杠杆(反)</td><td>10%</td><td>资产 / 权益</td></tr>
              <tr><td>流动比率</td><td>5%</td><td>流动资产 / 流动负债</td></tr>
              <tr><td>应计项目(反)</td><td>15%</td><td>(NI - OCF) / 资产</td></tr>
              <tr><td>现金流质量</td><td>10%</td><td>OCF / NI</td></tr>
            </tbody>
          </n-table>
        </n-tab-pane>
        <n-tab-pane name="momentum" tab="动量因子 (17%)">
          <n-table :bordered="false" size="small">
            <thead><tr><th>因子</th><th>权重</th><th>公式</th></tr></thead>
            <tbody>
              <tr><td>12月动量</td><td>30%</td><td>剔除最近1月的12月收益</td></tr>
              <tr><td>6月动量</td><td>20%</td><td>过去6个月收益</td></tr>
              <tr><td>相对强度</td><td>15%</td><td>股价 / 行业指数</td></tr>
              <tr><td>成交量动量</td><td>10%</td><td>5日均量 / 20日均量</td></tr>
              <tr><td>均线偏离</td><td>10%</td><td>(P - MA20) / MA20</td></tr>
              <tr><td>盈利修正</td><td>15%</td><td>EPS上调比例</td></tr>
            </tbody>
          </n-table>
        </n-tab-pane>
        <n-tab-pane name="sentiment" tab="情绪因子 (15%)">
          <n-table :bordered="false" size="small">
            <thead><tr><th>因子</th><th>权重</th><th>公式</th></tr></thead>
            <tbody>
              <tr><td>北向持股变化</td><td>25%</td><td>Δ北向持股 / 流通股</td></tr>
              <tr><td>机构持股比例</td><td>20%</td><td>机构持股 / 总股本</td></tr>
              <tr><td>分析师评级</td><td>15%</td><td>评级均值</td></tr>
              <tr><td>换手率(中性)</td><td>10%</td><td>成交量 / 流通股</td></tr>
              <tr><td>融资余额变化</td><td>15%</td><td>Δ融资余额 / 市值</td></tr>
              <tr><td>波动率(反)</td><td>15%</td><td>Std(Ret) × √252</td></tr>
            </tbody>
          </n-table>
        </n-tab-pane>
      </n-tabs>
    </n-modal>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, nextTick, watch, h } from 'vue'
import {
  NCard, NDataTable, NStatistic, NGrid, NGi, NTag, NButton, NSpace, NInputNumber,
  NModal, NDescriptions, NDescriptionsItem, NProgress, NAlert, NSelect, NSlider,
  NFormItem, NCollapseTransition, NTabs, NTabPane, NTable, useMessage
} from 'naive-ui'
import * as echarts from 'echarts'
import axios from 'axios'

const message = useMessage()

// 数据
const loading = ref(false)
const exporting = ref(false)
const topN = ref(50)
const stockPool = ref<any[]>([])
const statistics = ref<any>({})
const industryDist = ref<any>(null)
const showDetail = ref(false)
const selectedStock = ref<any>(null)
const showFilters = ref(false)
const showFactorInfo = ref(false)
const dynamicConfig = ref<any>(null)
const useDynamicWeights = ref(true)

// 筛选条件
const filters = ref({
  min_score: 0,
  min_value: null as number | null,
  min_growth: null as number | null,
  min_quality: null as number | null,
  industry: null as string | null,
  grade: null as string | null
})

// 行业选项
const industryOptions = ref<{label: string, value: string}[]>([])
const gradeOptions = [
  { label: 'A级', value: 'A' },
  { label: 'B+级', value: 'B+' },
  { label: 'B级', value: 'B' },
  { label: 'B-级', value: 'B-' },
  { label: 'C级', value: 'C' },
]

// 图表
const radarChartRef = ref<HTMLElement | null>(null)
let radarChart: echarts.ECharts | null = null

// 风格名称映射
const styleNames: Record<string, string> = {
  value: '价值',
  growth: '成长',
  quality: '质量',
  momentum: '动量',
  sentiment: '情绪'
}

// 市场环境名称
const regimeNames: Record<string, string> = {
  'bull': '牛市',
  'bear': '熊市',
  'sideways': '震荡市',
  'recovery': '反转期'
}

// 经济周期名称
const cycleNames: Record<string, string> = {
  'recovery': '复苏期',
  'expansion': '繁荣期',
  'slowdown': '衰退期',
  'recession': '萧条期'
}

// 获取市场环境类型
function getRegimeType(regime: string): 'success' | 'error' | 'warning' | 'info' {
  const types: Record<string, 'success' | 'error' | 'warning' | 'info'> = {
    'bull': 'success',
    'bear': 'error',
    'sideways': 'warning',
    'recovery': 'info'
  }
  return types[regime] || 'default'
}

function getRegimeName(regime: string): string {
  return regimeNames[regime] || regime
}

// 获取经济周期类型
function getCycleType(cycle: string): 'success' | 'info' | 'warning' | 'error' {
  const types: Record<string, 'success' | 'info' | 'warning' | 'error'> = {
    'recovery': 'success',
    'expansion': 'info',
    'slowdown': 'warning',
    'recession': 'error'
  }
  return types[cycle] || 'default'
}

function getCycleName(cycle: string): string {
  return cycleNames[cycle] || cycle
}

// 获取权重颜色
function getWeightColor(weight: number): string {
  if (weight > 0.25) return '#18a058'
  if (weight > 0.18) return '#2080f0'
  return '#f0a020'
}

// 分页
const pagination = ref({
  page: 1,
  pageSize: 20,
  showSizePicker: true,
  pageSizes: [20, 50, 100]
})

// 是否有筛选条件
const hasFilters = computed(() => {
  return filters.value.min_score > 0 || 
         filters.value.min_value || 
         filters.value.min_growth || 
         filters.value.min_quality ||
         filters.value.industry ||
         filters.value.grade
})

// 计算平均分
const avgScore = computed(() => {
  if (stockPool.value.length === 0) return 0
  return stockPool.value.reduce((sum, s) => sum + s.composite_score, 0) / stockPool.value.length
})

// 表格列
const columns = [
  {
    title: '排名',
    key: 'rank',
    width: 60,
    render: (row: any) => h(NTag, { type: row.rank <= 10 ? 'success' : 'default', size: 'small' }, () => row.rank)
  },
  {
    title: '代码',
    key: 'stock_code',
    width: 80
  },
  {
    title: '名称',
    key: 'stock_name',
    width: 100,
    render: (row: any) => h('span', { style: { fontWeight: 'bold' } }, row.stock_name)
  },
  {
    title: '行业',
    key: 'industry',
    width: 90,
    render: (row: any) => h(NTag, { size: 'small', type: 'default' }, () => row.industry || '-')
  },
  {
    title: '综合评分',
    key: 'composite_score',
    width: 100,
    sorter: (a: any, b: any) => a.composite_score - b.composite_score,
    render: (row: any) => h(NTag, { type: getGradeType(row.grade) }, () => `${row.composite_score}分`)
  },
  {
    title: '评级',
    key: 'grade',
    width: 60,
    render: (row: any) => h(NTag, { type: getGradeType(row.grade), size: 'large' }, () => row.grade)
  },
  {
    title: '价值',
    key: 'value_score',
    width: 70,
    render: (row: any) => renderScore(row.value_score)
  },
  {
    title: '成长',
    key: 'growth_score',
    width: 70,
    render: (row: any) => renderScore(row.growth_score)
  },
  {
    title: '质量',
    key: 'quality_score',
    width: 70,
    render: (row: any) => renderScore(row.quality_score)
  },
  {
    title: '动量',
    key: 'momentum_score',
    width: 70,
    render: (row: any) => renderScore(row.momentum_score)
  },
  {
    title: '情绪',
    key: 'sentiment_score',
    width: 70,
    render: (row: any) => renderScore(row.sentiment_score)
  },
  {
    title: '最新价',
    key: 'close',
    width: 80,
    render: (row: any) => row.close ? `¥${row.close}` : '-'
  },
  {
    title: '操作',
    key: 'actions',
    width: 70,
    render: (row: any) => h(NButton, { size: 'small', onClick: () => showStockDetail(row) }, () => '详情')
  }
]

// 渲染分数
function renderScore(score: number) {
  const color = score >= 70 ? '#18a058' : score >= 50 ? '#2080f0' : '#f0a020'
  return h('span', { style: { color, fontWeight: 'bold' } }, score?.toFixed(0) || '-')
}

// 获取评级颜色
function getGradeType(grade: string): 'success' | 'info' | 'warning' | 'error' | 'default' {
  const types: Record<string, 'success' | 'info' | 'warning' | 'error' | 'default'> = {
    'A': 'success',
    'B+': 'info',
    'B': 'info',
    'B-': 'warning',
    'C': 'warning',
    'D': 'error'
  }
  return types[grade] || 'default'
}

// 行样式
function rowClass(row: any) {
  if (row.rank <= 10) return 'top-10-row'
  if (row.rank <= 30) return 'top-30-row'
  return ''
}

// 获取股票池数据
async function fetchStockPool() {
  loading.value = true
  try {
    let url: string
    if (useDynamicWeights.value) {
      url = `/api/stock-scoring/v3/pool-with-dynamic-weights?top_n=${topN.value}&use_dynamic=true`
      const res = await axios.get(url)
      if (res.data.code === 0) {
        stockPool.value = res.data.data.stocks
        statistics.value = res.data.data
        industryDist.value = res.data.data.industry_distribution
        dynamicConfig.value = res.data.data.weight_config
      }
    } else {
      url = `/api/stock-scoring/v3/pool?top_n=${topN.value}&min_score=${filters.value.min_score}`
      if (filters.value.min_value) url += `&min_value=${filters.value.min_value}`
      if (filters.value.min_growth) url += `&min_growth=${filters.value.min_growth}`
      if (filters.value.min_quality) url += `&min_quality=${filters.value.min_quality}`
      if (filters.value.industry) url += `&industry=${encodeURIComponent(filters.value.industry)}`
      if (filters.value.grade) url += `&grade=${filters.value.grade}`
      
      const res = await axios.get(url)
      if (res.data.code === 0) {
        stockPool.value = res.data.data.stocks
        statistics.value = res.data.data
        industryDist.value = res.data.data.industry_distribution
      }
    }
  } catch (e: any) {
    message.error('获取股票池失败: ' + e.message)
  } finally {
    loading.value = false
  }
}

// 获取动态权重配置
async function fetchDynamicWeights() {
  try {
    const res = await axios.get('/api/stock-scoring/v3/dynamic-weights')
    if (res.data.code === 0) {
      dynamicConfig.value = res.data.data
    }
  } catch (e) {
    // ignore
  }
}

// 获取行业列表
async function fetchIndustries() {
  try {
    const res = await axios.get('/api/stock-scoring/v3/industries')
    if (res.data.code === 0) {
      industryOptions.value = res.data.data.industries.map((i: string) => ({ label: i, value: i }))
    }
  } catch (e) {
    // ignore
  }
}

// 导出
async function exportPool() {
  exporting.value = true
  try {
    const res = await axios.get(`/api/stock-scoring/v3/export?top_n=${topN.value}&min_score=${filters.value.min_score}`)
    if (res.data.code === 0) {
      message.success(`导出成功: ${res.data.data.file_name} (${res.data.data.row_count}行)`)
    }
  } catch (e: any) {
    message.error('导出失败: ' + e.message)
  } finally {
    exporting.value = false
  }
}

// 应用筛选
function applyFilters() {
  fetchStockPool()
}

// 重置筛选
function resetFilters() {
  filters.value = {
    min_score: 0,
    min_value: null,
    min_growth: null,
    min_quality: null,
    industry: null,
    grade: null
  }
  fetchStockPool()
}

// 显示股票详情
async function showStockDetail(stock: any) {
  selectedStock.value = stock
  showDetail.value = true
  await nextTick()
  renderRadarChart()
}

// 渲染雷达图
function renderRadarChart() {
  if (!radarChartRef.value || !selectedStock.value) return
  
  if (radarChart) {
    radarChart.dispose()
  }
  
  radarChart = echarts.init(radarChartRef.value)
  
  const scores = selectedStock.value.style_scores
  
  const option = {
    radar: {
      indicator: [
        { name: '价值', max: 100 },
        { name: '成长', max: 100 },
        { name: '质量', max: 100 },
        { name: '动量', max: 100 },
        { name: '情绪', max: 100 }
      ],
      radius: '60%'
    },
    series: [{
      type: 'radar',
      data: [{
        value: [
          scores.value,
          scores.growth,
          scores.quality,
          scores.momentum,
          scores.sentiment
        ],
        name: selectedStock.value.stock_name,
        areaStyle: { color: 'rgba(24, 160, 88, 0.3)' },
        lineStyle: { color: '#18a058', width: 2 },
        itemStyle: { color: '#18a058' }
      }]
    }]
  }
  
  radarChart.setOption(option)
}

// 初始化
onMounted(() => {
  fetchIndustries()
  fetchDynamicWeights()
  fetchStockPool()
})
</script>

<style scoped>
.stock-scoring {
  padding: 16px;
}

.mb-4 {
  margin-bottom: 16px;
}

.mt-2 {
  margin-top: 8px;
}

.mt-4 {
  margin-top: 16px;
}

:deep(.top-10-row) {
  background-color: rgba(24, 160, 88, 0.08) !important;
}

:deep(.top-30-row) {
  background-color: rgba(32, 128, 240, 0.05) !important;
}
</style>