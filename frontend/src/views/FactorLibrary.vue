<template>
  <div class="factor-library">
    <!-- 页面标题 -->
    <n-page-header title="因子库" subtitle="Factor Library">
      <template #extra>
        <n-space>
          <n-button type="primary" @click="initLibrary" :loading="initLoading">
            🔄 初始化因子库
          </n-button>
          <n-button @click="loadData">
            🔃 刷新数据
          </n-button>
        </n-space>
      </template>
    </n-page-header>

    <!-- 总览统计 -->
    <n-grid :cols="4" :x-gap="16" :y-gap="16" style="margin-top: 16px">
      <n-gi>
        <n-card hoverable>
          <n-statistic label="总因子数" :value="summary.totalFactors">
            <template #suffix>个</template>
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card hoverable>
          <n-statistic label="有效因子" :value="summary.validFactors">
            <template #suffix>个</template>
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card hoverable>
          <n-statistic label="选中因子" :value="summary.selectedFactors">
            <template #suffix>个</template>
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card hoverable>
          <n-statistic label="有效率" :value="summary.validRate">
            <template #suffix>%</template>
          </n-statistic>
        </n-card>
      </n-gi>
    </n-grid>

    <!-- 类别统计卡片 -->
    <n-card title="📊 因子类别分布" style="margin-top: 16px">
      <n-grid :cols="6" :x-gap="12" :y-gap="12">
        <n-gi v-for="(stats, category) in categoryStats" :key="category">
          <n-card size="small" :class="['category-card', activeCategory === category ? 'active' : '']"
                  @click="activeCategory = activeCategory === category ? null : category">
            <div class="category-name">{{ getCategoryName(category) }}</div>
            <div class="category-stats">
              <span>{{ stats.selected }}/{{ stats.total }}</span>
            </div>
            <n-progress 
              type="line" 
              :percentage="stats.total > 0 ? Math.round(stats.selected / stats.total * 100) : 0"
              :show-indicator="false"
              :height="4"
              :border-radius="2"
            />
          </n-card>
        </n-gi>
      </n-grid>
    </n-card>

    <!-- Tabs 切换 -->
    <n-tabs v-model:value="activeTab" type="line" style="margin-top: 16px">
      <n-tab-pane name="factors" tab="因子列表">
        <!-- 搜索和筛选 -->
        <n-space style="margin-bottom: 16px">
          <n-input v-model:value="searchText" placeholder="搜索因子名称或代码" clearable style="width: 200px" />
          <n-select v-model:value="filterStatus" :options="statusOptions" placeholder="筛选状态" clearable style="width: 150px" />
          <n-select v-model:value="sortBy" :options="sortOptions" placeholder="排序方式" style="width: 150px" />
        </n-space>

        <!-- 因子列表表格 -->
        <n-data-table
          :columns="columns"
          :data="filteredFactors"
          :pagination="pagination"
          :row-key="(row: Factor) => row.code"
        />
      </n-tab-pane>

      <n-tab-pane name="ic-analysis" tab="IC分析">
        <n-card title="IC Top 20 因子">
          <n-data-table
            :columns="icColumns"
            :data="topFactors"
            :pagination="false"
          />
        </n-card>
      </n-tab-pane>

      <n-tab-pane name="correlations" tab="相关性分析">
        <n-card title="高相关因子对（相关系数 > 0.8）">
          <n-alert type="info" style="margin-bottom: 16px">
            高相关因子对会被剔除其中一个，保留IC更高的因子
          </n-alert>
          <n-data-table
            :columns="corrColumns"
            :data="correlations"
            :pagination="{ pageSize: 20 }"
          />
        </n-card>
      </n-tab-pane>

      <n-tab-pane name="params" tab="参数敏感性">
        <n-card title="参数敏感性测试结果">
          <n-alert type="info" style="margin-bottom: 16px">
            对有参数的因子测试不同参数配置，选择IC最高的参数
          </n-alert>
          <n-collapse>
            <n-collapse-item v-for="group in paramGroups" :key="group.factor_type" :name="group.factor_type">
              <template #header>
                <n-space>
                  <span>{{ group.factor_type }}</span>
                  <n-tag v-if="group.bestParam" type="success" size="small">
                    最优: {{ group.bestParam.param_desc }} (IC={{ group.bestParam.ic?.toFixed(2) }})
                  </n-tag>
                </n-space>
              </template>
              <n-table :bordered="false" :single-line="false">
                <thead>
                  <tr>
                    <th>参数配置</th>
                    <th>IC值</th>
                    <th>测试股票数</th>
                    <th>状态</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="item in group.items" :key="item.factor_code" :class="{ 'best-row': item.is_best }">
                    <td>{{ item.param_desc }}</td>
                    <td>
                      <n-tag :type="item.ic > 0.5 ? 'success' : item.ic > 0.3 ? 'warning' : 'error'">
                        {{ item.ic?.toFixed(4) }}
                      </n-tag>
                    </td>
                    <td>{{ item.n_stocks }}</td>
                    <td>
                      <n-tag v-if="item.is_best" type="success">最优</n-tag>
                      <n-tag v-else>普通</n-tag>
                    </td>
                  </tr>
                </tbody>
              </n-table>
            </n-collapse-item>
          </n-collapse>
        </n-card>
      </n-tab-pane>

      <n-tab-pane name="selection" tab="筛选结果">
        <n-card title="因子筛选结果">
          <template v-if="selectionResult">
            <n-descriptions :column="2" bordered>
              <n-descriptions-item label="筛选日期">
                {{ selectionResult.selection_date }}
              </n-descriptions-item>
              <n-descriptions-item label="相关性阈值">
                {{ selectionResult.threshold }}
              </n-descriptions-item>
              <n-descriptions-item label="原始因子数">
                {{ selectionResult.original_factors }}
              </n-descriptions-item>
              <n-descriptions-item label="有效因子数">
                {{ selectionResult.valid_factors }}
              </n-descriptions-item>
              <n-descriptions-item label="选中因子数">
                <n-text type="success">{{ selectionResult.selected_factors }}</n-text>
              </n-descriptions-item>
              <n-descriptions-item label="剔除因子数">
                <n-text type="error">{{ selectionResult.removed_factors }}</n-text>
              </n-descriptions-item>
            </n-descriptions>

            <n-divider>最终因子列表</n-divider>
            <n-space wrap>
              <n-tag v-for="code in selectionResult.final_factor_codes" :key="code" type="success" round>
                {{ code }}
              </n-tag>
            </n-space>

            <n-divider>剔除因子列表</n-divider>
            <n-space wrap>
              <n-tag v-for="code in selectionResult.removed_factor_codes" :key="code" type="error" round>
                {{ code }}
              </n-tag>
            </n-space>
          </template>
          <n-empty v-else description="暂无筛选结果" />
        </n-card>
      </n-tab-pane>
    </n-tabs>

    <!-- 因子详情弹窗 -->
    <n-modal v-model:show="showDetail" preset="card" :title="currentFactor?.name" style="width: 600px; max-width: 90vw">
      <template v-if="currentFactor">
        <n-descriptions :column="2" bordered>
          <n-descriptions-item label="因子代码">{{ currentFactor.code }}</n-descriptions-item>
          <n-descriptions-item label="因子类别">
            <n-tag :type="getCategoryTagType(currentFactor.category)">
              {{ getCategoryName(currentFactor.category) }}
            </n-tag>
          </n-descriptions-item>
          <n-descriptions-item label="数据来源">{{ currentFactor.data_source }}</n-descriptions-item>
          <n-descriptions-item label="更新频率">{{ currentFactor.update_freq }}</n-descriptions-item>
          <n-descriptions-item label="因子方向">
            <n-tag :type="currentFactor.direction > 0 ? 'success' : currentFactor.direction < 0 ? 'error' : 'default'">
              {{ currentFactor.direction > 0 ? '正向' : currentFactor.direction < 0 ? '反向' : '中性' }}
            </n-tag>
          </n-descriptions-item>
          <n-descriptions-item label="默认权重">{{ (currentFactor.weight * 100).toFixed(1) }}%</n-descriptions-item>
          <n-descriptions-item label="计算公式" :span="2">
            <n-code :code="currentFactor.formula" language="text" />
          </n-descriptions-item>
          <n-descriptions-item label="描述" :span="2">{{ currentFactor.description }}</n-descriptions-item>
        </n-descriptions>

        <!-- 检验结果 -->
        <template v-if="currentFactor.test_result">
          <n-divider>检验结果</n-divider>
          <n-grid :cols="4" :x-gap="16">
            <n-gi>
              <n-statistic label="IC均值" :value="currentFactor.test_result.ic_mean">
                <template #suffix>
                  <n-tag :type="currentFactor.test_result.ic_mean > 0.05 ? 'success' : 'warning'" size="small">
                    {{ currentFactor.test_result.rating }}
                  </n-tag>
                </template>
              </n-statistic>
            </n-gi>
            <n-gi>
              <n-statistic label="IC标准差" :value="currentFactor.test_result.ic_std" />
            </n-gi>
            <n-gi>
              <n-statistic label="IR" :value="currentFactor.test_result.ir">
                <template #suffix>
                  <n-tag :type="currentFactor.test_result.ir > 0.3 ? 'success' : 'warning'" size="small">
                    {{ currentFactor.test_result.ir > 0.3 ? '有效' : '无效' }}
                  </n-tag>
                </template>
              </n-statistic>
            </n-gi>
            <n-gi>
              <n-statistic label="IC正值比" :value="(currentFactor.test_result.ic_positive_ratio * 100).toFixed(1)">
                <template #suffix>%</template>
              </n-statistic>
            </n-gi>
          </n-grid>
        </template>

        <!-- 状态标签 -->
        <n-divider>因子状态</n-divider>
        <n-space>
          <n-tag :type="currentFactor.is_tested ? 'success' : 'default'">
            {{ currentFactor.is_tested ? '已检验' : '未检验' }}
          </n-tag>
          <n-tag :type="currentFactor.is_valid ? 'success' : 'error'">
            {{ currentFactor.is_valid ? '有效' : '无效' }}
          </n-tag>
          <n-tag :type="currentFactor.is_selected ? 'success' : 'warning'">
            {{ currentFactor.is_selected ? '已选中' : '未选中' }}
          </n-tag>
          <n-tag v-if="currentFactor.is_removed" type="error">
            已剔除
          </n-tag>
        </n-space>
      </template>
    </n-modal>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, h } from 'vue'
import { NButton, NTag, useMessage } from 'naive-ui'
import axios from 'axios'

interface Factor {
  code: string
  name: string
  category: string
  formula: string
  description: string
  weight: number
  direction: number
  data_source: string
  update_freq: string
  is_tested: boolean
  is_valid: boolean
  is_selected: boolean
  is_removed: boolean
  ic_mean?: number
  ic_std?: number
  ir?: number
  ic_positive_ratio?: number
  rating?: string
  test_result?: {
    ic_mean: number
    ic_std: number
    ir: number
    ic_positive_ratio: number
    rating: string
    n_periods: number
    test_date: string
  }
}

interface CategoryStats {
  total: number
  tested: number
  valid: number
  selected: number
}

const message = useMessage()
const initLoading = ref(false)
const loading = ref(false)

// 数据
const factors = ref<Factor[]>([])
const categoryStats = ref<Record<string, CategoryStats>>({})
const topFactors = ref<any[]>([])
const correlations = ref<any[]>([])
const paramSensitivity = ref<any[]>([])
const selectionResult = ref<any>(null)

// UI状态
const activeTab = ref('factors')
const activeCategory = ref<string | null>(null)
const searchText = ref('')
const filterStatus = ref<string | null>(null)
const sortBy = ref('ir')
const showDetail = ref(false)
const currentFactor = ref<Factor | null>(null)

// 选项配置
const statusOptions = [
  { label: '全部', value: null },
  { label: '已选中', value: 'selected' },
  { label: '已剔除', value: 'removed' },
  { label: '有效', value: 'valid' }
]

const sortOptions = [
  { label: 'IR降序', value: 'ir' },
  { label: 'IC降序', value: 'ic' },
  { label: '名称', value: 'name' },
  { label: '类别', value: 'category' }
]

const pagination = {
  pageSize: 20
}

// 类别名称映射
const categoryNames: Record<string, string> = {
  value: '价值因子',
  growth: '成长因子',
  quality: '质量因子',
  momentum: '动量因子',
  sentiment: '情绪因子',
  technical: '技术因子'
}

const getCategoryName = (category: string) => categoryNames[category] || category

const getCategoryTagType = (category: string) => {
  const types: Record<string, any> = {
    value: 'success',
    growth: 'info',
    quality: 'warning',
    momentum: 'error',
    sentiment: 'default',
    technical: 'primary'
  }
  return types[category] || 'default'
}

// 统计汇总
const summary = computed(() => {
  const total = factors.value.length
  const valid = factors.value.filter(f => f.is_valid).length
  const selected = factors.value.filter(f => f.is_selected).length
  return {
    totalFactors: total,
    validFactors: valid,
    selectedFactors: selected,
    validRate: total > 0 ? Math.round(valid / total * 100) : 0
  }
})

// 筛选后的因子列表
const filteredFactors = computed(() => {
  let result = [...factors.value]

  // 按类别筛选
  if (activeCategory.value) {
    result = result.filter(f => f.category === activeCategory.value)
  }

  // 按文本搜索
  if (searchText.value) {
    const text = searchText.value.toLowerCase()
    result = result.filter(f => 
      f.name.toLowerCase().includes(text) || 
      f.code.toLowerCase().includes(text)
    )
  }

  // 按状态筛选
  if (filterStatus.value) {
    switch (filterStatus.value) {
      case 'selected':
        result = result.filter(f => f.is_selected)
        break
      case 'removed':
        result = result.filter(f => f.is_removed)
        break
      case 'valid':
        result = result.filter(f => f.is_valid)
        break
    }
  }

  // 排序
  switch (sortBy.value) {
    case 'ir':
      result.sort((a, b) => (b.ir || 0) - (a.ir || 0))
      break
    case 'ic':
      result.sort((a, b) => (b.ic_mean || 0) - (a.ic_mean || 0))
      break
    case 'name':
      result.sort((a, b) => a.name.localeCompare(b.name))
      break
    case 'category':
      result.sort((a, b) => a.category.localeCompare(b.category))
      break
  }

  return result
})

// 参数敏感性分组
const paramGroups = computed(() => {
  const groups: Record<string, any[]> = {}
  
  for (const item of paramSensitivity.value) {
    if (!groups[item.factor_type]) {
      groups[item.factor_type] = []
    }
    groups[item.factor_type].push(item)
  }

  return Object.entries(groups).map(([type, items]) => ({
    factor_type: type,
    items: items.sort((a, b) => (b.ic || 0) - (a.ic || 0)),
    bestParam: items.find(i => i.is_best)
  }))
})

// 表格列定义
const columns = [
  {
    title: '因子代码',
    key: 'code',
    width: 100,
    render: (row: Factor) => h('span', { 
      style: { fontFamily: 'monospace', fontWeight: 'bold' } 
    }, row.code)
  },
  {
    title: '因子名称',
    key: 'name',
    width: 120,
    ellipsis: { tooltip: true }
  },
  {
    title: '类别',
    key: 'category',
    width: 100,
    render: (row: Factor) => h(NTag, { 
      type: getCategoryTagType(row.category),
      size: 'small'
    }, () => getCategoryName(row.category))
  },
  {
    title: 'IC均值',
    key: 'ic_mean',
    width: 100,
    render: (row: Factor) => {
      if (row.ic_mean === undefined) return '-'
      const color = row.ic_mean > 0.05 ? 'success' : row.ic_mean > 0 ? 'warning' : 'error'
      return h(NTag, { type: color, size: 'small' }, () => row.ic_mean?.toFixed(4))
    }
  },
  {
    title: 'IR',
    key: 'ir',
    width: 100,
    render: (row: Factor) => {
      if (row.ir === undefined) return '-'
      const color = row.ir > 0.3 ? 'success' : 'warning'
      return h(NTag, { type: color, size: 'small' }, () => row.ir?.toFixed(4))
    }
  },
  {
    title: '评级',
    key: 'rating',
    width: 60,
    render: (row: Factor) => row.rating ? h(NTag, { 
      type: row.rating === 'A' ? 'success' : row.rating?.startsWith('B') ? 'info' : 'warning',
      size: 'small'
    }, () => row.rating) : '-'
  },
  {
    title: '状态',
    key: 'status',
    width: 100,
    render: (row: Factor) => {
      if (row.is_removed) {
        return h(NTag, { type: 'error', size: 'small' }, () => '已剔除')
      }
      if (row.is_selected) {
        return h(NTag, { type: 'success', size: 'small' }, () => '已选中')
      }
      if (row.is_valid) {
        return h(NTag, { type: 'info', size: 'small' }, () => '有效')
      }
      return h(NTag, { type: 'default', size: 'small' }, () => '未检验')
    }
  },
  {
    title: '操作',
    key: 'actions',
    width: 80,
    render: (row: Factor) => h(NButton, {
      size: 'small',
      onClick: () => showFactorDetail(row)
    }, () => '详情')
  }
]

const icColumns = [
  { title: '排名', key: 'rank', width: 60 },
  { title: '因子代码', key: 'factor_code', width: 100 },
  { title: 'IC均值', key: 'ic_mean', width: 100 },
  { title: 'IC标准差', key: 'ic_std', width: 100 },
  { title: 'IR', key: 'ir', width: 100 },
  { title: '评级', key: 'rating', width: 80 }
]

const corrColumns = [
  { title: '因子1', key: 'factor1', width: 120 },
  { title: '因子2', key: 'factor2', width: 120 },
  { 
    title: '相关系数', 
    key: 'correlation', 
    width: 120,
    render: (row: any) => {
      const color = Math.abs(row.correlation) > 0.9 ? 'error' : 'warning'
      return h(NTag, { type: color, size: 'small' }, () => row.correlation.toFixed(4))
    }
  }
]

// 方法
const loadData = async () => {
  loading.value = true
  try {
    // 并行加载数据
    const [factorsRes, statsRes, topRes, corrRes, paramRes, selRes] = await Promise.all([
      axios.get('/api/factor-library/factors'),
      axios.get('/api/factor-library/categories'),
      axios.get('/api/factor-library/test-results?limit=20'),
      axios.get('/api/factor-library/correlations'),
      axios.get('/api/factor-library/param-sensitivity'),
      axios.get('/api/factor-library/selection')
    ])

    factors.value = factorsRes.data.factors || []
    categoryStats.value = statsRes.data || {}
    topFactors.value = (topRes.data.results || []).map((r: any, i: number) => ({
      ...r,
      rank: i + 1
    }))
    correlations.value = corrRes.data.correlations || []
    paramSensitivity.value = paramRes.data.results || []
    selectionResult.value = selRes.data.selection_date ? selRes.data : null

    message.success('数据加载成功')
  } catch (e: any) {
    console.error(e)
    message.error(e.response?.data?.detail || '加载数据失败')
  } finally {
    loading.value = false
  }
}

const initLibrary = async () => {
  initLoading.value = true
  try {
    const res = await axios.post('/api/factor-library/init')
    message.success('因子库初始化成功')
    console.log('Init results:', res.data)
    await loadData()
  } catch (e: any) {
    console.error(e)
    message.error(e.response?.data?.detail || '初始化失败')
  } finally {
    initLoading.value = false
  }
}

const showFactorDetail = async (factor: Factor) => {
  try {
    const res = await axios.get(`/api/factor-library/factors/${factor.code}`)
    currentFactor.value = res.data
    showDetail.value = true
  } catch (e) {
    message.error('获取详情失败')
  }
}

onMounted(() => {
  loadData()
})
</script>

<style scoped>
.factor-library {
  max-width: 1400px;
  margin: 0 auto;
}

.category-card {
  cursor: pointer;
  transition: all 0.2s;
}

.category-card:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
}

.category-card.active {
  border: 2px solid #18a058;
}

.category-name {
  font-weight: 500;
  margin-bottom: 4px;
}

.category-stats {
  font-size: 12px;
  color: #666;
  margin-bottom: 8px;
}

.best-row {
  background-color: rgba(24, 160, 88, 0.1);
}

:deep(.n-statistic) {
  text-align: center;
}

:deep(.n-code) {
  font-size: 12px;
}
</style>