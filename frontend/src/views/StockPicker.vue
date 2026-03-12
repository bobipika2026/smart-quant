<template>
  <div class="stock-picker">
    <n-tabs type="line" animated>
      <!-- 条件选股 -->
      <n-tab-pane name="condition" tab="条件选股">
        <n-card>
          <n-form :model="condition" label-placement="left" label-width="80">
            <n-grid :cols="3" :x-gap="16">
              <n-gi>
                <n-form-item label="价格区间">
                  <n-input-group>
                    <n-input-number v-model:value="condition.price_min" placeholder="最低" size="small" />
                    <n-input-group-label>~</n-input-group-label>
                    <n-input-number v-model:value="condition.price_max" placeholder="最高" size="small" />
                  </n-input-group>
                </n-form-item>
              </n-gi>
              <n-gi>
                <n-form-item label="涨跌幅">
                  <n-input-group>
                    <n-input-number v-model:value="condition.change_min" placeholder="最低%" size="small" />
                    <n-input-group-label>~</n-input-group-label>
                    <n-input-number v-model:value="condition.change_max" placeholder="最高%" size="small" />
                  </n-input-group>
                </n-form-item>
              </n-gi>
              <n-gi>
                <n-form-item label="成交量(万手)">
                  <n-input-number v-model:value="condition.volume_min" placeholder="最小" size="small" />
                </n-form-item>
              </n-gi>
              <n-gi>
                <n-form-item label="交易所">
                  <n-select v-model:value="condition.exchange" :options="exchangeOptions" clearable size="small" />
                </n-form-item>
              </n-gi>
              <n-gi>
                <n-form-item label="行业">
                  <n-select v-model:value="condition.industry" :options="industryOptions" clearable size="small" />
                </n-form-item>
              </n-gi>
            </n-grid>
            <n-space style="margin-top: 16px">
              <n-button type="primary" :loading="loading" @click="pickByCondition">
                🔍 开始选股
              </n-button>
              <n-button @click="resetCondition">重置</n-button>
            </n-space>
          </n-form>
        </n-card>
      </n-tab-pane>

      <!-- 策略选股 -->
      <n-tab-pane name="strategy" tab="策略选股">
        <n-card>
          <n-form :model="strategyPick" label-placement="left" label-width="80">
            <n-grid :cols="3" :x-gap="16">
              <n-gi>
                <n-form-item label="选择策略">
                  <n-select v-model:value="strategyPick.strategy_id" :options="strategyOptions" size="small" />
                </n-form-item>
              </n-gi>
              <n-gi>
                <n-form-item label="信号类型">
                  <n-select v-model:value="strategyPick.signal_type" :options="signalOptions" size="small" />
                </n-form-item>
              </n-gi>
            </n-grid>
            <n-space style="margin-top: 16px">
              <n-button type="primary" :loading="loading" @click="pickByStrategy">
                🎯 策略选股
              </n-button>
            </n-space>
          </n-form>
        </n-card>
      </n-tab-pane>
    </n-tabs>

    <!-- 选股结果 -->
    <n-card v-if="results.length > 0" title="选股结果" style="margin-top: 16px">
      <template #header-extra>
        <n-space>
          <n-text>共 {{ results.length }} 只</n-text>
          <n-button type="primary" size="small" @click="batchAddMonitor">
            一键添加监控
          </n-button>
        </n-space>
      </template>
      <n-data-table 
        :columns="resultColumns" 
        :data="results" 
        :row-key="(row: any) => row.code"
        :checked-row-keys="checkedKeys"
        @update:checked-row-keys="handleCheck"
      />
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, computed, h } from 'vue'
import { NButton, NTag } from 'naive-ui'
import axios from 'axios'

const loading = ref(false)
const results = ref<any[]>([])
const checkedKeys = ref<string[]>([])

const condition = ref({
  price_min: null,
  price_max: null,
  change_min: null,
  change_max: null,
  volume_min: null,
  exchange: null,
  industry: null
})

const strategyPick = ref({
  strategy_id: 'ma_cross',
  signal_type: 'buy'
})

const industries = ref<string[]>([])
const strategies = ref<any[]>([])

const exchangeOptions = [
  { label: '上交所', value: 'SH' },
  { label: '深交所', value: 'SZ' },
  { label: '北交所', value: 'BJ' }
]

const industryOptions = computed(() => 
  industries.value.map(i => ({ label: i, value: i }))
)

const strategyOptions = computed(() =>
  strategies.value.map(s => ({ label: s.name, value: s.id }))
)

const signalOptions = [
  { label: '买入信号', value: 'buy' },
  { label: '卖出信号', value: 'sell' }
]

const resultColumns = [
  { type: 'selection' },
  { title: '代码', key: 'code', width: 100 },
  { title: '名称', key: 'name', width: 120 },
  { 
    title: '价格', 
    key: 'price',
    width: 100,
    render: (row: any) => row.price ? `¥${row.price}` : '-'
  },
  { 
    title: '涨跌幅', 
    key: 'change',
    width: 100,
    render: (row: any) => {
      if (row.change === undefined || row.change === null) return '-'
      const color = row.change >= 0 ? '#18a058' : '#d03050'
      return h('span', { style: { color } }, `${row.change >= 0 ? '+' : ''}${row.change}%`)
    }
  },
  { title: '交易所', key: 'exchange', width: 80 },
  {
    title: '操作',
    key: 'actions',
    width: 100,
    render: (row: any) => h(NButton, {
      size: 'small',
      onClick: () => addMonitor(row.code)
    }, { default: () => '监控' })
  }
]

const pickByCondition = async () => {
  loading.value = true
  try {
    const res = await axios.post('/api/picker/condition', condition.value)
    results.value = res.data.results || []
    checkedKeys.value = []
  } catch (e) {
    console.error('选股失败', e)
  } finally {
    loading.value = false
  }
}

const pickByStrategy = async () => {
  loading.value = true
  try {
    const res = await axios.post(
      `/api/picker/strategy?strategy_id=${strategyPick.value.strategy_id}&signal_type=${strategyPick.value.signal_type}`
    )
    results.value = res.data.results || []
    checkedKeys.value = []
  } catch (e) {
    console.error('策略选股失败', e)
  } finally {
    loading.value = false
  }
}

const resetCondition = () => {
  condition.value = {
    price_min: null,
    price_max: null,
    change_min: null,
    change_max: null,
    volume_min: null,
    exchange: null,
    industry: null
  }
  results.value = []
}

const handleCheck = (keys: string[]) => {
  checkedKeys.value = keys
}

const addMonitor = async (code: string) => {
  try {
    await axios.post('/api/monitor/add', { code })
    alert('已添加到监控列表')
  } catch (e) {
    console.error('添加监控失败', e)
  }
}

const batchAddMonitor = async () => {
  if (checkedKeys.value.length === 0) {
    alert('请先选择股票')
    return
  }
  try {
    const res = await axios.post('/api/picker/batch-monitor', checkedKeys.value)
    alert(res.data.message)
  } catch (e) {
    console.error('批量添加失败', e)
  }
}

onMounted(async () => {
  try {
    const [indRes, stratRes] = await Promise.all([
      axios.get('/api/picker/industries'),
      axios.get('/api/strategy/list')
    ])
    industries.value = indRes.data.industries || []
    strategies.value = stratRes.data.strategies || []
  } catch (e) {
    console.error('加载数据失败', e)
  }
})
</script>