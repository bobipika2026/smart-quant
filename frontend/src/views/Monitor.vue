<template>
  <div class="monitor">
    <n-card title="添加监控">
      <n-input-group>
        <n-input 
          v-model:value="newStock" 
          placeholder="输入股票代码，如：000001" 
          style="width: 200px"
          @keyup.enter="addStock"
        />
        <n-button type="primary" :loading="adding" @click="addStock">
          添加监控
        </n-button>
      </n-input-group>
    </n-card>

    <n-card title="监控列表" style="margin-top: 16px">
      <template #header-extra>
        <n-space>
          <n-button size="small" @click="loadMonitors">
            刷新行情
          </n-button>
          <n-button size="small" type="primary" @click="checkSignals">
            检测信号
          </n-button>
        </n-space>
      </template>

      <n-spin :show="loading">
        <n-empty v-if="monitors.length === 0" description="暂无监控股票，添加股票开始监控" />
        <n-data-table v-else :columns="columns" :data="monitors" :bordered="false" />
      </n-spin>
    </n-card>

    <n-card title="最近信号" style="margin-top: 16px">
      <n-spin :show="loadingSignals">
        <n-empty v-if="signals.length === 0" description="暂无交易信号" />
        <n-list v-else bordered>
          <n-list-item v-for="signal in signals" :key="signal.id">
            <n-thing>
              <template #header>
                <n-space align="center">
                  <span>{{ signal.stock_name }} ({{ signal.stock_code }})</span>
                  <n-tag :type="signal.signal_type === 'buy' ? 'success' : 'error'" size="small">
                    {{ signal.signal_type === 'buy' ? '买入' : '卖出' }}
                  </n-tag>
                </n-space>
              </template>
              <template #description>
                <n-space>
                  <span>价格: ¥{{ signal.price }}</span>
                  <span>{{ signal.reason }}</span>
                </n-space>
              </template>
              <template #footer>
                <span style="color: #999; font-size: 12px">{{ signal.created_at }}</span>
              </template>
            </n-thing>
          </n-list-item>
        </n-list>
      </n-spin>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, h } from 'vue'
import { NButton, NTag } from 'naive-ui'
import axios from 'axios'

const newStock = ref('')
const adding = ref(false)
const loading = ref(false)
const loadingSignals = ref(false)
const monitors = ref<any[]>([])
const signals = ref<any[]>([])

const columns = [
  { title: '代码', key: 'code', width: 100 },
  { title: '名称', key: 'name', width: 120 },
  { 
    title: '最新价', 
    key: 'price',
    width: 100,
    render: (row: any) => row.price ? `¥${row.price}` : '-'
  },
  { 
    title: '涨跌幅', 
    key: 'change',
    width: 100,
    render: (row: any) => {
      if (!row.change) return '-'
      const color = row.change >= 0 ? '#18a058' : '#d03050'
      return h('span', { style: { color } }, `${row.change >= 0 ? '+' : ''}${row.change}%`)
    }
  },
  { title: '交易所', key: 'exchange', width: 80 },
  {
    title: '操作',
    key: 'actions',
    width: 80,
    render: (row: any) => h(NButton, {
      size: 'small',
      type: 'error',
      onClick: () => removeStock(row.code)
    }, { default: () => '移除' })
  }
]

const loadMonitors = async () => {
  loading.value = true
  try {
    const res = await axios.get('/api/monitor/list')
    monitors.value = res.data.monitors || []
  } catch (e) {
    console.error('加载监控列表失败', e)
  } finally {
    loading.value = false
  }
}

const addStock = async () => {
  if (!newStock.value.trim()) return
  
  adding.value = true
  try {
    const res = await axios.post('/api/monitor/add', { code: newStock.value.trim() })
    newStock.value = ''
    await loadMonitors()
  } catch (e: any) {
    alert(e.response?.data?.detail || '添加失败')
  } finally {
    adding.value = false
  }
}

const removeStock = async (code: string) => {
  try {
    await axios.post('/api/monitor/remove', { code })
    await loadMonitors()
  } catch (e: any) {
    alert(e.response?.data?.detail || '移除失败')
  }
}

const checkSignals = async () => {
  loadingSignals.value = true
  try {
    const res = await axios.post('/api/monitor/check-signals')
    if (res.data.signals?.length > 0) {
      alert(res.data.message)
    } else {
      alert('暂无新信号')
    }
    await loadSignals()
  } catch (e: any) {
    console.error('检测信号失败', e)
  } finally {
    loadingSignals.value = false
  }
}

const loadSignals = async () => {
  loadingSignals.value = true
  try {
    const res = await axios.get('/api/monitor/signals')
    signals.value = res.data.signals || []
  } catch (e) {
    console.error('加载信号失败', e)
  } finally {
    loadingSignals.value = false
  }
}

onMounted(() => {
  loadMonitors()
  loadSignals()
})
</script>