<template>
  <div class="monitor">
    <n-card title="监控配置">
      <n-alert type="info" style="margin-bottom: 16px">
        从回测页面添加股票+策略组合，系统将监控特定策略的信号。
      </n-alert>
    </n-card>

    <n-card title="监控列表" style="margin-top: 16px">
      <template #header-extra>
        <n-button size="small" @click="loadMonitors">
          刷新行情
        </n-button>
      </template>

      <n-spin :show="loading">
        <n-empty v-if="monitors.length === 0" description="暂无监控配置，请从回测页面添加" />
        <n-data-table v-else :columns="columns" :data="monitors" :bordered="false" />
      </n-spin>
    </n-card>

    <n-card title="最近信号" style="margin-top: 16px">
      <template #header-extra>
        <n-button type="primary" size="small" @click="checkSignals" :loading="checkingSignals">
          检测信号
        </n-button>
      </template>
      
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

const loading = ref(false)
const loadingSignals = ref(false)
const checkingSignals = ref(false)
const monitors = ref<any[]>([])
const signals = ref<any[]>([])

const columns = [
  { title: '股票代码', key: 'stock_code', width: 100 },
  { title: '股票名称', key: 'stock_name', width: 120 },
  { title: '策略', key: 'strategy_name', width: 100 },
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
  {
    title: '操作',
    key: 'actions',
    width: 80,
    render: (row: any) => h(NButton, {
      size: 'small',
      type: 'error',
      onClick: () => removeConfig(row.id)
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

const removeConfig = async (id: number) => {
  try {
    await axios.post(`/api/monitor/remove-config/${id}`)
    await loadMonitors()
  } catch (e: any) {
    alert(e.response?.data?.detail || '移除失败')
  }
}

const checkSignals = async () => {
  checkingSignals.value = true
  try {
    const res = await axios.post('/api/monitor/check-signals')
    await loadSignals()
    if (res.data.signals?.length > 0) {
      alert(res.data.message)
    } else {
      alert('暂无新信号')
    }
  } catch (e: any) {
    console.error('检测信号失败', e)
  } finally {
    checkingSignals.value = false
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