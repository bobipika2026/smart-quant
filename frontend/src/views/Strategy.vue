<template>
  <div class="strategy">
    <n-card title="可用策略列表">
      <n-data-table
        :columns="columns"
        :data="strategies"
        :loading="loading"
      />
    </n-card>

    <n-card title="策略说明" style="margin-top: 16px">
      <n-alert type="info">
        选择一个策略，配置参数后即可运行回测验证效果。
      </n-alert>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, h } from 'vue'
import { NButton, NTag } from 'naive-ui'
import axios from 'axios'

const loading = ref(false)
const strategies = ref<any[]>([])

const columns = [
  {
    title: '策略ID',
    key: 'id'
  },
  {
    title: '策略名称',
    key: 'name'
  },
  {
    title: '类型',
    key: 'type',
    render: (row: any) => h(NTag, { type: 'info' }, { default: () => row.id })
  },
  {
    title: '操作',
    key: 'actions',
    render: (row: any) => h(NButton, {
      type: 'primary',
      size: 'small',
      onClick: () => handleBacktest(row)
    }, { default: () => '运行回测' })
  }
]

const handleBacktest = (strategy: any) => {
  // 跳转到回测页面
  window.location.href = `/backtest?strategy=${strategy.id}`
}

onMounted(async () => {
  loading.value = true
  try {
    const res = await axios.get('/api/strategy/list')
    strategies.value = res.data.strategies || []
  } catch (e) {
    console.error('获取策略列表失败', e)
  } finally {
    loading.value = false
  }
})
</script>