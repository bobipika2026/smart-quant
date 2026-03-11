<template>
  <div class="stock-picker">
    <n-card title="股票搜索">
      <n-input
        v-model:value="keyword"
        placeholder="输入股票代码或名称搜索..."
        @keyup.enter="searchStock"
      >
        <template #prefix>🔍</template>
      </n-input>
      <n-button type="primary" style="margin-top: 12px" @click="searchStock">
        搜索
      </n-button>
    </n-card>

    <n-card v-if="stocks.length > 0" title="搜索结果" style="margin-top: 16px">
      <n-data-table :columns="columns" :data="stocks" />
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import axios from 'axios'

const keyword = ref('')
const stocks = ref<any[]>([])

const columns = [
  { title: '代码', key: '代码' },
  { title: '名称', key: '名称' },
  { title: '最新价', key: '最新价' },
  { title: '涨跌幅', key: '涨跌幅' },
  { title: '成交量', key: '成交量' }
]

const searchStock = async () => {
  if (!keyword.value) return
  try {
    const res = await axios.get(`/api/stock/search?keyword=${keyword.value}`)
    stocks.value = res.data.stocks || []
  } catch (e) {
    console.error('搜索失败')
  }
}
</script>