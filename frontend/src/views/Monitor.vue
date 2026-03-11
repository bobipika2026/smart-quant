<template>
  <div class="monitor">
    <n-card title="实时监控">
      <n-alert type="info" style="margin-bottom: 16px">
        添加股票到监控列表，系统将实时监控并推送交易信号。
      </n-alert>

      <n-input-group>
        <n-input v-model:value="newStock" placeholder="输入股票代码" style="width: 200px" />
        <n-button type="primary" @click="addStock">添加监控</n-button>
      </n-input-group>
    </n-card>

    <n-card title="监控列表" style="margin-top: 16px">
      <n-empty v-if="monitorList.length === 0" description="暂无监控股票" />
      <n-list v-else bordered>
        <n-list-item v-for="stock in monitorList" :key="stock.code">
          <template #prefix>
            <n-tag type="success">监控中</n-tag>
          </template>
          <n-thing :title="`${stock.name} (${stock.code})`">
            <template #description>
              最新价: {{ stock.price || '-' }} | 涨跌幅: {{ stock.change || '-' }}%
            </template>
          </n-thing>
          <template #suffix>
            <n-button size="small" type="error" @click="removeStock(stock.code)">
              移除
            </n-button>
          </template>
        </n-list-item>
      </n-list>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'

const newStock = ref('')
const monitorList = ref<any[]>([])

const addStock = () => {
  if (!newStock.value) return
  monitorList.value.push({
    code: newStock.value,
    name: '加载中...',
    price: null,
    change: null
  })
  newStock.value = ''
}

const removeStock = (code: string) => {
  monitorList.value = monitorList.value.filter(s => s.code !== code)
}
</script>