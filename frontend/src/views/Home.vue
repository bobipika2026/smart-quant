<template>
  <div class="home">
    <n-grid :cols="4" :x-gap="16" :y-gap="16">
      <n-gi>
        <n-card title="📈 策略数量" hoverable>
          <n-statistic :value="stats.strategies">
            <template #suffix>个</template>
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card title="📡 监控股票" hoverable>
          <n-statistic :value="stats.monitoring">
            <template #suffix>只</template>
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card title="🔔 今日信号" hoverable>
          <n-statistic :value="stats.signals">
            <template #suffix>条</template>
          </n-statistic>
        </n-card>
      </n-gi>
      <n-gi>
        <n-card title="📊 回测次数" hoverable>
          <n-statistic :value="stats.backtests">
            <template #suffix>次</template>
          </n-statistic>
        </n-card>
      </n-gi>
    </n-grid>

    <n-card title="快速操作" style="margin-top: 16px">
      <n-space>
        <n-button type="primary" @click="$router.push('/strategy')">
          📈 管理策略
        </n-button>
        <n-button type="info" @click="$router.push('/backtest')">
          🔄 运行回测
        </n-button>
        <n-button type="success" @click="$router.push('/monitor')">
          📡 开始监控
        </n-button>
      </n-space>
    </n-card>

    <n-card title="系统状态" style="margin-top: 16px">
      <n-descriptions :column="3" bordered>
        <n-descriptions-item label="后端状态">
          <n-tag type="success">运行中</n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="数据源">
          <n-tag type="info">AkShare</n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="版本">
          v0.1.0
        </n-descriptions-item>
      </n-descriptions>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import axios from 'axios'

const stats = ref({
  strategies: 5,
  monitoring: 0,
  signals: 0,
  backtests: 0
})

onMounted(async () => {
  try {
    const res = await axios.get('/api/strategy/list')
    stats.value.strategies = res.data.strategies?.length || 5
  } catch (e) {
    console.log('API not ready')
  }
})
</script>

<style scoped>
.home {
  max-width: 1200px;
}
</style>