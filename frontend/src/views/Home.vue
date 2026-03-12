<template>
  <div class="home">
    <!-- 统计卡片 - 响应式网格 -->
    <n-grid :cols="gridCols" :x-gap="16" :y-gap="16">
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
      <n-space :size="[8, 8]" wrap>
        <n-button type="primary" @click="$router.push('/strategy')">
          📈 管理策略
        </n-button>
        <n-button type="info" @click="$router.push('/backtest')">
          🔄 运行回测
        </n-button>
        <n-button type="success" @click="$router.push('/monitor')">
          📡 开始监控
        </n-button>
        <n-button @click="$router.push('/stock-picker')">
          🔍 智能选股
        </n-button>
      </n-space>
    </n-card>

    <n-card title="系统状态" style="margin-top: 16px">
      <n-descriptions :column="descCols" bordered>
        <n-descriptions-item label="后端状态">
          <n-tag type="success">运行中</n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="数据源">
          <n-tag type="info">新浪财经</n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="版本">
          v0.4.0
        </n-descriptions-item>
        <n-descriptions-item label="股票数据">
          5190只
        </n-descriptions-item>
      </n-descriptions>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, computed, onUnmounted } from 'vue'
import axios from 'axios'

const windowWidth = ref(window.innerWidth)

const gridCols = computed(() => {
  if (windowWidth.value < 576) return 1  // 手机
  if (windowWidth.value < 768) return 2  // 大手机
  if (windowWidth.value < 1024) return 2 // 平板
  return 4  // 桌面
})

const descCols = computed(() => {
  if (windowWidth.value < 576) return 1
  if (windowWidth.value < 768) return 2
  return 3
})

const stats = ref({
  strategies: 5,
  monitoring: 0,
  signals: 0,
  backtests: 0
})

const handleResize = () => {
  windowWidth.value = window.innerWidth
}

onMounted(async () => {
  window.addEventListener('resize', handleResize)
  try {
    const [stratRes, monRes, sigRes] = await Promise.all([
      axios.get('/api/strategy/list'),
      axios.get('/api/monitor/list'),
      axios.get('/api/monitor/signals')
    ])
    stats.value.strategies = stratRes.data.strategies?.length || 5
    stats.value.monitoring = monRes.data.monitors?.length || 0
    stats.value.signals = sigRes.data.signals?.length || 0
  } catch (e) {
    console.log('API not ready')
  }
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
})
</script>

<style scoped>
.home {
  max-width: 1200px;
  margin: 0 auto;
}

/* 手机适配 */
@media (max-width: 576px) {
  :deep(.n-card) {
    font-size: 14px;
  }
  
  :deep(.n-statistic) {
    text-align: center;
  }
}
</style>