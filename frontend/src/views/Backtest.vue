<template>
  <div class="backtest">
    <n-card title="回测配置">
      <n-form :model="form" label-placement="left" label-width="100">
        <n-grid :cols="2" :x-gap="24">
          <n-gi>
            <n-form-item label="股票代码">
              <n-input v-model:value="form.stockCode" placeholder="输入股票代码，如：000001" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="选择策略">
              <n-select v-model:value="form.strategyId" :options="strategyOptions" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="开始日期">
              <n-date-picker v-model:value="form.startDate" type="date" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="结束日期">
              <n-date-picker v-model:value="form.endDate" type="date" />
            </n-form-item>
          </n-gi>
          <n-gi>
            <n-form-item label="初始资金">
              <n-input-number v-model:value="form.initialCapital" :min="10000" :step="10000">
                <template #prefix>¥</template>
              </n-input-number>
            </n-form-item>
          </n-gi>
        </n-grid>
        <n-space style="margin-top: 16px">
          <n-button type="primary" :loading="running" @click="runBacktest">
            🚀 开始回测
          </n-button>
        </n-space>
      </n-form>
    </n-card>

    <n-card v-if="result" title="回测结果" style="margin-top: 16px">
      <n-descriptions :column="4" bordered>
        <n-descriptions-item label="初始资金">
          ¥{{ result.initial_capital?.toLocaleString() }}
        </n-descriptions-item>
        <n-descriptions-item label="最终市值">
          ¥{{ result.final_value?.toLocaleString() }}
        </n-descriptions-item>
        <n-descriptions-item label="总收益率">
          <n-tag :type="result.total_return >= 0 ? 'success' : 'error'">
            {{ result.total_return }}%
          </n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="年化收益">
          <n-tag :type="result.annual_return >= 0 ? 'success' : 'error'">
            {{ result.annual_return }}%
          </n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="最大回撤">
          <n-tag type="warning">{{ result.max_drawdown }}%</n-tag>
        </n-descriptions-item>
        <n-descriptions-item label="夏普比率">
          {{ result.sharpe_ratio }}
        </n-descriptions-item>
        <n-descriptions-item label="胜率">
          {{ result.win_rate }}%
        </n-descriptions-item>
        <n-descriptions-item label="交易次数">
          {{ result.trade_count }}次
        </n-descriptions-item>
      </n-descriptions>
    </n-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import axios from 'axios'

const running = ref(false)
const result = ref<any>(null)
const strategies = ref<any[]>([])

const form = ref({
  stockCode: '000001',
  strategyId: 'ma_cross',
  startDate: Date.now() - 365 * 24 * 60 * 60 * 1000,
  endDate: Date.now(),
  initialCapital: 100000
})

const strategyOptions = computed(() =>
  strategies.value.map(s => ({ label: s.name, value: s.id }))
)

const runBacktest = async () => {
  running.value = true
  try {
    const res = await axios.post('/api/strategy/backtest', {
      stock_code: form.value.stockCode,
      strategy_id: form.value.strategyId,
      start_date: new Date(form.value.startDate).toISOString().slice(0, 10).replace(/-/g, ''),
      end_date: new Date(form.value.endDate).toISOString().slice(0, 10).replace(/-/g, ''),
      initial_capital: form.value.initialCapital
    })
    result.value = res.data
  } catch (e: any) {
    console.error('回测失败', e)
    alert(e.response?.data?.detail || '回测失败')
  } finally {
    running.value = false
  }
}

onMounted(async () => {
  try {
    const res = await axios.get('/api/strategy/list')
    strategies.value = res.data.strategies || []
  } catch (e) {
    console.error('获取策略列表失败')
  }
})
</script>