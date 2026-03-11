<template>
  <n-config-provider>
    <n-message-provider>
      <n-layout has-sider>
        <n-layout-sider
          bordered
          collapse-mode="width"
          :collapsed-width="64"
          :width="200"
          :collapsed="collapsed"
          show-trigger
          @collapse="collapsed = true"
          @expand="collapsed = false"
        >
          <div class="logo">
            <span v-if="!collapsed">📊 Smart Quant</span>
            <span v-else>📊</span>
          </div>
          <n-menu
            :collapsed="collapsed"
            :collapsed-width="64"
            :collapsed-icon-size="22"
            :options="menuOptions"
            @update:value="handleMenuClick"
          />
        </n-layout-sider>
        <n-layout>
          <n-layout-header bordered class="header">
            <h2>{{ pageTitle }}</h2>
          </n-layout-header>
          <n-layout-content class="content">
            <router-view />
          </n-layout-content>
        </n-layout>
      </n-layout>
    </n-message-provider>
  </n-config-provider>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import type { MenuOption } from 'naive-ui'

const router = useRouter()
const route = useRoute()
const collapsed = ref(false)

const menuOptions: MenuOption[] = [
  {
    label: '首页',
    key: 'home',
    icon: () => '🏠'
  },
  {
    label: '策略管理',
    key: 'strategy',
    icon: () => '📈'
  },
  {
    label: '回测中心',
    key: 'backtest',
    icon: () => '🔄'
  },
  {
    label: '选股系统',
    key: 'stock-picker',
    icon: () => '🔍'
  },
  {
    label: '实时监控',
    key: 'monitor',
    icon: () => '📡'
  }
]

const pageTitle = computed(() => {
  const titles: Record<string, string> = {
    home: '首页概览',
    strategy: '策略管理',
    backtest: '回测中心',
    'stock-picker': '选股系统',
    monitor: '实时监控'
  }
  return titles[route.name as string] || 'Smart Quant'
})

const handleMenuClick = (key: string) => {
  router.push({ name: key })
}
</script>

<style scoped>
.logo {
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 18px;
  font-weight: bold;
  border-bottom: 1px solid #e0e0e0;
}

.header {
  height: 60px;
  padding: 0 24px;
  display: flex;
  align-items: center;
}

.content {
  padding: 24px;
  min-height: calc(100vh - 60px);
  background: #f5f7f9;
}
</style>