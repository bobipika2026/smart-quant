<template>
  <n-config-provider>
    <n-message-provider>
      <n-layout has-sider>
        <n-layout-sider
          bordered
          collapse-mode="width"
          :collapsed-width="64"
          :width="200"
          :collapsed="isCollapsed"
          show-trigger
          @collapse="isCollapsed = true"
          @expand="isCollapsed = false"
          :mobile="isMobile"
          :native-scrollbar="false"
        >
          <div class="logo">
            <span v-if="!isCollapsed">📊 Smart Quant</span>
            <span v-else>📊</span>
          </div>
          <n-menu
            :collapsed="isCollapsed"
            :collapsed-width="64"
            :collapsed-icon-size="22"
            :options="menuOptions"
            :value="currentMenu"
            @update:value="handleMenuClick"
          />
        </n-layout-sider>
        <n-layout>
          <n-layout-header bordered class="header">
            <n-button 
              v-if="isMobile" 
              quaternary 
              circle 
              @click="isCollapsed = !isCollapsed"
              style="margin-right: 12px"
            >
              <template #icon>☰</template>
            </n-button>
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
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import type { MenuOption } from 'naive-ui'

const router = useRouter()
const route = useRoute()
const isCollapsed = ref(false)
const windowWidth = ref(window.innerWidth)

const isMobile = computed(() => windowWidth.value < 768)
const isTablet = computed(() => windowWidth.value >= 768 && windowWidth.value < 1024)

// 监听窗口大小变化
const handleResize = () => {
  windowWidth.value = window.innerWidth
  // 手机端默认折叠侧边栏
  if (isMobile.value) {
    isCollapsed.value = true
  }
}

onMounted(() => {
  window.addEventListener('resize', handleResize)
  handleResize() // 初始化
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
})

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
    label: '策略说明',
    key: 'strategy-guide',
    icon: () => '📚'
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
  },
  {
    label: '因子矩阵',
    key: 'factor-matrix',
    icon: () => '📊'
  }
]

const currentMenu = computed(() => route.name as string)

const pageTitle = computed(() => {
  const titles: Record<string, string> = {
    home: '首页概览',
    strategy: '策略管理',
    'strategy-guide': '策略说明',
    backtest: '回测中心',
    'stock-picker': '选股系统',
    monitor: '实时监控',
    'factor-matrix': '因子矩阵'
  }
  return titles[route.name as string] || 'Smart Quant'
})

const handleMenuClick = (key: string) => {
  router.push({ name: key })
  // 手机端点击菜单后自动折叠
  if (isMobile.value) {
    isCollapsed.value = true
  }
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

.header h2 {
  margin: 0;
  font-size: 18px;
}

.content {
  padding: 16px;
  min-height: calc(100vh - 60px);
  background: #f5f7f9;
}

/* 平板适配 */
@media (max-width: 1024px) {
  .content {
    padding: 12px;
  }
}

/* 手机适配 */
@media (max-width: 768px) {
  .content {
    padding: 8px;
  }
  
  .header h2 {
    font-size: 16px;
  }
}
</style>