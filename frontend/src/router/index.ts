import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    name: 'home',
    component: () => import('@/views/Home.vue')
  },
  {
    path: '/strategy',
    name: 'strategy',
    component: () => import('@/views/Strategy.vue')
  },
  {
    path: '/strategy-guide',
    name: 'strategy-guide',
    component: () => import('@/views/StrategyGuide.vue')
  },
  {
    path: '/backtest',
    name: 'backtest',
    component: () => import('@/views/Backtest.vue')
  },
  {
    path: '/stock-picker',
    name: 'stock-picker',
    component: () => import('@/views/StockPicker.vue')
  },
  {
    path: '/monitor',
    name: 'monitor',
    component: () => import('@/views/Monitor.vue')
  },
  {
    path: '/factor-matrix',
    name: 'factor-matrix',
    component: () => import('@/views/FactorMatrix.vue')
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router