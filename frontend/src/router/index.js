import { createRouter, createWebHistory } from 'vue-router'

import MarketSentiment from '../views/MarketSentiment.vue'
import NewHighStocks from '../views/NewHighStocks.vue'
import HeimaAnalysis from '../views/HeimaAnalysis.vue'
import BaimaAnalysis from '../views/BaimaAnalysis.vue'
import MoneyEffect from '../views/MoneyEffect.vue'
import SectorAnalysis from '../views/SectorAnalysis.vue'
import IndexAnalysis from '../views/IndexAnalysis.vue'
import FundsManagementPage from '../views/FundsManagementPage.vue'
import StrategyWatch from '../views/StrategyWatch.vue'

const routes = [
  {
    path: '/',
    redirect: '/index-analysis'
  },
  {
    path: '/sentiment',
    name: 'MarketSentiment',
    component: MarketSentiment,
    meta: {
      title: '市场情绪',
      icon: 'Odometer'
    }
  },
  {
    path: '/sectors',
    name: 'SectorAnalysis',
    component: SectorAnalysis,
    meta: {
      title: '板块分析',
      icon: 'Grid'
    }
  },
  {
    path: '/strong-stocks',
    redirect: '/strong-stocks/new-high',
    meta: {
      title: '强势股分析',
      icon: 'TrendCharts',
      isParent: true
    }
  },
  {
    path: '/strong-stocks/new-high',
    name: 'NewHighStocks',
    component: NewHighStocks,
    meta: {
      title: '新高股票',
      icon: 'TrendCharts',
      parent: 'strong-stocks'
    }
  },
  {
    path: '/strong-stocks/heima',
    name: 'HeimaAnalysis',
    component: HeimaAnalysis,
    meta: {
      title: '黑马分析',
      icon: 'Lightning',
      parent: 'strong-stocks'
    }
  },
  {
    path: '/strong-stocks/baima',
    name: 'BaimaAnalysis',
    component: BaimaAnalysis,
    meta: {
      title: '白马分析',
      icon: 'Star',
      parent: 'strong-stocks'
    }
  },
  {
    path: '/strong-stocks/money-effect',
    name: 'MoneyEffect',
    component: MoneyEffect,
    meta: {
      title: '赚钱效应',
      icon: 'Money',
      parent: 'strong-stocks'
    }
  },
  {
    path: '/index-analysis',
    name: 'IndexAnalysis',
    component: IndexAnalysis,
    meta: {
      title: '指数分析',
      icon: 'TrendCharts'
    }
  },
  {
    path: '/funds-management',
    name: 'FundsManagementPage',
    component: FundsManagementPage,
    meta: {
      title: '资金管理',
      icon: 'Money'
    }
  },
  {
    path: '/strategy-watch',
    name: 'StrategyWatch',
    component: StrategyWatch,
    meta: {
      title: '策略看盘',
      icon: 'ChatDotRound'
    }
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

router.beforeEach((to, from, next) => {
  if (to.meta.title) {
    document.title = `${to.meta.title} - 股票分析系统`
  }
  console.log(`路由切换: ${from.path} -> ${to.path}`)
  next()
})

export default router
