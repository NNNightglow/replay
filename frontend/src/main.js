import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'
import * as echarts from 'echarts'

// 将ECharts挂载到全局window对象
window.echarts = echarts

// 处理ResizeObserver错误
const debounce = (fn, delay) => {
  let timeoutId
  return (...args) => {
    clearTimeout(timeoutId)
    timeoutId = setTimeout(() => fn.apply(null, args), delay)
  }
}

const _ResizeObserver = window.ResizeObserver
window.ResizeObserver = class ResizeObserver extends _ResizeObserver {
  constructor(callback) {
    callback = debounce(callback, 20)
    super(callback)
  }
}

// 全局错误处理
window.addEventListener('error', (e) => {
  if (e.message === 'ResizeObserver loop completed with undelivered notifications.') {
    const resizeObserverErrDiv = document.getElementById('webpack-dev-server-client-overlay-div')
    const resizeObserverErr = document.getElementById('webpack-dev-server-client-overlay')
    if (resizeObserverErr) {
      resizeObserverErr.setAttribute('style', 'display: none')
    }
    if (resizeObserverErrDiv) {
      resizeObserverErrDiv.setAttribute('style', 'display: none')
    }
  }
})

// 创建Vue应用
const app = createApp(App)

// 注册Element Plus图标
for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
  app.component(key, component)
}

// 使用插件
app.use(router)
app.use(ElementPlus)

// 挂载应用
app.mount('#app')

console.log('🚀 股票分析系统前端启动成功')
console.log('📊 Vue.js + Element Plus + ECharts')
