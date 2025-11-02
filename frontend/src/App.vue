<template>
  <div id="app">
    <!-- 顶部导航栏 -->
    <el-header class="app-header">
      <div class="header-content">
        <div class="logo">
          <el-icon><TrendCharts /></el-icon>
          <span class="title">股票分析系统</span>
          <el-tag type="success" size="small">Vue + Flask</el-tag>
        </div>
        <div class="header-info">
          <el-text type="info">真正的模块化 · 高性能 · 现代化</el-text>
        </div>
        <div class="header-actions">
          <el-dropdown trigger="click" @command="handleManualUpdate">
            <span class="settings-trigger" :class="{ 'is-updating': isAnyUpdating }">
              <el-icon v-if="!isAnyUpdating"><Setting /></el-icon>
              <el-icon v-else class="loading-icon"><Loading /></el-icon>
            </span>
            <template #dropdown>
              <el-dropdown-menu class="manual-update-menu">
                <el-dropdown-item
                  v-for="option in manualUpdateOptions"
                  :key="option.type"
                  :command="option.type"
                  :disabled="updatingStatus[option.type]"
                >
                  <div class="menu-item-content">
                    <el-icon class="menu-item-icon">
                      <component :is="option.icon" />
                    </el-icon>
                    <span class="menu-item-label">{{ option.label }}</span>
                    <el-icon
                      v-if="updatingStatus[option.type]"
                      class="menu-item-loading"
                    >
                      <Loading />
                    </el-icon>
                  </div>
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>
      </div>
    </el-header>

    <!-- 主要内容区域 -->
    <el-container class="app-container">
      <!-- 侧边栏导航 -->
      <el-aside width="250px" class="app-sidebar">
        <el-menu
          :default-active="$route.path"
          :default-openeds="defaultOpeneds"
          router
          class="sidebar-menu"
          background-color="#304156"
          text-color="#bfcbd9"
          active-text-color="#ffffff"
        >
          <el-menu-item index="/index-analysis">
            <el-icon><TrendCharts /></el-icon>
            <span>指数分析</span>
          </el-menu-item>

          <el-menu-item index="/sentiment">
            <el-icon><Odometer /></el-icon>
            <span>市场情绪</span>
          </el-menu-item>

          <el-menu-item index="/sectors">
            <el-icon><Grid /></el-icon>
            <span>板块分析</span>
          </el-menu-item>



          <!-- 强势股分析子菜单 -->
          <el-sub-menu index="strong-stocks">
            <template #title>
              <el-icon><TrendCharts /></el-icon>
              <span>强势股分析</span>
            </template>
            <el-menu-item index="/strong-stocks/new-high">
              <el-icon><TrendCharts /></el-icon>
              <span>新高股票</span>
            </el-menu-item>
            <el-menu-item index="/strong-stocks/heima">
              <el-icon><Lightning /></el-icon>
              <span>黑马分析</span>
            </el-menu-item>
            <el-menu-item index="/strong-stocks/baima">
              <el-icon><Star /></el-icon>
              <span>白马分析</span>
            </el-menu-item>
            <el-menu-item index="/strong-stocks/money-effect">
              <el-icon><Money /></el-icon>
              <span>赚亏钱效应</span>
            </el-menu-item>
          </el-sub-menu>

          <!-- 资金管理（紧跟强势股分析后面） -->
          <el-menu-item index="/funds-management">
            <el-icon><Money /></el-icon>
            <span>资金管理</span>
          </el-menu-item>
        </el-menu>
      </el-aside>

      <!-- 主内容区域 -->
      <el-main class="app-main">
        <router-view />
      </el-main>
    </el-container>

    <!-- 全局加载遮罩 -->
    <el-loading
      v-loading="globalLoading"
      element-loading-text="正在加载数据..."
      element-loading-background="rgba(0, 0, 0, 0.8)"
      body-style="{ overflow: 'hidden' }"
    />
  </div>
</template>

<script>
import { ref, computed, reactive } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import ApiService from './services/api'

export default {
  name: 'App',
  setup() {
    const globalLoading = ref(false)
    const route = useRoute()

    const manualUpdateOptions = [
      { type: 'stocks', label: '股票更新', icon: 'TrendCharts' },
      { type: 'sectors', label: '板块更新', icon: 'Grid' },
      { type: 'indices', label: '指数更新', icon: 'Histogram' },
      { type: 'market-states', label: '股票状态数据更新', icon: 'DataAnalysis' }
    ]

    const updatingStatus = reactive({
      stocks: false,
      sectors: false,
      indices: false,
      'market-states': false
    })

    const isAnyUpdating = computed(() => manualUpdateOptions.some(option => updatingStatus[option.type]))

    const handleManualUpdate = async (type) => {
      const optionExists = manualUpdateOptions.some(option => option.type === type)
      if (!optionExists) {
        ElMessage.warning('未知的更新类型')
        return
      }

      if (updatingStatus[type]) {
        return
      }

      updatingStatus[type] = true
      try {
        const response = await ApiService.triggerManualUpdate(type)
        if (response?.success) {
          ElMessage.success(response.message || '更新成功')
        } else {
          ElMessage.warning(response?.message || '未返回更新结果')
        }
      } catch (error) {
        console.error('手动更新失败:', error)
        if (!error || !error.message) {
          ElMessage.error('更新失败，请稍后重试')
        }
      } finally {
        updatingStatus[type] = false
      }
    }

    // 计算默认展开的子菜单
    const defaultOpeneds = computed(() => {
      const path = route.path
      const openeds = []

      // 如果是强势股分析的子页面，展开强势股分析菜单
      if (path.startsWith('/strong-stocks/')) {
        openeds.push('strong-stocks')
      }

      return openeds
    })

    return {
      globalLoading,
      defaultOpeneds,
      manualUpdateOptions,
      updatingStatus,
      isAnyUpdating,
      handleManualUpdate
    }
  }
}
</script>

<style>
/* 全局样式 */
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

#app {
  font-family: 'Helvetica Neue', Helvetica, 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', '微软雅黑', Arial, sans-serif;
  height: 100vh;
  display: flex;
  flex-direction: column;
}

/* 顶部导航栏 */
.app-header {
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  padding: 0 20px;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
}

.header-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
  height: 100%;
}

.logo {
  display: flex;
  align-items: center;
  gap: 10px;
}

.logo .title {
  font-size: 20px;
  font-weight: bold;
}

.header-info {
  font-size: 14px;
  opacity: 0.9;
  flex: 1;
  text-align: center;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 12px;
}

.settings-trigger {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  border-radius: 50%;
  cursor: pointer;
  transition: background-color 0.2s ease, transform 0.2s ease;
  color: #ffffff;
}

.settings-trigger.is-updating {
  cursor: wait;
  background-color: rgba(255, 255, 255, 0.12);
}

.settings-trigger:hover {
  background-color: rgba(255, 255, 255, 0.15);
  transform: rotate(15deg);
}

.settings-trigger.is-updating:hover {
  transform: none;
}

.settings-trigger .el-icon {
  font-size: 20px;
}

.settings-trigger .loading-icon {
  animation: manual-spin 1s linear infinite;
  font-size: 20px;
}

.manual-update-menu {
  min-width: 220px;
}

.menu-item-content {
  display: flex;
  align-items: center;
  gap: 10px;
  width: 100%;
}

.menu-item-icon {
  color: #409EFF;
}

.menu-item-label {
  flex: 1;
}

.menu-item-loading {
  animation: manual-spin 1s linear infinite;
  color: #409EFF;
}

@keyframes manual-spin {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}

/* 主容器 */
.app-container {
  flex: 1;
  height: calc(100vh - 60px);
}

/* 侧边栏 */
.app-sidebar {
  background-color: #304156;
  box-shadow: 2px 0 6px rgba(0, 21, 41, 0.35);
}

.sidebar-menu {
  border: none;
  height: 100%;
}

.sidebar-menu .el-menu-item {
  height: 50px;
  line-height: 50px;
}

.sidebar-menu .el-menu-item:hover {
  background-color: #263445 !important;
}

.sidebar-menu .el-menu-item.is-active {
  background-color: #409EFF !important;
  color: #ffffff !important;
}

.sidebar-menu .el-menu-item.is-active span {
  color: #ffffff !important;
}

.sidebar-menu .el-menu-item.is-active .el-icon {
  color: #ffffff !important;
}

/* 子菜单激活状态样式 */
.sidebar-menu .el-sub-menu .el-menu-item.is-active {
  background-color: #409EFF !important;
  color: #ffffff !important;
}

.sidebar-menu .el-sub-menu .el-menu-item.is-active span {
  color: #ffffff !important;
}

.sidebar-menu .el-sub-menu .el-menu-item.is-active .el-icon {
  color: #ffffff !important;
}

/* 子菜单悬停效果 */
.sidebar-menu .el-sub-menu .el-menu-item:hover {
  background-color: #337ecc !important;
  color: #ffffff !important;
}

/* 主内容区域 */
.app-main {
  background-color: #f0f2f5;
  padding: 20px;
  overflow-y: auto;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .app-sidebar {
    width: 200px !important;
  }
  
  .logo .title {
    display: none;
  }
  
  .header-info {
    display: none;
  }
}

/* 滚动条样式 */
::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}

::-webkit-scrollbar-track {
  background: #f1f1f1;
}

::-webkit-scrollbar-thumb {
  background: #c1c1c1;
  border-radius: 3px;
}

::-webkit-scrollbar-thumb:hover {
  background: #a8a8a8;
}

/* 动画效果 */
.fade-enter-active, .fade-leave-active {
  transition: opacity 0.3s;
}

.fade-enter-from, .fade-leave-to {
  opacity: 0;
}

/* Element Plus 组件样式覆盖 */
.el-card {
  border-radius: 8px;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
}

.el-button {
  border-radius: 6px;
}

.el-table {
  border-radius: 8px;
  overflow: hidden;
}

.el-input {
  border-radius: 6px;
}

.el-select {
  border-radius: 6px;
}
</style>
