<template>
  <div id="app">
    <el-header class="app-header">
      <div class="header-content">
        <div class="logo">
          <el-icon><TrendCharts /></el-icon>
          <span class="title">股票分析系统</span>
          <el-tag type="success" size="small">Vue + Flask</el-tag>
        </div>
        <div class="header-info">股票是一种艺术</div>
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
                    <el-icon v-if="updatingStatus[option.type]" class="menu-item-loading">
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

    <el-container class="app-container">
      <el-aside :width="isSidebarCollapsed ? '64px' : '250px'" class="app-sidebar">
        <el-menu
          :default-active="$route.path"
          :default-openeds="defaultOpeneds"
          :collapse="isSidebarCollapsed"
          :collapse-transition="false"
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
              <span>赚钱效应</span>
            </el-menu-item>
          </el-sub-menu>

          <el-menu-item index="/funds-management">
            <el-icon><Money /></el-icon>
            <span>资金管理</span>
          </el-menu-item>

          <el-menu-item index="/strategy-watch">
            <el-icon><ChatDotRound /></el-icon>
            <span>策略看盘</span>
          </el-menu-item>
        </el-menu>
        <el-button class="sidebar-float-toggle" circle @click="toggleSidebar">
          <el-icon>
            <ArrowRight v-if="isSidebarCollapsed" />
            <ArrowLeft v-else />
          </el-icon>
        </el-button>
      </el-aside>

      <el-main class="app-main" :class="{ 'app-main-no-scroll': route.path === '/strategy-watch' }">
        <router-view />
      </el-main>
    </el-container>
  </div>
</template>

<script>
import { computed, reactive, ref } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import ApiService from './services/api'

export default {
  name: 'App',
  setup() {
    const route = useRoute()

    const manualUpdateOptions = [
      { type: 'stocks', label: '股票更新', icon: 'TrendCharts' },
      { type: 'sectors', label: '板块更新', icon: 'Grid' },
      { type: 'indices', label: '指数更新', icon: 'Histogram' },
      { type: 'market-states', label: '股票状态更新', icon: 'DataAnalysis' },
      { type: 'market-metadata', label: '市场元数据更新', icon: 'DataAnalysis' }
    ]

    const updatingStatus = reactive({
      stocks: false,
      sectors: false,
      indices: false,
      'market-states': false,
      'market-metadata': false
    })

    const isAnyUpdating = computed(() => {
      return manualUpdateOptions.some(option => updatingStatus[option.type])
    })

    const defaultOpeneds = computed(() => {
      const path = route.path
      const openeds = []
      if (path.startsWith('/strong-stocks/')) {
        openeds.push('strong-stocks')
      }
      return openeds
    })

    const handleManualUpdate = async (type) => {
      if (!manualUpdateOptions.some(option => option.type === type)) {
        ElMessage.warning('未知更新类型')
        return
      }
      if (updatingStatus[type]) return

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
      } finally {
        updatingStatus[type] = false
      }
    }

    const isSidebarCollapsed = ref(false)

    const toggleSidebar = () => {
      isSidebarCollapsed.value = !isSidebarCollapsed.value
    }

    return {
      route,
      manualUpdateOptions,
      updatingStatus,
      isAnyUpdating,
      defaultOpeneds,
      handleManualUpdate,
      isSidebarCollapsed,
      toggleSidebar
    }
  }
}
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

#app {
  font-family: 'Helvetica Neue', Helvetica, 'PingFang SC', 'Microsoft YaHei', Arial, sans-serif;
  height: 100vh;
  display: flex;
  flex-direction: column;
}

.app-header {
  background: linear-gradient(135deg, #0e4f8d 0%, #2872a3 45%, #56a38d 100%);
  color: white;
  padding: 0 18px;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.15);
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
  font-size: 19px;
  font-weight: 700;
}

.header-info {
  font-size: 13px;
  opacity: 0.9;
  flex: 1;
  text-align: center;
}

.header-actions {
  display: flex;
  align-items: center;
}

.settings-trigger {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  border-radius: 50%;
  cursor: pointer;
  color: #fff;
  transition: all 0.2s ease;
}

.settings-trigger:hover {
  background-color: rgba(255, 255, 255, 0.16);
}

.settings-trigger.is-updating {
  cursor: wait;
  background-color: rgba(255, 255, 255, 0.14);
}

.settings-trigger .el-icon {
  font-size: 20px;
}

.loading-icon {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
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

.menu-item-label {
  flex: 1;
}

.menu-item-loading {
  color: #409eff;
  animation: spin 1s linear infinite;
}

.app-container {
  flex: 1;
  height: calc(100vh - 60px);
}

.app-sidebar {
  background-color: #304156;
  box-shadow: 2px 0 6px rgba(0, 21, 41, 0.35);
  position: relative;
}

.sidebar-float-toggle.el-button {
  position: absolute;
  top: 50%;
  right: -14px;
  transform: translateY(-50%);
  width: 28px;
  height: 28px;
  padding: 0;
  border: 1px solid rgba(255, 255, 255, 0.25);
  background-color: #8ec5ff;
  color: #0f2f4a;
  box-shadow: 0 4px 10px rgba(0, 0, 0, 0.25);
}

.sidebar-float-toggle.el-button:hover {
  background-color: #a7d4ff;
  border-color: rgba(255, 255, 255, 0.35);
  color: #ffffff;
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
  background-color: #2f8cb7 !important;
  color: #fff !important;
}

.sidebar-menu .el-sub-menu .el-menu-item.is-active {
  background-color: #2f8cb7 !important;
  color: #fff !important;
}

.app-main {
  background-color: #f0f2f5;
  padding: 20px;
  overflow-y: auto;
}

.app-main.app-main-no-scroll {
  overflow: hidden;
}

@media (max-width: 768px) {
  .app-sidebar {
    width: 210px !important;
  }

  .logo .title {
    display: none;
  }

  .header-info {
    display: none;
  }
}

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
</style>
