<template>
  <div class="baima-analysis">
    <div class="page-header">
      <h1>🐴 白马分析</h1>
      <p>白马股票多时间区间涨跌幅分析，支持排序和筛选</p>
    </div>

    <el-card class="control-panel">
      <el-row :gutter="20" align="middle">
        <el-col :span="6">
          <div class="control-item">
            <label>股票筛选</label>
            <div style="display: flex; flex-wrap: wrap; gap: 10px;">
              <el-checkbox v-model="filterOptions.includeST">包含ST股票</el-checkbox>
              <el-checkbox v-model="filterOptions.includeNonMainBoard">包含非主板股票</el-checkbox>
            </div>
          </div>
        </el-col>
        <el-col :span="4">
          <div class="control-item">
            <label>&nbsp;</label>
            <div style="display: flex; gap: 8px;">
              <el-button size="small" type="primary" @click="showSaveIntervalGroup = true">💾 保存区间组合</el-button>
              <el-button size="small" @click="openLoadIntervalGroup">📂 加载区间组合</el-button>
            </div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="control-item">
            <label>快速时间区间</label>
            <el-select v-model="selectedQuickIntervals" multiple placeholder="选择快速区间" style="width: 100%">
              <el-option label="最近30天" value="30d" />
              <el-option label="最近90天" value="90d" />
              <el-option label="本年度" value="ytd" />
              <el-option label="最近6个月" value="6m" />
            </el-select>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="control-item">
            <label>自定义时间区间</label>
            <el-row :gutter="10">
              <el-col :span="6">
                <el-date-picker
                  v-model="customInterval.startDate"
                  type="date"
                  placeholder="开始日期"
                  format="YYYY-MM-DD"
                  value-format="YYYY-MM-DD"
                  style="width: 100%"
                />
              </el-col>
              <el-col :span="6">
                <el-date-picker
                  v-model="customInterval.endDate"
                  type="date"
                  placeholder="结束日期"
                  format="YYYY-MM-DD"
                  value-format="YYYY-MM-DD"
                  style="width: 100%"
                />
              </el-col>
              <el-col :span="6">
                <el-input
                  v-model="customInterval.name"
                  placeholder="区间名称"
                  style="width: 100%"
                />
              </el-col>
              <el-col :span="6">
                <el-button @click="addCustomInterval" :disabled="!canAddCustomInterval" style="width: 100%">
                  添加区间
                </el-button>
              </el-col>
            </el-row>
          </div>
        </el-col>
        <el-col :span="4">
          <div class="control-item">
            <label>&nbsp;</label>
            <el-button type="primary" @click="analyzeBaima" :loading="loading" style="width: 100%">
              <el-icon><Star /></el-icon>
              分析白马股票
            </el-button>
          </div>
        </el-col>
      </el-row>

      <!-- 显示已添加的时间区间 -->
      <el-row v-if="customIntervals.length > 0" style="margin-top: 15px;">
        <el-col :span="24">
          <div class="control-item">
            <label>已添加的时间区间</label>
            <div style="display: flex; flex-wrap: wrap; gap: 8px;">
              <el-tag
                v-for="(interval, index) in customIntervals"
                :key="index"
                closable
                @close="removeCustomInterval(index)"
                type="info"
              >
                {{ interval.name }} ({{ interval.start_date }} 至 {{ interval.end_date }})
              </el-tag>
            </div>
          </div>
        </el-col>
      </el-row>
    </el-card>

    <!-- 分析结果统计 -->
    <el-card v-if="baimaData.stocks?.length > 0" class="stats-panel">
      <el-row :gutter="20">
        <el-col :span="8">
          <el-statistic title="股票总数" :value="baimaData.total_count || 0" />
        </el-col>
        <el-col :span="8">
          <el-statistic title="时间区间" :value="baimaData.intervals?.length || 0" />
        </el-col>
        <el-col :span="8">
          <el-statistic title="涨跌幅列" :value="baimaData.change_columns?.length || 0" />
        </el-col>
      </el-row>
    </el-card>

    <!-- 股票列表 -->
    <el-card v-if="baimaData.stocks?.length > 0">
      <template #header>
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <span>白马股票列表 ({{ baimaData.stocks.length }}只)</span>
          <el-input
            v-model="searchText"
            placeholder="搜索股票名称或代码"
            style="width: 200px;"
            clearable
          >
            <template #prefix>
              <el-icon><Search /></el-icon>
            </template>
          </el-input>
        </div>
      </template>

      <el-table
        :data="filteredStocks"
        stripe
        :default-sort="{ prop: defaultSortColumn, order: 'descending' }"
        style="width: 100%"
        height="600"
      >
        <el-table-column prop="代码" label="代码" width="80" fixed="left" />
        <el-table-column prop="名称" label="名称" width="120" fixed="left" />
        <el-table-column prop="行业" label="行业" width="150" show-overflow-tooltip sortable>
          <template #default="{ row }">
            <span v-if="row.行业">{{ row.行业 }}</span>
            <span v-else style="color: #909399;">-</span>
          </template>
        </el-table-column>


        <!-- 动态生成涨跌幅列 -->
        <el-table-column
          v-for="column in baimaData.change_columns"
          :key="column"
          :prop="column"
          :label="column.replace('涨跌幅', '')"
          width="120"
          align="right"
          sortable
        >
          <template #default="{ row }">
            <span
              :class="getChangeClass(row[column])"
              v-if="row[column] !== null && row[column] !== undefined"
            >
              {{ formatPercent(row[column]) }}
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-empty v-else-if="!loading" description="请设置筛选条件并点击分析按钮" />
  </div>
</template>

<script>
import { ref, onMounted, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { Star, Search } from '@element-plus/icons-vue'
import ApiService, { utils } from '../services/api'

export default {
  name: 'BaimaAnalysis',
  components: {
    Star,
    Search
  },
  setup() {
    const loading = ref(false)
    const baimaData = ref({})
    const searchText = ref('')
    const selectedQuickIntervals = ref(['30d', '90d'])

    const filterOptions = ref({
      includeST: false,           // 包含ST股票
      includeNonMainBoard: false  // 包含非主板股票
    })

    // 自定义时间区间
    const customInterval = ref({
      startDate: '',
      endDate: '',
      name: ''
    })

    const customIntervals = ref([])
    const showSaveIntervalGroup = ref(false)
    const showLoadIntervalGroup = ref(false)
    const saveIntervalGroupForm = ref({ name: '', description: '' })
    const intervalGroups = ref([])

    // 计算默认排序列（第一个涨跌幅列）
    const defaultSortColumn = computed(() => {
      if (baimaData.value.change_columns && baimaData.value.change_columns.length > 0) {
        return baimaData.value.change_columns[0]
      }
      return ''
    })

    // 检查是否可以添加自定义时间区间
    const canAddCustomInterval = computed(() => {
      return customInterval.value.startDate &&
             customInterval.value.endDate &&
             customInterval.value.name.trim()
    })

    // 过滤股票列表
    const filteredStocks = computed(() => {
      if (!baimaData.value.stocks) return []

      if (!searchText.value) return baimaData.value.stocks

      const search = searchText.value.toLowerCase()
      return baimaData.value.stocks.filter(stock =>
        (stock.名称 && stock.名称.toLowerCase().includes(search)) ||
        (stock.代码 && stock.代码.toLowerCase().includes(search))
      )
    })

    // 添加自定义时间区间
    const addCustomInterval = () => {
      if (!canAddCustomInterval.value) return

      customIntervals.value.push({
        start_date: customInterval.value.startDate,
        end_date: customInterval.value.endDate,
        name: customInterval.value.name.trim()
      })

      // 清空输入
      customInterval.value = {
        startDate: '',
        endDate: '',
        name: ''
      }
    }

    // 移除自定义时间区间
    const removeCustomInterval = (index) => {
      customIntervals.value.splice(index, 1)
    }

    // 生成时间区间
    const generateIntervals = () => {
      const intervals = []
      const today = new Date()

      // 添加快速选择的时间区间
      selectedQuickIntervals.value.forEach(interval => {
        let startDate, endDate, name

        switch (interval) {
          case '30d':
            startDate = new Date(today.getTime() - 30 * 24 * 60 * 60 * 1000)
            endDate = today
            name = '最近30天'
            break
          case '90d':
            startDate = new Date(today.getTime() - 90 * 24 * 60 * 60 * 1000)
            endDate = today
            name = '最近90天'
            break
          case '6m':
            startDate = new Date(today.getTime() - 180 * 24 * 60 * 60 * 1000)
            endDate = today
            name = '最近6个月'
            break
          case 'ytd':
            startDate = new Date(today.getFullYear(), 0, 1)
            endDate = today
            name = '本年度'
            break
        }

        if (startDate && endDate) {
          intervals.push({
            start_date: startDate.toISOString().split('T')[0],
            end_date: endDate.toISOString().split('T')[0],
            name: name
          })
        }
      })

      // 添加自定义时间区间
      customIntervals.value.forEach(interval => {
        intervals.push({
          start_date: interval.start_date,
          end_date: interval.end_date,
          name: interval.name
        })
      })

      return intervals
    }

    const analyzeBaima = async () => {
      // 生成时间区间
      const intervals = generateIntervals()

      if (intervals.length === 0) {
        ElMessage.warning('请至少选择一个时间区间或添加自定义时间区间')
        return
      }

      try {
        loading.value = true

        // 构建请求参数
        const requestData = {
          intervals: intervals,
          min_market_cap: 1,  // 设置为1亿，基本不过滤
          exclude_st: !filterOptions.value.includeST,  // 反转逻辑
          include_main_board: true,  // 总是包含主板
          // 根据"包含非主板股票"选项来决定是否包含科创板/创业板和北交所
          include_kcb_cyb: filterOptions.value.includeNonMainBoard,     // 科创板/创业板
          include_bjs: filterOptions.value.includeNonMainBoard,         // 北交所
          include_non_main_board: filterOptions.value.includeNonMainBoard
        }

        console.log('🐴 发送白马分析请求:', requestData)

        const response = await ApiService.postBaimaAnalysis(requestData)

        if (response && response.success && response.data) {
          const backendData = response.data

          if (backendData.error) {
            ElMessage.error('白马分析失败: ' + backendData.error)
            baimaData.value = {}
          } else {
            baimaData.value = backendData
            console.log('🐴 白马分析成功，获得', backendData.total_count || 0, '只股票')

            // 调试：检查前端接收到的数据结构
            if (backendData.stocks && backendData.stocks.length > 0) {
              const sampleStock = backendData.stocks[0]
              console.log('🔧 DEBUG 前端: 示例股票字段:', Object.keys(sampleStock))
              console.log('🔧 DEBUG 前端: 示例股票数据:', sampleStock)
              if (sampleStock.行业) {
                console.log('🔧 DEBUG 前端: 行业字段值:', sampleStock.行业)
              } else {
                console.log('🔧 DEBUG 前端: ❌ 缺少行业字段')
              }
            }

            ElMessage.success(`分析完成，找到 ${backendData.total_count || 0} 只白马股票`)
          }
        } else {
          baimaData.value = {}
          ElMessage.warning('未找到白马分析数据')
        }
      } catch (error) {
        console.error('白马分析失败:', error)
        ElMessage.error('白马分析失败: ' + error.message)
        baimaData.value = {}
      } finally {
        loading.value = false
      }
    }

    // 区间组合：保存
    const saveIntervalGroup = async () => {
      if (!saveIntervalGroupForm.value.name.trim()) {
        ElMessage.warning('请输入组合名称')
        return
      }
      try {
        loading.value = true
        const payload = {
          name: saveIntervalGroupForm.value.name.trim(),
          description: saveIntervalGroupForm.value.description.trim(),
          selected_quick_intervals: selectedQuickIntervals.value,
          custom_intervals: customIntervals.value,
          comparison: null
        }
        const res = await ApiService.saveIntervalGroup(payload)
        if (res.success) {
          ElMessage.success(res.message || '保存成功')
          showSaveIntervalGroup.value = false
          saveIntervalGroupForm.value = { name: '', description: '' }
        } else {
          ElMessage.error(res.error || '保存失败')
        }
      } catch (e) {
        ElMessage.error('保存失败: ' + e.message)
      } finally {
        loading.value = false
      }
    }

    const openLoadIntervalGroup = async () => {
      try {
        loading.value = true
        const res = await ApiService.getIntervalGroups()
        if (res.success) {
          intervalGroups.value = res.data || []
          showLoadIntervalGroup.value = true
        } else {
          ElMessage.error(res.error || '加载失败')
        }
      } catch (e) {
        ElMessage.error('加载失败: ' + e.message)
      } finally {
        loading.value = false
      }
    }

    const applyIntervalGroup = (group) => {
      selectedQuickIntervals.value = Array.isArray(group.selected_quick_intervals) ? group.selected_quick_intervals : []
      customIntervals.value = Array.isArray(group.custom_intervals) ? group.custom_intervals : []
      showLoadIntervalGroup.value = false
      ElMessage.success(`已应用组合: ${group.name}`)
    }

    // 格式化函数
    const formatPercent = (value) => {
      if (value === null || value === undefined) return '-'
      return (value >= 0 ? '+' : '') + value.toFixed(2) + '%'
    }

    const formatMarketCap = (value) => {
      if (!value) return '-'
      return (value / 100000000).toFixed(1)
    }

    const getChangeClass = (value) => {
      if (value === null || value === undefined) return ''
      return value >= 0 ? 'up-color' : 'down-color'
    }

    onMounted(() => {
      console.log('🐴 白马分析页面挂载')
    })

    return {
      loading,
      baimaData,
      searchText,
      selectedQuickIntervals,
      filterOptions,
      customInterval,
      customIntervals,
      canAddCustomInterval,
      defaultSortColumn,
      filteredStocks,
      addCustomInterval,
      removeCustomInterval,
      analyzeBaima,
      formatPercent,
      getChangeClass,
      // 区间组合
      showSaveIntervalGroup,
      showLoadIntervalGroup,
      saveIntervalGroupForm,
      intervalGroups,
      saveIntervalGroup,
      openLoadIntervalGroup,
      applyIntervalGroup
    }
  }
}
</script>

<style scoped>
.baima-analysis {
  padding: 20px;
}

.page-header {
  margin-bottom: 20px;
}

.page-header h1 {
  color: #303133;
  margin-bottom: 8px;
}

.page-header p {
  color: #909399;
  font-size: 14px;
}

.control-panel {
  margin-bottom: 20px;
}

.stats-panel {
  margin-bottom: 20px;
}

.control-item {
  margin-bottom: 10px;
}

.control-item label {
  display: block;
  margin-bottom: 5px;
  font-size: 14px;
  color: #606266;
  font-weight: 500;
}

.up-color {
  color: #f56c6c;
  font-weight: bold;
}

.down-color {
  color: #67c23a;
  font-weight: bold;
}

/* 表格样式优化 */
:deep(.el-table) {
  font-size: 13px;
}

:deep(.el-table th) {
  background-color: #fafafa;
  font-weight: 600;
}

:deep(.el-table td) {
  padding: 8px 0;
}

/* 固定列样式 */
:deep(.el-table__fixed-column--left) {
  box-shadow: 2px 0 4px rgba(0, 0, 0, 0.1);
}

/* 统计面板样式 */
:deep(.el-statistic__content) {
  font-size: 24px;
  font-weight: bold;
  color: #303133;
}

:deep(.el-statistic__title) {
  font-size: 14px;
  color: #909399;
  margin-bottom: 8px;
}
</style>
