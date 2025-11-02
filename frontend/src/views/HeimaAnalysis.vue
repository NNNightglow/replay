<template>
  <div class="heima-analysis">
    <div class="page-header">
      <h1>🐎 黑马分析</h1>
      <p>涨停股票分析，独立模块加载</p>
    </div>

    <el-card class="control-panel">
      <el-row :gutter="20" align="middle">
        <el-col :span="6">
          <div class="control-item">
            <label>选择日期</label>
            <trading-date-picker
              v-model="selectedDate"
              placeholder="选择分析日期"
              format="YYYY-MM-DD"
              value-format="YYYY-MM-DD"
              style="width: 100%"
            />
          </div>
        </el-col>
        <el-col :span="6">
          <div class="control-item">
            <label>过滤选项</label>
            <el-checkbox v-model="filterOptions.excludeST" style="display: block; margin-bottom: 8px;">
              去掉ST股票
            </el-checkbox>
            <el-checkbox v-model="filterOptions.includeNonMainBoard">
              包含非主板股票
            </el-checkbox>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="control-item">
            <label>&nbsp;</label>
            <el-button type="primary" @click="analyzeHeima" :loading="loading" style="width: 100%">
              <el-icon><Lightning /></el-icon>
              分析黑马股票
            </el-button>
          </div>
        </el-col>
      </el-row>
    </el-card>

    <el-card v-if="heimaData.limit_up_stocks?.length > 0">
      <template #header>
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <span>涨停股票列表 ({{ heimaData.total || 0 }}只)</span>
          <el-text type="info" size="small">
            💡 点击列头可进行排序，默认按连板情况排序
          </el-text>
        </div>
      </template>
      
      <el-table
        :data="heimaData.limit_up_stocks"
        stripe
        :default-sort="{prop: '连板天数', order: 'descending'}"
      >
        <el-table-column prop="名称" label="股票名称" />
        <el-table-column prop="代码" label="股票代码" />
        <el-table-column prop="收盘" label="收盘价" align="right">
          <template #default="{ row }">
            <span class="price">{{ formatNumber(row.收盘) }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="涨跌幅" label="涨跌幅" align="right" sortable>
          <template #default="{ row }">
            <span class="up-color">{{ formatPercent(row.涨跌幅) }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="成交额" label="成交额" align="right" sortable>
          <template #default="{ row }">
            <span>{{ formatAmount(row.成交额) }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="换手率" label="换手率" align="right" sortable>
          <template #default="{ row }">
            <span>{{ formatPercent(row.换手率) }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="连板天数" label="连板情况" align="center" sortable :sort-method="sortByConsecutiveDays">
          <template #default="{ row }">
            <el-tag :type="getConsecutiveTagType(row.连板天数)">
              {{ row.连板天数 || '首板' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="5日涨跌幅" label="5日涨跌幅" align="right" sortable>
          <template #default="{ row }">
            <span :class="row['5日涨跌幅'] >= 0 ? 'up-color' : 'down-color'">
              {{ formatPercent(row['5日涨跌幅']) }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="10日涨跌幅" label="10日涨跌幅" align="right" sortable>
          <template #default="{ row }">
            <span :class="row['10日涨跌幅'] >= 0 ? 'up-color' : 'down-color'">
              {{ formatPercent(row['10日涨跌幅']) }}
            </span>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-empty v-else-if="!loading" description="请选择日期并点击分析按钮" />
  </div>
</template>

<script>
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import ApiService, { utils } from '../services/api'
import TradingDatePicker from '@/components/TradingDatePicker.vue'

export default {
  name: 'HeimaAnalysis',
  components: {
    TradingDatePicker
  },
  setup() {
    const selectedDate = ref(new Date().toISOString().split('T')[0])
    const loading = ref(false)
    const heimaData = ref({})
    const filterOptions = ref({
      excludeST: true,        // 默认去掉ST股票
      includeNonMainBoard: false  // 默认不包含非主板股票
    })

    const analyzeHeima = async () => {
      if (!selectedDate.value) {
        ElMessage.warning('请选择分析日期')
        return
      }

      try {
        loading.value = true
        const response = await ApiService.getHeimaAnalysis(selectedDate.value, filterOptions.value)

        // 修复：适配axios响应拦截器处理后的数据结构
        // axios拦截器返回data，所以response就是后端的{success, data, timestamp}
        if (response && response.success && response.data) {
          heimaData.value = {
            limit_up_stocks: response.data,
            total: response.data.length
          }
          console.log('🐎 黑马分析成功，获得', response.data.length, '只涨停股票')
        } else {
          heimaData.value = { limit_up_stocks: [], total: 0 }
          ElMessage.warning('未找到涨停股票数据')
        }
      } catch (error) {
        console.error('黑马分析失败:', error)
        ElMessage.error('黑马分析失败: ' + error.message)
        heimaData.value = { limit_up_stocks: [], total: 0 }
      } finally {
        loading.value = false
      }
    }

    const formatNumber = (value) => utils.formatNumber(value)
    const formatPercent = (value) => utils.formatPercent(value)
    const formatAmount = (value) => utils.formatAmount(value)

    const getConsecutiveTagType = (consecutiveDays) => {
      if (!consecutiveDays) return 'info'

      // 提取板数来决定标签颜色
      const extractBoardCount = (str) => {
        if (str.includes('首板')) return 1
        const boardMatch = str.match(/(\d+)板/)
        return boardMatch ? parseInt(boardMatch[1]) : 0
      }

      const boardCount = extractBoardCount(consecutiveDays.toString())

      if (boardCount >= 5) return 'danger'    // 5板及以上：红色（高板）
      if (boardCount >= 3) return 'warning'   // 3-4板：橙色（中板）
      if (boardCount >= 2) return 'success'   // 2板：绿色（二板）
      return 'info'                           // 1板：蓝色（首板）
    }

    const sortByConsecutiveDays = (a, b) => {
      // 自定义排序方法：按连板板数排序
      const extractBoardCount = (str) => {
        if (!str) return 0

        // 处理不同格式的连板天数
        // 主要格式: "X天Y板" -> 提取Y（板数）
        // 例如: "1天1板" -> 1, "5天2板" -> 2, "15天12板" -> 12

        if (str.includes('首板')) return 1

        // 提取"Y板"中的数字Y（板数）
        const boardMatch = str.match(/(\d+)板/)
        if (boardMatch) {
          return parseInt(boardMatch[1])
        }

        // 如果没有找到板数，返回0
        return 0
      }

      const boardCountA = extractBoardCount(a.连板天数)
      const boardCountB = extractBoardCount(b.连板天数)

      // 降序排列：板数多的在前面（高板优先）
      return boardCountB - boardCountA
    }

    // 初始化最新日期
    const initializeLatestDate = async () => {
      try {
        const response = await ApiService.getLatestMarketDate()
        if (response.success && response.data.latest_date) {
          selectedDate.value = response.data.latest_date
          console.log(`🔧 DEBUG: 设置最新可用日期: ${selectedDate.value}`)
        }
      } catch (error) {
        console.warn('获取最新市场日期失败，使用当前日期:', error)
        // 保持默认的当前日期
      }
    }

    onMounted(async () => {
      console.log('🐎 黑马分析页面挂载')
      await initializeLatestDate()
    })

    return {
      selectedDate,
      loading,
      heimaData,
      filterOptions,
      analyzeHeima,
      formatNumber,
      formatPercent,
      formatAmount,
      getConsecutiveTagType,
      sortByConsecutiveDays
    }
  }
}
</script>

<style scoped>
.heima-analysis {
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

.control-item {
  margin-bottom: 10px;
}

.control-item label {
  display: block;
  margin-bottom: 5px;
  font-size: 14px;
  color: #606266;
}

.price {
  font-weight: bold;
  color: #303133;
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
:deep(.el-table th) {
  background-color: #f5f7fa;
  font-weight: bold;
}

:deep(.el-table .el-table__row:hover) {
  background-color: #f0f9ff;
}

/* 连板标签样式 */
:deep(.el-tag) {
  font-weight: bold;
  border-radius: 12px;
}

/* 排序图标样式 */
:deep(.el-table th.is-sortable .caret-wrapper) {
  height: 20px;
}
</style>
