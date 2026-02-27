<template>
  <div class="new-high-stocks">
    <!-- 页面标题 -->
    <div class="page-header">
      <h1>🔥 新高股票分析</h1>
    </div>

    <!-- 控制面板 -->
    <el-card class="control-panel">
      <el-row :gutter="20" align="middle">
        <el-col :xs="24" :sm="8" :md="6">
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

        <el-col :xs="24" :sm="8" :md="6">
          <div class="control-item">
            <label>新高周期</label>
            <el-select v-model="selectedPeriod" placeholder="选择周期" style="width: 100%">
              <el-option label="5日新高" :value="5" />
              <el-option label="10日新高" :value="10" />
              <el-option label="20日新高" :value="20" />
              <el-option label="60日新高" :value="60" />
            </el-select>
          </div>
        </el-col>

        <el-col :xs="24" :sm="8" :md="6">
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

        <el-col :xs="24" :sm="8" :md="6">
          <div class="control-item">
            <label>&nbsp;</label>
            <el-button
              type="primary"
              @click="analyzeNewHigh"
              :loading="analyzing"
              style="width: 100%"
            >
              <el-icon><Search /></el-icon>
              分析新高股票
            </el-button>
          </div>
        </el-col>

        <el-col :xs="24" :sm="24" :md="6">
          <div class="result-summary" v-if="stockList.length > 0">
            <el-tag type="success" size="large">
              找到 {{ stockList.length }} 只新高股票
            </el-tag>
          </div>
        </el-col>
      </el-row>
    </el-card>

    <!-- 主要内容区域 -->
    <el-row :gutter="20" v-if="stockList.length > 0">
      <!-- 左侧：股票列表 -->
      <el-col :xs="24" :lg="14">
        <el-card class="stock-list-card">
          <template #header>
            <div class="card-header">
              <span>{{ selectedPeriod }}日新高股票列表</span>
              <el-button type="text" @click="exportData">
                <el-icon><Download /></el-icon>
                导出数据
              </el-button>
            </div>
          </template>

          <el-table 
            :data="stockList" 
            stripe 
            highlight-current-row
            @current-change="handleStockSelect"
            style="width: 100%"
            max-height="600"
          >
            <el-table-column prop="名称" label="名称" width="100" fixed="left">
              <template #default="{ row }">
                <el-button type="text" @click="selectStock(row)">
                  {{ row.名称 }}
                </el-button>
              </template>
            </el-table-column>
            
            <el-table-column prop="代码" label="代码" width="80" />
            
            <el-table-column prop="收盘价" label="收盘价" width="80" align="right">
              <template #default="{ row }">
                <span class="price">{{ formatNumber(row.收盘价) }}</span>
              </template>
            </el-table-column>
            
            <el-table-column prop="涨跌幅" label="当日涨跌幅" width="100" align="right">
              <template #default="{ row }">
                <span :class="getChangeClass(row.涨跌幅)">
                  {{ formatPercent(row.涨跌幅) }}
                </span>
              </template>
            </el-table-column>
            
            <el-table-column prop="5日涨跌幅(%)" label="5日涨跌幅" width="100" align="right" sortable>
              <template #default="{ row }">
                <span :class="getChangeClass(row['5日涨跌幅(%)'])">
                  {{ formatPercent(row['5日涨跌幅(%)']) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column prop="10日涨跌幅(%)" label="10日涨跌幅" width="100" align="right" sortable>
              <template #default="{ row }">
                <span :class="getChangeClass(row['10日涨跌幅(%)'])">
                  {{ formatPercent(row['10日涨跌幅(%)']) }}
                </span>
              </template>
            </el-table-column>
            
            <el-table-column prop="20日涨跌幅(%)" label="20日涨跌幅" width="100" align="right" sortable>
              <template #default="{ row }">
                <span :class="getChangeClass(row['20日涨跌幅(%)'])">
                  {{ formatPercent(row['20日涨跌幅(%)']) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column prop="成交量" label="成交量" width="100" align="right">
              <template #default="{ row }">
                <span class="volume">{{ formatVolume(row.成交量) }}</span>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>

      <!-- 右侧：K线图 -->
      <el-col :xs="24" :lg="10">
        <el-card class="kline-card">
          <template #header>
            <div class="card-header">
              <span v-if="selectedStock">
                {{ selectedStock.名称 }} ({{ selectedStock.代码 }}) K线图
              </span>
              <span v-else>请选择股票查看K线图</span>
            </div>
          </template>

          <div class="kline-container">
            <div v-if="!selectedStock" class="empty-state">
              <el-empty description="请从左侧列表选择股票" />
            </div>
            
            <div v-else-if="loadingKline" class="loading-state">
              <el-skeleton :rows="8" animated />
            </div>
            
            <div v-else-if="klineData.length > 0" class="kline-chart">
              <v-chart 
                :option="klineOption" 
                :style="{ height: '400px', width: '100%' }"
                autoresize
              />
            </div>
            
            <div v-else class="error-state">
              <el-empty description="暂无K线数据" />
            </div>
          </div>

          <!-- 股票信息 -->
          <div v-if="selectedStock" class="stock-info">
            <el-descriptions :column="2" size="small" border>
              <el-descriptions-item label="股票名称">
                {{ selectedStock.名称 }}
              </el-descriptions-item>
              <el-descriptions-item label="股票代码">
                {{ selectedStock.代码 }}
              </el-descriptions-item>
              <el-descriptions-item label="收盘价">
                <span class="price">{{ formatNumber(selectedStock.收盘价) }}</span>
              </el-descriptions-item>
              <el-descriptions-item label="当日涨跌幅">
                <span :class="getChangeClass(selectedStock.涨跌幅)">
                  {{ formatPercent(selectedStock.涨跌幅) }}
                </span>
              </el-descriptions-item>
            </el-descriptions>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 空状态 -->
    <el-card v-if="!analyzing && stockList.length === 0" class="empty-card">
      <el-empty description="请选择日期和周期，然后点击分析按钮" />
    </el-card>
  </div>
</template>

<script>
import { ref, onMounted, computed, watch } from 'vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart, CandlestickChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent
} from 'echarts/components'
import VChart from 'vue-echarts'
import ApiService, { utils } from '../services/api'
import TradingDatePicker from '@/components/TradingDatePicker.vue'

// 注册ECharts组件
use([
  CanvasRenderer,
  LineChart,
  CandlestickChart,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent
])

export default {
  name: 'NewHighStocks',
  components: {
    VChart,
    TradingDatePicker
  },
  setup() {
    // 响应式数据
    const selectedDate = ref(new Date().toISOString().split('T')[0])
    const selectedPeriod = ref(5)
    const analyzing = ref(false)
    const stockList = ref([])
    const selectedStock = ref(null)
    const loadingKline = ref(false)
    const klineData = ref([])
    const filterOptions = ref({
      excludeST: true,        // 默认去掉ST股票
      includeNonMainBoard: false  // 默认不包含非主板股票
    })

    // 计算属性
    const formatNumber = computed(() => utils.formatNumber)
    const formatPercent = computed(() => utils.formatPercent)
    const formatVolume = computed(() => utils.formatVolume)

    const klineOption = computed(() => {
      if (klineData.value.length === 0) return {}

      const dates = klineData.value.map(item => item.date)
      const candlestickData = klineData.value.map(item => [
        item.open, item.close, item.low, item.high
      ])
      const volumeData = klineData.value.map(item => item.volume)
      const ma5Data = klineData.value.map(item => item.ma5)
      const ma10Data = klineData.value.map(item => item.ma10)
      const ma20Data = klineData.value.map(item => item.ma20)

      return {
        title: {
          text: `${selectedStock.value?.名称} K线图`,
          left: 'center'
        },
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'cross'
          }
        },
        legend: {
          data: [
            {
              name: 'MA5',
              icon: 'rect',
              itemStyle: {
                color: '#4ECDC4'
              }
            },
            {
              name: 'MA10',
              icon: 'rect',
              itemStyle: {
                color: '#ffbf00'
              }
            },
            {
              name: 'MA20',
              icon: 'rect',
              itemStyle: {
                color: '#f92672'
              }
            }
          ],
          top: 30,
          right: 10
        },
        grid: [
          {
            left: '10%',
            right: '8%',
            height: '50%'
          },
          {
            left: '10%',
            right: '8%',
            top: '70%',
            height: '16%'
          }
        ],
        xAxis: [
          {
            type: 'category',
            data: dates,
            scale: true,
            boundaryGap: false,
            axisLine: { onZero: false },
            splitLine: { show: false },
            min: 'dataMin',
            max: 'dataMax'
          },
          {
            type: 'category',
            gridIndex: 1,
            data: dates,
            scale: true,
            boundaryGap: false,
            axisLine: { onZero: false },
            axisTick: { show: false },
            splitLine: { show: false },
            axisLabel: { show: false },
            min: 'dataMin',
            max: 'dataMax'
          }
        ],
        yAxis: [
          {
            scale: true,
            splitArea: {
              show: true
            }
          },
          {
            scale: true,
            gridIndex: 1,
            splitNumber: 2,
            axisLabel: { show: false },
            axisLine: { show: false },
            axisTick: { show: false },
            splitLine: { show: false }
          }
        ],
        dataZoom: [
          {
            type: 'inside',
            xAxisIndex: [0, 1],
            start: 50,
            end: 100
          },
          {
            show: true,
            xAxisIndex: [0, 1],
            type: 'slider',
            top: '85%',
            start: 50,
            end: 100
          }
        ],
        series: [
          {
            name: 'K线',
            type: 'candlestick',
            data: candlestickData,
            itemStyle: {
              color: '#ef232a',
              color0: '#14b143',
              borderColor: '#ef232a',
              borderColor0: '#14b143'
            }
          },
          {
            name: 'MA5',
            type: 'line',
            data: ma5Data,
            smooth: true,
            lineStyle: {
              color: '#4ECDC4',
              width: 1
            },
            itemStyle: {
              color: '#4ECDC4'
            },
            symbol: 'none'
          },
          {
            name: 'MA10',
            type: 'line',
            data: ma10Data,
            smooth: true,
            lineStyle: {
              color: '#ffbf00',
              width: 1
            },
            itemStyle: {
              color: '#ffbf00'
            },
            symbol: 'none'
          },
          {
            name: 'MA20',
            type: 'line',
            data: ma20Data,
            smooth: true,
            lineStyle: {
              color: '#f92672',
              width: 1
            },
            itemStyle: {
              color: '#f92672'
            },
            symbol: 'none'
          },
          {
            name: '成交量',
            type: 'bar',
            xAxisIndex: 1,
            yAxisIndex: 1,
            data: volumeData.map((volume, index) => {
              const klineItem = candlestickData[index]
              const isUp = klineItem && klineItem[1] >= klineItem[0]
              return {
                value: volume,
                itemStyle: {
                  color: isUp ? '#ef232a' : '#14b143'
                }
              }
            })
          }
        ]
      }
    })

    // 方法
    const analyzeNewHigh = async () => {
      if (!selectedDate.value) {
        ElMessage.warning('请选择分析日期')
        return
      }

      try {
        analyzing.value = true
        console.log(`🔄 分析${selectedPeriod.value}日新高股票...`)

        const response = await ApiService.getNewHighStocks(selectedDate.value, selectedPeriod.value, filterOptions.value)
        stockList.value = response.data.stocks || []

        console.log(`✅ 找到 ${stockList.value.length} 只新高股票`)
      } catch (error) {
        console.error('❌ 新高股票分析失败:', error)
        stockList.value = []
      } finally {
        analyzing.value = false
      }
    }

    const selectStock = async (stock) => {
      if (selectedStock.value?.代码 === stock.代码) return

      selectedStock.value = stock
      await loadKlineData(stock.代码)
    }

    const handleStockSelect = (currentRow) => {
      if (currentRow) {
        selectStock(currentRow)
      }
    }

    const loadKlineData = async (stockCode) => {
      try {
        loadingKline.value = true
        console.log(`🔄 加载股票 ${stockCode} K线数据，日期: ${selectedDate.value}`)

        const response = await ApiService.getStockKline(stockCode, 30, selectedDate.value, 'data')
        klineData.value = response.data?.data?.kline_data || []

        console.log(`✅ 获取 ${stockCode} K线数据: ${klineData.value.length} 条记录`)
      } catch (error) {
        console.error(`❌ 获取股票 ${stockCode} K线数据失败:`, error)
        klineData.value = []
      } finally {
        loadingKline.value = false
      }
    }

    const getChangeClass = (change) => {
      if (change > 0) return 'up-color'
      if (change < 0) return 'down-color'
      return 'neutral-color'
    }

    const exportData = () => {
      console.log('导出数据功能')
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
      }
    }

    // 监听日期变化，自动更新K线图
    watch(selectedDate, (newDate, oldDate) => {
      if (newDate !== oldDate && selectedStock.value) {
        console.log(`📅 日期变化: ${oldDate} → ${newDate}，重新加载K线数据`)
        loadKlineData(selectedStock.value.代码)
      }
    })

    // 生命周期
    onMounted(async () => {
      console.log('🔥 新高股票分析页面挂载')
      await initializeLatestDate()
    })

    return {
      selectedDate,
      selectedPeriod,
      analyzing,
      stockList,
      selectedStock,
      loadingKline,
      klineData,
      klineOption,
      filterOptions,
      formatNumber,
      formatPercent,
      formatVolume,
      analyzeNewHigh,
      selectStock,
      handleStockSelect,
      loadKlineData,
      getChangeClass,
      exportData
    }
  }
}
</script>

<style scoped>
.new-high-stocks {
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

.result-summary {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
}

.stock-list-card,
.kline-card {
  height: 700px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.kline-container {
  height: 500px;
}

.empty-state,
.loading-state,
.error-state {
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
}

.stock-info {
  margin-top: 20px;
}

.price {
  font-weight: bold;
  color: #303133;
}

.volume {
  font-size: 12px;
  color: #909399;
}

.up-color {
  color: #f56c6c;
}

.down-color {
  color: #67c23a;
}

.neutral-color {
  color: #909399;
}

.empty-card {
  margin-top: 40px;
  text-align: center;
  padding: 60px 20px;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .control-item {
    margin-bottom: 15px;
  }
  
  .stock-list-card,
  .kline-card {
    height: auto;
    margin-bottom: 20px;
  }
  
  .kline-container {
    height: 300px;
  }
}
</style>
