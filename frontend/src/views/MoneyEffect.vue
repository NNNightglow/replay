<template>
  <div class="money-effect">
    <!-- 页面标题 -->
    <div class="page-header">
      <h1>💰 赚亏钱效应分析</h1>
      <p class="page-description">分析当日最低点到收盘价的涨跌幅（赚钱效应）和最高点到收盘价的涨跌幅（亏钱效应），支持全部个股和近期强势股（曾3板以上）两种模式</p>
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
            <label>过滤选项</label>
            <el-checkbox v-model="filterOptions.excludeST" style="display: block; margin-bottom: 8px;">
              去掉ST和退市股票
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
              @click="analyzeMoneyEffect" 
              :loading="analyzing"
              style="width: 100%"
            >
              <el-icon><TrendCharts /></el-icon>
              分析效应
            </el-button>
          </div>
        </el-col>

        <el-col :xs="24" :sm="8" :md="6">
          <div class="control-item">
            <label>&nbsp;</label>
            <el-button 
              type="success" 
              @click="refreshData" 
              :loading="refreshing"
              style="width: 100%"
            >
              <el-icon><Refresh /></el-icon>
              刷新数据
            </el-button>
          </div>
        </el-col>
      </el-row>
    </el-card>

    <!-- 统计信息 -->
    <div v-if="allStatsData || strongStatsData">
      <!-- 全部股票统计 -->
      <el-card class="stats-panel" style="margin-bottom: 15px;" v-if="allStatsData">
        <template #header>
          <span>📊 全部股票统计</span>
        </template>
        <el-row :gutter="20">
          <el-col :xs="12" :sm="6">
            <div class="stat-item">
              <div class="stat-value positive">{{ allStatsData.avgLowToClose }}%</div>
              <div class="stat-label">平均赚钱效应</div>
            </div>
          </el-col>
          <el-col :xs="12" :sm="6">
            <div class="stat-item">
              <div class="stat-value negative">{{ allStatsData.avgHighToClose }}%</div>
              <div class="stat-label">平均亏钱效应</div>
            </div>
          </el-col>
          <el-col :xs="12" :sm="6">
            <div class="stat-item">
              <div class="stat-value positive">{{ allStatsData.avg5DayChange }}%</div>
              <div class="stat-label">平均5日涨跌幅</div>
            </div>
          </el-col>
          <el-col :xs="12" :sm="6">
            <div class="stat-item">
              <div class="stat-value positive">{{ allStatsData.avg10DayChange }}%</div>
              <div class="stat-label">平均10日涨跌幅</div>
            </div>
          </el-col>
        </el-row>
      </el-card>

      <!-- 强势股统计 -->
      <el-card class="stats-panel" style="margin-bottom: 15px;" v-if="strongStatsData">
        <template #header>
          <span>🚀 强势股统计</span>
        </template>
        <el-row :gutter="20">
          <el-col :xs="12" :sm="6">
            <div class="stat-item">
              <div class="stat-value positive">{{ strongStatsData.avgLowToClose }}%</div>
              <div class="stat-label">平均赚钱效应</div>
            </div>
          </el-col>
          <el-col :xs="12" :sm="6">
            <div class="stat-item">
              <div class="stat-value negative">{{ strongStatsData.avgHighToClose }}%</div>
              <div class="stat-label">平均亏钱效应</div>
            </div>
          </el-col>
          <el-col :xs="12" :sm="6">
            <div class="stat-item">
              <div class="stat-value positive">{{ allStatsData.avg5DayChange }}%</div>
              <div class="stat-label">平均5日涨跌幅</div>
            </div>
          </el-col>
          <el-col :xs="12" :sm="6">
            <div class="stat-item">
              <div class="stat-value positive">{{ strongStatsData.avg10DayChange }}%</div>
              <div class="stat-label">平均10日涨跌幅</div>
            </div>
          </el-col>
        </el-row>
      </el-card>
    </div>

    <!-- 主要内容区域 - 左右布局 -->
    <div v-if="allStocksList.length > 0 || strongStocksList.length > 0">

      <!-- 全部个股赚亏钱效应 - 左右布局 -->
      <el-card class="stock-list-card" style="margin-bottom: 20px;" v-if="allStocksList.length > 0">
        <template #header>
          <div class="card-header">
            <span>📊 赚亏钱效应</span>
            <div>
              <el-autocomplete
                v-model="stockSearchQuery"
                :fetch-suggestions="queryStockSuggestions"
                placeholder="输入代码/名称搜索个股"
                clearable
                @select="onStockSuggestionSelect"
                style="width: 240px; margin-right: 10px;"
              >
                <template #suffix>
                  <el-icon>
                    <Search />
                  </el-icon>
                </template>
              </el-autocomplete>
              <el-button type="text" @click="exportData('all')">
                <el-icon><Download /></el-icon>
                导出全部股票数据
              </el-button>
            </div>
          </div>
        </template>

        <!-- 左右布局容器 -->
        <el-row :gutter="20">
          <!-- 左侧：股票表格 -->
          <el-col :span="14">
            <el-table
              :data="allStocksList"
              stripe
              highlight-current-row
              @current-change="handleStockSelect"
              style="width: 100%"
              max-height="500"
            >
            <el-table-column prop="名称" label="名称" width="100" fixed="left">
              <template #default="{ row }">
                <el-button type="text" @click="selectStock(row)">
                  {{ row.名称 }}
                </el-button>
              </template>
            </el-table-column>
            
            <el-table-column prop="代码" label="代码" width="80" />
            
            <el-table-column width="120" align="right" sortable prop="最低到收盘涨幅">
              <template #header>
                <span>赚钱效应</span>
                <el-tooltip content="当日最低价到收盘价的涨跌幅，反映股票的赚钱效应" placement="top">
                  <el-icon style="margin-left: 4px; color: #909399; cursor: help;">
                    <QuestionFilled />
                  </el-icon>
                </el-tooltip>
              </template>
              <template #default="{ row }">
                <span :class="getChangeClass(row.最低到收盘涨幅)">
                  {{ formatPercent(row.最低到收盘涨幅) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column width="120" align="right" sortable prop="最高到收盘涨幅">
              <template #header>
                <span>亏钱效应</span>
                <el-tooltip content="当日最高价到收盘价的涨跌幅，反映股票的亏钱效应" placement="top">
                  <el-icon style="margin-left: 4px; color: #909399; cursor: help;">
                    <QuestionFilled />
                  </el-icon>
                </el-tooltip>
              </template>
              <template #default="{ row }">
                <span :class="getChangeClass(row.最高到收盘涨幅)">
                  {{ formatPercent(row.最高到收盘涨幅) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column prop="当日涨跌幅" label="当日涨跌幅" width="100" align="right" sortable>
              <template #default="{ row }">
                <span :class="getChangeClass(row.当日涨跌幅)">
                  {{ formatPercent(row.当日涨跌幅) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column prop="5日涨跌幅" label="5日涨跌幅" width="100" align="right" sortable>
              <template #default="{ row }">
                <span :class="getChangeClass(row['5日涨跌幅'])">
                  {{ formatPercent(row['5日涨跌幅']) }}
                </span>
              </template>
            </el-table-column>

            <el-table-column prop="10日涨跌幅" label="10日涨跌幅" width="100" align="right" sortable>
              <template #default="{ row }">
                <span :class="getChangeClass(row['10日涨跌幅'])">
                  {{ formatPercent(row['10日涨跌幅']) }}
                </span>
              </template>
            </el-table-column>
            </el-table>
          </el-col>

          <!-- 右侧：K线图 -->
          <el-col :span="10">
            <div class="kline-container">
              <div v-if="selectedStock" class="kline-header">
                <h4>{{ selectedStock.名称 }} ({{ selectedStock.代码 }}) K线图</h4>
                <div class="stock-info">
                  <span class="info-item">赚钱效应:
                    <span :class="getChangeClass(selectedStock.最低到收盘涨幅)">
                      {{ formatPercent(selectedStock.最低到收盘涨幅) }}
                    </span>
                  </span>
                  <span class="info-item">亏钱效应:
                    <span :class="getChangeClass(selectedStock.最高到收盘涨幅)">
                      {{ formatPercent(selectedStock.最高到收盘涨幅) }}
                    </span>
                  </span>
                  <span class="info-item">当日涨跌幅:
                    <span :class="getChangeClass(selectedStock.当日涨跌幅)">
                      {{ formatPercent(selectedStock.当日涨跌幅) }}
                    </span>
                  </span>
                </div>
              </div>

              <div v-if="klineLoading" class="loading-container">
                <el-icon class="is-loading"><Loading /></el-icon>
                <span>加载K线图中...</span>
              </div>

              <div v-else-if="selectedStock && klineData.length > 0" class="kline-chart">
                <v-chart
                  :option="klineOption"
                  :style="{ height: '400px', width: '100%' }"
                  autoresize
                />
              </div>

              <div v-else-if="!selectedStock" class="no-selection">
                <el-icon><TrendCharts /></el-icon>
                <p>点击左侧股票名称查看K线图</p>
              </div>

              <div v-else class="no-data">
                <el-icon><Warning /></el-icon>
                <p>暂无K线图数据</p>
              </div>
            </div>
          </el-col>
        </el-row>
      </el-card>

      <!-- 近期强势股赚亏钱效应（曾3板以上） - 左右布局 -->
      <el-card class="stock-list-card" style="margin-bottom: 20px;" v-if="strongStocksList.length > 0">
        <template #header>
          <div class="card-header">
            <span>🚀 近期强势股赚亏钱效应（曾3板以上） - {{ strongStocksList.length }}只</span>
            <div>
              <el-button type="text" @click="exportData('strong')">
                <el-icon><Download /></el-icon>
                导出强势股数据
              </el-button>
            </div>
          </div>
        </template>

        <el-row :gutter="20">
          <!-- 左侧：股票列表 -->
          <el-col :span="14">
            <el-table
              :data="strongStocksList"
              stripe
              highlight-current-row
              @current-change="handleStockSelect"
              style="width: 100%"
              max-height="400"
            >
              <el-table-column prop="名称" label="名称" width="100" fixed="left">
                <template #default="{ row }">
                  <el-button type="text" @click="selectStock(row)">
                    {{ row.名称 }}
                  </el-button>
                </template>
              </el-table-column>

              <el-table-column prop="代码" label="代码" width="80" />

              <el-table-column prop="历史最高连板" label="历史最高连板" width="100" align="center" sortable>
                <template #default="{ row }">
                  <el-tag type="warning">
                    {{ row.历史最高连板 }}板
                  </el-tag>
                </template>
              </el-table-column>

              <el-table-column width="120" align="right" sortable prop="最低到收盘涨幅">
                <template #header>
                  <span>赚钱效应</span>
                  <el-tooltip content="当日最低价到收盘价的涨跌幅，反映股票的赚钱效应" placement="top">
                    <el-icon style="margin-left: 4px; color: #909399; cursor: help;">
                      <QuestionFilled />
                    </el-icon>
                  </el-tooltip>
                </template>
                <template #default="{ row }">
                  <span :class="getChangeClass(row.最低到收盘涨幅)">
                    {{ formatPercent(row.最低到收盘涨幅) }}
                  </span>
                </template>
              </el-table-column>

              <el-table-column width="120" align="right" sortable prop="最高到收盘涨幅">
                <template #header>
                  <span>亏钱效应</span>
                  <el-tooltip content="当日最高价到收盘价的涨跌幅，反映股票的亏钱效应" placement="top">
                    <el-icon style="margin-left: 4px; color: #909399; cursor: help;">
                      <QuestionFilled />
                    </el-icon>
                  </el-tooltip>
                </template>
                <template #default="{ row }">
                  <span :class="getChangeClass(row.最高到收盘涨幅)">
                    {{ formatPercent(row.最高到收盘涨幅) }}
                  </span>
                </template>
              </el-table-column>

              <el-table-column prop="当日涨跌幅" label="当日涨跌幅" width="100" align="right" sortable>
                <template #default="{ row }">
                  <span :class="getChangeClass(row.当日涨跌幅)">
                    {{ formatPercent(row.当日涨跌幅) }}
                  </span>
                </template>
              </el-table-column>

              <el-table-column prop="5日涨跌幅" label="5日涨跌幅" width="100" align="right" sortable>
                <template #default="{ row }">
                  <span :class="getChangeClass(row['5日涨跌幅'])">
                    {{ formatPercent(row['5日涨跌幅']) }}
                  </span>
                </template>
              </el-table-column>

              <el-table-column prop="10日涨跌幅" label="10日涨跌幅" width="100" align="right" sortable>
                <template #default="{ row }">
                  <span :class="getChangeClass(row['10日涨跌幅'])">
                    {{ formatPercent(row['10日涨跌幅']) }}
                  </span>
                </template>
              </el-table-column>
            </el-table>
          </el-col>

          <!-- 右侧：K线图 -->
          <el-col :span="10">
            <div class="kline-container">
              <div v-if="selectedStock" class="kline-header">
                <h4>{{ selectedStock.名称 }} ({{ selectedStock.代码 }}) K线图</h4>
                <div class="stock-info">
                  <span class="info-item">赚钱效应:
                    <span :class="getChangeClass(selectedStock.最低到收盘涨幅)">
                      {{ formatPercent(selectedStock.最低到收盘涨幅) }}
                    </span>
                  </span>
                  <span class="info-item">亏钱效应:
                    <span :class="getChangeClass(selectedStock.最高到收盘涨幅)">
                      {{ formatPercent(selectedStock.最高到收盘涨幅) }}
                    </span>
                  </span>
                  <span class="info-item">当日涨跌幅:
                    <span :class="getChangeClass(selectedStock.当日涨跌幅)">
                      {{ formatPercent(selectedStock.当日涨跌幅) }}
                    </span>
                  </span>
                </div>
              </div>

              <div v-if="klineLoading" class="loading-container">
                <el-icon class="is-loading"><Loading /></el-icon>
                <span>加载K线图中...</span>
              </div>

              <div v-else-if="selectedStock && klineData.length > 0" class="kline-chart">
                <v-chart
                  :option="klineOption"
                  :style="{ height: '400px', width: '100%' }"
                  autoresize
                />
              </div>

              <div v-else-if="!selectedStock" class="no-selection">
                <el-icon><TrendCharts /></el-icon>
                <p>点击左侧股票名称查看K线图</p>
              </div>

              <div v-else class="no-data">
                <el-icon><Warning /></el-icon>
                <p>暂无K线图数据</p>
              </div>
            </div>
          </el-col>
        </el-row>
      </el-card>


    </div>

    <!-- 无数据状态 -->
    <el-empty
      v-if="!analyzing && allStocksList.length === 0 && strongStocksList.length === 0"
      description="暂无数据，请选择日期进行分析"
      :image-size="200"
    />
  </div>
</template>

<script>
import { ref, reactive, onMounted, watch, computed } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Search, Download, Refresh, TrendCharts, Loading, Warning, QuestionFilled } from '@element-plus/icons-vue'
import axios from 'axios'
import VChart from 'vue-echarts'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { CandlestickChart, LineChart, BarChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  ToolboxComponent
} from 'echarts/components'
import ApiService, { utils } from '@/services/api'
import TradingDatePicker from '@/components/TradingDatePicker.vue'

use([
  CanvasRenderer,
  CandlestickChart,
  LineChart,
  BarChart,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  ToolboxComponent
])

export default {
  name: 'MoneyEffect',
  components: {
    Search,
    Download,
    Refresh,
    TrendCharts,
    Loading,
    Warning,
    QuestionFilled,
    VChart,
    TradingDatePicker
  },
  setup() {
    // 响应式数据
    const selectedDate = ref(new Date().toISOString().split('T')[0])
    const analyzing = ref(false)
    const stockSearchQuery = ref('')
    const lastStockSearch = { term: '', results: [] }

    const queryStockSuggestions = async (queryString, cb) => {
      const term = (queryString || '').trim()
      if (!term) { cb([]); return }
      try {
        // 简单缓存避免重复请求
        if (lastStockSearch.term === term && lastStockSearch.results?.length) {
          cb(lastStockSearch.results)
          return
        }
        const resp = await ApiService.searchStocks(term)
        const list = (resp?.data || []).map(item => ({
          value: `${item.名称} (${item.代码})`,
          raw: item
        }))
        lastStockSearch.term = term
        lastStockSearch.results = list
        cb(list)
      } catch (e) {
        cb([])
      }
    }

    const onStockSuggestionSelect = (item) => {
      const stock = item?.raw
      if (!stock) return
      // 兼容现有数据结构，最少要有 名称/代码 字段
      const row = {
        名称: stock.名称,
        代码: stock.代码,
        最低到收盘涨幅: 0,
        最高到收盘涨幅: 0,
        当日涨跌幅: stock.涨跌幅 ?? 0
      }
      selectStock(row)
    }
    const refreshing = ref(false)
    const allStocksList = ref([])      // 全部股票列表（前300）
    const strongStocksList = ref([])   // 强势股列表（曾3板以上）
    const selectedStock = ref(null)
    const klineData = ref([])          // K线数据
    const keyLevels = ref([])           // 关键位（当前价到历史高点之间）
    const klineLoading = ref(false)
    const allStatsData = ref(null)     // 全部股票统计数据
    const strongStatsData = ref(null)  // 强势股统计数据
    const filterOptions = ref({
      excludeST: true,        // 默认去掉ST和退市股票
      includeNonMainBoard: false  // 默认不包含非主板股票
    })

    // API基础URL
    const API_BASE = 'http://localhost:5000'

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

      const volumeData = klineData.value.map(item => {
        const volume = item.amount || item.volume || 0
        return {
          value: volume,
          itemStyle: {
            color: item.close >= item.open ? '#ef232a' : '#14b143'
          }
        }
      })

      // MA线数据
      const ma5Data = klineData.value.map(item => item.ma5)
      const ma10Data = klineData.value.map(item => item.ma10)
      const ma20Data = klineData.value.map(item => item.ma20)

      // 构建关键位 markLine（黄色水平线）
      const levelMarkLine = keyLevels.value && keyLevels.value.length > 0 ? {
        symbol: 'none',
        silent: true,
        lineStyle: {
          color: '#FFD700',
          type: 'dashed',
          width: 1.5
        },
        label: {
          show: true,
          position: 'insideEndTop',
          color: '#a37f00',
          formatter: function(p) {
            const v = p.value
            if (v == null) return ''
            const n = Number(v)
            return isFinite(n) ? n.toFixed(2) : ''
          }
        },
        data: keyLevels.value.map(level => ({ yAxis: level }))
      } : undefined

      return {
        animation: false,
        color: ['#4ECDC4', '#ffbf00', '#f92672'],
        title: {
          text: selectedStock.value ? `${selectedStock.value.名称} (${selectedStock.value.代码})` : 'K线图',
          left: 'center',
          textStyle: {
            color: '#333',
            fontSize: 16
          }
        },
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'cross'
          },
          formatter: function(params) {
            let result = `${params[0].axisValue}<br/>`
            params.forEach(param => {
              if (param.seriesName === 'K线') {
                const data = param.data
                result += `开盘: ${data[1]}<br/>收盘: ${data[2]}<br/>最低: ${data[3]}<br/>最高: ${data[4]}<br/>`
              } else if (param.seriesName === '成交额') {
                result += `成交额: ${utils.formatVolume(param.value)}<br/>`
              } else {
                result += `${param.seriesName}: ${param.value}<br/>`
              }
            })
            return result
          }
        },
        legend: {
          data: [ 'MA5', 'MA10', 'MA20', ],
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
            splitNumber: 20,
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
            splitNumber: 20,
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
            },
            markLine: levelMarkLine
          },
          {
            name: 'MA5',
            type: 'line',
            data: ma5Data,
            smooth: true,
            symbol: 'none',
            lineStyle: {
              opacity: 0.8,
              color: '#4ECDC4'
            },
            itemStyle: {
              color: '#4ECDC4'  // 统一图例颜色
            }
          },
          {
            name: 'MA10',
            type: 'line',
            data: ma10Data,
            smooth: true,
            symbol: 'none',
            lineStyle: {
              opacity: 0.8,
              color: '#ffbf00'
            },
            itemStyle: {
              color: '#ffbf00'  // 统一图例颜色
            }
          },
          {
            name: 'MA20',
            type: 'line',
            data: ma20Data,
            smooth: true,
            symbol: 'none',
            lineStyle: {
              opacity: 0.8,
              color: '#f92672'
            },
            itemStyle: {
              color: '#f92672'  // 统一图例颜色
            }
          },
          {
            name: '成交额',
            type: 'bar',
            xAxisIndex: 1,
            yAxisIndex: 1,
            data: volumeData
          }
        ]
      }
    })

    // 分析赚钱效应 - 同时获取两种类型的数据
    const analyzeMoneyEffect = async () => {
      if (!selectedDate.value) {
        ElMessage.warning('请选择分析日期')
        return
      }

      analyzing.value = true
      try {
        console.log('🔍 开始分析赚钱效应...', {
          date: selectedDate.value
        })

        // 同时请求两种类型的数据
        const [allStocksResponse, strongStocksResponse] = await Promise.all([
          axios.get(`${API_BASE}/api/money-effect`, {
            params: {
              date: selectedDate.value,
              type: 'all',
              exclude_st: filterOptions.value.excludeST,
              include_non_main_board: filterOptions.value.includeNonMainBoard
            }
          }),
          axios.get(`${API_BASE}/api/money-effect`, {
            params: {
              date: selectedDate.value,
              type: 'strong',
              exclude_st: filterOptions.value.excludeST,
              include_non_main_board: filterOptions.value.includeNonMainBoard
            }
          })
        ])

        // 处理全部股票数据
        if (allStocksResponse.data.success) {
          allStocksList.value = allStocksResponse.data.stocks || []
          allStatsData.value = allStocksResponse.data.stats || {}
          console.log('✅ 全部股票分析完成', {
            stockCount: allStocksList.value.length
          })
        } else {
          allStocksList.value = []
          allStatsData.value = null
        }

        // 处理强势股数据
        if (strongStocksResponse.data.success) {
          strongStocksList.value = strongStocksResponse.data.stocks || []
          strongStatsData.value = strongStocksResponse.data.stats || {}
          console.log('✅ 强势股分析完成', {
            stockCount: strongStocksList.value.length
          })
        } else {
          strongStocksList.value = []
          strongStatsData.value = null
        }

        // 显示结果消息
        const totalStocks = allStocksList.value.length + strongStocksList.value.length
        if (totalStocks > 0) {
          ElMessage.success(`分析完成：全部股票${allStocksList.value.length}只，强势股${strongStocksList.value.length}只`)
        } else {
          ElMessage.warning('当前日期无数据')
        }

      } catch (error) {
        console.error('❌ 赚钱效应分析失败:', error)
        ElMessage.error('分析失败: ' + (error.response?.data?.message || error.message))
        allStocksList.value = []
        strongStocksList.value = []
        allStatsData.value = null
        strongStatsData.value = null
      } finally {
        analyzing.value = false
      }
    }

    // 选择股票
    const selectStock = async (stock) => {
      selectedStock.value = stock
      await loadKlineChart(stock)
    }

    // 处理表格行选择
    const handleStockSelect = (currentRow) => {
      if (currentRow) {
        selectStock(currentRow)
      }
    }

    // 加载K线图
    const loadKlineChart = async (stock) => {
      if (!stock) return

      klineLoading.value = true
      try {
        console.log('📈 加载K线图:', stock.名称, stock.代码)

        // 扩大量级到近十年（约3650天），尽量覆盖历史高点
        const response = await ApiService.getStockKline(stock.代码, 3650)
        // 修复数据路径：response.data.data.kline_data
        klineData.value = response.data?.data?.kline_data || []

        // 优先从后端获取带缓存的关键位，失败则在前端计算
        try {
          const levelsResp = await ApiService.getStockLevels(stock.代码, 3650)
          const levels = levelsResp?.data?.levels || []
          keyLevels.value = Array.isArray(levels) ? levels : []
        } catch (e) {
          computeKeyLevels()
        }

        console.log(`✅ 获取 ${stock.代码} K线数据: ${klineData.value.length} 条记录`)
      } catch (error) {
        console.error('❌ K线图加载失败:', error)
        ElMessage.error('K线图加载失败: ' + (error.response?.data?.message || error.message))
        klineData.value = []
        keyLevels.value = []
      } finally {
        klineLoading.value = false
      }
    }

    // 计算关键位（轻量版）：
    // - 使用价格-成交额直方图找高成交额节点(HVN)
    // - 提取局部摆动高点
    // - 合并去重，仅保留当前价到历史高点之间的若干价位
    const computeKeyLevels = () => {
      try {
        const data = klineData.value || []
        if (!data.length) { keyLevels.value = []; return }

        const closes = data.map(d => Number(d.close) || 0)
        const highs = data.map(d => Number(d.high) || 0)
        const lows = data.map(d => Number(d.low) || 0)
        const amounts = data.map(d => Number(d.amount) || Number(d.volume) || 0)

        const currentPrice = closes[closes.length - 1]
        const allTimeHigh = Math.max(...highs.filter(v => isFinite(v)))
        const allTimeLow = Math.min(...lows.filter(v => isFinite(v)))
        if (!isFinite(allTimeHigh) || !isFinite(allTimeLow) || allTimeHigh <= allTimeLow) {
          keyLevels.value = []
          return
        }

        const priceMin = allTimeLow
        const priceMax = allTimeHigh
        const priceRange = priceMax - priceMin
        if (!isFinite(priceRange) || priceRange <= 0) { keyLevels.value = []; return }

        // 1) 成交额-价格直方图（用收盘价分箱）
        const numBins = Math.min(60, Math.max(20, Math.floor(data.length / 15)))
        const binSize = priceRange / numBins
        if (!isFinite(binSize) || binSize <= 0) { keyLevels.value = []; return }
        const bins = new Array(numBins).fill(0)

        for (let i = 0; i < data.length; i++) {
          const c = closes[i]
          if (c >= priceMin && c <= priceMax) {
            let idx = Math.floor((c - priceMin) / binSize)
            if (idx >= numBins) idx = numBins - 1
            if (idx < 0) idx = 0
            const w = amounts[i] || 0
            bins[idx] += isFinite(w) ? w : 0
          }
        }

        // 识别局部峰值 + Z分数阈值
        const mean = bins.reduce((a,b)=>a+b,0) / (bins.length || 1)
        const sd = Math.sqrt(bins.reduce((s,v)=>s + Math.pow(v - mean, 2), 0) / (bins.length || 1))
        const hvnCandidates = []
        for (let i = 1; i < bins.length - 1; i++) {
          const v = bins[i]
          if (v > bins[i-1] && v >= bins[i+1]) {
            const z = sd > 0 ? (v - mean) / sd : 0
            if (z > 1.0) {
              const priceAtBin = priceMin + (i + 0.5) * binSize
              hvnCandidates.push({ price: priceAtBin, score: v })
            }
          }
        }
        hvnCandidates.sort((a,b)=>b.score - a.score)

        // 2) 摆动高点（简单局部最高）与摆动低点（简单局部最低）
        const swingHighCandidates = []
        for (let i = 2; i < highs.length - 2; i++) {
          const h = highs[i]
          if (
            isFinite(h) && h >= priceMin && h <= priceMax &&
            h > highs[i-1] && h >= highs[i+1] &&
            h > highs[i-2] && h >= highs[i+2]
          ) {
            swingHighCandidates.push({ price: h, score: h })
          }
        }
        const swingLowCandidates = []
        for (let i = 2; i < lows.length - 2; i++) {
          const lo = lows[i]
          if (
            isFinite(lo) && lo >= priceMin && lo <= priceMax &&
            lo < lows[i-1] && lo <= lows[i+1] &&
            lo < lows[i-2] && lo <= lows[i+2]
          ) {
            swingLowCandidates.push({ price: lo, score: lo })
          }
        }

        // 3) 合并去重，优先保留HVN -> 高点 -> 低点
        const minGap = Math.max(priceRange * 0.02, binSize * 0.8)
        const merged = []
        const pushIfFar = (p) => {
          for (let j = 0; j < merged.length; j++) {
            if (Math.abs(merged[j] - p) < minGap) return false
          }
          merged.push(p)
          return true
        }

        for (const item of hvnCandidates) {
          if (merged.length >= 8) break
          pushIfFar(item.price)
        }
        if (merged.length < 8) {
          for (const item of swingHighCandidates) {
            if (merged.length >= 8) break
            pushIfFar(item.price)
          }
        }
        if (merged.length < 8) {
          for (const item of swingLowCandidates) {
            if (merged.length >= 8) break
            pushIfFar(item.price)
          }
        }

        // 升序排序
        merged.sort((a,b)=>a-b)
        keyLevels.value = merged
      } catch (e) {
        console.warn('计算关键位失败:', e)
        keyLevels.value = []
      }
    }

    // 刷新数据
    const refreshData = async () => {
      refreshing.value = true
      try {
        await analyzeMoneyEffect()
        ElMessage.success('数据刷新完成')
      } catch (error) {
        ElMessage.error('数据刷新失败')
      } finally {
        refreshing.value = false
      }
    }

    // 导出数据
    const exportData = (type = 'all') => {
      let dataToExport = []
      let filename = ''

      if (type === 'all') {
        if (allStocksList.value.length === 0) {
          ElMessage.warning('暂无全部股票数据可导出')
          return
        }
        dataToExport = allStocksList.value
        filename = `全部股票赚钱效应_${selectedDate.value}.csv`
      } else if (type === 'strong') {
        if (strongStocksList.value.length === 0) {
          ElMessage.warning('暂无强势股数据可导出')
          return
        }
        dataToExport = strongStocksList.value
        filename = `强势股赚钱效应_${selectedDate.value}.csv`
      }

      try {
        const csvContent = generateCSV(dataToExport, type)
        downloadCSV(csvContent, filename)
        ElMessage.success('数据导出成功')
      } catch (error) {
        console.error('导出失败:', error)
        ElMessage.error('数据导出失败')
      }
    }

    // 生成CSV内容
    const generateCSV = (data, type = 'all') => {
      let headers = ['名称', '代码', '连板天数', '最低到收盘涨幅', '当日涨跌幅', '5日涨跌幅', '10日涨跌幅', '20日涨跌幅', '收盘价', '成交额']

      // 强势股类型增加历史最高连板列
      if (type === 'strong') {
        headers = ['名称', '代码', '连板天数', '历史最高连板', '最低到收盘涨幅', '当日涨跌幅', '5日涨跌幅', '10日涨跌幅', '20日涨跌幅', '收盘价', '成交额']
      }

      const csvRows = [headers.join(',')]

      data.forEach(row => {
        const values = headers.map(header => {
          const value = row[header]
          return typeof value === 'string' ? `"${value}"` : value
        })
        csvRows.push(values.join(','))
      })

      return csvRows.join('\n')
    }

    // 下载CSV文件
    const downloadCSV = (content, filename) => {
      const blob = new Blob(['\ufeff' + content], { type: 'text/csv;charset=utf-8;' })
      const link = document.createElement('a')
      const url = URL.createObjectURL(blob)
      link.setAttribute('href', url)
      link.setAttribute('download', filename)
      link.style.visibility = 'hidden'
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
    }

    // 获取涨跌幅样式类
    const getChangeClass = (value) => {
      if (value === null || value === undefined) return ''
      const num = Number(value)
      if (num > 0) return 'positive'
      if (num < 0) return 'negative'
      return ''
    }

    // 获取连板标签类型
    const getBoardTagType = (days) => {
      if (days >= 6) return 'danger'
      if (days >= 4) return 'warning'
      return 'success'
    }

    // 监听日期变化
    watch(selectedDate, () => {
      if (allStocksList.value.length > 0 || strongStocksList.value.length > 0) {
        analyzeMoneyEffect()
      }
    })

    // 组件挂载时初始化
    onMounted(() => {
      analyzeMoneyEffect()
    })

    return {
      selectedDate,
      analyzing,
      refreshing,
      allStocksList,
      strongStocksList,
      selectedStock,
      klineData,
      klineOption,
      klineLoading,
      keyLevels,
      allStatsData,
      strongStatsData,
      filterOptions,
      analyzeMoneyEffect,
      selectStock,
      handleStockSelect,
      refreshData,
      exportData,
      formatNumber,
      formatPercent,
      formatVolume,
      getChangeClass,
      getBoardTagType,
      stockSearchQuery,
      queryStockSuggestions,
      onStockSuggestionSelect
    }
  }
}
</script>

<style scoped>
.money-effect {
  padding: 20px;
  background-color: #f5f7fa;
  min-height: 100vh;
}

.page-header {
  text-align: center;
  margin-bottom: 30px;
}

.page-header h1 {
  color: #2c3e50;
  margin-bottom: 10px;
  font-size: 28px;
  font-weight: 600;
}

.page-description {
  color: #7f8c8d;
  font-size: 16px;
  margin: 0;
}

.control-panel {
  margin-bottom: 20px;
}

.control-item {
  display: flex;
  flex-direction: column;
}

.control-item label {
  font-weight: 500;
  color: #606266;
  margin-bottom: 8px;
  font-size: 14px;
}

.stats-panel {
  margin-bottom: 20px;
}

.stat-item {
  text-align: center;
  padding: 15px;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  border-radius: 8px;
  color: white;
}

.stat-value {
  font-size: 24px;
  font-weight: bold;
  margin-bottom: 5px;
}

.stat-value.positive {
  color: #67c23a;
}

.stat-label {
  font-size: 12px;
  opacity: 0.9;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: 600;
  color: #2c3e50;
}

.stock-list-card {
  height: 100%;
}

.kline-card {
  height: 100%;
}

.kline-container {
  min-height: 500px;
  display: flex;
  flex-direction: column;
  border: 1px solid #e4e7ed;
  border-radius: 4px;
  background-color: #fafafa;
}

.kline-header {
  padding: 15px;
  border-bottom: 1px solid #e4e7ed;
  background-color: #fff;
}

.kline-header h4 {
  margin: 0 0 10px 0;
  color: #2c3e50;
  font-size: 16px;
  font-weight: 600;
}

.stock-info {
  display: flex;
  gap: 20px;
  font-size: 14px;
}

.info-item {
  color: #606266;
}

.loading-container {
  display: flex;
  flex-direction: column;
  align-items: center;
  color: #909399;
}

.loading-container .el-icon {
  font-size: 32px;
  margin-bottom: 10px;
}

.no-data, .no-selection {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  color: #c0c4cc;
  font-size: 16px;
  flex: 1;
  padding: 40px 20px;
}

.no-data .el-icon, .no-selection .el-icon {
  font-size: 48px;
  margin-bottom: 10px;
}

.kline-chart {
  width: 100%;
  height: 450px;
  flex: 1;
  overflow: hidden;
}

/* 表格样式 */
.positive {
  color: #f56c6c;
  font-weight: 500;
}

.negative {
  color: #67c23a;
  font-weight: 500;
}

.price {
  font-weight: 500;
  color: #2c3e50;
}

.volume {
  color: #909399;
  font-size: 12px;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .money-effect {
    padding: 10px;
  }

  .page-header h1 {
    font-size: 24px;
  }

  .stat-item {
    margin-bottom: 10px;
  }

  .stat-value {
    font-size: 20px;
  }

  /* 小屏幕上改为上下布局 */
  .el-row .el-col:first-child {
    margin-bottom: 20px;
  }

  .kline-container {
    min-height: 300px;
  }

  .kline-chart {
    height: 300px;
  }

  .stock-info {
    flex-direction: column;
    gap: 10px;
  }
}

/* 表格行悬停效果 */
.el-table tbody tr:hover > td {
  background-color: #f5f7fa !important;
}

/* 按钮样式优化 */
.el-button--text {
  color: #409eff;
  font-weight: 500;
}

.el-button--text:hover {
  color: #66b1ff;
  background-color: #ecf5ff;
}

/* 标签样式 */
.el-tag {
  font-weight: 500;
}
</style>
