<template>
  <div class="index-analysis">
    <div class="page-header">
      <h1>📈 指数分析</h1>
    </div>

    <!-- 控制面板 -->
    <div class="control-panel">
      <el-row :gutter="20">
        <el-col :span="8">
          <trading-date-picker
            v-model="selectedDate"
            placeholder="选择分析日期"
            format="YYYY-MM-DD"
            value-format="YYYYMMDD"
            @change="onDateChange"
          />
        </el-col>
        <el-col :span="8">
          <el-slider
            v-model="daysRange"
            :min="30"
            :max="365"
            :step="30"
            show-input
            @change="loadKlineData"
          />
          <span class="slider-label">天数范围: {{ daysRange }}天</span>
        </el-col>
        <el-col :span="8">
          <el-button type="primary" @click="refreshData" :loading="loading">
            <i class="el-icon-refresh"></i> 刷新数据
          </el-button>
        </el-col>
      </el-row>
    </div>

    <!-- 北证50和微盘股分析 -->
    <div class="sentiment-analysis">
      <el-row :gutter="20">
        <!-- 北证50 -->
        <el-col :span="8">
          <el-card class="sentiment-card">
            <template #header>
              <div class="card-header">
                <span>📊 北证50</span>
              </div>
            </template>
            <div v-if="beijingData.change_pct !== null">
              <div class="metric-value" :class="getChangeClass(beijingData.change_pct)">
                {{ beijingData.change_pct?.toFixed(2) }}%
              </div>
              <div class="sentiment-status" v-html="beijingData.status"></div>
            </div>
            <div v-else class="no-data">❌ 暂无数据</div>
          </el-card>
        </el-col>

        <!-- 微盘股 -->
        <el-col :span="8">
          <el-card class="sentiment-card">
            <template #header>
              <div class="card-header">
                <span>📈 微盘股</span>
              </div>
            </template>
            <div v-if="microcapData.change_pct !== null">
              <div class="metric-value" :class="getChangeClass(microcapData.change_pct)">
                {{ microcapData.change_pct?.toFixed(2) }}%
              </div>
              <div class="sentiment-status" v-html="microcapData.status"></div>
            </div>
            <div v-else class="no-data">❌ 暂无数据</div>
          </el-card>
        </el-col>

        <!-- 策略建议 -->
        <el-col :span="8">
          <el-card class="strategy-card">
            <template #header>
              <div class="card-header">
                <span>💡 策略建议</span>
              </div>
            </template>
            <div class="strategy-content">
              <div class="strategy-emoji">{{ strategy.emoji }}</div>
              <div class="strategy-text" :class="getStrategyClass(strategy.risk_level)">
                {{ strategy.strategy }}
              </div>
              <div class="strategy-description">{{ strategy.description }}</div>
            </div>
          </el-card>
        </el-col>
      </el-row>
    </div>

    <!-- 市场量能图 -->
    <div class="market-volume-chart">
      <el-card v-loading="loadingVolume">
        <template #header>
          <div class="card-header">
            <span>📈 市场量能分析</span>
            <el-button type="text" @click="loadVolumeData" :loading="loadingVolume">
              刷新量能数据
            </el-button>
          </div>
        </template>
        
        <div v-if="volumeChartOptions && Object.keys(volumeChartOptions).length > 0" class="chart-container">
          <v-chart
            ref="volumeChart"
            :option="volumeChartOptions"
            :style="{ height: '500px', width: '100%' }"
            autoresize
          />
        </div>
        <div v-else-if="volumeHtml" class="chart-container">
          <div ref="volumeChartContainer" v-html="volumeHtml"></div>
        </div>
        <div v-else-if="!loadingVolume" class="no-chart">
          <el-empty description="暂无市场量能数据" />
        </div>
      </el-card>
    </div>

    <!-- 指数选择 -->
    <div class="index-selection">
      <el-card>
        <template #header>
          <div class="card-header">
            <span>📊 指数K线图对比</span>
            <el-button type="text" @click="loadAvailableIndices" :loading="loadingIndices">
              刷新指数列表
            </el-button>
          </div>
        </template>
        
        <div class="index-selector">
          <el-checkbox-group v-model="selectedIndices" @change="loadKlineData">
            <el-checkbox 
              v-for="index in availableIndices" 
              :key="index.code" 
              :label="index.name"
              :disabled="!index.available"
            >
              {{ index.name }}
              <span v-if="!index.available" class="unavailable-tag">(暂无数据)</span>
            </el-checkbox>
          </el-checkbox-group>
        </div>
      </el-card>
    </div>

    <!-- K线图显示区域 -->
    <div class="kline-chart">
      <el-card v-loading="loadingKline">
        <template #header>
          <div class="card-header">
            <span>📈 多指数K线图对比</span>
            <span class="selected-count">已选择 {{ selectedIndices.length }} 个指数</span>
          </div>
        </template>
        
        <div v-if="chartOptions && chartOptions.type === 'multiple'" class="multiple-charts-container">
          <div
            v-for="chart in chartOptions.charts"
            :key="chart.name"
            class="single-chart-container"
          >
            <v-chart
              :ref="`chart-${chart.name}`"
              :option="chart.option"
              :style="{ height: '500px', width: '100%', marginBottom: '20px' }"
              autoresize
            />
          </div>
        </div>
        <div v-else-if="chartOptions && Object.keys(chartOptions).length > 0" class="chart-container">
          <v-chart
            ref="klineChart"
            :option="chartOptions"
            :style="{ height: '600px', width: '100%' }"
            autoresize
          />
        </div>
        <div v-else-if="klineHtml" class="chart-container">
          <div ref="chartContainer" v-html="klineHtml"></div>
        </div>
        <div v-else-if="!loadingKline" class="no-chart">
          <el-empty description="请选择指数以显示K线图" />
        </div>
      </el-card>
    </div>
  </div>
</template>

<script>
import ApiService from '@/services/api'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart, CandlestickChart, BarChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  ToolboxComponent
} from 'echarts/components'
import VChart from 'vue-echarts'
import TradingDatePicker from '@/components/TradingDatePicker.vue'

// 注册ECharts组件
use([
  CanvasRenderer,
  LineChart,
  CandlestickChart,
  BarChart,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  ToolboxComponent
])

export default {
  name: 'IndexAnalysis',
  components: {
    VChart,
    TradingDatePicker
  },
  data() {
    return {
      loading: false,
      loadingKline: false,
      loadingIndices: false,
      loadingVolume: false,
      selectedDate: new Date().toISOString().slice(0, 10), // 改为 YYYY-MM-DD 格式
      daysRange: 180,
      
      // 北证50和微盘股数据
      beijingData: {
        change_pct: null,
        status: '加载中...'
      },
      microcapData: {
        change_pct: null,
        status: '加载中...'
      },
      
      // 策略建议
      strategy: {
        strategy: '加载中...',
        emoji: '⏳',
        risk_level: 'unknown',
        description: '正在分析市场数据...'
      },
      
      // 市场量能相关
      volumeHtml: '',
      volumeChartOptions: null,
      volumeData: {
        current_total: 0,
        previous_total: 0,
        change_amount: 0,
        change_pct: 0
      },
      
      // 指数相关
      availableIndices: [],
      selectedIndices: ['上证指数', '中证2000', '创业板指'],
      klineHtml: '',
      chartOptions: null
    }
  },
  
  computed: {
    // 计算属性：将 YYYY-MM-DD 格式转换为 YYYYMMDD 格式
    formattedDate() {
      if (!this.selectedDate) return ''
      return this.selectedDate.replace(/-/g, '')
    }
  },
  
  mounted() {
    this.initData()
  },
  
  methods: {
    async onDateChange() {
      await this.loadAnalysisData()
      await this.loadVolumeData()
      await this.loadKlineData()
    },
    async initData() {
      await this.initializeLatestDate()
      await this.loadAvailableIndices()
      await this.loadAnalysisData()
      await this.loadVolumeData()
      await this.loadKlineData()
    },
    
    async loadAnalysisData() {
      this.loading = true
      try {
        const response = await ApiService.getIndicesAnalysis(this.formattedDate)
        
        if (response.success) {
          const data = response.data
          this.beijingData = data.beijing_data.beijing_50
          this.microcapData = data.beijing_data.microcap
          this.strategy = data.strategy
        }
      } catch (error) {
        console.error('加载分析数据失败:', error)
        this.$message.error('加载分析数据失败')
      } finally {
        this.loading = false
      }
    },

    async loadVolumeData() {
      this.loadingVolume = true
      try {
        console.log('🔄 开始加载市场量能数据...', { date: this.selectedDate })
        
        const response = await ApiService.getMarketVolume(this.selectedDate)
        
        console.log('📊 市场量能数据响应:', response)
        
        if (response.success) {
          const data = response.data
          this.volumeData = data.comparison_data
          
          // 优先使用ECharts配置
          if (data.chart_options) {
            this.volumeChartOptions = this.processChartOptions(data.chart_options)
            this.volumeHtml = ''
            console.log('✅ 使用ECharts配置渲染市场量能图')
          } else if (data.chart_html && data.chart_html.length > 0) {
            this.volumeHtml = data.chart_html
            this.volumeChartOptions = null
            console.log('✅ 使用HTML渲染市场量能图')
            // 强制Vue重新渲染
            this.$nextTick(() => {
              this.executeVolumeChartScripts()
            })
          } else {
            console.warn('⚠️ 没有可用的市场量能图表数据')
          }
        } else {
          console.error('❌ 市场量能数据加载失败:', response.data)
          this.$message.warning('暂无市场量能数据')
        }
      } catch (error) {
        console.error('❌ 加载市场量能数据失败:', error)
        this.$message.error('加载市场量能数据失败')
      } finally {
        this.loadingVolume = false
      }
    },
    
    async loadAvailableIndices() {
      this.loadingIndices = true
      try {
        const response = await ApiService.getIndicesAvailable()
        
        if (response.success) {
          this.availableIndices = response.data.available_indices
        }
      } catch (error) {
        console.error('加载指数列表失败:', error)
        this.$message.error('加载指数列表失败')
      } finally {
        this.loadingIndices = false
      }
    },
    
    async loadKlineData() {
      if (this.selectedIndices.length === 0) {
        this.klineHtml = ''
        return
      }

      this.loadingKline = true
      try {
        console.log('🔄 开始加载K线数据...', {
          date: this.formattedDate,
          days_range: this.daysRange,
          indices: this.selectedIndices
        })

        const response = await ApiService.getMultiIndexKline({
          selected_indices: this.selectedIndices,
          date_str: this.formattedDate,
          days_range: this.daysRange
        })

        console.log('📊 K线数据响应:', response)

        if (response.success) {
          // 修复数据路径
          const klineData = response.data?.kline_data || {}
          const chartHtml = klineData.chart_html
          const chartOptions = klineData.chart_options

          console.log('📈 K线图HTML长度:', chartHtml ? chartHtml.length : 0)
          console.log('📊 图表配置:', chartOptions ? '已获取' : '未获取')

          // 优先使用ECharts配置，如果没有则使用HTML
          if (chartOptions) {
            this.chartOptions = this.processChartOptions(chartOptions)
            this.klineHtml = ''
            console.log('✅ 使用ECharts配置渲染图表')
          } else if (chartHtml && chartHtml.length > 0) {
            this.klineHtml = chartHtml
            this.chartOptions = null
            console.log('✅ 使用HTML渲染图表')
            // 强制Vue重新渲染
            this.$nextTick(() => {
              console.log('🔄 Vue nextTick 完成，DOM应该已更新')
              this.executeChartScripts()
            })
          } else {
            console.warn('⚠️ 没有可用的图表数据')
          }
        } else {
          console.error('❌ K线数据加载失败:', response.data)
        }
      } catch (error) {
        console.error('❌ 加载K线数据失败:', error)
        this.$message.error('加载K线数据失败')
      } finally {
        this.loadingKline = false
      }
    },

    async initializeLatestDate() {
      try {
        const res = await ApiService.getLatestMarketDate()
        if (res.success && res.data.latest_date) {
          this.selectedDate = res.data.latest_date
        }
      } catch (e) {
        // 忽略，使用默认日期
      }
    },
    
    async refreshData() {
      await this.initData()
      this.$message.success('数据刷新完成')
    },

    executeVolumeChartScripts() {
      // 执行HTML中的JavaScript代码
      this.$nextTick(() => {
        const container = this.$refs.volumeChartContainer
        if (container) {
          const scripts = container.querySelectorAll('script')
          scripts.forEach(script => {
            try {
              // 创建新的script元素并执行
              const newScript = document.createElement('script')
              newScript.textContent = script.textContent
              document.head.appendChild(newScript)
              document.head.removeChild(newScript)
              console.log('📊 执行了市场量能图表脚本')
            } catch (error) {
              console.error('❌ 执行市场量能图表脚本失败:', error)
            }
          })
        }
      })
    },

    processChartOptions(options) {
      /**
       * 处理图表配置中的JavaScript函数
       */
      const processObject = (obj) => {
        if (Array.isArray(obj)) {
          return obj.map(item => processObject(item))
        } else if (obj && typeof obj === 'object') {
          const processed = {}
          for (const [key, value] of Object.entries(obj)) {
            if (key === '__js_function__' && typeof value === 'string') {
              // 将字符串形式的JavaScript函数转换为真正的函数
              try {
                processed[key.replace('__js_function__', '')] = new Function('return ' + value)()
                delete processed['__js_function__']
              } catch (e) {
                console.error('处理JavaScript函数失败:', e)
                processed[key] = value
              }
            } else if (typeof value === 'object' && value !== null && value.__js_function__) {
              // 处理包含__js_function__的对象
              try {
                processed[key] = new Function('return ' + value.__js_function__)()
              } catch (e) {
                console.error('处理JavaScript函数失败:', e)
                processed[key] = processObject(value)
              }
            } else {
              processed[key] = processObject(value)
            }
          }
          return processed
        }
        return obj
      }

      // 统一日期轴显示为 YYYY-MM-DD
      const formatDateTick = (value) => {
        if (value === null || value === undefined) return ''
        const str = String(value)
        if (/^\d{8}$/.test(str)) {
          return `${str.slice(0, 4)}-${str.slice(4, 6)}-${str.slice(6, 8)}`
        }
        if (/^\d{4}-\d{2}-\d{2}$/.test(str)) {
          return str
        }
        const d = new Date(str)
        if (!isNaN(d.getTime())) {
          const y = d.getFullYear()
          const m = String(d.getMonth() + 1).padStart(2, '0')
          const dd = String(d.getDate()).padStart(2, '0')
          return `${y}-${m}-${dd}`
        }
        return str
      }

      const enforceXAxisDateFormatter = (opt) => {
        const applyOnXAxis = (xAxis) => {
          if (Array.isArray(xAxis)) {
            xAxis.forEach(applyOnXAxis)
            return
          }
          if (!xAxis || typeof xAxis !== 'object') return
          // 仅对类目轴设置格式化
          if (!xAxis.type || xAxis.type === 'category') {
            xAxis.axisLabel = xAxis.axisLabel || {}
            if (typeof xAxis.axisLabel.formatter !== 'function') {
              xAxis.axisLabel.formatter = function(value) { return formatDateTick(value) }
            }
          }
        }

        if (!opt || typeof opt !== 'object') return opt
        if (opt.xAxis) applyOnXAxis(opt.xAxis)
        // 多图模式时，逐个应用
        if (Array.isArray(opt.charts)) {
          opt.charts.forEach(c => {
            if (c && c.option) {
              enforceXAxisDateFormatter(c.option)
            }
          })
        }
        return opt
      }

      console.log('🔧 处理图表配置中的JavaScript函数...')
      const processedOptions = processObject(options)
      console.log('✅ JavaScript函数处理完成')
      // 应用日期格式化
      const finalOptions = enforceXAxisDateFormatter(processedOptions)
      return finalOptions
    },

    executeChartScripts() {
      // 执行HTML中的JavaScript代码
      this.$nextTick(() => {
        const container = this.$refs.chartContainer
        if (container) {
          const scripts = container.querySelectorAll('script')
          scripts.forEach(script => {
            try {
              // 创建新的script元素并执行
              const newScript = document.createElement('script')
              newScript.textContent = script.textContent
              document.head.appendChild(newScript)
              document.head.removeChild(newScript)
              console.log('📊 执行了图表脚本')
            } catch (error) {
              console.error('❌ 执行图表脚本失败:', error)
            }
          })
        }
      })
    },

    getChangeClass(changePct) {
      if (changePct > 0) return 'positive'
      if (changePct < 0) return 'negative'
      return 'neutral'
    },
    
    getStrategyClass(riskLevel) {
      const classMap = {
        '激进': 'strategy-aggressive',
        '积极': 'strategy-positive',
        '中性': 'strategy-neutral',
        '谨慎': 'strategy-cautious',
        '保守': 'strategy-conservative',
        '观望': 'strategy-wait'
      }
      return classMap[riskLevel] || 'strategy-neutral'
    }
  }
}
</script>

<style scoped>
.index-analysis {
  padding: 20px;
}

.page-header {
  margin-bottom: 20px;
  text-align: center;
}

.page-header h1 {
  margin: 0 0 10px 0;
  color: #2c3e50;
}

.page-header p {
  margin: 0;
  color: #7f8c8d;
}

.control-panel {
  margin-bottom: 20px;
  padding: 20px;
  background: #f8f9fa;
  border-radius: 8px;
}

.slider-label {
  display: block;
  margin-top: 10px;
  font-size: 14px;
  color: #666;
}

.sentiment-analysis {
  margin-bottom: 20px;
}

.sentiment-card, .strategy-card {
  height: 200px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: bold;
}

.metric-value {
  font-size: 36px;
  font-weight: bold;
  text-align: center;
  margin: 20px 0;
}

.metric-value.positive {
  color: #e74c3c;
}

.metric-value.negative {
  color: #27ae60;
}

.metric-value.neutral {
  color: #95a5a6;
}

.sentiment-status {
  text-align: center;
  font-size: 16px;
  font-weight: bold;
}

.no-data {
  text-align: center;
  color: #95a5a6;
  font-size: 16px;
  margin-top: 50px;
}

.strategy-content {
  text-align: center;
  padding-top: 10px;
}

.strategy-emoji {
  font-size: 29px;
  margin: 0 0 3px 0;
}

.strategy-text {
  font-size: 16px;
  font-weight: bold;
  margin: 5px 0;
}

.strategy-aggressive {
  color: #e74c3c;
}

.strategy-positive {
  color: #3498db;
}

.strategy-neutral {
  color: #f39c12;
}

.strategy-cautious {
  color: #f39c12;
}

.strategy-conservative {
  color: #e74c3c;
}

.strategy-wait {
  color: #95a5a6;
}

.strategy-description {
  font-size: 14px;
  color: #7f8c8d;
  margin: 5px 0;
  line-height: 1.4;
}

.risk-warning {
  font-size: 12px;
  color: #e67e22;
  margin-top: 15px;
}

.market-volume-chart {
  margin-bottom: 20px;
}

.index-selection {
  margin-bottom: 20px;
}

.index-selector {
  display: flex;
  flex-wrap: wrap;
  gap: 15px;
}

.unavailable-tag {
  color: #95a5a6;
  font-size: 12px;
}

.selected-count {
  color: #3498db;
  font-size: 14px;
}

.kline-chart {
  margin-bottom: 20px;
}

.chart-container {
  min-height: 500px;
  width: 100%;
}

.multiple-charts-container {
  margin-top: 20px;
}

.single-chart-container {
  margin-bottom: 20px;
  padding: 20px;
  background: white;
  border-radius: 8px;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
}

.single-chart-container:last-child {
  margin-bottom: 0;
}

.no-chart {
  height: 400px;
  display: flex;
  align-items: center;
  justify-content: center;
}
</style>
