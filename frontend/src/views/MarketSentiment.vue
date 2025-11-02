<template>
  <div class="market-sentiment">
    <div class="header">
      <div class="header-content">
        <div>
          <h1>🎭 市场情绪分析</h1>
          <p>全面分析市场情绪指标，把握市场脉搏</p>
        </div>
        <div class="header-actions">
          <el-button type="primary" @click="refreshCharts" :loading="loading">
            <el-icon><Refresh /></el-icon>
            刷新图表
          </el-button>
        </div>
      </div>
    </div>

    <!-- 控制面板 -->
    <div class="controls">
      <el-row :gutter="20">
        <el-col :span="6">
          <TradingDatePicker
            v-model="selectedDate"
            type="date"
            placeholder="选择分析日期"
            format="YYYY-MM-DD"
            value-format="YYYY-MM-DD"
            :enable-holiday-marking="true"
            :disable-non-trading-days="true"
            :disable-future-dates="true"
            @change="loadData"
          />
        </el-col>
        <el-col :span="6">
          <el-select v-model="daysBack" placeholder="分析周期" @change="loadData">
            <el-option label="最近7天" :value="7" />
            <el-option label="最近15天" :value="15" />
            <el-option label="最近30天" :value="30" />
            <el-option label="最近60天" :value="60" />
            <el-option label="最近90天" :value="90" />
          </el-select>
        </el-col>
        <el-col :span="6">
          <el-button type="primary" @click="loadData" :loading="loading">
            <i class="el-icon-refresh"></i> 刷新数据
          </el-button>
        </el-col>
      </el-row>
    </div>

    <!-- 加载状态 -->
    <div v-if="loading" class="loading">
      <el-skeleton :rows="5" animated />
    </div>

    <!-- 市场情绪概览 -->
    <div v-if="!loading" class="sentiment-overview">
      <el-row :gutter="20">
        <el-col :span="6">
          <el-card class="metric-card">
            <div class="metric">
              <div class="metric-value">{{ formatPercentage(sentimentData.red_ratio) }}%</div>
              <div class="metric-label">红盘率</div>
              <div class="metric-change" v-if="sentimentData.changes">
                <span :class="getChangeClass(sentimentData.changes.red_ratio_change)">
                  {{ formatChange(sentimentData.changes.red_ratio_change, '%') }}
                </span>
              </div>
              <div class="metric-trend" :class="getTrendClass(sentimentData.red_ratio, 50)">
                {{ getTrendIcon(sentimentData.red_ratio, 50) }}
              </div>
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card class="metric-card">
            <div class="metric">
              <div class="metric-value">{{ sentimentData.limit_up_count || 0 }}</div>
              <div class="metric-label">涨停数量</div>
              <div class="metric-change" v-if="sentimentData.changes">
                <span :class="getChangeClass(sentimentData.changes.limit_up_change)">
                  {{ formatChange(sentimentData.changes.limit_up_change) }}
                </span>
              </div>
              <div class="metric-trend positive">📈</div>
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card class="metric-card">
            <div class="metric">
              <div class="metric-value">{{ sentimentData.limit_down_count || 0 }}</div>
              <div class="metric-label">跌停数量</div>
              <div class="metric-change" v-if="sentimentData.changes">
                <span :class="getChangeClass(sentimentData.changes.limit_down_change)">
                  {{ formatChange(sentimentData.changes.limit_down_change) }}
                </span>
              </div>
              <div class="metric-trend negative">📉</div>
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card class="metric-card">
            <div class="metric">
              <div class="metric-value">{{ formatAmount(sentimentData.total_amount) }}</div>
              <div class="metric-label">沪深成交额</div>
              <div class="metric-change" v-if="sentimentData.changes">
                <span :class="getChangeClass(sentimentData.changes.total_amount_change)">
                  {{ formatAmountChange(sentimentData.changes.total_amount_change, sentimentData.changes.total_amount_change_pct) }}
                </span>
              </div>
              <div class="metric-trend">💰</div>
            </div>
          </el-card>
        </el-col>
      </el-row>
    </div>

    <!-- 图表区域 -->
    <div v-if="!loading" class="charts-section">
      <!-- 涨跌幅分布和地天炸板分析 -->
      <div class="charts-row">
        <!-- 左半部分：涨跌幅分布图 -->
        <el-card class="chart-card chart-half">
          <template #header>
            <div class="card-header">
              <span>📊 涨跌幅分布</span>
            </div>
          </template>
          <div v-if="charts.change_distribution"
               v-html="charts.change_distribution"
               class="chart-container">
          </div>
          <div v-else class="no-data">暂无涨跌幅分布数据</div>
        </el-card>

        <!-- 右半部分：地天炸板图 -->
        <el-card class="chart-card chart-half">
          <template #header>
            <div class="card-header">
              <span>⚡ 地天板|天地板|炸板分析</span>
            </div>
          </template>
          <div v-if="charts.ground_ceiling_explosion"
               v-html="charts.ground_ceiling_explosion"
               class="chart-container">
          </div>
          <div v-else class="no-data">暂无地天炸板数据</div>
        </el-card>
      </div>

      <!-- 红盘率量能分析和涨跌停统计并排显示 -->
      <div class="charts-row">
        <!-- 左半部分：市场红盘率与量能分析 -->
        <el-card class="chart-card chart-half">
          <template #header>
            <div class="card-header">
              <span>🔴 市场红盘率与量能分析</span>
            </div>
          </template>
          <div class="combined-chart-container">
            <!-- 红盘率部分 -->
            <div class="red-ratio-section">
              <div v-if="charts.red_ratio_chart"
                   v-html="charts.red_ratio_chart"
                   class="chart-container">
              </div>
              <div v-else class="no-data">暂无红盘率数据</div>
            </div>
          </div>
        </el-card>

        <!-- 右半部分：涨停/跌停股数量走势 -->
        <el-card class="chart-card chart-half">
          <template #header>
            <div class="card-header">
              <span>📊 涨停/跌停股数量走势</span>
            </div>
          </template>
          <div v-if="charts.limit_counts"
               v-html="charts.limit_counts"
               class="chart-container">
          </div>
          <div v-else class="no-data">暂无涨跌停数据</div>
        </el-card>
      </div>


      <!-- 连板分布 -->
      <el-card class="chart-card">
        <template #header>
          <div class="card-header">
            <span>🔥 连板分布统计</span>
          </div>
        </template>
        <div v-if="charts.continuous_limit"
             v-html="charts.continuous_limit"
             class="chart-container">
        </div>
        <div v-else class="no-data">暂无连板数据</div>
      </el-card>
    </div>

    <!-- 错误提示 -->
    <div v-if="error" class="error-message">
      <el-alert
        :title="error"
        type="error"
        show-icon
        :closable="false">
      </el-alert>
    </div>
  </div>
</template>

<script>
import ApiService from '@/services/api'
import { ElMessage } from 'element-plus'
import TradingDatePicker from '@/components/TradingDatePicker.vue'

export default {
  name: 'MarketSentiment',
  components: {
    TradingDatePicker
  },
  data() {
    return {
      loading: false,
      error: null,
      selectedDate: '2025-07-15', // 使用最新可用的数据日期
      daysBack: 30,
      sentimentData: {},
      metadataStats: {},
      charts: {}
    }
  },
  async mounted() {
    await this.initializeLatestDate()
    this.loadData()
  },
  methods: {
    async initializeLatestDate() {
      try {
        const response = await ApiService.getLatestMarketDate()
        if (response.success && response.data.latest_date) {
          this.selectedDate = response.data.latest_date
          console.log(`🔧 DEBUG: 设置最新可用日期: ${this.selectedDate}`)
        }
      } catch (error) {
        console.warn('获取最新市场日期失败，使用当前日期:', error)
        // 保持默认的当前日期
      }
    },



    async loadData() {
      this.loading = true
      this.error = null

      try {
        // 并行加载数据
        console.log(`🔧 DEBUG: 加载数据，日期: ${this.selectedDate}, 周期: ${this.daysBack}天`)
        const [sentimentResponse, metadataResponse, chartsResponse] = await Promise.all([
          ApiService.getMarketSentiment(this.selectedDate),
          ApiService.getMarketMetadata(this.daysBack),
          ApiService.getMarketSentimentCharts(this.selectedDate, this.daysBack)  // 传递daysBack参数
        ])

        if (sentimentResponse.success) {
          this.sentimentData = sentimentResponse.data
          console.log('市场情绪数据:', this.sentimentData)
        } else {
          console.error('市场情绪API失败:', sentimentResponse)
        }

        if (metadataResponse.success) {
          this.metadataStats = metadataResponse.data.stats || {}
          console.log('元数据统计:', this.metadataStats)
        } else {
          console.error('元数据API失败:', metadataResponse)
        }

        if (chartsResponse.success) {
          const rawCharts = chartsResponse.data.charts || {}
          console.log('🔧 DEBUG: 原始图表数据键名:', Object.keys(rawCharts))
          console.log('🔧 DEBUG: 涨跌幅分布图数据:', rawCharts.change_distribution ? '存在' : '不存在')

          // 直接使用Flask后端返回的字段名
          this.charts = {
            red_ratio_and_amount: rawCharts.red_ratio_and_amount,
            red_ratio_chart: rawCharts.red_ratio_chart || rawCharts.red_ratio_and_amount,
            limit_counts: rawCharts.limit_up_count,
            break_counts: rawCharts.ground_ceiling_count,
            continuous_limit: rawCharts.continuous_limit_up,
            ground_ceiling_explosion: rawCharts.ground_ceiling_explosion || rawCharts.ground_ceiling_count,
            change_distribution: rawCharts.change_distribution  // 添加涨跌幅分布图
          }

          console.log('🔧 DEBUG: 处理后的图表数据:', Object.keys(this.charts))
          console.log('🔧 DEBUG: 涨跌幅分布图内容长度:', this.charts.change_distribution ? this.charts.change_distribution.length : 0)

          console.log('映射后图表数据:', Object.keys(this.charts))
          console.log('图表内容检查:')
          Object.keys(this.charts).forEach(key => {
            const content = this.charts[key]
            console.log(`  ${key}: ${content ? '有内容(' + content.length + '字符)' : '无内容'}`)
          })

          // 在下一个tick中初始化图表
          this.$nextTick(() => {
            this.initializeCharts()
            // 移除createChangeDistributionChart()，现在使用后端生成的图表
          })
        } else {
          console.error('图表API失败:', chartsResponse)
        }

      } catch (error) {
        console.error('加载市场情绪数据失败:', error)
        this.error = '加载数据失败，请稍后重试'
      } finally {
        this.loading = false
      }
    },



    getTrendClass(value, threshold) {
      if (value > threshold) return 'positive'
      if (value < threshold) return 'negative'
      return 'neutral'
    },

    getTrendIcon(value, threshold) {
      if (value > threshold) return '📈'
      if (value < threshold) return '📉'
      return '➡️'
    },

    formatAmount(amount) {
      if (!amount) return '0亿'
      return `${amount}亿`
    },

    initializeCharts() {
      // 处理连板分布统计的复合图表结构
      this.$nextTick(() => {
        const chartContainers = document.querySelectorAll('.chart-container')
        chartContainers.forEach((container, index) => {
          if (container && container.innerHTML.trim()) {
            const chartContent = container.innerHTML

            // 检查是否是连板分布统计的复合结构
            if (chartContent.includes('ladder-chart-container')) {
              console.log('检测到连板分布统计复合结构，特殊处理')
              this.handleContinuousLimitChart(container, chartContent)
            }
            // 检查是否是完整的HTML文档
            else if (chartContent.includes('<!DOCTYPE html>')) {
              console.log('检测到完整HTML文档，使用iframe显示')

              // 创建iframe来显示完整的HTML
              const iframe = document.createElement('iframe')
              iframe.style.width = '100%'
              iframe.style.height = '500px'
              iframe.style.border = 'none'
              iframe.srcdoc = chartContent

              // 清空容器并添加iframe
              container.innerHTML = ''
              container.appendChild(iframe)

              console.log(`图表 ${index + 1} 已使用iframe显示`)
            } else {
              // 如果不是完整HTML，尝试直接显示
              console.log('检测到图表片段，直接显示')

              // 设置容器样式
              container.style.minHeight = '400px'
              container.style.width = '100%'

              // 执行JavaScript代码
              setTimeout(() => {
                const scripts = container.querySelectorAll('script')
                scripts.forEach(script => {
                  if (script.textContent) {
                    try {
                      eval(script.textContent)
                    } catch (error) {
                      console.error('JavaScript执行失败:', error)
                    }
                  }
                })
              }, 100)
            }
          }
        })
      })
    },

    handleContinuousLimitChart(container, chartContent) {
      // 处理连板分布统计的复合图表
      console.log('处理连板分布统计复合图表')

      // 创建一个包装容器，适配新的堆叠图高度
      const wrapper = document.createElement('div')
      wrapper.style.width = '100%'
      wrapper.style.minHeight = '700px'  // 增加高度以适配新的600px堆叠图
      wrapper.style.overflow = 'visible'  // 修复：允许内容溢出，避免图表被截断
      wrapper.innerHTML = chartContent

      // 清空原容器并添加包装容器
      container.innerHTML = ''
      container.appendChild(wrapper)

      // 等待DOM更新后处理图表切换
      setTimeout(() => {
        this.setupChartSwitching(wrapper)
      }, 100)
    },

    setupChartSwitching(wrapper) {
      // 设置图表切换功能
      const lineBtn = wrapper.querySelector('#lineBtn')
      const stackBtn = wrapper.querySelector('#stackBtn')
      const lineChart = wrapper.querySelector('#lineChart')
      const stackChart = wrapper.querySelector('#stackChart')

      if (lineBtn && stackBtn && lineChart && stackChart) {
        console.log('找到图表切换元素，设置事件监听')

        // 默认显示折线图（修复显示问题）
        lineChart.style.display = 'block'
        stackChart.style.display = 'none'
        lineBtn.classList.add('active')
        stackBtn.classList.remove('active')

        console.log('默认显示折线图')

        // 设置切换事件
        lineBtn.addEventListener('click', () => {
          lineChart.style.display = 'block'
          stackChart.style.display = 'none'
          lineBtn.classList.add('active')
          stackBtn.classList.remove('active')
          console.log('切换到折线图')

          // 重要：切换后重新渲染图表
          this.resizeChartsInContainer(lineChart)
        })

        stackBtn.addEventListener('click', () => {
          lineChart.style.display = 'none'
          stackChart.style.display = 'block'
          lineBtn.classList.remove('active')
          stackBtn.classList.add('active')
          console.log('切换到堆叠图')

          // 重要：切换后重新渲染图表
          this.resizeChartsInContainer(stackChart)
        })

        // 执行图表内的JavaScript代码
        this.executeChartScripts(lineChart)
        this.executeChartScripts(stackChart)

        // 初始化时重新渲染当前显示的图表
        this.resizeChartsInContainer(lineChart)
      } else {
        console.warn('未找到图表切换元素')
      }
    },

    executeChartScripts(chartContainer) {
      // 执行图表容器内的JavaScript代码
      if (chartContainer) {
        const scripts = chartContainer.querySelectorAll('script')
        scripts.forEach(script => {
          if (script.textContent) {
            try {
              eval(script.textContent)
              console.log('图表JavaScript代码执行成功')
            } catch (error) {
              console.error('图表JavaScript执行失败:', error)
            }
          }
        })
      }
    },

    resizeChartsInContainer(container) {
      // 重新渲染容器内的ECharts图表
      setTimeout(() => {
        if (window.echarts && container) {
          const chartElements = container.querySelectorAll('[_echarts_instance_]')
          chartElements.forEach(element => {
            const chartInstance = window.echarts.getInstanceByDom(element)
            if (chartInstance) {
              // 强制设置容器尺寸
              element.style.width = '100%'
              element.style.height = '500px'

              // 重新渲染图表
              chartInstance.resize()
              console.log('图表已重新渲染，容器:', element)
            }
          })
        }
      }, 100) // 延迟100ms确保DOM更新完成
    },

    async refreshCharts() {
      try {
        this.loading = true
        console.log(`🔧 DEBUG: 刷新图表，日期: ${this.selectedDate}, 周期: ${this.daysBack}天`)
        const response = await ApiService.getMarketSentimentCharts(this.selectedDate, this.daysBack)  // 传递daysBack参数

        if (response.success) {
          const rawCharts = response.data.charts || {}
          console.log('🔧 DEBUG: 刷新获取的图表键名:', Object.keys(rawCharts))

          // 直接使用Flask后端返回的字段名
          this.charts = {
            red_ratio_and_amount: rawCharts.red_ratio_and_amount,
            limit_counts: rawCharts.limit_up_count,
            break_counts: rawCharts.ground_ceiling_count,
            continuous_limit: rawCharts.continuous_limit_up,
            ground_ceiling_explosion: rawCharts.ground_ceiling_explosion || rawCharts.ground_ceiling_count,
            change_distribution: rawCharts.change_distribution  // 添加涨跌幅分布图
          }

          console.log('刷新图表成功:', Object.keys(this.charts))
          console.log('刷新后图表内容检查:')
          Object.keys(this.charts).forEach(key => {
            const content = this.charts[key]
            console.log(`  ${key}: ${content ? '有内容(' + content.length + '字符)' : '无内容'}`)
          })

          // 重新初始化图表
          this.$nextTick(() => {
            this.initializeCharts()
          })
        }
      } catch (error) {
        console.error('刷新图表失败:', error)
        this.$message.error('刷新图表失败')
      } finally {
        this.loading = false
      }
    },

    formatPercentage(value) {
      // 格式化百分比，保留两位小数
      if (value === null || value === undefined) {
        return '0.00'
      }
      return Number(value).toFixed(2)
    },

    formatAmount(value) {
      // 格式化金额，显示为亿元
      if (value === null || value === undefined || value === 0) {
        return '0亿'
      }
      return `${Number(value).toFixed(0)}亿`
    },

    // 新增：格式化变化值
    formatChange(value, unit = '') {
      if (value === null || value === undefined || value === 0) {
        return '无变化'
      }
      const prefix = value > 0 ? '+' : ''
      return `${prefix}${Number(value).toFixed(2)}${unit}`
    },

    // 新增：格式化成交额变化
    formatAmountChange(amountChange, percentChange) {
      if (amountChange === null || amountChange === undefined || amountChange === 0) {
        return '无变化'
      }
      const prefix = amountChange > 0 ? '+' : ''
      const amountStr = `${prefix}${Number(amountChange).toFixed(0)}亿`

      if (percentChange !== null && percentChange !== undefined && percentChange !== 0) {
        const pctPrefix = percentChange > 0 ? '+' : ''
        return `${amountStr} (${pctPrefix}${Number(percentChange).toFixed(1)}%)`
      }
      return amountStr
    },

    // 新增：获取变化样式类
    getChangeClass(value) {
      if (value === null || value === undefined || value === 0) {
        return 'change-neutral'
      }
      return value > 0 ? 'change-positive' : 'change-negative'
    },

    getTrendClass(value, threshold = 50) {
      if (value > threshold) return 'positive'
      if (value < threshold) return 'negative'
      return 'neutral'
    },

    getTrendIcon(value, threshold = 50) {
      if (value > threshold) return '📈'
      if (value < threshold) return '📉'
      return '➡️'
    },

    createChangeDistributionChart() {
      // 创建涨跌幅分布图
      const chartContainer = document.getElementById('changeDistributionChart')
      if (!chartContainer) {
        console.warn('涨跌幅分布图容器未找到')
        return
      }

      // 模拟涨跌幅分布数据（基于市场情绪数据）
      const distributionData = this.generateChangeDistributionData()

      // 确保ECharts已加载
      if (typeof echarts === 'undefined') {
        console.warn('ECharts未加载，尝试从CDN加载')
        this.loadEChartsAndCreateChart(chartContainer, distributionData)
        return
      }

      this.renderChangeDistributionChart(chartContainer, distributionData)
    },

    generateChangeDistributionData() {
      // 基于当前市场情绪数据生成涨跌幅分布
      const { up_count = 0, down_count = 0, flat_count = 0 } = this.sentimentData
      const total = up_count + down_count + flat_count

      if (total === 0) {
        return []
      }

      // 模拟各涨跌幅区间的分布
      const ranges = [
        { label: '跌停', range: '≤-9.5%', count: Math.floor(down_count * 0.05), color: '#8B0000' },
        { label: '大跌', range: '-9.5%~-5%', count: Math.floor(down_count * 0.15), color: '#DC143C' },
        { label: '中跌', range: '-5%~-3%', count: Math.floor(down_count * 0.25), color: '#FF6347' },
        { label: '小跌', range: '-3%~-1%', count: Math.floor(down_count * 0.35), color: '#FFA07A' },
        { label: '微跌', range: '-1%~0%', count: Math.floor(down_count * 0.20), color: '#FFB6C1' },
        { label: '平盘', range: '0%', count: flat_count, color: '#808080' },
        { label: '微涨', range: '0%~1%', count: Math.floor(up_count * 0.20), color: '#98FB98' },
        { label: '小涨', range: '1%~3%', count: Math.floor(up_count * 0.35), color: '#90EE90' },
        { label: '中涨', range: '3%~5%', count: Math.floor(up_count * 0.25), color: '#32CD32' },
        { label: '大涨', range: '5%~9.5%', count: Math.floor(up_count * 0.15), color: '#228B22' },
        { label: '涨停', range: '≥9.5%', count: Math.floor(up_count * 0.05), color: '#006400' }
      ]

      return ranges.map(item => ({
        ...item,
        percentage: total > 0 ? ((item.count / total) * 100).toFixed(2) : '0.00'
      }))
    },

    loadEChartsAndCreateChart(container, data) {
      // 动态加载ECharts
      const script = document.createElement('script')
      script.src = 'https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js'
      script.onload = () => {
        console.log('ECharts加载成功')
        this.renderChangeDistributionChart(container, data)
      }
      script.onerror = () => {
        console.error('ECharts加载失败')
        container.innerHTML = '<div class="no-data">图表加载失败</div>'
      }
      document.head.appendChild(script)
    },

    renderChangeDistributionChart(container, data) {
      // 渲染涨跌幅分布图
      const chart = echarts.init(container)

      const option = {
        title: {
          text: '涨跌幅分布',
          left: 'center',
          textStyle: {
            fontSize: 16,
            fontWeight: 'bold'
          }
        },
        tooltip: {
          trigger: 'item',
          formatter: function(params) {
            return `${params.name}<br/>
                    数量: ${params.data.count}只<br/>
                    占比: ${params.data.percentage}%<br/>
                    区间: ${params.data.range}`
          }
        },
        legend: {
          orient: 'vertical',
          right: 10,
          top: 'middle',
          textStyle: {
            fontSize: 12
          }
        },
        series: [{
          name: '涨跌幅分布',
          type: 'pie',
          radius: ['35%', '65%'],
          center: ['40%', '50%'],
          avoidLabelOverlap: false,
          itemStyle: {
            borderRadius: 5,
            borderColor: '#fff',
            borderWidth: 2
          },
          label: {
            show: false,
            position: 'center'
          },
          emphasis: {
            label: {
              show: true,
              fontSize: 14,
              fontWeight: 'bold',
              formatter: function(params) {
                return `${params.name}\n${params.data.count}只\n${params.data.percentage}%`
              }
            }
          },
          labelLine: {
            show: false
          },
          data: data.map(item => ({
            name: item.label,
            value: item.count,
            count: item.count,
            percentage: item.percentage,
            range: item.range,
            itemStyle: {
              color: item.color
            }
          }))
        }]
      }

      chart.setOption(option)

      // 响应式调整
      window.addEventListener('resize', () => {
        chart.resize()
      })

      console.log('涨跌幅分布图渲染完成')
    },

    createMarketVolumeChart() {
      // 创建市场量能折线图
      const chartContainer = document.getElementById('marketVolumeChart')
      if (!chartContainer) {
        console.warn('市场量能图容器未找到')
        return
      }

      // 生成市场量能数据
      const volumeData = this.generateMarketVolumeData()

      // 确保ECharts已加载
      if (typeof echarts === 'undefined') {
        console.warn('ECharts未加载，尝试从CDN加载')
        this.loadEChartsAndCreateVolumeChart(chartContainer, volumeData)
        return
      }

      this.renderMarketVolumeChart(chartContainer, volumeData)
    },

    generateMarketVolumeData() {
      // 生成最近30天的市场量能数据
      const days = 30
      const data = []
      const today = new Date()

      for (let i = days - 1; i >= 0; i--) {
        const date = new Date(today)
        date.setDate(date.getDate() - i)

        // 跳过周末
        if (date.getDay() === 0 || date.getDay() === 6) {
          continue
        }

        const dateStr = date.toISOString().split('T')[0]

        // 模拟市场量能数据（基于当前市场数据）
        const baseVolume = this.sentimentData.total_amount || 15000
        const randomFactor = 0.8 + Math.random() * 0.4 // 0.8-1.2的随机因子
        const volume = Math.round(baseVolume * randomFactor)

        data.push({
          date: dateStr,
          volume: volume,
          displayDate: `${date.getMonth() + 1}/${date.getDate()}`
        })
      }

      return data
    },

    loadEChartsAndCreateVolumeChart(container, data) {
      // 动态加载ECharts（如果还没加载）
      if (typeof echarts !== 'undefined') {
        this.renderMarketVolumeChart(container, data)
        return
      }

      const script = document.createElement('script')
      script.src = 'https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js'
      script.onload = () => {
        console.log('ECharts加载成功（量能图）')
        this.renderMarketVolumeChart(container, data)
      }
      script.onerror = () => {
        console.error('ECharts加载失败（量能图）')
        container.innerHTML = '<div class="no-data">图表加载失败</div>'
      }
      document.head.appendChild(script)
    },

    renderMarketVolumeChart(container, data) {
      // 渲染市场量能折线图
      const chart = echarts.init(container)

      const option = {
        title: {
          text: '',  // 移除标题，因为已经有section-title
          left: 'center',
          textStyle: {
            fontSize: 14,
            fontWeight: 'bold'
          }
        },
        tooltip: {
          trigger: 'axis',
          formatter: function(params) {
            const data = params[0]
            return `${data.axisValue}<br/>
                    成交额: ${data.value}亿元`
          }
        },
        grid: {
          left: '3%',
          right: '4%',
          bottom: '3%',
          containLabel: true
        },
        xAxis: {
          type: 'category',
          boundaryGap: false,
          data: data.map(item => item.displayDate),
          axisLabel: {
            fontSize: 12,
            color: '#666'
          },
          axisLine: {
            lineStyle: {
              color: '#ddd'
            }
          }
        },
        yAxis: {
          type: 'value',
          name: '成交额(亿)',
          nameTextStyle: {
            color: '#666',
            fontSize: 12
          },
          axisLabel: {
            fontSize: 12,
            color: '#666',
            formatter: '{value}亿'
          },
          axisLine: {
            lineStyle: {
              color: '#ddd'
            }
          },
          splitLine: {
            lineStyle: {
              color: '#f0f0f0'
            }
          }
        },
        series: [{
          name: '成交额',
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 6,
          lineStyle: {
            color: '#409EFF',
            width: 3
          },
          itemStyle: {
            color: '#409EFF',
            borderColor: '#fff',
            borderWidth: 2
          },
          areaStyle: {
            color: {
              type: 'linear',
              x: 0,
              y: 0,
              x2: 0,
              y2: 1,
              colorStops: [{
                offset: 0, color: 'rgba(64, 158, 255, 0.3)'
              }, {
                offset: 1, color: 'rgba(64, 158, 255, 0.05)'
              }]
            }
          },
          data: data.map(item => item.volume)
        }]
      }

      chart.setOption(option)

      // 响应式调整
      window.addEventListener('resize', () => {
        chart.resize()
      })

      console.log('市场量能折线图渲染完成')
    }
  }
}
</script>

<style scoped>
.market-sentiment {
  padding: 20px;
  background-color: #f5f7fa;
  min-height: 100vh;
}

.header {
  margin-bottom: 30px;
  padding: 20px;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  border-radius: 10px;
  color: white;
}

.header-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.header-actions {
  display: flex;
  gap: 10px;
}

.header h1 {
  color: #2c3e50;
  margin-bottom: 10px;
}

.header p {
  color: #7f8c8d;
  font-size: 16px;
}

.controls {
  margin-bottom: 30px;
  padding: 20px;
  background: white;
  border-radius: 8px;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
}

.loading {
  padding: 40px;
  background: white;
  border-radius: 8px;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
}

.sentiment-overview {
  margin-bottom: 30px;
}

.metric-card {
  margin-bottom: 20px;
}

.metric {
  text-align: center;
  position: relative;
}

.metric-value {
  font-size: 32px;
  font-weight: bold;
  color: #2c3e50;
  margin-bottom: 8px;
}

.metric-label {
  font-size: 14px;
  color: #7f8c8d;
  margin-bottom: 8px;
}

.metric-trend {
  font-size: 20px;
  position: absolute;
  top: 0;
  right: 0;
}

.metric-trend.positive {
  color: #67c23a;
}

.metric-trend.negative {
  color: #f56c6c;
}

.metric-trend.neutral {
  color: #909399;
}

/* 新增：变化显示样式 */
.metric-change {
  font-size: 12px;
  margin: 4px 0;
  font-weight: 500;
}

.change-positive {
  color: #f56c6c;
}

.change-negative {
  color: #67c23a;
}

.change-neutral {
  color: #909399;
}

.charts-section {
  display: flex;
  flex-direction: column;
  gap: 30px;
}

.chart-card {
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: bold;
  color: #2c3e50;
}

.chart-container {
  min-height: 400px;
  width: 100%;
  padding: 10px;
  position: relative;
}

/* 连板天梯图表特殊样式 */
.chart-container:has(.ladder-chart-container) {
  min-height: 700px;
  padding: 15px;
}

/* 连板天梯复合图表容器 */
.ladder-chart-container {
  width: 100%;
  min-height: 650px;
  background: #fafafa;
  border-radius: 8px;
  padding: 10px;
}

/* 连板天梯切换按钮样式 */
.ladder-chart-container .chart-controls {
  margin-bottom: 15px;
  text-align: center;
}

.ladder-chart-container .chart-controls button {
  margin: 0 5px;
  padding: 8px 16px;
  border: 1px solid #dcdfe6;
  background: white;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.3s;
}

.ladder-chart-container .chart-controls button.active {
  background: #409eff;
  color: white;
  border-color: #409eff;
}

.ladder-chart-container .chart-controls button:hover {
  border-color: #409eff;
  color: #409eff;
}

/* 确保ECharts容器有正确的尺寸 */
.chart-container > div {
  width: 100% !important;
  min-height: 380px !important;
}

/* 连板天梯图表的ECharts容器 */
.ladder-chart-container .chart-content {
  width: 100% !important;
  min-height: 500px !important;
}

/* 连板天梯图表内的ECharts实例 */
.ladder-chart-container .chart-content > div {
  width: 100% !important;
  height: 500px !important;
}

/* ECharts canvas样式 */
.chart-container canvas {
  width: 100% !important;
  height: auto !important;
}

.no-data {
  text-align: center;
  padding: 60px;
  color: #909399;
  font-size: 16px;
}

.error-message {
  margin-top: 20px;
}

/* 左右布局样式 */
.charts-row {
  display: flex;
  gap: 20px;
  margin-bottom: 20px;
}

.chart-half {
  flex: 1;
  min-width: 0; /* 防止flex项目溢出 */
}

/* 确保涨跌幅分布图容器有正确的尺寸 */
#changeDistributionChart {
  width: 100%;
  height: 350px;
}

/* 市场量能折线图容器 */
#marketVolumeChart {
  width: 100%;
  height: 300px;
}



/* 合并图表容器样式 */
.combined-chart-container {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.red-ratio-section {
  flex: 1;
}

.volume-section {
  flex: 1;
}

.section-title {
  margin: 0 0 15px 0;
  font-size: 16px;
  font-weight: bold;
  color: #333;
  border-left: 4px solid #409EFF;
  padding-left: 10px;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .charts-row {
    flex-direction: column;
  }

  .chart-half {
    flex: none;
  }

  #marketVolumeChart {
    height: 250px;
  }

  .section-title {
    font-size: 14px;
  }
}
</style>
