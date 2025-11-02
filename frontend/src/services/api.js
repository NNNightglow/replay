import axios from 'axios'
import { ElMessage, ElLoading } from 'element-plus'

// 创建axios实例
const api = axios.create({
  baseURL: 'http://localhost:5000',  // 修复：移除重复的/api路径
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json'
  }
})

// 全局加载实例
let loadingInstance = null

// 请求拦截器
api.interceptors.request.use(
  config => {
    // 显示加载动画
    if (!config.hideLoading) {
      loadingInstance = ElLoading.service({
        lock: true,
        text: '正在加载数据...',
        background: 'rgba(0, 0, 0, 0.7)'
      })
    }
    
    console.log(`🚀 API请求: ${config.method?.toUpperCase()} ${config.url}`)
    return config
  },
  error => {
    if (loadingInstance) {
      loadingInstance.close()
    }
    console.error('❌ 请求错误:', error)
    return Promise.reject(error)
  }
)

// 响应拦截器
api.interceptors.response.use(
  response => {
    // 关闭加载动画
    if (loadingInstance) {
      loadingInstance.close()
      loadingInstance = null
    }
    
    const { data } = response
    
    // 检查业务状态码
    if (data.success === false) {
      ElMessage.error(data.message || '请求失败')
      return Promise.reject(new Error(data.message || '请求失败'))
    }
    
    console.log(`✅ API响应: ${response.config.url}`, data)
    return data
  },
  error => {
    // 关闭加载动画
    if (loadingInstance) {
      loadingInstance.close()
      loadingInstance = null
    }
    
    console.error('❌ 响应错误:', error)
    
    // 处理不同类型的错误
    let message = '网络请求失败'
    
    if (error.response) {
      // 服务器响应了错误状态码
      const { status, data } = error.response
      switch (status) {
        case 400:
          message = '请求参数错误'
          break
        case 401:
          message = '未授权访问'
          break
        case 403:
          message = '禁止访问'
          break
        case 404:
          message = '请求的资源不存在'
          break
        case 500:
          message = '服务器内部错误'
          break
        default:
          message = data?.message || `请求失败 (${status})`
      }
    } else if (error.request) {
      // 请求已发出但没有收到响应
      message = '网络连接超时，请检查网络'
    } else {
      // 其他错误
      message = error.message || '未知错误'
    }
    
    ElMessage.error(message)
    return Promise.reject(error)
  }
)

// API服务类
class ApiService {
  // 通用日期格式化方法
  static formatDateForAPI(date) {
    if (!date) return date
    // 如果是完整的日期时间格式，只取日期部分
    if (date.includes('T') || date.includes(' ')) {
      return date.split('T')[0].split(' ')[0]
    } else if (date.length > 10) {
      // 如果是其他格式的长日期，只取前10位（YYYY-MM-DD）
      return date.substring(0, 10)
    }
    return date
  }

  // 市场概览相关API
  static async getMarketOverview() {
    return api.get('/api/market/overview')
  }
  
  static async getMarketSentiment(date) {
    const formattedDate = this.formatDateForAPI(date)
    return api.get('/api/market/sentiment', {
      params: { date: formattedDate }
    })
  }

  static async getLatestMarketDate() {
    return api.get('/api/market/latest-date')
  }

  static async getMarketMetadata(daysBack) {
    return api.get('/api/market/metadata', {
      params: { days_back: daysBack }
    })
  }

  static async getMarketSentimentCharts(date, daysBack = 30) {
    const formattedDate = this.formatDateForAPI(date)
    return api.get('/api/market/sentiment/charts', {
      params: {
        date: formattedDate,
        days_back: daysBack
      }
    })
  }
  
  static async getIndexData(date, days = 30) {
    const formattedDate = this.formatDateForAPI(date)
    return api.get('/api/market/indices', {
      params: { date: formattedDate, days }
    })
  }
  
  // 股票相关API
  static async getNewHighStocks(date, period, filters = {}) {
    const formattedDate = this.formatDateForAPI(date)
    return api.post('/api/stocks/new-high', {
      date: formattedDate,
      period,
      exclude_st: filters.excludeST,
      include_non_main_board: filters.includeNonMainBoard
    })
  }
  
  static async getStockKline(stockCode, days = 30, date = null, format = 'data') {
    const params = {
      code: stockCode,
      days,
      format: format
    }
    if (date) {
      params.date = this.formatDateForAPI(date)
    }
    return api.get('/api/stocks/kline', {
      params
    })
  }

  static async getStockLevels(stockCode, windowDays = 3650, date = null, methodVer = 'v1') {
    const params = {
      code: stockCode,
      window: windowDays,
      method_ver: methodVer
    }
    if (date) {
      params.date = this.formatDateForAPI(date)
    }
    return api.get('/api/stocks/levels', { params })
  }
  
  // 分析相关API
  static async getHeimaAnalysis(date, filters = {}) {
    const formattedDate = this.formatDateForAPI(date)
    return api.get('/api/analysis/heima', {
      params: {
        date: formattedDate,
        exclude_st: filters.excludeST,
        include_non_main_board: filters.includeNonMainBoard
      }
    })
  }
  
  static async getBaimaAnalysis(startDate, endDate, filters = {}) {
    const formattedStartDate = this.formatDateForAPI(startDate)
    const formattedEndDate = this.formatDateForAPI(endDate)
    return api.post('/api/analysis/baima', {
      start_date: formattedStartDate,
      end_date: formattedEndDate,
      min_market_cap: 100,  // 默认最小市值100亿
      exclude_st: filters.excludeST,
      include_non_main_board: filters.includeNonMainBoard
    })
  }

  // 新的白马分析API - 支持多时间区间
  static async postBaimaAnalysis(requestData) {
    return api.post('/api/analysis/baima', requestData)
  }
  
  // 板块相关API
  static async getSectorData(date = null, filters = {}) {
    const params = {}
    if (date) params.date = this.formatDateForAPI(date)

    // 添加筛选参数
    if (filters.include_sectors !== undefined) {
      params.include_sectors = filters.include_sectors
    }
    if (filters.include_concepts !== undefined) {
      params.include_concepts = filters.include_concepts
    }

    // 根据筛选条件设置type参数
    let type = 'both'
    if (filters.include_sectors === true && filters.include_concepts === false) {
      type = 'sectors'
    } else if (filters.include_sectors === false && filters.include_concepts === true) {
      type = 'concepts'
    }
    params.type = type

    console.log('🔍 板块数据API调用参数:', params)
    // 修复：使用正确的统一板块数据API
    const response = await api.get('/api/sectors', { params })
    console.log('🔍 板块数据API响应:', response)
    return response
  }

  static async getSectorNames(type = 'both') {
    return api.get('/api/sectors/names', {
      params: { type }
    })
  }

  static async getSectorKline(params) {
    return api.post('/api/market/sectors/kline', params)
  }

  static async getSectorCustomPeriod(params) {
    // 后端为GET接口，使用查询参数调用
    return api.get('/api/market/sectors/custom-period', { params })
  }

  static async searchStocks(query) {
    return api.get('/api/stocks/search', {
      params: { query }
    })
  }

  static async getStockComparison(params) {
    return api.post('/api/stocks/comparison', params)
  }

  static async getSectorComparison(params) {
    // params: { sector_names: string[], normalize: boolean, days_back?: number, start_date?: string, end_date?: string }
    return api.post('/api/sectors/comparison', params)
  }

  static async getSectorStocks(sectorName, params = {}) {
    return api.get(`/api/sectors/${encodeURIComponent(sectorName)}/stocks`, { params })
  }

  // 股票组合管理
  static async getStockGroups() {
    return api.get('/api/stock-groups')
  }

  static async saveStockGroup(payload) {
    return api.post('/api/stock-groups', payload)
  }

  static async deleteStockGroup(groupId) {
    return api.delete(`/api/stock-groups/${groupId}`)
  }

  // 时间区间组合 - 保存/加载/删除
  static async getIntervalGroups() {
    return api.get('/api/interval-groups')
  }

  static async saveIntervalGroup(payload) {
    return api.post('/api/interval-groups', payload)
  }

  static async deleteIntervalGroup(groupId) {
    return api.delete(`/api/interval-groups/${groupId}`)
  }

  static async getSingleSectorKline(sectorName, params = {}) {
    const { overlay_index, days_range = 30, format = 'chart', date } = params
    const apiParams = {
      overlay_index,
      days_range,
      format
    }
    if (date) {
      apiParams.date = this.formatDateForAPI(date)
    }
    return api.get(`/api/sectors/${encodeURIComponent(sectorName)}/kline`, {
      params: apiParams
    })
  }

  static async getMultiIndexKline(params) {
    const { selected_indices, date_str, days_range = 30 } = params
    const formattedDate = this.formatDateForAPI(date_str)
    return api.post('/api/indices/kline', {
      indices: selected_indices,
      date: formattedDate,
      days_range,
      format: 'chart'
    })
  }

  // 指数分析页专用API
  static async getIndicesAvailable() {
    return api.get('/api/indices/available')
  }

  static async getIndicesAnalysis(date) {
    const formattedDate = this.formatDateForAPI(date)
    return api.get('/api/indices/analysis', {
      params: { date: formattedDate }
    })
  }

  // 市场量能数据API
  static async getMarketVolume(date, previousDate = null) {
    const params = {}
    if (date) {
      params.date = this.formatDateForAPI(date)
    }
    if (previousDate) {
      params.previous_date = this.formatDateForAPI(previousDate)
    }
    
    console.log('📊 调用市场量能API，参数:', params)
    return api.get('/api/market/volume', { params })
  }

  // 节假日和交易日相关API
  static async getNonTradingDays(year, month) {
    return api.get('/api/holidays/non-trading-days', {
      params: { year, month }
    })
  }

  static async checkDateTradingStatus(date) {
    const formattedDate = this.formatDateForAPI(date)
    return api.get('/api/holidays/check-date', {
      params: { date: formattedDate }
    })
  }

  static async getNonTradingDaysRange(startDate, endDate) {
    const formattedStartDate = this.formatDateForAPI(startDate)
    const formattedEndDate = this.formatDateForAPI(endDate)
    return api.get('/api/holidays/range', {
      params: { 
        start_date: formattedStartDate, 
        end_date: formattedEndDate 
      }
    })
  }

  // 手动数据更新
  static async triggerManualUpdate(target) {
    return api.post(`/api/admin/update/${target}`)
  }

}

// 工具函数
export const utils = {
  // 格式化日期
  formatDate(date, format = 'YYYY-MM-DD') {
    if (!date) return ''
    
    const d = new Date(date)
    const year = d.getFullYear()
    const month = String(d.getMonth() + 1).padStart(2, '0')
    const day = String(d.getDate()).padStart(2, '0')
    
    return format
      .replace('YYYY', year)
      .replace('MM', month)
      .replace('DD', day)
  },
  
  // 格式化数字
  formatNumber(num, decimals = 2) {
    if (num === null || num === undefined) return '--'
    return Number(num).toFixed(decimals)
  },

  // 格式化成交额（智能单位转换）
  formatAmount(amount) {
    if (amount === null || amount === undefined || amount === 0) return '--'

    const num = Number(amount)
    if (isNaN(num)) return '--'

    // 转换为不同单位
    if (num >= 100000000) {
      // 大于等于1亿，显示为亿
      return `${(num / 100000000).toFixed(2)}亿`
    } else if (num >= 10000000) {
      // 大于等于1千万，显示为千万
      return `${(num / 10000000).toFixed(2)}千万`
    } else if (num >= 1000) {
      // 大于等于1千，显示为万（包括小数）
      return `${(num / 10000).toFixed(2)}万`
    } else {
      // 小于1千，直接显示
      return num.toFixed(2)
    }
  },
  
  // 格式化百分比
  formatPercent(num, decimals = 2) {
    if (num === null || num === undefined) return '--'
    const formatted = Number(num).toFixed(decimals)
    return `${formatted}%`
  },
  
  // 格式化成交量
  formatVolume(volume) {
    if (!volume) return '--'
    
    const num = Number(volume)
    if (num >= 100000000) {
      return `${(num / 100000000).toFixed(2)}亿`
    } else if (num >= 10000) {
      return `${(num / 10000).toFixed(2)}万`
    } else {
      return num.toString()
    }
  },
  
  // 获取涨跌颜色
  getChangeColor(change) {
    if (change > 0) return '#f56c6c'  // 红色
    if (change < 0) return '#67c23a'  // 绿色
    return '#909399'  // 灰色
  },
  
  // 获取涨跌图标
  getChangeIcon(change) {
    if (change > 0) return 'ArrowUp'
    if (change < 0) return 'ArrowDown'
    return 'Minus'
  }
}

// 缓存管理
export const cache = {
  // 缓存数据
  data: new Map(),
  
  // 设置缓存
  set(key, value, ttl = 5 * 60 * 1000) { // 默认5分钟
    const expiry = Date.now() + ttl
    this.data.set(key, { value, expiry })
  },
  
  // 获取缓存
  get(key) {
    const item = this.data.get(key)
    if (!item) return null
    
    if (Date.now() > item.expiry) {
      this.data.delete(key)
      return null
    }
    
    return item.value
  },
  
  // 删除缓存
  delete(key) {
    this.data.delete(key)
  },
  
  // 清空缓存
  clear() {
    this.data.clear()
  }
}

// 带缓存的API调用
export const cachedApi = {
  async getMarketOverview() {
    const cacheKey = 'market_overview'
    const cached = cache.get(cacheKey)
    if (cached) return cached
    
    const result = await ApiService.getMarketOverview()
    cache.set(cacheKey, result, 2 * 60 * 1000) // 缓存2分钟
    return result
  },
  
  async getStockKline(stockCode, days = 30, date = null) {
    const cacheKey = `stock_kline_${stockCode}_${days}_${date || 'current'}`
    const cached = cache.get(cacheKey)
    if (cached) return cached

    const result = await ApiService.getStockKline(stockCode, days, date)
    cache.set(cacheKey, result, 5 * 60 * 1000) // 缓存5分钟
    return result
  }
}

export default ApiService
