import axios from 'axios'
import { ElLoading, ElMessage } from 'element-plus'

const LONG_TASK_TIMEOUT_MS = 6 * 60 * 60 * 1000

const api = axios.create({
  baseURL: 'http://localhost:5000',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json'
  }
})

let loadingInstance = null

api.interceptors.request.use(
  config => {
    if (config.data instanceof FormData) {
      delete config.headers['Content-Type']
    }

    if (!config.hideLoading) {
      loadingInstance = ElLoading.service({
        lock: true,
        text: '正在加载数据...',
        background: 'rgba(0, 0, 0, 0.7)'
      })
    }
    return config
  },
  error => {
    if (loadingInstance) {
      loadingInstance.close()
      loadingInstance = null
    }
    return Promise.reject(error)
  }
)

api.interceptors.response.use(
  response => {
    if (loadingInstance) {
      loadingInstance.close()
      loadingInstance = null
    }
    const { data } = response
    if (data?.success === false) {
      ElMessage.error(data.message || data.error || '请求失败')
      return Promise.reject(new Error(data.message || data.error || '请求失败'))
    }
    return data
  },
  error => {
    if (loadingInstance) {
      loadingInstance.close()
      loadingInstance = null
    }

    let message = '网络请求失败'
    if (error.response) {
      const { status, data } = error.response
      switch (status) {
        case 400:
          message = data?.message || data?.error || '请求参数错误'
          break
        case 401:
          message = '未授权访问'
          break
        case 403:
          message = '禁止访问'
          break
        case 404:
          message = data?.message || data?.error || '资源不存在'
          break
        case 500:
          message = data?.message || data?.error || '服务器内部错误'
          break
        default:
          message = data?.message || data?.error || `请求失败 (${status})`
      }
    } else if (error.request) {
      message = '网络连接超时，请检查服务状态'
    } else {
      message = error.message || '未知错误'
    }
    ElMessage.error(message)
    return Promise.reject(error)
  }
)

class ApiService {
  static formatDateForAPI(date) {
    if (!date) return date
    if (typeof date !== 'string') return date
    if (date.includes('T') || date.includes(' ')) {
      return date.split('T')[0].split(' ')[0]
    }
    if (date.length > 10) {
      return date.substring(0, 10)
    }
    return date
  }

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
    const params = { code: stockCode, days, format }
    if (date) params.date = this.formatDateForAPI(date)
    return api.get('/api/stocks/kline', { params })
  }

  static async getStockLevels(stockCode, windowDays = 3650, date = null) {
    const params = { code: stockCode, window: windowDays }
    if (date) params.date = this.formatDateForAPI(date)
    return api.get('/api/stocks/levels', { params })
  }

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
      min_market_cap: 100,
      exclude_st: filters.excludeST,
      include_non_main_board: filters.includeNonMainBoard
    })
  }

  static async postBaimaAnalysis(requestData) {
    return api.post('/api/analysis/baima', requestData)
  }

  static async getSectorData(date = null, filters = {}) {
    const params = {}
    if (date) params.date = this.formatDateForAPI(date)
    if (filters.include_sectors !== undefined) params.include_sectors = filters.include_sectors
    if (filters.include_concepts !== undefined) params.include_concepts = filters.include_concepts

    let type = 'both'
    if (filters.include_sectors === true && filters.include_concepts === false) type = 'sectors'
    if (filters.include_sectors === false && filters.include_concepts === true) type = 'concepts'
    params.type = type

    return api.get('/api/sectors', { params })
  }

  static async getSectorNames(type = 'both') {
    return api.get('/api/sectors/names', { params: { type } })
  }

  static async getSectorCustomPeriod(params) {
    return api.get('/api/market/sectors/custom-period', { params })
  }

  static async searchStocks(query) {
    return api.get('/api/stocks/search', { params: { query } })
  }

  static async getStockComparison(params) {
    return api.post('/api/stocks/comparison', params)
  }

  static async getSectorComparison(params) {
    return api.post('/api/sectors/comparison', params)
  }

  static async getSectorStocks(sectorName, params = {}) {
    return api.get(`/api/sectors/${encodeURIComponent(sectorName)}/stocks`, { params })
  }

  static async getStockGroups() {
    return api.get('/api/stock-groups')
  }

  static async saveStockGroup(payload) {
    return api.post('/api/stock-groups', payload)
  }

  static async deleteStockGroup(groupId) {
    return api.delete(`/api/stock-groups/${groupId}`)
  }

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
    const apiParams = { overlay_index, days_range, format }
    if (date) apiParams.date = this.formatDateForAPI(date)
    return api.get(`/api/sectors/${encodeURIComponent(sectorName)}/kline`, { params: apiParams })
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

  static async getIndicesAvailable() {
    return api.get('/api/indices/available')
  }

  static async getIndicesAnalysis(date) {
    const formattedDate = this.formatDateForAPI(date)
    return api.get('/api/indices/analysis', { params: { date: formattedDate } })
  }

  static async getIndexKlineChart(indexName, daysRange = 30) {
    return api.get('/api/indices/kline', {
      params: {
        index_name: indexName,
        days_range: daysRange,
        format: 'chart'
      }
    })
  }

  static async getMarketVolume(date, previousDate = null) {
    const params = {}
    if (date) params.date = this.formatDateForAPI(date)
    if (previousDate) params.previous_date = this.formatDateForAPI(previousDate)
    return api.get('/api/market/volume', { params })
  }

  static async getNonTradingDays(year, month) {
    return api.get('/api/holidays/non-trading-days', { params: { year, month } })
  }

  static async checkDateTradingStatus(date) {
    const formattedDate = this.formatDateForAPI(date)
    return api.get('/api/holidays/check-date', { params: { date: formattedDate } })
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

  static async getStrategyRuntime() {
    return api.get('/api/strategy-watch/runtime')
  }

  static async getStrategyConversations() {
    return api.get('/api/strategy-watch/conversations')
  }

  static async createStrategyConversation(payload = {}) {
    return api.post('/api/strategy-watch/conversations', payload)
  }

  static async renameStrategyConversation(conversationId, title) {
    return api.patch(`/api/strategy-watch/conversations/${conversationId}`, { title })
  }

  static async deleteStrategyConversation(conversationId) {
    return api.delete(`/api/strategy-watch/conversations/${conversationId}`)
  }

  static async getStrategyMessages(conversationId) {
    return api.get(`/api/strategy-watch/conversations/${conversationId}/messages`)
  }

  static async sendStrategyMessage(conversationId, payload) {
    return api.post(`/api/strategy-watch/conversations/${conversationId}/messages`, payload, {
      timeout: 180000
    })
  }

  static async streamStrategyMessage(conversationId, payload, handlers = {}) {
    const { onMeta, onDelta, onDone, onError } = handlers
    const baseURL = api.defaults.baseURL || ''
    const res = await fetch(
      `${baseURL}/api/strategy-watch/conversations/${conversationId}/messages/stream`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload || {})
      }
    )

    if (!res.ok) {
      const text = await res.text()
      const err = new Error(text || `HTTP ${res.status}`)
      if (onError) onError(err)
      throw err
    }

    const reader = res.body.getReader()
    const decoder = new TextDecoder('utf-8')
    let buffer = ''

    const handleEvent = (eventText) => {
      const lines = eventText.split('\n')
      const dataLines = []
      for (const line of lines) {
        if (line.startsWith('data:')) {
          dataLines.push(line.slice(5).trimStart())
        }
      }
      if (!dataLines.length) return
      const dataStr = dataLines.join('\n')
      let payload
      try {
        payload = JSON.parse(dataStr)
      } catch (err) {
        return
      }
      if (payload.type === 'meta' && onMeta) onMeta(payload)
      if (payload.type === 'delta' && onDelta) onDelta(payload)
      if (payload.type === 'done' && onDone) onDone(payload)
      if (payload.type === 'error' && onError) onError(payload)
    }

    try {
      while (true) {
        const { value, done } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        let idx
        while ((idx = buffer.indexOf('\n\n')) !== -1) {
          const eventText = buffer.slice(0, idx)
          buffer = buffer.slice(idx + 2)
          if (eventText.trim()) handleEvent(eventText)
        }
      }
      if (buffer.trim()) handleEvent(buffer)
    } catch (err) {
      if (onError) onError(err)
      throw err
    }
  }

  static async getStrategyResources(config = {}) {
    return api.get('/api/strategy-watch/resources', config)
  }

  static async uploadStrategyResources(formData) {
    return api.post('/api/strategy-watch/resources', formData, {
      timeout: LONG_TASK_TIMEOUT_MS
    })
  }

  static async getStrategyResourceJob(jobId, config = {}) {
    return api.get(`/api/strategy-watch/resources/jobs/${jobId}`, config)
  }

  static async listStrategyResourceJobs(config = {}) {
    return api.get('/api/strategy-watch/resources/jobs', config)
  }

  static async renameStrategyResourceGroup(groupId, groupName) {
    return api.patch(`/api/strategy-watch/resource-groups/${groupId}`, { group_name: groupName })
  }

  static async createStrategyResourceGroup(groupName) {
    return api.post('/api/strategy-watch/resource-groups', { group_name: groupName })
  }

  static async transferStrategyResourceGroup(payload) {
    return api.post('/api/strategy-watch/resource-groups/transfer', payload)
  }

  static async transferStrategyResources(payload) {
    return api.post('/api/strategy-watch/resources/transfer', payload)
  }

  static async setActiveStrategyResource(resourceId) {
    return api.patch('/api/strategy-watch/resources/active', { resource_id: resourceId })
  }

  static async deleteStrategyResource(resourceId) {
    return api.delete(`/api/strategy-watch/resources/${resourceId}`)
  }

  static async renameStrategyResource(resourceId, name) {
    return api.patch(`/api/strategy-watch/resources/${resourceId}`, { name })
  }

  static async getStrategyResourceMarkdown(resourceId) {
    return api.get(`/api/strategy-watch/resources/${resourceId}/markdown`)
  }

  static async downloadStrategyResourceFile(resourceId, format = 'markdown', params = {}) {
    return api.get(`/api/strategy-watch/resources/${resourceId}/download`, {
      params: { format, ...(params || {}) },
      responseType: 'blob'
    })
  }

  static async downloadStrategyResourceAiSummary(resourceId, params = {}) {
    return api.get(`/api/strategy-watch/resources/${resourceId}/ai-summary/download`, {
      params,
      responseType: 'blob'
    })
  }

  static async downloadCrawledFile(relpath) {
    return api.get('/api/strategy-watch/crawled/download', {
      params: { path: relpath },
      responseType: 'blob',
      hideLoading: true
    })
  }

  static async getStrategyWatchStrategies() {
    return api.get('/api/strategy-watch/strategies')
  }

  static async createStrategyWatchStrategy(payload = {}) {
    return api.post('/api/strategy-watch/strategies', payload)
  }

  static async updateStrategyWatchStrategy(strategyId, payload = {}) {
    return api.patch(`/api/strategy-watch/strategies/${strategyId}`, payload)
  }

  static async deleteStrategyWatchStrategy(strategyId) {
    return api.delete(`/api/strategy-watch/strategies/${strategyId}`)
  }

  static async setActiveStrategyWatchStrategy(strategyId) {
    return api.patch('/api/strategy-watch/strategies/active', { strategy_id: strategyId })
  }

  static async generateStrategyWatchView(strategyId, payload = {}) {
    return api.post(`/api/strategy-watch/strategies/${strategyId}/generate-view`, payload)
  }

  static async getStrategyAgentLogs(params = {}) {
    return api.get('/api/strategy-watch/agent-logs', { params, hideLoading: true })
  }

  static async getMemoryProfiles() {
    return api.get('/api/strategy-watch/memory-profiles')
  }

  static async createMemoryProfile(payload = {}) {
    return api.post('/api/strategy-watch/memory-profiles', payload)
  }

  static async updateMemoryProfile(profileId, payload = {}) {
    return api.patch(`/api/strategy-watch/memory-profiles/${profileId}`, payload)
  }

  static async deleteMemoryProfile(profileId) {
    return api.delete(`/api/strategy-watch/memory-profiles/${profileId}`)
  }

  static async setActiveMemoryProfile(profileId) {
    return api.patch('/api/strategy-watch/memory-profiles/active', { profile_id: profileId || '' })
  }

  static async bindMemoryProfileResources(profileId, resourceIds = []) {
    return api.post(`/api/strategy-watch/memory-profiles/${profileId}/bind-resources`, {
      resource_ids: Array.isArray(resourceIds) ? resourceIds : []
    })
  }

  static async bindMemoryProfileGroup(profileId, groupId) {
    return api.post(`/api/strategy-watch/memory-profiles/${profileId}/bind-group`, {
      group_id: groupId || ''
    })
  }

  static async syncMemoryProfileGroup(profileId, groupId = '') {
    return api.post(`/api/strategy-watch/memory-profiles/${profileId}/sync-group`, {
      group_id: groupId || ''
    })
  }

  static async extractMemoryPortraitDraft(profileId, payload = {}) {
    return api.post(`/api/strategy-watch/memory-profiles/${profileId}/extract-portrait-draft`, payload, {
      timeout: LONG_TASK_TIMEOUT_MS
    })
  }

  static async getMemoryPortrait(profileId) {
    return api.get(`/api/strategy-watch/memory-profiles/${profileId}/portrait`)
  }

  static async updateMemoryPortrait(profileId, payload = {}) {
    return api.patch(`/api/strategy-watch/memory-profiles/${profileId}/portrait`, payload)
  }

  static async exportMemoryPortrait(profileId, payload = {}) {
    return api.post(`/api/strategy-watch/memory-profiles/${profileId}/portrait/export`, payload, {
      responseType: 'blob',
      hideLoading: true,
      timeout: LONG_TASK_TIMEOUT_MS
    })
  }

  static async previewMemoryProfileContext(profileId) {
    return api.get(`/api/strategy-watch/memory-profiles/${profileId}/preview-context`)
  }

  static async triggerManualUpdate(target) {
    return api.post(`/api/admin/update/${target}`)
  }
}

export const utils = {
  formatDate(date, format = 'YYYY-MM-DD') {
    if (!date) return ''
    const d = new Date(date)
    const year = d.getFullYear()
    const month = String(d.getMonth() + 1).padStart(2, '0')
    const day = String(d.getDate()).padStart(2, '0')
    return format.replace('YYYY', year).replace('MM', month).replace('DD', day)
  },

  formatNumber(num, decimals = 2) {
    if (num === null || num === undefined) return '--'
    return Number(num).toFixed(decimals)
  },

  formatAmount(amount) {
    if (amount === null || amount === undefined || amount === 0) return '--'
    const num = Number(amount)
    if (Number.isNaN(num)) return '--'
    if (num >= 100000000) return `${(num / 100000000).toFixed(2)}亿`
    if (num >= 10000000) return `${(num / 10000000).toFixed(2)}千万`
    if (num >= 1000) return `${(num / 10000).toFixed(2)}万`
    return num.toFixed(2)
  },

  formatPercent(num, decimals = 2) {
    if (num === null || num === undefined) return '--'
    return `${Number(num).toFixed(decimals)}%`
  },

  formatVolume(volume) {
    if (volume === null || volume === undefined) return '--'
    const num = Number(volume)
    if (Number.isNaN(num)) return '--'
    if (num >= 100000000) return `${(num / 100000000).toFixed(2)}亿`
    if (num >= 10000) return `${(num / 10000).toFixed(2)}万`
    return num.toString()
  },

  getChangeColor(change) {
    if (change > 0) return '#f56c6c'
    if (change < 0) return '#67c23a'
    return '#909399'
  },

  getChangeIcon(change) {
    if (change > 0) return 'ArrowUp'
    if (change < 0) return 'ArrowDown'
    return 'Minus'
  }
}

export const cache = {
  data: new Map(),

  set(key, value, ttl = 5 * 60 * 1000) {
    const expiry = Date.now() + ttl
    this.data.set(key, { value, expiry })
  },

  get(key) {
    const item = this.data.get(key)
    if (!item) return null
    if (Date.now() > item.expiry) {
      this.data.delete(key)
      return null
    }
    return item.value
  },

  delete(key) {
    this.data.delete(key)
  },

  clear() {
    this.data.clear()
  }
}

export const cachedApi = {
  async getMarketOverview() {
    const cacheKey = 'market_overview'
    const cached = cache.get(cacheKey)
    if (cached) return cached
    const result = await ApiService.getMarketOverview()
    cache.set(cacheKey, result, 2 * 60 * 1000)
    return result
  },

  async getStockKline(stockCode, days = 30, date = null) {
    const cacheKey = `stock_kline_${stockCode}_${days}_${date || 'current'}`
    const cached = cache.get(cacheKey)
    if (cached) return cached
    const result = await ApiService.getStockKline(stockCode, days, date)
    cache.set(cacheKey, result, 5 * 60 * 1000)
    return result
  }
}

export default ApiService
