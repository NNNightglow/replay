<template>
  <div class="trading-date-picker-wrapper">
    <!-- 左右箭头按钮 -->
    <div class="date-navigation">
      <el-button 
        type="text" 
        class="nav-button prev-button"
        @click="goToPrevTradingDay"
        :disabled="loading"
        title="上一个交易日"
      >
        <el-icon><ArrowLeft /></el-icon>
      </el-button>
      
      <el-date-picker
        v-model="internalValue"
        v-bind="$attrs"
        :disabled-date="disabledDateHandler"
        :cell-class-name="cellClassNameHandler"
        @change="handleChange"
        @visible-change="handleVisibleChange"
        @panel-change="handlePanelChange"
        class="date-picker-input"
      />
      
      <el-button 
        type="text" 
        class="nav-button next-button"
        @click="goToNextTradingDay"
        :disabled="loading"
        title="下一个交易日"
      >
        <el-icon><ArrowRight /></el-icon>
      </el-button>
    </div>
  </div>
</template>

<script>
import { ref, watch, nextTick } from 'vue'
import { ArrowLeft, ArrowRight } from '@element-plus/icons-vue'
import ApiService from '@/services/api'

export default {
  name: 'TradingDatePicker',
  components: {
    ArrowLeft,
    ArrowRight
  },
  props: {
    modelValue: {
      type: [String, Date, null],
      default: null
    },
    // 是否启用节假日标记
    enableHolidayMarking: {
      type: Boolean,
      default: true
    },
    // 是否禁用非交易日选择
    disableNonTradingDays: {
      type: Boolean,
      default: true
    },
    // 是否禁用未来日期
    disableFutureDates: {
      type: Boolean,
      default: true
    },
    // 自定义禁用日期函数
    customDisabledDate: {
      type: Function,
      default: null
    },
    // 值格式
    valueFormat: {
      type: String,
      default: 'YYYY-MM-DD'
    }
  },
  emits: ['update:modelValue', 'change'],
  setup(props, { emit, attrs }) {
    // 工具：将日期规范到本地同一天的中午，避免 DST 边界问题
    const normalizeLocalNoon = (date) => {
      const d = new Date(date)
      d.setHours(12, 0, 0, 0)
      return d
    }
    // 工具：用本地时区格式化为 YYYY-MM-DD
    const formatLocalYMD = (date) => {
      const d = normalizeLocalNoon(date)
      const y = d.getFullYear()
      const m = String(d.getMonth() + 1).padStart(2, '0')
      const day = String(d.getDate()).padStart(2, '0')
      return `${y}-${m}-${day}`
    }

    // 获取 valueFormat，优先使用 attrs 中的值，否则使用 props 默认值
    const getValueFormat = () => {
      return attrs['value-format'] || props.valueFormat || 'YYYY-MM-DD'
    }

    // 内部值管理
    const internalValue = ref(props.modelValue)
    
    // 非交易日缓存：key = 'YYYY-M'（不补零），值为 [{ date:'YYYY-MM-DD', ... }]
    const nonTradingDaysCache = ref(new Map())
    
    // 加载状态
    const loading = ref(false)
    
    // 当前显示的年月
    const currentYear = ref(new Date().getFullYear())
    const currentMonth = ref(new Date().getMonth() + 1)
    
    // 监听外部值变化
    watch(() => props.modelValue, (newValue) => {
      internalValue.value = newValue
    })
    
    // 注意：不通过内部值 watcher 主动发射 v-model，避免竞态；在变更处显式发射
    
    // 获取指定月份的非交易日（保留原始数据）
    const getNonTradingDaysForMonth = async (year, month) => {
      const cacheKey = `${year}-${month}`
      
      if (nonTradingDaysCache.value.has(cacheKey)) {
        return nonTradingDaysCache.value.get(cacheKey)
      }

      try {
        loading.value = true
        const response = await ApiService.getNonTradingDays(year, month)

        if (response && response.success && response.data?.non_trading_days) {
          const nonTradingDays = response.data.non_trading_days.map(d => ({
            date: d.date,
            type: d.type || '',
            reason: d.reason || '',
            description: d.description || ''
          }))

          nonTradingDaysCache.value.set(cacheKey, nonTradingDays)
          return nonTradingDays
        }

        return []
      } catch (err) {
        console.error(`获取${year}-${month}非交易日异常`, err)
        return []
      } finally {
        loading.value = false
      }
    }

    // 检查指定日期是否非交易日（统一规则）
    const checkIsNonTradingDay = (dateInput) => {
      const dateObj = normalizeLocalNoon(dateInput)
      const dateStr = formatLocalYMD(dateObj)
      const frontendWeekday = dateObj.getDay() // 0=Sun ... 6=Sat
      const backendWeekday = frontendWeekday === 0 ? 6 : frontendWeekday - 1 // 0=Mon ... 6=Sun

      const year = dateObj.getFullYear()
      const month = dateObj.getMonth() + 1
      const prevMonthYear = month === 1 ? year - 1 : year
      const prevMonth = month === 1 ? 12 : month - 1
      const nextMonthYear = month === 12 ? year + 1 : year
      const nextMonth = month === 12 ? 1 : month + 1

      const monthKeys = [
        `${prevMonthYear}-${prevMonth}`,
        `${year}-${month}`,
        `${nextMonthYear}-${nextMonth}`
      ]

      let isInApiNonTrading = false

      for (const key of monthKeys) {
        if (nonTradingDaysCache.value.has(key)) {
          if (nonTradingDaysCache.value.get(key).some(d => d.date === dateStr)) {
            isInApiNonTrading = true
            break
          }
        }
      }

      // 规则：
      // 1. API 明确返回非交易日 → 禁用
      if (isInApiNonTrading) return true
      // 2. API 没返回，但当天是周末 → 禁用
      if (backendWeekday >= 5) return true
      // 3. 其他情况 → 可选
      return false
    }

    // 获取下一个交易日
    const getNextTradingDay = async (currentDate) => {
      let nextDate = normalizeLocalNoon(currentDate || new Date())
      let attempts = 0
      const maxAttempts = 60
      
      while (attempts < maxAttempts) {
        nextDate.setDate(nextDate.getDate() + 1)
        if (!checkIsNonTradingDay(nextDate)) {
          return formatLocalYMD(nextDate)
        }
        attempts++
      }
      return null
    }
    
    // 获取上一个交易日
    const getPrevTradingDay = async (currentDate) => {
      let prevDate = normalizeLocalNoon(currentDate || new Date())
      let attempts = 0
      const maxAttempts = 60
      
      while (attempts < maxAttempts) {
        prevDate.setDate(prevDate.getDate() - 1)
        if (!checkIsNonTradingDay(prevDate)) {
          return formatLocalYMD(prevDate)
        }
        attempts++
      }
      return null
    }

    // 跳转到下一个交易日
    const goToNextTradingDay = async () => {
      try {
        loading.value = true
        const currentDate = internalValue.value ? new Date(internalValue.value) : new Date()
        const nextTradingDay = await getNextTradingDay(currentDate)
        
        if (nextTradingDay) {
          // 根据 valueFormat 格式化日期
          let formattedDate = nextTradingDay
          if (getValueFormat() === 'YYYYMMDD') {
            formattedDate = nextTradingDay.replace(/-/g, '')
          } else if (getValueFormat() === 'YYYY-MM-DD') {
            formattedDate = nextTradingDay
          } else if (getValueFormat() === 'YYYY/MM/DD') {
            formattedDate = nextTradingDay.replace(/-/g, '/')
          }
          
          internalValue.value = formattedDate
          emit('update:modelValue', formattedDate)
          await nextTick()
          emit('change', formattedDate)
          console.log('📅 跳转到下一个交易日:', formattedDate)
        } else {
          console.warn('未找到下一个交易日')
        }
      } catch (error) {
        console.error('跳转到下一个交易日失败:', error)
      } finally {
        loading.value = false
      }
    }
    
    // 跳转到上一个交易日
    const goToPrevTradingDay = async () => {
      try {
        loading.value = true
        const currentDate = internalValue.value ? new Date(internalValue.value) : new Date()
        const prevTradingDay = await getPrevTradingDay(currentDate)
        
        if (prevTradingDay) {
          // 根据 valueFormat 格式化日期
          let formattedDate = prevTradingDay
          if (getValueFormat() === 'YYYYMMDD') {
            formattedDate = prevTradingDay.replace(/-/g, '')
          } else if (getValueFormat() === 'YYYY-MM-DD') {
            formattedDate = prevTradingDay
          } else if (getValueFormat() === 'YYYY/MM/DD') {
            formattedDate = prevTradingDay.replace(/-/g, '/')
          }
          
          internalValue.value = formattedDate
          emit('update:modelValue', formattedDate)
          await nextTick()
          emit('change', formattedDate)
          console.log('📅 跳转到上一个交易日:', formattedDate)
        } else {
          console.warn('未找到上一个交易日')
        }
      } catch (error) {
        console.error('跳转到上一个交易日失败:', error)
      } finally {
        loading.value = false
      }
    }

    // 禁用日期处理
    const disabledDateHandler = (time) => {
      const now = new Date()
      now.setHours(23, 59, 59, 999)
      if (props.disableFutureDates && time.getTime() > now.getTime()) return true
      if (props.customDisabledDate && props.customDisabledDate(time)) return true

      if (props.disableNonTradingDays) {
        return checkIsNonTradingDay(time)
      }

      return false
    }

    // 单元格样式处理
    const cellClassNameHandler = (time) => {
      const classes = []
      if (!props.enableHolidayMarking) return ''

      const dateObj = normalizeLocalNoon(time)
      const dateStr = formatLocalYMD(dateObj)
      const frontendWeekday = dateObj.getDay()
      const backendWeekday = frontendWeekday === 0 ? 6 : frontendWeekday - 1

      const year = dateObj.getFullYear()
      const month = dateObj.getMonth() + 1
      const prevMonthYear = month === 1 ? year - 1 : year
      const prevMonth = month === 1 ? 12 : month - 1
      const nextMonthYear = month === 12 ? year + 1 : year
      const nextMonth = month === 12 ? 1 : month + 1

      const monthKeys = [
        `${prevMonthYear}-${prevMonth}`,
        `${year}-${month}`,
        `${nextMonthYear}-${nextMonth}`
      ]

      let apiDay = null
      for (const key of monthKeys) {
        if (nonTradingDaysCache.value.has(key)) {
          const found = nonTradingDaysCache.value.get(key).find(d => d.date === dateStr)
          if (found) {
            apiDay = found
            break
          }
        }
      }

      if (apiDay) {
        if (apiDay.type === 'holiday' || apiDay.type === 'special' || apiDay.reason || apiDay.description) {
          classes.push('holiday-cell', 'non-trading-cell')
        } else {
          classes.push('non-trading-cell')
        }
      } else if (backendWeekday >= 5) {
        classes.push('weekend-cell', 'non-trading-cell')
      }

      return classes.join(' ')
    }
    
    // 值变化处理
    const handleChange = async (value) => {
      if (!value) {
        emit('change', value)
        return
      }

      const normalized = formatLocalYMD(value instanceof Date ? value : new Date(value))
      
      // 如果启用了非交易日禁用，检查选择的日期
      if (props.disableNonTradingDays) {
        const isNonTrading = checkIsNonTradingDay(new Date(normalized))
        if (isNonTrading) {
          // 重置为原值
          nextTick(() => {
            internalValue.value = props.modelValue
          })
          
          // 提示用户
          console.warn('不能选择非交易日:', normalized)
          return
        }
      }
      
      // 根据 valueFormat 格式化日期
      let formattedDate = normalized
      if (getValueFormat() === 'YYYYMMDD') {
        formattedDate = normalized.replace(/-/g, '')
      } else if (getValueFormat() === 'YYYY-MM-DD') {
        formattedDate = normalized
      } else if (getValueFormat() === 'YYYY/MM/DD') {
        formattedDate = normalized.replace(/-/g, '/')
      }
      
      internalValue.value = formattedDate
      // 先同步 v-model，再触发 change，避免父组件拿到旧值
      emit('update:modelValue', formattedDate)
      await nextTick()
      emit('change', formattedDate)
    }
    
    // 日历面板显示状态变化
    const handleVisibleChange = async (visible) => {
      console.log('日历面板可见性变化:', visible);
      if (visible && props.enableHolidayMarking) {
        // 当日历打开时，预加载当前月份和前后月份的非交易日数据
        const promises = []
        
        // 当前月份
        promises.push(getNonTradingDaysForMonth(currentYear.value, currentMonth.value))
        
        // 上个月
        let prevYear = currentYear.value
        let prevMonth = currentMonth.value - 1
        if (prevMonth < 1) {
          prevMonth = 12
          prevYear -= 1
        }
        promises.push(getNonTradingDaysForMonth(prevYear, prevMonth))
        
        // 下个月
        let nextYear = currentYear.value
        let nextMonth = currentMonth.value + 1
        if (nextMonth > 12) {
          nextMonth = 1
          nextYear += 1
        }
        promises.push(getNonTradingDaysForMonth(nextYear, nextMonth))
        
        await Promise.all(promises)
        console.log('📅 已预加载相邻月份的非交易日数据')
      }
    }
    
    // 日历面板年月变化
    const handlePanelChange = async (date, mode) => {
      if (!props.enableHolidayMarking) return
      
      if (date && (mode === 'month' || mode === 'year')) {
        const newYear = date.getFullYear()
        const newMonth = date.getMonth() + 1
        
        // 更新当前年月
        currentYear.value = newYear
        currentMonth.value = newMonth
        
        // 预加载新月份的非交易日数据
        await getNonTradingDaysForMonth(newYear, newMonth)
      }
    }
    
    // 初始化时预加载当前时间前后半年的非交易日数据
    const initializeData = async () => {
      if (props.enableHolidayMarking) {
        console.log('📅 初始化交易日期选择器，开始预加载数据...')
        
        const now = new Date()
        const currYear = now.getFullYear()
        const currMonth = now.getMonth() + 1
        
        const promises = []
        const monthsToLoad = []
        
        // 向前6个月（含当月）
        for (let i = -6; i <= 0; i++) {
          let y = currYear
          let m = currMonth + i
          while (m < 1) { m += 12; y -= 1 }
          while (m > 12) { m -= 12; y += 1 }
          monthsToLoad.push({ year: y, month: m })
        }
        
        // 向后6个月
        for (let i = 1; i <= 6; i++) {
          let y = currYear
          let m = currMonth + i
          while (m < 1) { m += 12; y -= 1 }
          while (m > 12) { m -= 12; y += 1 }
          monthsToLoad.push({ year: y, month: m })
        }
        
        const uniqueMonths = Array.from(
          new Set(monthsToLoad.map(item => `${item.year}-${item.month}`))
        ).map(key => {
          const [y, m] = key.split('-').map(Number)
          return { year: y, month: m }
        })
        
        console.log(`📅 准备预加载${uniqueMonths.length}个月的数据:`, uniqueMonths)
        
        for (const { year, month } of uniqueMonths) {
          promises.push(getNonTradingDaysForMonth(year, month))
        }
        
        await Promise.all(promises)
        console.log('📅 已预加载前后半年的非交易日数据')
      }
    }
    
    // 将组件日期同步为“最新交易日”
    const syncToLatestTradingDate = async () => {
      try {
        const res = await ApiService.getLatestMarketDate()
        if (res && res.success && res.data && res.data.latest_date) {
          const latest = res.data.latest_date
          // 根据 valueFormat 输出
          let formatted = latest
          if (getValueFormat() === 'YYYYMMDD') {
            formatted = latest.replace(/-/g, '')
          } else if (getValueFormat() === 'YYYY/MM/DD') {
            formatted = latest.replace(/-/g, '/')
          }

          internalValue.value = formatted
          emit('update:modelValue', formatted)
          await nextTick()
          emit('change', formatted)

          // 同步面板年月
          const d = new Date(latest)
          currentYear.value = d.getFullYear()
          currentMonth.value = d.getMonth() + 1
        }
      } catch (e) {
        console.warn('获取最新交易日失败，保持现有值', e)
      }
    }

    // 组件挂载时初始化数据
    const initializeComponent = async () => {
      console.log('📅 清除可能存在的错误缓存数据...')
      nonTradingDaysCache.value.clear()
      
      await initializeData()
      // 初始化完成后，强制同步到“最新交易日”
      await syncToLatestTradingDate()
      console.log('📅 组件初始化完成并同步到最新交易日')
    }
    
    // 立即初始化组件
    initializeComponent()
    
    return {
      internalValue,
      loading,
      disabledDateHandler,
      cellClassNameHandler,
      handleChange,
      handleVisibleChange,
      handlePanelChange,
      goToNextTradingDay,
      goToPrevTradingDay
    }
  }
}
</script>

<style scoped>
.trading-date-picker-wrapper {
  display: inline-block;
  position: relative;
}

.date-navigation {
  display: flex;
  align-items: center;
  gap: 8px;
}

.nav-button {
  padding: 8px;
  border-radius: 4px;
  transition: all 0.3s ease;
  color: #606266;
}

.nav-button:hover {
  background-color: #f5f7fa;
  color: #409eff;
}

.nav-button:disabled {
  color: #c0c4cc;
  cursor: not-allowed;
}

.nav-button:disabled:hover {
  background-color: transparent;
  color: #c0c4cc;
}

.date-picker-input {
  flex: 1;
}

/* 节假日样式 - 更明显的红色背景 */
:deep(.holiday-cell) {
  background-color: #ffebee !important;
  color: #d32f2f !important;
  font-weight: bold !important;
  border: 1px solid #ffcdd2 !important;
}

:deep(.holiday-cell:hover) {
  background-color: #ffcdd2 !important;
  color: #b71c1c !important;
}

/* 周末样式 - 更明显的灰色背景 */
:deep(.weekend-cell) {
  background-color: #f3f4f6 !important;
  color: #6b7280 !important;
  font-weight: bold !important;
  border: 1px solid #e5e7eb !important;
}

:deep(.weekend-cell:hover) {
  background-color: #e5e7eb !important;
  color: #4b5563 !important;
}

/* 非交易日通用样式 */
:deep(.non-trading-cell) {
  opacity: 0.8;
  font-style: italic;
  text-decoration: line-through;
  position: relative;
}

/* 非交易日的斜线标记 */
:deep(.non-trading-cell::after) {
  content: '';
  position: absolute;
  top: 2px;
  left: 2px;
  right: 2px;
  bottom: 2px;
  background: linear-gradient(45deg, transparent 45%, #ccc 45%, #ccc 55%, transparent 55%);
  pointer-events: none;
  opacity: 0.3;
}

/* 禁用状态样式 */
:deep(.el-date-table td.disabled .holiday-cell) {
  background-color: #f5f5f5 !important;
  color: #c0c4cc !important;
  cursor: not-allowed !important;
}

:deep(.el-date-table td.disabled .weekend-cell) {
  background-color: #f5f5f5 !important;
  color: #c0c4cc !important;
  cursor: not-allowed !important;
}

/* 今天的样式保持不变 */
:deep(.el-date-table td.today span) {
  background-color: #409eff !important;
  color: #fff !important;
}

/* 选中日期的样式 */
:deep(.el-date-table td.current span) {
  background-color: #409eff !important;
  color: #fff !important;
}

/* 悬停效果 */
:deep(.el-date-table td:not(.disabled):hover .non-trading-cell) {
  transform: none;
  font-weight: bold;
}
</style>
