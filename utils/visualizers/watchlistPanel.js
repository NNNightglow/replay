export const WATCHLIST_DEFAULT_COLUMNS = ['code', 'name', 'change_pct', 'amount']

export const WATCHLIST_COLUMN_OPTIONS = [
  { value: 'code', label: '代码' },
  { value: 'name', label: '名称' },
  { value: 'type', label: '类型' },
  { value: 'latest_price', label: '最新价' },
  { value: 'change_pct', label: '涨跌幅%' },
  { value: 'pct5', label: '5日涨幅%' },
  { value: 'pct10', label: '10日涨幅%' },
  { value: 'amount', label: '成交额' }
]

const WATCHLIST_COLUMN_SET = new Set(WATCHLIST_COLUMN_OPTIONS.map(item => item.value))
const WATCHLIST_COLUMN_LABEL = WATCHLIST_COLUMN_OPTIONS.reduce((acc, item) => {
  acc[item.value] = item.label
  return acc
}, {})

export const filterWatchlistColumns = (columns) => {
  const source = Array.isArray(columns) ? columns : WATCHLIST_DEFAULT_COLUMNS
  const valid = source.filter(col => WATCHLIST_COLUMN_SET.has(col))
  return valid.length ? valid : [...WATCHLIST_DEFAULT_COLUMNS]
}

export const getWatchlistColumnLabel = (col) => WATCHLIST_COLUMN_LABEL[col] || col

export const buildWatchlistGridTemplate = (columns) => {
  const valid = filterWatchlistColumns(columns)
  return valid.map((col) => {
    if (col === 'code') return '100px'
    if (col === 'name') return 'minmax(130px, 1.2fr)'
    if (col === 'type') return '76px'
    if (col === 'amount') return 'minmax(110px, 1fr)'
    return 'minmax(86px, 1fr)'
  }).join(' ')
}

export const getWatchlistCellClass = (col, item, watchChangeClassFn) => {
  if (col === 'change_pct') return `change ${watchChangeClassFn?.(item?.change_pct) || ''}`.trim()
  if (col === 'name') return 'name'
  if (col === 'code') return 'code'
  return ''
}

export const formatWatchlistCell = (item, col, formatters = {}) => {
  const row = item && typeof item === 'object' ? item : {}
  if (col === 'code') return row.type === 'stock' ? (row.code || '--') : '--'
  if (col === 'name') return row.name || row.code || '--'
  if (col === 'type') {
    if (row.type === 'stock') return '股票'
    if (row.type === 'index') return '指数'
    if (row.type === 'sector') return '板块'
    return '--'
  }
  if (row.type !== 'stock') return '--'
  if (col === 'latest_price') return formatters.formatPrice?.(row.latest_price) || '--'
  if (col === 'change_pct') return formatters.formatChange?.(row.change_pct) || '--'
  if (col === 'pct5') return formatters.formatChange?.(row.pct5) || '--'
  if (col === 'pct10') return formatters.formatChange?.(row.pct10) || '--'
  if (col === 'amount') return formatters.formatAmount?.(row.amount) || '--'
  return '--'
}
