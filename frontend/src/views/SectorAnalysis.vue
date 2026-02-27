<template>
  <div class="sector-analysis">
    <!-- 页面标题 -->
    <div class="page-header">
      <h1>🏢 板块分析</h1>
      <p>实时行业和概念板块数据分析与K线图可视化</p>
    </div>

    <!-- 顶部控制面板 -->
    <el-card class="control-panel-top">

      <el-row :gutter="20">
        <!-- 板块类型选择 - 最左边，上下排列 -->
        <el-col :span="3">
          <div class="control-section">
            <h4>板块类型</h4>
            <div class="filter-controls-vertical">
              <el-checkbox v-model="includeSectors" @change="onFilterChange" class="blue-checkbox">行业板块</el-checkbox>
              <el-checkbox v-model="includeConcepts" @change="onFilterChange" class="blue-checkbox">概念板块</el-checkbox>
            </div>
          </div>
        </el-col>

        <!-- 日期选择 -->
        <el-col :span="4">
          <div class="control-section">
            <h4>分析日期</h4>
            <trading-date-picker
              v-model="globalDate"
              placeholder="选择分析日期"
              format="YYYY-MM-DD"
              value-format="YYYY-MM-DD"
              style="width: 100%;"
              @change="onGlobalDateChange"
            />
          </div>
        </el-col>

        <!-- 多时间区间设置 - 扩展空间 -->
        <el-col :span="14">
          <div class="control-section">
            <h4>多时间区间分析</h4>
            <div class="multi-period-controls-horizontal">
              <!-- 快速区间选择 - 左移并缩小 -->
              <div class="quick-intervals">
                <el-select
                  v-model="selectedQuickIntervals"
                  multiple
                  placeholder="快速区间"
                  style="width: 200px;"
                  size="small"
                >
                  <el-option label="最近5天" value="5d" />
                  <el-option label="最近10天" value="10d" />
                  <el-option label="最近30天" value="30d" />
                  <el-option label="最近90天" value="90d" />
                </el-select>
              </div>

              <!-- 自定义区间输入 - 更多空间 -->
              <div class="custom-interval-input-expanded">
                <el-input
                  v-model="customInterval.name"
                  placeholder="区间名称"
                  size="small"
                  style="width: 100px; margin-right: 8px;"
                />
                <el-date-picker
                  v-model="customInterval.startDate"
                  type="date"
                  placeholder="开始日期"
                  format="YYYY-MM-DD"
                  value-format="YYYY-MM-DD"
                  style="width: 120px; margin-right: 8px;"
                  size="small"
                />
                <el-date-picker
                  v-model="customInterval.endDate"
                  type="date"
                  placeholder="结束日期"
                  format="YYYY-MM-DD"
                  value-format="YYYY-MM-DD"
                  style="width: 120px; margin-right: 8px;"
                  size="small"
                />
                <el-button
                  type="primary"
                  size="small"
                  @click="addCustomInterval"
                  :disabled="!canAddCustomInterval"
                >
                  添加区间
                </el-button>
              </div>
              <!-- 区间组合操作（始终可见） -->
              <div style="display: flex; align-items: center; gap: 8px; margin-left: auto;">
                <el-button size="small" type="primary" @click="showSaveIntervalGroup = true">💾 保存区间组合</el-button>
                <el-button size="small" @click="openLoadIntervalGroup">📂 加载区间组合</el-button>
              </div>
            </div>
          </div>
        </el-col>

        <!-- 操作按钮 -->
        <el-col :span="3">
          <div class="control-section">
            <h4>&nbsp;</h4>
            <el-button type="primary" @click="loadAllData" :loading="loading" style="width: 100%;">
              <el-icon><Refresh /></el-icon>
              刷新数据
            </el-button>
          </div>
        </el-col>
      </el-row>

      <!-- 显示已添加的时间区间 -->
      <el-row v-if="customIntervals.length > 0" style="margin-top: 15px;">
        <el-col :span="24">
          <div class="control-section">
            <h4>已添加的时间区间</h4>
            <div style="display: flex; flex-wrap: wrap; gap: 8px;">
              <el-tag
                v-for="(interval, index) in customIntervals"
                :key="index"
                closable
                @close="removeCustomInterval(index)"
                type="info"
                size="small"
              >
                {{ interval.name }} ({{ interval.start_date }} 至 {{ interval.end_date }})
              </el-tag>
            </div>
          </div>
        </el-col>
      </el-row>
    </el-card>

    <!-- 统计概览 -->
    <el-row :gutter="15" class="stats-row">
      <el-col :span="6">
        <el-card class="stat-card">
          <div class="stat-content">
            <div class="stat-value">{{ summary.total_sectors || 0 }}</div>
            <div class="stat-label">总板块数</div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card class="stat-card">
          <div class="stat-content">
            <div class="stat-value up-color">{{ summary.up_sectors || 0 }}</div>
            <div class="stat-label">上涨板块</div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card class="stat-card">
          <div class="stat-content">
            <div class="stat-value down-color">{{ summary.down_sectors || 0 }}</div>
            <div class="stat-label">下跌板块</div>
          </div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card class="stat-card">
          <div class="stat-content">
            <div class="stat-value" :class="summary.avg_change >= 0 ? 'up-color' : 'down-color'">
              {{ formatPercent(summary.avg_change) }}
            </div>
            <div class="stat-label">平均涨跌幅</div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 第一行：板块排行榜 + 板块K线图 -->
    <el-row :gutter="20">
      <!-- 左侧：板块排行榜 -->
      <el-col :span="14">
        <el-card>
          <template #header>
            <div class="card-header" style="display:flex; align-items:center; gap: 12px;">
              <span>📊 板块排行榜</span>
              <div class="search-controls" style="margin-left:auto;">
                <el-input
                  v-model="sectorSearchQuery"
                  placeholder="输入板块名称搜索（行业/概念）"
                  clearable
                  style="width: 260px;"
                >
                  <template #prefix>
                    <el-icon><Search /></el-icon>
                  </template>
                </el-input>
              </div>
            </div>
          </template>

          <el-table
            :data="filteredTopSectors"
            stripe
            height="400"
            @row-click="selectSector"
            highlight-current-row
            :row-class-name="getSectorRowClass"
          >
            <el-table-column prop="板块名称" label="板块名称" width="120">
              <template #default="{ row }">
                {{ row.板块名称 }}
              </template>
            </el-table-column>
            <el-table-column prop="板块类型" label="类型" align="center" width="80">
              <template #default="{ row }">
                <el-tag
                  :type="row.板块类型 === '行业' ? 'success' : 'primary'"
                  size="small"
                >
                  {{ row.板块类型 === '行业' ? '行业' : '概念' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="涨跌幅" label="日涨跌幅" align="right" width="90" sortable :sort-method="(a, b) => sortByNumber(a, b, '涨跌幅')">
              <template #default="{ row }">
                <span :class="parseFloat(row.涨跌幅 || 0) >= 0 ? 'up-color' : 'down-color'">
                  {{ row.涨跌幅 !== null && row.涨跌幅 !== undefined ? formatPercent(row.涨跌幅) : '--' }}
                </span>
              </template>
            </el-table-column>
            <el-table-column prop="5日涨跌幅" label="5日涨跌幅" align="right" width="90" sortable :sort-method="(a, b) => sortByNumber(a, b, '5日涨跌幅')">
              <template #default="{ row }">
                <span :class="parseFloat(row['5日涨跌幅'] || 0) >= 0 ? 'up-color' : 'down-color'">
                  {{ row['5日涨跌幅'] !== null && row['5日涨跌幅'] !== undefined ? formatPercent(row['5日涨跌幅']) : '--' }}
                </span>
              </template>
            </el-table-column>
            <el-table-column prop="10日涨跌幅" label="10日涨跌幅" align="right" width="90" sortable :sort-method="(a, b) => sortByNumber(a, b, '10日涨跌幅')">
              <template #default="{ row }">
                <span :class="parseFloat(row['10日涨跌幅'] || 0) >= 0 ? 'up-color' : 'down-color'">
                  {{ row['10日涨跌幅'] !== null && row['10日涨跌幅'] !== undefined ? formatPercent(row['10日涨跌幅']) : '--' }}
                </span>
              </template>
            </el-table-column>
            <!-- 动态区间涨跌幅列 -->
            <el-table-column
              v-for="interval in allIntervals"
              :key="interval.key"
              :prop="interval.key"
              :label="interval.name"
              align="right"
              width="90"
              sortable
              :sort-method="(a, b) => sortByNumber(a, b, interval.key)"
            >
              <template #default="{ row }">
                <span :class="parseFloat(row[interval.key] || 0) >= 0 ? 'up-color' : 'down-color'">
                  {{ row[interval.key] !== null && row[interval.key] !== undefined ? formatPercent(row[interval.key]) : '--' }}
                </span>
              </template>
            </el-table-column>
            <el-table-column prop="成交额" label="成交额" align="right" width="100" sortable :sort-method="(a, b) => sortByNumber(a, b, '成交额_原始')">
              <template #default="{ row }">
                {{ row.成交额_格式化 || formatAmount(row.成交额) }}
              </template>
            </el-table-column>
            <el-table-column prop="成交额量比" label="量比" align="right" width="80" sortable :sort-method="(a, b) => sortByNumber(a, b, '成交额量比')">
              <template #default="{ row }">
                <span :class="parseFloat(row.成交额量比 || 1) >= 1 ? 'up-color' : 'down-color'">
                  {{ row.成交额量比 ? parseFloat(row.成交额量比).toFixed(2) : '--' }}
                </span>
              </template>
            </el-table-column>
            <el-table-column prop="连阳天数" label="连阳天数" align="center" width="90" sortable :sort-method="(a, b) => sortByNumber(a, b, '连阳天数')">
              <template #default="{ row }">
                <span v-if="row.连阳天数 && row.连阳天数 > 0" class="consecutive-days-badge">
                  {{ row.连阳天数 }}天
                </span>
                <span v-else class="no-consecutive-days">--</span>
              </template>
            </el-table-column>
            <!-- 移除股票数量列以提升加载性能 -->
          </el-table>
        </el-card>
      </el-col>

      <!-- 右侧：板块K线图 -->
      <el-col :span="10">
        <el-card>
          <template #header>
            <div class="card-header">
              <span>{{ selectedSector ? `${selectedSector.板块名称} - K线图` : '板块K线图' }}</span>
            </div>
          </template>

          <!-- 板块K线图 -->
          <div v-if="sectorKlineData.length > 0" class="chart-container">
            <v-chart 
              :option="sectorKlineOption" 
              :style="{ height: '400px', width: '100%' }"
              autoresize
            />
          </div>

          <div v-else class="no-chart">
            <div v-if="klineLoading" class="loading-state">
              <el-skeleton :rows="8" animated />
              <div style="text-align: center; margin-top: 10px; color: #666;">
                正在加载板块K线图...
              </div>
            </div>
            <el-empty v-else description="请点击左侧板块查看K线图" />
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 第二行：板块成分股 + 个股K线图 -->
    <el-row :gutter="20" class="chart-row">
      <!-- 左侧：成分股列表 -->
      <el-col :span="12">
        <el-card>
          <template #header>
            <div class="card-header" style="display:flex; align-items:center; gap: 12px;">
              <span>{{ selectedSector ? `${selectedSector.板块名称} - 成分股` : '板块成分股' }}</span>
              <div v-if="selectedSector" class="sector-info" style="gap:8px;">
                <el-tag
                  :type="selectedSector.板块类型 === '行业' ? 'success' : 'primary'"
                  size="small"
                >
                  {{ selectedSector.板块类型 === '行业' ? '行业' : '概念' }}
                </el-tag>
                <span
                  class="sector-change"
                  :class="selectedSector.涨跌幅 >= 0 ? 'up-color' : 'down-color'"
                  style="font-weight: 500;"
                >
                  {{ selectedSector.涨跌幅 !== null && selectedSector.涨跌幅 !== undefined ? formatPercent(selectedSector.涨跌幅) : '--' }}
                </span>
              </div>
              <div class="search-controls" style="margin-left:auto;">
                <el-input
                  v-model="sectorStocksSearchQuery"
                  placeholder="输入股票名称或代码搜索成分股"
                  clearable
                  style="width: 260px;"
                >
                  <template #prefix>
                    <el-icon><Search /></el-icon>
                  </template>
                </el-input>
              </div>
            </div>
          </template>

          <div v-if="selectedSector">
            <el-table
              :data="filteredSectorStocks"
              stripe
              height="500"
              @row-click="selectStock"
              highlight-current-row
              :row-class-name="getStockRowClass"
            >
              <el-table-column prop="名称" label="股票名称" width="120" />
              <el-table-column prop="代码" label="代码" width="100" />
              <el-table-column prop="涨跌幅" label="涨跌幅" align="right" width="100" sortable :sort-method="(a, b) => sortByNumber(a, b, '涨跌幅')">
                <template #default="{ row }">
                  <span :class="parseFloat(row.涨跌幅 || 0) >= 0 ? 'up-color' : 'down-color'">
                    {{ row.涨跌幅 !== null && row.涨跌幅 !== undefined ? formatPercent(row.涨跌幅) : '--' }}
                  </span>
                </template>
              </el-table-column>
              <el-table-column prop="5日涨跌幅" label="5日涨跌幅" align="right" width="100" sortable :sort-method="(a, b) => sortByNumber(a, b, '5日涨跌幅')">
                <template #default="{ row }">
                  <span :class="parseFloat(row['5日涨跌幅'] || 0) >= 0 ? 'up-color' : 'down-color'">
                    {{ row['5日涨跌幅'] !== null && row['5日涨跌幅'] !== undefined ? formatPercent(row['5日涨跌幅']) : '--' }}
                  </span>
                </template>
              </el-table-column>
              <el-table-column prop="10日涨跌幅" label="10日涨跌幅" align="right" width="100" sortable :sort-method="(a, b) => sortByNumber(a, b, '10日涨跌幅')">
                <template #default="{ row }">
                  <span :class="parseFloat(row['10日涨跌幅'] || 0) >= 0 ? 'up-color' : 'down-color'">
                    {{ row['10日涨跌幅'] !== null && row['10日涨跌幅'] !== undefined ? formatPercent(row['10日涨跌幅']) : '--' }}
                  </span>
                </template>
              </el-table-column>
              <el-table-column prop="成交额" label="成交额" align="right" width="100" sortable :sort-method="(a, b) => sortByNumber(a, b, '成交额')">
                <template #default="{ row }">
                  {{ formatAmount(row.成交额) }}
                </template>
              </el-table-column>
              <el-table-column prop="趋势" label="趋势" align="center" width="110" sortable :sort-method="sortByTrend">
                <template #default="{ row }">
                  <span v-if="row.连阳天数 && row.连阳天数 > 0" style="display:inline-block;border:1px solid #f56c6c;color:#f56c6c;padding:2px 6px;border-radius:4px;min-width:40px;">
                    +{{ row.连阳天数 }}
                  </span>
                  <span v-else-if="row.连阴天数 && row.连阴天数 > 0" style="display:inline-block;border:1px solid #67c23a;color:#67c23a;padding:2px 6px;border-radius:4px;min-width:40px;">
                    -{{ row.连阴天数 }}
                  </span>
                  <span v-else class="no-consecutive-days">--</span>
                </template>
              </el-table-column>
              <!-- 动态区间涨跌幅列（成分股） -->
              <el-table-column
                v-for="interval in allIntervals"
                :key="'stock-' + interval.key"
                :prop="interval.key"
                :label="interval.name"
                align="right"
                width="110"
                sortable
                :sort-method="(a, b) => sortByNumber(a, b, interval.key)"
              >
                <template #default="{ row }">
                  <span :class="parseFloat(row[interval.key] || 0) >= 0 ? 'up-color' : 'down-color'">
                    {{ row[interval.key] !== null && row[interval.key] !== undefined ? formatPercent(row[interval.key]) : '--' }}
                  </span>
                </template>
              </el-table-column>
              <el-table-column prop="最新价" label="最新价" align="right" width="100" sortable :sort-method="(a, b) => sortByNumber(a, b, '最新价')">
                <template #default="{ row }">
                  {{ row.最新价 !== null && row.最新价 !== undefined ? parseFloat(row.最新价).toFixed(2) : '--' }}
                </template>
              </el-table-column>
            </el-table>
          </div>
          <div v-else class="no-data">
            <el-empty description="请点击左侧板块查看成分股" />
          </div>
        </el-card>
      </el-col>

      <!-- 右侧：个股K线图 -->
      <el-col :span="12">
        <el-card>
          <template #header>
            <div class="card-header">
              <span>{{ selectedStock ? `${selectedStock.名称}(${selectedStock.代码}) - K线图` : '个股K线图' }}</span>
            </div>
          </template>

          <!-- 个股K线图 -->
          <div v-if="stockKlineData.length > 0" class="chart-container">
            <v-chart 
              :option="stockKlineOption" 
              :style="{ height: '600px', width: '100%' }"
              autoresize
            />
          </div>

          <div v-else class="no-chart">
            <div v-if="stockKlineLoading" class="loading-state">
              <el-skeleton :rows="8" animated />
              <div style="text-align: center; margin-top: 10px; color: #666;">
                正在加载个股K线图...
              </div>
            </div>
            <el-empty v-else description="请点击左侧成分股查看个股K线图" />
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 第三行：多个板块K线图对比 -->
    <el-row :gutter="20" class="chart-row">
      <el-col :span="24">
        <el-card>
          <template #header>
            <div class="card-header">
              <span>📊 多对象对比</span>
              <div class="stock-comparison-controls">
                <el-switch
                  v-model="normalizeComparison"
                  active-text="涨跌幅对比"
                  inactive-text="价格对比"
                  style="margin-right: 15px;"
                />
                <div class="time-range-controls">
                  <el-radio-group v-model="timeRangeType" style="margin-right: 15px;">
                    <el-radio-button label="preset">快速选择</el-radio-button>
                    <el-radio-button label="custom">自定义时间</el-radio-button>
                  </el-radio-group>

                  <div v-if="timeRangeType === 'preset'" class="preset-controls">
                    <el-select
                      v-model="comparisonDays"
                      style="width: 120px; margin-right: 10px;"
                    >
                      <el-option label="最近15天" :value="15" />
                      <el-option label="最近30天" :value="30" />
                      <el-option label="最近60天" :value="60" />
                      <el-option label="最近90天" :value="90" />
                    </el-select>
                  </div>

                  <div v-else class="custom-controls">
                    <el-date-picker
                      v-model="customStartDate"
                      type="date"
                      placeholder="开始日期"
                      format="YYYY-MM-DD"
                      value-format="YYYY-MM-DD"
                      style="width: 140px; margin-right: 8px;"
                      size="default"
                    />
                    <el-date-picker
                      v-model="customEndDate"
                      type="date"
                      placeholder="结束日期"
                      format="YYYY-MM-DD"
                      value-format="YYYY-MM-DD"
                      style="width: 140px; margin-right: 10px;"
                      size="default"
                    />
                  </div>
                </div>

                <el-button
                  type="primary"
                  @click="generateMixedComparison()"
                  :loading="comparisonLoading"
                  :disabled="(selectedStocks.length===0 && selectedSectors.length===0) || !isTimeRangeValid"
                >
                  <el-icon><TrendCharts /></el-icon>
                  生成对比图
                </el-button>
              </div>
            </div>
          </template>

          <!-- 搜索和选择区域（股票+板块混合） -->
          <div class="stock-selection-area">
            <div class="search-section">
              <h4>添加对象（股票/板块）</h4>
              <div class="search-controls">
                <el-input
                  v-model="compareSearchQuery"
                  placeholder="输入股票代码/名称或板块名称搜索"
                  style="width: 300px; margin-right: 10px;"
                  @input="onCompareSearch()"
                  clearable
                >
                  <template #prefix>
                    <el-icon><Search /></el-icon>
                  </template>
                </el-input>
                <el-button type="info" @click="clearAllCompareItems">清空所有</el-button>
              </div>

              <!-- 股票搜索结果 -->
              <div v-if="stockSearchResults.length > 0" class="search-results">
                <div class="search-results-header">搜索结果：</div>
                <div class="search-results-list">
                  <el-tag
                    v-for="stock in stockSearchResults"
                    :key="stock.代码"
                    class="search-result-tag"
                    @click="addStock(stock)"
                    :type="isStockSelected(stock.代码) ? 'success' : 'info'"
                  >
                    {{ stock.名称 }}({{ stock.代码 }})
                    <span class="stock-price" :class="parseFloat(stock.涨跌幅) >= 0 ? 'up-color' : 'down-color'">
                      {{ stock.涨跌幅 }}%
                    </span>
                  </el-tag>
                </div>
              </div>

              <!-- 板块搜索结果 -->
              <div v-if="sectorSearchResults.length > 0" class="search-results">
                <div class="search-results-header">搜索结果：</div>
                <div class="search-results-list">
                  <el-tag
                    v-for="sec in sectorSearchResults"
                    :key="sec"
                    class="search-result-tag"
                    @click="addSector(sec)"
                    :type="isSectorSelected(sec) ? 'success' : 'info'"
                  >
                    {{ sec }}
                  </el-tag>
                </div>
              </div>
            </div>

            <!-- 已选择的股票/板块 -->
            <div class="selected-section">
              <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                <h4>已选择对象 ({{ selectedStocks.length + selectedSectors.length }}/30)</h4>
                <div style="display: flex; gap: 10px;">
                  <el-button size="small" @click="showSaveGroupDialog = true" :disabled="selectedStocks.length + selectedSectors.length === 0">
                    💾 保存组合
                  </el-button>
                  <el-button size="small" @click="showLoadGroupDialog = true">
                    📂 加载组合
                  </el-button>
                </div>
              </div>
              <div v-if="selectedStocks.length > 0 || selectedSectors.length > 0" class="selected-stocks">
                <el-tag
                  v-for="stock in selectedStocks"
                  :key="stock.代码"
                  closable
                  @close="removeStock(stock.代码)"
                  type="success"
                  size="large"
                  class="selected-stock-tag"
                >
                  {{ stock.名称 }}({{ stock.代码 }})
                </el-tag>
                <el-tag
                  v-for="sec in selectedSectors"
                  :key="sec"
                  closable
                  @close="removeSector(sec)"
                  type="primary"
                  size="large"
                  class="selected-stock-tag"
                >
                  {{ sec }}
                </el-tag>
              </div>
              <div v-else class="no-stocks-selected">
                <el-empty description="请搜索并添加股票/板块进行对比分析" :image-size="80" />
              </div>
            </div>
          </div>

          <!-- 对比图表 -->
          <div class="comparison-chart-area">
            <EChartsRenderer
              v-if="stockComparisonChart"
              :chartHtml="stockComparisonChart"
              height="800px" />
            <div v-else class="no-chart">
              <el-empty description="添加股票/板块并点击生成对比图按钮查看对比分析" />
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 板块涨跌幅排行榜、资金流向分析、趋势分析功能已删除 -->

    <!-- 保存对象组合对话框 -->
    <el-dialog v-model="showSaveGroupDialog" title="💾 保存对象组合" width="500px">
      <el-form :model="saveGroupForm" label-width="80px">
        <el-form-item label="组合名称" required>
          <el-input v-model="saveGroupForm.name" placeholder="请输入组合名称" maxlength="50" />
        </el-form-item>
        <el-form-item label="组合描述">
          <el-input
            v-model="saveGroupForm.description"
            type="textarea"
            placeholder="请输入组合描述（可选）"
            maxlength="200"
            :rows="3"
          />
        </el-form-item>
        <el-form-item label="对象数量">
          <span>{{ selectedStocks.length + selectedSectors.length }} 个对象（{{ selectedStocks.length }}只股票，{{ selectedSectors.length }}个板块）</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showSaveGroupDialog = false">取消</el-button>
        <el-button type="primary" @click="saveStockGroup" :loading="savingGroup">保存</el-button>
      </template>
    </el-dialog>

    <!-- 保存时间区间组合对话框 -->
    <el-dialog v-model="showSaveIntervalGroup" title="💾 保存时间区间组合" width="500px">
      <el-form :model="saveIntervalGroupForm" label-width="80px">
        <el-form-item label="组合名称" required>
          <el-input v-model="saveIntervalGroupForm.name" placeholder="请输入组合名称" maxlength="50" />
        </el-form-item>
        <el-form-item label="组合描述">
          <el-input v-model="saveIntervalGroupForm.description" type="textarea" placeholder="请输入组合描述（可选）" maxlength="200" :rows="3" />
        </el-form-item>
        <el-form-item label="快速区间">
          <div>{{ selectedQuickIntervals.join(', ') || '无' }}</div>
        </el-form-item>
        <el-form-item label="自定义区间">
          <div>
            <el-tag v-for="(it, idx) in customIntervals" :key="idx" style="margin-right: 6px;">
              {{ it.name }} ({{ it.start_date || it.startDate }} 至 {{ it.end_date || it.endDate }})
            </el-tag>
            <span v-if="customIntervals.length === 0">无</span>
          </div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showSaveIntervalGroup = false">取消</el-button>
        <el-button type="primary" @click="saveIntervalGroup">保存</el-button>
      </template>
    </el-dialog>

    <!-- 加载时间区间组合对话框 -->
    <el-dialog v-model="showLoadIntervalGroup" title="📂 加载时间区间组合" width="700px">
      <div v-if="loading" style="text-align: center; padding: 20px;">
        <el-icon class="is-loading"><Loading /></el-icon>
        <p>正在加载区间组合...</p>
      </div>
      <div v-else>
        <el-table :data="intervalGroups" style="width: 100%">
          <el-table-column prop="name" label="组合名称" width="180" />
          <el-table-column prop="description" label="描述" show-overflow-tooltip />
          <el-table-column prop="created_at" label="创建时间" width="170">
            <template #default="{ row }">{{ formatDate(row.created_at) }}</template>
          </el-table-column>
          <el-table-column label="操作" width="200" align="center">
            <template #default="{ row }">
              <el-button size="small" type="primary" @click="applyIntervalGroup(row)">应用</el-button>
              <el-button size="small" type="danger" @click="deleteIntervalGroup(row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>
      </div>
      <template #footer>
        <el-button @click="showLoadIntervalGroup = false">关闭</el-button>
      </template>
    </el-dialog>

    <!-- 加载对象组合对话框 -->
    <el-dialog v-model="showLoadGroupDialog" title="📂 加载对象组合" width="600px">
      <div v-if="loadingGroups" style="text-align: center; padding: 20px;">
        <el-icon class="is-loading"><Loading /></el-icon>
        <p>正在加载对象组合...</p>
      </div>
      <div v-else-if="stockGroups.length === 0" style="text-align: center; padding: 20px;">
        <el-empty description="暂无保存的对象组合" />
      </div>
      <div v-else>
        <el-table :data="stockGroups" style="width: 100%">
          <el-table-column prop="name" label="组合名称" width="150" />
          <el-table-column prop="description" label="描述" show-overflow-tooltip />
          <el-table-column label="对象数量" width="100" align="center">
            <template #default="{ row }">
              {{ row.object_count ?? ((row.stock_count || 0) + (row.sector_count || 0)) }}
            </template>
          </el-table-column>
          <el-table-column prop="created_at" label="创建时间" width="120">
            <template #default="{ row }">
              {{ formatDate(row.created_at) }}
            </template>
          </el-table-column>
          <el-table-column label="操作" width="120" align="center">
            <template #default="{ row }">
              <el-button size="small" @click="loadStockGroup(row)">加载</el-button>
              <el-button size="small" type="danger" @click="deleteStockGroup(row)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>
      </div>
      <template #footer>
        <el-button @click="showLoadGroupDialog = false">关闭</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script>
import { ref, onMounted, computed, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import ApiService, { utils } from '../services/api'
import EChartsRenderer from '../components/EChartsRenderer.vue'
import { TrendCharts, Search, Loading } from '@element-plus/icons-vue'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { LineChart, CandlestickChart, BarChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent
} from 'echarts/components'
import VChart from 'vue-echarts'
import TradingDatePicker from '../components/TradingDatePicker.vue'

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
  DataZoomComponent
])

export default {
  name: 'SectorAnalysis',
  components: {
    EChartsRenderer,
    VChart,
    TrendCharts,
    Search,
    Loading,
    TradingDatePicker
  },
  setup() {
    const sectorData = ref({})
    const charts = ref({})
    const loading = ref(false)

    // 全局日期控制
    const globalDate = ref(new Date().toISOString().split('T')[0])

    // 筛选控制
    const includeSectors = ref(true)  // 默认包含行业板块
    const includeConcepts = ref(true) // 默认包含概念板块

    // 多时间区间控制（默认选中5日/10日，确保成分股显示对应涨跌幅）
    const selectedQuickIntervals = ref(['5d', '10d'])
    const customInterval = ref({
      startDate: '',
      endDate: '',
      name: ''
    })
    const customIntervals = ref([])

    // 旧的自定义区间控制（保留兼容性）
    const oldCustomStartDate = ref('')
    const oldCustomEndDate = ref('')
    const showCustomPeriod = ref(false)

    // K线图相关数据
    const selectedIndex = ref('')
    const sectorKlineChart = ref('')
    const sectorKlineData = ref([])
    const klineLoading = ref(false)

    // 板块和个股选择相关数据
    const selectedSector = ref(null)
    const selectedStock = ref(null)
    const sectorStocks = ref([])
    const sectorStocksSearchQuery = ref('')
    const stocksLoading = ref(false)
    const stockKlineChart = ref('')
    const stockKlineData = ref([])
    const stockKlineLoading = ref(false)

    // 个股对比相关数据
    const stockSearchQuery = ref('')
    const stockSearchResults = ref([])
    const selectedStocks = ref([])
    const stockComparisonChart = ref('')
    const comparisonLoading = ref(false)
    const normalizeComparison = ref(true)  // 默认使用涨跌幅对比
    const comparisonDays = ref(30)  // 默认30天
    const timeRangeType = ref('preset')  // 时间范围类型：preset 或 custom
    const customStartDate = ref('')  // 自定义开始日期
    const customEndDate = ref('')    // 自定义结束日期
    // 对比对象统一搜索（股票+板块）
    const compareSearchQuery = ref('')
    const sectorSearchResults = ref([])
    const selectedSectors = ref([])
    let searchTimeout = null

    // 股票组合管理相关数据
    const showSaveGroupDialog = ref(false)
    const showLoadGroupDialog = ref(false)
    // 新增：时间区间组合弹窗
    const showSaveIntervalGroup = ref(false)
    const showLoadIntervalGroup = ref(false)
    const savingGroup = ref(false)
    const loadingGroups = ref(false)
    const stockGroups = ref([])
    const saveGroupForm = ref({
      name: '',
      description: ''
    })
    const saveIntervalGroupForm = ref({
      name: '',
      description: ''
    })
    const intervalGroups = ref([])

    // 计算属性
    const summary = computed(() => sectorData.value.summary || {})
    const topSectors = computed(() => sectorData.value.top_sectors || [])
    const sectorSearchQuery = ref('')
    const filteredTopSectors = computed(() => {
      const q = (sectorSearchQuery.value || '').trim()
      if (!q) return topSectors.value
      return topSectors.value.filter(s => (s.板块名称 || '').includes(q))
    })
    const filteredSectorStocks = computed(() => {
      const q = (sectorStocksSearchQuery.value || '').trim()
      if (!q) return sectorStocks.value
      return sectorStocks.value.filter(stk => (
        (stk.名称 && stk.名称.includes(q)) ||
        (stk.代码 && String(stk.代码).includes(q))
      ))
    })

    // 检查是否可以添加自定义时间区间
    const canAddCustomInterval = computed(() => {
      return customInterval.value.startDate &&
             customInterval.value.endDate &&
             customInterval.value.name.trim()
    })

    // 所有时间区间（包括快速选择和自定义）
    const allIntervals = computed(() => {
      const intervals = []

      // 固定列已包含：5日/10日涨跌幅，动态列需排除
      const fixedKeys = new Set(['5日涨跌幅', '10日涨跌幅'])

      // 添加快速选择的区间（排除已在固定列中的键）
      selectedQuickIntervals.value.forEach(interval => {
        let name, key
        switch (interval) {
          case '5d':
            name = '5日涨跌幅'
            key = '5日涨跌幅'
            break
          case '10d':
            name = '10日涨跌幅'
            key = '10日涨跌幅'
            break
          case '30d':
            name = '30日涨跌幅'
            key = '30日涨跌幅'
            break
          case '90d':
            name = '90日涨跌幅'
            key = '90日涨跌幅'
            break
        }
        if (name && key && !fixedKeys.has(key)) {
          intervals.push({ name, key })
        }
      })

      // 添加自定义区间
      customIntervals.value.forEach(interval => {
        const key = `区间_${interval.name}`
        if (!fixedKeys.has(key)) {
          intervals.push({
            name: interval.name,
            key
          })
        }
      })

      return intervals
    })

    // 验证时间范围是否有效
    const isTimeRangeValid = computed(() => {
      if (timeRangeType.value === 'preset') {
        return comparisonDays.value > 0
      } else {
        return customStartDate.value && customEndDate.value &&
               new Date(customStartDate.value) < new Date(customEndDate.value)
      }
    })

    // 图表标题
    const chartTitle = computed(() => {
      if (selectedSector.value && selectedStock.value) {
        return `${selectedSector.value.板块名称} - ${selectedStock.value.名称}K线图`
      } else if (selectedSector.value) {
        return `${selectedSector.value.板块名称}K线图`
      } else {
        return 'K线图'
      }
    })

    // 板块K线图配置
    const sectorKlineOption = computed(() => {
      if (sectorKlineData.value.length === 0) return {}

      const dates = sectorKlineData.value.map(item => item.date)
      const candlestickData = sectorKlineData.value.map(item => [
        item.open, item.close, item.low, item.high
      ])
      const volumeData = sectorKlineData.value.map(item => item.volume || item.amount || 0)

      return {
        title: { show: false },
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'cross'
          }
        },
        grid: [
          {
            left: '10%',
            right: '8%',
            height: '60%'
          },
          {
            left: '10%',
            right: '8%',
            top: '75%',
            height: '15%'
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
            bottom: 6,
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
            name: '成交量',
            type: 'bar',
            xAxisIndex: 1,
            yAxisIndex: 1,
            data: volumeData.map((volume, index) => {
              const klineItem = candlestickData[index]
              const isUp = klineItem && klineItem[1] >= klineItem[0] // close >= open
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

    // 个股K线图配置
    const stockKlineOption = computed(() => {
      if (stockKlineData.value.length === 0) return {}

      const dates = stockKlineData.value.map(item => item.date)
      const candlestickData = stockKlineData.value.map(item => [
        item.open, item.close, item.low, item.high
      ])
      const volumeData = stockKlineData.value.map(item => item.volume || 0)
      const ma5Data = stockKlineData.value.map(item => item.ma5)
      const ma10Data = stockKlineData.value.map(item => item.ma10)
      const ma20Data = stockKlineData.value.map(item => item.ma20)

      return {
        title: { show: false },
        tooltip: {
          trigger: 'axis',
          axisPointer: {
            type: 'cross'
          }
        },
        legend: {
          data: ['MA5', 'MA10', 'MA20'],
          top: 30,
          right: 10
        },
        grid: [
          {
            left: '10%',
            right: '8%',
            height: '60%'
          },
          {
            left: '10%',
            right: '8%',
            top: '75%',
            height: '15%'
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
            bottom: 6,
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
              color: '#4ECDC4'  // 统一图例颜色
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
              color: '#ffbf00'  // 统一图例颜色
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
              color: '#f92672'  // 统一图例颜色
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
              const isUp = klineItem && klineItem[1] >= klineItem[0] // close >= open
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

    const loadData = async (date = null) => {
      try {
        loading.value = true

        // 使用指定日期或全局日期
        const targetDate = date || globalDate.value

        // 获取合并的板块数据，传递筛选参数
        const response = await ApiService.getSectorData(targetDate, {
          include_sectors: includeSectors.value,
          include_concepts: includeConcepts.value
        })

        // 修复数据结构问题：后端返回的data字段直接是板块数组
        let sectors = null

        if (response.data && response.data.data && Array.isArray(response.data.data)) {
          sectors = response.data.data
        } else if (response.data && Array.isArray(response.data)) {
          sectors = response.data
        }

        if (sectors && sectors.length > 0) {
          // 计算统计信息（处理None值）
          const toNumber = (v) => {
            if (v === null || v === undefined) return 0
            const num = parseFloat(v)
            return isNaN(num) ? 0 : num
          }
          const totalSectors = sectors.length
          const changes = sectors.map(s => toNumber(s['涨跌幅'] ?? s['涨跌幅_原始']))
          const upSectors = changes.filter(c => c > 0).length
          const downSectors = changes.filter(c => c < 0).length
          const flatSectors = totalSectors - upSectors - downSectors
          const sumChange = changes.reduce((acc, v) => acc + v, 0)
          const avgChange = totalSectors > 0 ? +(sumChange / totalSectors).toFixed(2) : 0

          // 包装成前端期望的数据结构
          // 合并自定义区间与快速区间（如果有）
          let mergedSectors = sectors.map(s => ({ ...s }))

          // 快速区间列（5/10/30/90）后端若已提供直接展示，无需请求
          // 自定义区间：批量请求并合并（排除5/10日，避免覆盖后端已提供列）
          const intervals = generateIntervals().filter(it => !['5日涨跌幅', '10日涨跌幅'].includes(it.key))
          if (intervals.length > 0) {
            try {
              // 逐个区间获取并合并
              for (const it of intervals) {
                const resp = await ApiService.getSectorCustomPeriod({
                  start_date: it.start_date,
                  end_date: it.end_date,
                  include_sectors: includeSectors.value,
                  include_concepts: includeConcepts.value
                })
                if (resp && resp.success) {
                  const data = resp.data || []
                  const map = new Map(data.map(d => [d.板块名称, d.区间涨跌幅]))
                  mergedSectors = mergedSectors.map(s => ({
                    ...s,
                    [it.key]: map.has(s.板块名称) ? map.get(s.板块名称) : null
                  }))
                }
              }
            } catch (e) {
              console.warn('合并自定义区间失败:', e)
            }
          }

          sectorData.value = {
            top_sectors: mergedSectors,
            summary: {
              total_sectors: totalSectors,
              up_sectors: upSectors,
              down_sectors: downSectors,
              flat_sectors: flatSectors,
              avg_change: avgChange
            }
          }

          console.log('板块数据加载成功:', sectors.length, '条记录')
          console.log('统计信息:', sectorData.value.summary)
        } else {
          sectorData.value = { top_sectors: [], summary: {} }
          console.warn('板块数据格式异常:', response.data)
          ElMessage.error('板块数据加载失败')
        }

        // 图表功能已删除

      } catch (error) {
        console.error('获取板块数据失败:', error)
        ElMessage.error('获取数据失败，请稍后重试')
      } finally {
        loading.value = false
      }
    }

    // 全局日期变化处理
    const onGlobalDateChange = (date) => {
      if (date) {
        // 重新加载所有数据
        loadAllData()
      }
    }

    // 筛选条件变化处理
    const onFilterChange = () => {
      // 至少要选择一个类型
      if (!includeSectors.value && !includeConcepts.value) {
        ElMessage.warning('至少要选择一种板块类型')
        // 恢复之前的状态
        if (!includeSectors.value) includeSectors.value = true
        if (!includeConcepts.value) includeConcepts.value = true
        return
      }

      // 重新加载数据
      loadAllData()
    }

    // 加载所有数据
    const loadAllData = async () => {
      await loadData()
      // 如果有K线图，也重新生成
      if (sectorKlineChart.value) {
        await loadSectorKline()
      }
    }

    // 自定义区间变化处理
    const onCustomPeriodChange = () => {
      if (oldCustomStartDate.value && oldCustomEndDate.value) {
        // 验证日期有效性
        const start = new Date(oldCustomStartDate.value)
        const end = new Date(oldCustomEndDate.value)

        if (start >= end) {
          ElMessage.warning('开始日期必须早于结束日期')
          return
        }

        // 检查日期范围是否合理（不超过1年）
        const diffDays = (end - start) / (1000 * 60 * 60 * 24)
        if (diffDays > 365) {
          ElMessage.warning('自定义区间不能超过1年')
          return
        }
      }
    }

    // 计算自定义区间涨跌幅
    const calculateCustomPeriod = async () => {
      if (!oldCustomStartDate.value || !oldCustomEndDate.value) {
        ElMessage.warning('请选择开始和结束日期')
        return
      }

      try {
        loading.value = true

        // 调用API计算自定义区间涨跌幅
        const response = await ApiService.getSectorCustomPeriod({
          start_date: oldCustomStartDate.value,
          end_date: oldCustomEndDate.value,
          include_sectors: includeSectors.value,
          include_concepts: includeConcepts.value
        })

        if (response.success) {
          // 合并自定义区间数据到现有数据
          const customData = response.data
          const updatedSectors = topSectors.value.map(sector => {
            const customSector = customData.find(item => item.板块名称 === sector.板块名称)
            return {
              ...sector,
              自定义涨跌幅: customSector ? customSector.区间涨跌幅 : null
            }
          })

          // 更新数据
          sectorData.value = {
            ...sectorData.value,
            top_sectors: updatedSectors
          }

          showCustomPeriod.value = true
          ElMessage.success(`已计算 ${oldCustomStartDate.value} 至 ${oldCustomEndDate.value} 的区间涨跌幅`)
        } else {
          ElMessage.error('计算自定义区间涨跌幅失败')
        }

      } catch (error) {
        console.error('计算自定义区间涨跌幅失败:', error)
        ElMessage.error('计算失败，请稍后重试')
      } finally {
        loading.value = false
      }
    }

    // 多时间区间方法
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

      ElMessage.success(`已添加时间区间: ${customInterval.value.name}`)
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
          custom_intervals: customIntervals.value.map(i => ({
            start_date: i.start_date || i.startDate,
            end_date: i.end_date || i.endDate,
            name: i.name
          })),
          global_date: globalDate.value,
          comparison: {
            time_range_type: timeRangeType.value,
            days: comparisonDays.value,
            custom_start_date: customStartDate.value,
            custom_end_date: customEndDate.value
          }
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

    // 区间组合：加载列表
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

    // 区间组合：应用
    const applyIntervalGroup = (group) => {
      selectedQuickIntervals.value = Array.isArray(group.selected_quick_intervals) ? group.selected_quick_intervals : []
      customIntervals.value = Array.isArray(group.custom_intervals) ? group.custom_intervals : []
      if (group.global_date) {
        globalDate.value = group.global_date
      }
      if (group.comparison) {
        const cmp = group.comparison
        if (cmp.time_range_type === 'custom') {
          timeRangeType.value = 'custom'
          customStartDate.value = cmp.custom_start_date || ''
          customEndDate.value = cmp.custom_end_date || ''
        } else {
          timeRangeType.value = 'preset'
          comparisonDays.value = cmp.days || comparisonDays.value
        }
      }
      showLoadIntervalGroup.value = false
      ElMessage.success(`已应用组合: ${group.name}`)
      // 应用后自动刷新
      loadAllData()
    }

    // 区间组合：删除
    const deleteIntervalGroup = async (group) => {
      try {
        await ElMessageBox.confirm(`确定要删除区间组合 "${group.name}" 吗？`, '确认删除', {
          confirmButtonText: '删除',
          cancelButtonText: '取消',
          type: 'warning'
        })
        const res = await ApiService.deleteIntervalGroup(group.id)
        if (res.success) {
          ElMessage.success(res.message || '删除成功')
          // 刷新列表
          const listRes = await ApiService.getIntervalGroups()
          intervalGroups.value = listRes.success ? (listRes.data || []) : []
        } else {
          ElMessage.error(res.error || '删除失败')
        }
      } catch (e) {
        if (e !== 'cancel') {
          ElMessage.error('删除失败: ' + e.message)
        }
      }
    }

    const removeCustomInterval = (index) => {
      const removed = customIntervals.value.splice(index, 1)[0]
      ElMessage.info(`已移除时间区间: ${removed.name}`)
    }

    // 生成所有时间区间
    const generateIntervals = () => {
      const intervals = []
      // 使用选中的全局日期作为区间结束日期；若未选则用今天
      const endRef = globalDate.value ? new Date(globalDate.value) : new Date()

      // 添加快速选择的时间区间
      selectedQuickIntervals.value.forEach(interval => {
        let startDate, endDate, name, key

        switch (interval) {
          case '5d':
            startDate = new Date(endRef.getTime() - 5 * 24 * 60 * 60 * 1000)
            endDate = endRef
            name = '5日涨跌幅'
            key = '5日涨跌幅'
            break
          case '10d':
            startDate = new Date(endRef.getTime() - 10 * 24 * 60 * 60 * 1000)
            endDate = endRef
            name = '10日涨跌幅'
            key = '10日涨跌幅'
            break
          case '30d':
            startDate = new Date(endRef.getTime() - 30 * 24 * 60 * 60 * 1000)
            endDate = endRef
            name = '30日涨跌幅'
            key = '30日涨跌幅'
            break
          case '90d':
            startDate = new Date(endRef.getTime() - 90 * 24 * 60 * 60 * 1000)
            endDate = endRef
            name = '90日涨跌幅'
            key = '90日涨跌幅'
            break
        }

        if (startDate && endDate) {
          intervals.push({
            start_date: startDate.toISOString().split('T')[0],
            end_date: endDate.toISOString().split('T')[0],
            name: name,
            key: key
          })
        }
      })

      // 添加自定义时间区间
      customIntervals.value.forEach(interval => {
        intervals.push({
          start_date: interval.start_date,
          end_date: interval.end_date,
          name: interval.name,
          key: `区间_${interval.name}`
        })
      })

      return intervals
    }

    const formatNumber = (value) => {
      if (!value) return '0'
      return utils.formatNumber(value)
    }

    const formatPercent = (value) => {
      if (!value) return '0.00%'
      return utils.formatPercent(value)
    }

    const formatMarketCap = (value) => {
      if (!value) return '0'
      const num = parseFloat(value)
      if (num >= 100000000) {
        return (num / 100000000).toFixed(2) + '亿'
      } else if (num >= 10000) {
        return (num / 10000).toFixed(2) + '万'
      }
      return num.toFixed(2)
    }

    const formatAmount = (value) => {
      if (!value) return '0'
      const num = parseFloat(value)
      if (num >= 100000000) {
        return (num / 100000000).toFixed(2) + '亿'
      } else if (num >= 10000000) {
        return (num / 10000000).toFixed(2) + '千万'
      } else if (num >= 10000) {
        return (num / 10000).toFixed(2) + '万'
      }
      return num.toFixed(2)
    }

    const formatVolume = (value) => {
      if (!value) return '0'
      const num = parseFloat(value)
      if (num >= 100000000) {
        return (num / 100000000).toFixed(2) + '亿'
      } else if (num >= 10000) {
        return (num / 10000).toFixed(2) + '万'
      }
      return num.toFixed(0)
    }

    const formatVolumeRatio = (value) => {
      if (!value && value !== 0) return '--'
      const num = parseFloat(value)
      if (isNaN(num)) return '--'

      // 显示倍数，保留2位小数
      return num.toFixed(2) + '倍'
    }

    const loadSectorKline = async () => {
      if (!selectedSector.value) {
        ElMessage.warning('请先选择板块')
        return
      }

      try {
        klineLoading.value = true

        // 构建请求参数，使用data格式，传递全局日期
        const params = {
          days_range: 30,  // 显示30天的K线数据
          format: 'data',  // 请求数据格式而不是HTML
          date: globalDate.value  // 传递选中的日期
        }

        console.log('🔧 请求板块K线数据:', selectedSector.value.板块名称, params)

        const response = await ApiService.getSingleSectorKline(selectedSector.value.板块名称, params)

        if (response && response.success && response.data.kline_data) {
          sectorKlineData.value = response.data.kline_data
          console.log('✅ 板块K线数据加载成功:', sectorKlineData.value.length, '条记录')
          // 清空旧的HTML格式
          sectorKlineChart.value = ''
        } else {
          ElMessage.error('获取板块K线数据失败: ' + (response.error || '未知错误'))
          sectorKlineData.value = []
        }

      } catch (error) {
        console.error('获取板块K线数据失败:', error)
        ElMessage.error('获取板块K线数据失败: ' + error.message)
        sectorKlineData.value = []
      } finally {
        klineLoading.value = false
      }
    }

    // 排序方法
    const sortByNumber = (a, b, field) => {
      const aValue = parseFloat(a[field]) || 0
      const bValue = parseFloat(b[field]) || 0
      return aValue - bValue
    }

    // 趋势排序方法：优先按连阳天数排序，其次按连阴天数排序
    const sortByTrend = (a, b) => {
      // 获取连阳天数（正值）或连阴天数（负值）
      const getTrendValue = (row) => {
        if (row.连阳天数 && row.连阳天数 > 0) {
          return row.连阳天数  // 连阳为正数
        } else if (row.连阴天数 && row.连阴天数 > 0) {
          return -row.连阴天数  // 连阴为负数
        }
        return 0  // 无趋势
      }

      const aValue = getTrendValue(a)
      const bValue = getTrendValue(b)
      return bValue - aValue  // 降序排列，连阳天数多的排在前面
    }

    // 统一搜索相关方法（股票+板块）
    const onCompareSearch = () => {
      if (searchTimeout) {
        clearTimeout(searchTimeout)
      }

      searchTimeout = setTimeout(async () => {
        const q = (compareSearchQuery.value || '').trim()
        if (!q) {
          stockSearchResults.value = []
          sectorSearchResults.value = []
          return
        }
        await Promise.all([searchStocks(q), searchSectors(q)])
      }, 300)
    }

    // 保留兼容：历史调用入口
    const onStockSearch = onCompareSearch
    const onSectorSearch = onCompareSearch

    const searchStocks = async (queryText = '') => {
      const q = (queryText || compareSearchQuery.value || '').trim()
      if (!q) {
        stockSearchResults.value = []
        return
      }
      try {
        const response = await ApiService.searchStocks(q)
        if (response.success) {
          stockSearchResults.value = response.data || []
        } else {
          ElMessage.error('搜索股票失败: ' + response.error)
          stockSearchResults.value = []
        }
      } catch (error) {
        console.error('搜索股票失败:', error)
        ElMessage.error('搜索股票失败，请稍后重试')
        stockSearchResults.value = []
      }
    }

    const searchSectors = async (queryText = '') => {
      const q = (queryText || compareSearchQuery.value || '').trim()
      if (!q) {
        sectorSearchResults.value = []
        return
      }
      try {
        const names = (topSectors.value || []).map(s => s.板块名称).filter(Boolean)
        const uniq = Array.from(new Set(names))
        sectorSearchResults.value = uniq.filter(n => n.includes(q)).slice(0, 50)
        if (sectorSearchResults.value.length === 0) {
          const resp = await ApiService.getSectorNames('both')
          if (resp.success) {
            const all = [
              ...(resp.data?.sector_names || []),
              ...(resp.data?.concept_names || [])
            ]
            sectorSearchResults.value = (all || []).filter(n => (n || '').includes(q)).slice(0, 50)
          }
        }
      } catch (e) {
        sectorSearchResults.value = []
      }
    }

    const isStockSelected = (stockCode) => {
      return selectedStocks.value.some(stock => stock.代码 === stockCode)
    }

    const addStock = (stock) => {
      const totalSelected = selectedStocks.value.length + selectedSectors.value.length
      if (totalSelected >= 30) {
        ElMessage.warning('最多只能选择30个对象进行对比')
        return
      }

      if (!isStockSelected(stock.代码)) {
        selectedStocks.value.push(stock)
        ElMessage.success(`已添加 ${stock.名称}(${stock.代码})`)

        // 清空搜索结果
        compareSearchQuery.value = ''
        stockSearchResults.value = []
        sectorSearchResults.value = []
      } else {
        ElMessage.info('该股票已经添加过了')
      }
    }

    const removeStock = (stockCode) => {
      const index = selectedStocks.value.findIndex(stock => stock.代码 === stockCode)
      if (index > -1) {
        const removed = selectedStocks.value.splice(index, 1)[0]
        ElMessage.info(`已移除 ${removed.名称}(${removed.代码})`)

        // 如果有对比图，清空它
        if (stockComparisonChart.value) {
          stockComparisonChart.value = ''
        }
      }
    }

    const clearAllStocks = () => {
      if (selectedStocks.value.length > 0) {
        selectedStocks.value = []
        stockComparisonChart.value = ''
        ElMessage.info('已清空所有选择的股票')
      }
    }

    const isSectorSelected = (name) => selectedSectors.value.includes(name)
    const addSector = (name) => {
      if (!isSectorSelected(name)) {
        const totalSelected = selectedStocks.value.length + selectedSectors.value.length
        if (totalSelected >= 30) {
          ElMessage.warning('最多只能选择30个对象进行对比')
          return
        }
        selectedSectors.value.push(name)
        ElMessage.success(`已添加板块 ${name}`)
        compareSearchQuery.value = ''
        sectorSearchResults.value = []
        stockSearchResults.value = []
      }
    }
    const removeSector = (name) => {
      const idx = selectedSectors.value.indexOf(name)
      if (idx >= 0) {
        selectedSectors.value.splice(idx, 1)
        ElMessage.info(`已移除板块 ${name}`)
        if (stockComparisonChart.value) stockComparisonChart.value = ''
      }
    }
    const clearAllSectors = () => {
      if (selectedSectors.value.length > 0) {
        selectedSectors.value = []
        stockComparisonChart.value = ''
        ElMessage.info('已清空所有选择的板块')
      }
    }

    const clearAllCompareItems = () => {
      const totalSelected = selectedStocks.value.length + selectedSectors.value.length
      if (totalSelected > 0) {
        selectedStocks.value = []
        selectedSectors.value = []
        stockComparisonChart.value = ''
        stockSearchResults.value = []
        sectorSearchResults.value = []
        compareSearchQuery.value = ''
        ElMessage.info('已清空所有已选对象')
      }
    }

    const generateStockComparison = async () => {
      if (selectedStocks.value.length === 0) {
        ElMessage.warning('请先添加股票')
        return
      }

      if (!isTimeRangeValid.value) {
        ElMessage.warning('请选择有效的时间范围')
        return
      }

      try {
        comparisonLoading.value = true

        const stockCodes = selectedStocks.value.map(stock => stock.代码)

        // 构建请求参数
        const params = {
          stock_codes: stockCodes,
          normalize: normalizeComparison.value
        }

        // 根据时间范围类型设置参数
        if (timeRangeType.value === 'preset') {
          params.days_back = comparisonDays.value
        } else {
          params.start_date = customStartDate.value
          params.end_date = customEndDate.value
        }

        const response = await ApiService.getStockComparison(params)

        if (response.success) {
          stockComparisonChart.value = response.data.chart_html
          const timeDesc = timeRangeType.value === 'preset'
            ? `最近${comparisonDays.value}天`
            : `${customStartDate.value} 至 ${customEndDate.value}`
          ElMessage.success(`已生成 ${response.data.stock_count} 只股票的对比图 (${timeDesc})`)
        } else {
          ElMessage.error('生成对比图失败: ' + response.error)
        }

      } catch (error) {
        console.error('生成股票对比图失败:', error)
        ElMessage.error('生成对比图失败，请稍后重试')
      } finally {
        comparisonLoading.value = false
      }
    }

    // 生成混合对比（股票+板块）
    const generateMixedComparison = async () => {
      if (selectedStocks.value.length === 0 && selectedSectors.value.length === 0) {
        ElMessage.warning('请先添加股票或板块')
        return
      }
      if (!isTimeRangeValid.value) {
        ElMessage.warning('请选择有效的时间范围')
        return
      }
      try {
        comparisonLoading.value = true
        const params = {
          normalize: normalizeComparison.value
        }
        if (selectedStocks.value.length > 0) {
          params.stock_codes = selectedStocks.value.map(stock => stock.代码)
        }
        if (selectedSectors.value.length > 0) {
          params.sector_names = selectedSectors.value
        }
        if (timeRangeType.value === 'preset') {
          params.days_back = comparisonDays.value
        } else {
          params.start_date = customStartDate.value
          params.end_date = customEndDate.value
        }
        const resp = await ApiService.getSectorComparison(params)
        if (resp.success) {
          stockComparisonChart.value = resp.data.chart_html
          const timeDesc = timeRangeType.value === 'preset'
            ? `最近${comparisonDays.value}天`
            : `${customStartDate.value} 至 ${customEndDate.value}`
          const objectCount = resp.data.object_count || (
            (resp.data.stock_count || 0) + (resp.data.sector_count || 0)
          )
          ElMessage.success(`已生成 ${objectCount} 个对象的对比图 (${timeDesc})`)
        } else {
          ElMessage.error('生成对比图失败: ' + (resp.error || '未知错误'))
        }
      } catch (e) {
        ElMessage.error('生成对比图失败: ' + e.message)
      } finally {
        comparisonLoading.value = false
      }
    }

    // 板块和个股选择方法
    const selectSector = async (sector) => {
      selectedSector.value = sector
      selectedStock.value = null
      stockKlineChart.value = ''
      stockKlineData.value = []

      console.log(`🔄 开始加载板块: ${sector.板块名称}`)
      const startTime = performance.now()

      try {
        // 并行加载板块成分股和K线图，提升加载速度
        await Promise.all([
          loadSectorStocks(),
          loadSectorKline()
        ])

        const endTime = performance.now()
        console.log(`✅ 板块 ${sector.板块名称} 加载完成，耗时: ${(endTime - startTime).toFixed(2)}ms`)
      } catch (error) {
        console.error(`❌ 板块 ${sector.板块名称} 加载失败:`, error)
        ElMessage.error('板块加载失败，请重试')
      }
    }

    const selectStock = async (stock) => {
      selectedStock.value = stock

      // 加载个股K线图
      await loadStockKline()
    }

    const loadSectorStocks = async () => {
      if (!selectedSector.value) return

      try {
        stocksLoading.value = true
        
        // 构建请求参数，传递全局日期与时间区间
        const params = {}
        if (globalDate.value) {
          params.date = globalDate.value
        }
        // 传递区间数组用于服务器端计算每只成分股的区间涨跌幅
        // 仅传递非固定列的时间区间，避免覆盖5/10日固定列
        const intervals = generateIntervals().filter(it => !['5日涨跌幅', '10日涨跌幅'].includes(it.key))
        if (intervals.length > 0) {
          try {
            params.intervals = JSON.stringify(intervals)
          } catch (e) {
            // 忽略序列化失败
          }
        }
        
        console.log('🔧 请求板块成分股数据:', selectedSector.value.板块名称, params)
        
        const response = await ApiService.getSectorStocks(selectedSector.value.板块名称, params)

        if (response.success) {
          sectorStocks.value = response.data
          console.log('✅ 板块成分股数据加载成功:', sectorStocks.value.length, '条记录')
        } else {
          ElMessage.error('加载成分股失败: ' + response.error)
        }
      } catch (error) {
        ElMessage.error('加载成分股失败: ' + error.message)
      } finally {
        stocksLoading.value = false
      }
    }

    const loadStockKline = async () => {
      if (!selectedStock.value) return

      stockKlineLoading.value = true
      try {
        console.log('🔄 加载个股K线数据:', selectedStock.value.名称, selectedStock.value.代码)

        // 确保使用股票代码而不是整个对象
        const stockCode = selectedStock.value.代码 || selectedStock.value.code || selectedStock.value
        console.log('📊 股票代码:', stockCode, '股票对象:', selectedStock.value)

        // 使用ApiService调用个股K线数据API，使用data格式，传递全局日期
        const response = await ApiService.getStockKline(stockCode, 30, globalDate.value, 'data')

        if (response.success && response.data.data && response.data.data.kline_data) {
          stockKlineData.value = response.data.data.kline_data
          console.log('✅ 个股K线数据加载成功:', stockKlineData.value.length, '条记录')
          // 清空旧的HTML格式
          stockKlineChart.value = ''
        } else {
          ElMessage.error('加载个股K线数据失败: ' + (response.error || response.message))
          stockKlineData.value = []
        }
      } catch (error) {
        ElMessage.error('加载个股K线数据失败: ' + error.message)
        stockKlineData.value = []
      } finally {
        stockKlineLoading.value = false
      }
    }

    const getSectorRowClass = ({ row }) => {
      return selectedSector.value && selectedSector.value.板块名称 === row.板块名称 ? 'selected-row' : ''
    }

    const getStockRowClass = ({ row }) => {
      return selectedStock.value && selectedStock.value.代码 === row.代码 ? 'selected-row' : ''
    }

    // 股票组合管理方法
    const parseResponse = async (response) => {
      const text = await response.text()
      let data = null
      try {
        data = JSON.parse(text)
      } catch (e) {
        // 非 JSON，例如代理错误页
      }
      if (!response.ok) {
        const msg = (data && (data.error || data.message)) || (text ? text.slice(0, 200) : `HTTP ${response.status}`)
        throw new Error(msg)
      }
      if (data) return data
      throw new Error('服务返回非JSON内容，可能是代理错误或后端未启动')
    }
    const loadStockGroups = async () => {
      try {
        loadingGroups.value = true
        const resp = await ApiService.getStockGroups()
        const data = resp

        if (data.success) {
          stockGroups.value = (data.data || []).map(group => {
            const stockCount = group.stock_count ?? (group.stock_codes || []).length
            const sectorCount = group.sector_count ?? (group.sector_names || []).length
            return {
              ...group,
              stock_codes: group.stock_codes || [],
              sector_names: group.sector_names || [],
              stock_count: stockCount,
              sector_count: sectorCount,
              object_count: group.object_count ?? (stockCount + sectorCount)
            }
          })
        } else {
          ElMessage.error('加载对象组合失败: ' + data.error)
        }
      } catch (error) {
        ElMessage.error('加载对象组合失败: ' + error.message)
      } finally {
        loadingGroups.value = false
      }
    }

    const saveStockGroup = async () => {
      if (!saveGroupForm.value.name.trim()) {
        ElMessage.warning('请输入组合名称')
        return
      }
      if (selectedStocks.value.length + selectedSectors.value.length === 0) {
        ElMessage.warning('请先添加股票或板块')
        return
      }

      try {
        savingGroup.value = true
        const stockCodes = selectedStocks.value.map(stock => stock.代码)
        const sectorNames = [...selectedSectors.value]

        const data = await ApiService.saveStockGroup({
          name: saveGroupForm.value.name.trim(),
          description: saveGroupForm.value.description.trim(),
          stock_codes: stockCodes,
          sector_names: sectorNames
        })

        if (data.success) {
          ElMessage.success(data.message)
          showSaveGroupDialog.value = false
          saveGroupForm.value = { name: '', description: '' }
          // 刷新组合列表
          await loadStockGroups()
        } else {
          ElMessage.error('保存失败: ' + data.error)
        }
      } catch (error) {
        ElMessage.error('保存失败: ' + error.message)
      } finally {
        savingGroup.value = false
      }
    }

    const loadStockGroup = async (group) => {
      try {
        // 清空当前选择
        selectedStocks.value = []
        selectedSectors.value = []
        stockComparisonChart.value = ''

        // 根据股票代码搜索并添加股票
        const stockCodes = group.stock_codes || []
        const sectorNames = group.sector_names || []

        for (const code of stockCodes) {
          try {
            const response = await ApiService.searchStocks(code)
            if (response.success && response.data.length > 0) {
              const stock = response.data.find(s => s.代码 === code)
              if (stock && !selectedStocks.value.some(s => s.代码 === stock.代码)) {
                selectedStocks.value.push(stock)
              }
            }
          } catch (error) {
            console.warn(`无法加载股票 ${code}:`, error)
          }
        }

        selectedSectors.value = Array.from(new Set(sectorNames))

        ElMessage.success(
          `已加载组合 "${group.name}"，成功添加 ${selectedStocks.value.length} 只股票、${selectedSectors.value.length} 个板块`
        )
        showLoadGroupDialog.value = false
      } catch (error) {
        ElMessage.error('加载组合失败: ' + error.message)
      }
    }

    const deleteStockGroup = async (group) => {
      try {
        await ElMessageBox.confirm(
          `确定要删除对象组合 "${group.name}" 吗？`,
          '确认删除',
          {
            confirmButtonText: '删除',
            cancelButtonText: '取消',
            type: 'warning'
          }
        )

        const data = await ApiService.deleteStockGroup(group.id)

        if (data.success) {
          ElMessage.success(data.message)
          await loadStockGroups()
        } else {
          ElMessage.error('删除失败: ' + data.error)
        }
      } catch (error) {
        if (error !== 'cancel') {
          ElMessage.error('删除失败: ' + error.message)
        }
      }
    }

    const formatDate = (dateString) => {
      return new Date(dateString).toLocaleDateString('zh-CN')
    }

    // 监听加载组合对话框打开
    watch(showLoadGroupDialog, (newVal) => {
      if (newVal) {
        loadStockGroups()
      }
    })

    onMounted(() => {
      loadData()
    })

    return {
      sectorData,
      charts,
      loading,
      globalDate,
      includeSectors,
      includeConcepts,
      // 排行榜与成分股搜索
      sectorSearchQuery,
      filteredTopSectors,
      sectorStocksSearchQuery,
      filteredSectorStocks,
      // 多时间区间相关
      selectedQuickIntervals,
      customInterval,
      customIntervals,
      canAddCustomInterval,
      allIntervals,
      addCustomInterval,
      removeCustomInterval,
      generateIntervals,
      // 旧的自定义区间（保留兼容性）
      showCustomPeriod,
      summary,
      topSectors,
      selectedIndex,
      sectorKlineChart,
      sectorKlineData,
      sectorKlineOption,
      klineLoading,
      // 板块和个股选择相关
      selectedSector,
      selectedStock,
      sectorStocks,
      stocksLoading,
      stockKlineChart,
      stockKlineData,
      stockKlineOption,
      stockKlineLoading,
      chartTitle,
      selectSector,
      selectStock,
      loadSectorStocks,
      loadStockKline,
      getSectorRowClass,
      getStockRowClass,
      // 个股对比相关
      stockSearchQuery,
      stockSearchResults,
      selectedStocks,
      stockComparisonChart,
      comparisonLoading,
      normalizeComparison,
      comparisonDays,
      timeRangeType,
      customStartDate,
      customEndDate,
      isTimeRangeValid,
      compareSearchQuery,
      sectorSearchResults,
      selectedSectors,
      onCompareSearch,
      onStockSearch,
      onSectorSearch,
      searchStocks,
      isStockSelected,
      isSectorSelected,
      addStock,
      addSector,
      removeStock,
      removeSector,
      clearAllStocks,
      clearAllSectors,
      clearAllCompareItems,
      generateStockComparison,
      generateMixedComparison,
      // 股票组合管理相关
      showSaveGroupDialog,
      showLoadGroupDialog,
      savingGroup,
      loadingGroups,
      stockGroups,
      saveGroupForm,
      saveStockGroup,
      loadStockGroup,
      deleteStockGroup,
      // 区间组合
      showSaveIntervalGroup,
      showLoadIntervalGroup,
      saveIntervalGroupForm,
      intervalGroups,
      saveIntervalGroup,
      openLoadIntervalGroup,
      applyIntervalGroup,
      deleteIntervalGroup,
      formatDate,
      loadData,
      loadAllData,
      onGlobalDateChange,
      onFilterChange,
      onCustomPeriodChange,
      calculateCustomPeriod,
      loadSectorKline,
      formatNumber,
      formatPercent,
      formatMarketCap,
      formatAmount,
      formatVolume,
      formatVolumeRatio,
      sortByNumber,
      sortByTrend
    }
  }
}
</script>

<style scoped>
.sector-analysis {
  padding: 20px;
}

/* 页面标题样式 */
.page-header {
  margin-bottom: 20px;
  text-align: center;
}

.page-header h1 {
  margin: 0 0 10px 0;
  color: #303133;
  font-size: 28px;
  font-weight: 600;
}

.page-header p {
  margin: 0;
  color: #909399;
  font-size: 14px;
}

/* 顶部控制面板样式 */
.control-panel-top {
  margin-bottom: 20px;
}

.control-panel-top .panel-header {
  font-size: 16px;
  font-weight: 600;
  color: #303133;
}

.control-section {
  margin-bottom: 0;
}

.control-section h4 {
  margin: 0 0 10px 0;
  color: #606266;
  font-size: 14px;
  font-weight: 600;
}

/* 垂直排列的复选框控件 - 板块类型选择 */
.filter-controls-vertical {
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding-top: 0;
}

/* 水平排列的复选框控件（保留兼容性） */
.filter-controls-horizontal {
  display: flex;
  align-items: center;
  gap: 15px;
  padding-top: 25px; /* 与其他列的标题高度对齐 */
}

.filter-controls {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding-top: 25px; /* 与其他列的标题高度对齐 */
}

/* 蓝色复选框样式 */
.blue-checkbox {
  color: #409eff !important;
}

:deep(.blue-checkbox .el-checkbox__label) {
  color: #409eff !important;
  font-weight: 500;
}

:deep(.blue-checkbox .el-checkbox__input.is-checked .el-checkbox__inner) {
  background-color: #409eff !important;
  border-color: #409eff !important;
}

:deep(.blue-checkbox .el-checkbox__input .el-checkbox__inner) {
  border-color: #409eff !important;
}

:deep(.blue-checkbox .el-checkbox__input.is-checked + .el-checkbox__label) {
  color: #409eff !important;
}

.custom-period-horizontal {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 5px;
}

/* 多时间区间控件样式 */
.multi-period-controls {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

/* 水平布局的多时间区间控件 */
.multi-period-controls-horizontal {
  display: flex;
  align-items: flex-start;
  gap: 20px;
  flex-wrap: wrap;
}

.quick-intervals {
  flex-shrink: 0;
}

.custom-interval-input-expanded {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  flex: 1;
  min-width: 500px;
}

.custom-interval-input {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 5px;
}

/* 蓝色标签样式 */
.blue-tag {
  background-color: #409eff !important;
  border-color: #409eff !important;
  color: white !important;
}

.main-content {
  padding: 0;
}

.page-header {
  margin-bottom: 20px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 20px;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  border-radius: 8px;
  color: white;
}

.header-left h1 {
  color: white;
  margin-bottom: 5px;
  font-size: 24px;
}

.header-left p {
  color: rgba(255, 255, 255, 0.8);
  font-size: 14px;
  margin: 0;
}

.header-controls {
  display: flex;
  align-items: center;
}

.filter-controls {
  display: flex;
  align-items: center;
  margin-right: 15px;
  padding: 8px 12px;
  background: rgba(255, 255, 255, 0.1);
  border-radius: 6px;
  border: 1px solid rgba(255, 255, 255, 0.2);
}

.filter-controls .el-checkbox {
  margin-right: 15px;
  color: white !important;
}

.filter-controls .el-checkbox:last-child {
  margin-right: 0;
}

/* 更强的白色字体样式覆盖 */
:deep(.filter-controls .el-checkbox__label) {
  color: white !important;
}

:deep(.filter-controls .el-checkbox .el-checkbox__label) {
  color: white !important;
}

:deep(.el-checkbox__label) {
  color: white !important;
}

.filter-controls :deep(.el-checkbox__label) {
  color: white !important;
}

:deep(.filter-controls .el-checkbox__input.is-checked .el-checkbox__inner) {
  background-color: #409eff;
  border-color: #409eff;
}

:deep(.filter-controls .el-checkbox__input .el-checkbox__inner) {
  border-color: rgba(255, 255, 255, 0.6);
}

.stats-row {
  margin-bottom: 20px;
}

.stat-card {
  text-align: center;
  border: none;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
}

.stat-content {
  padding: 10px 0;
}

.stat-value {
  font-size: 28px;
  font-weight: bold;
  margin-bottom: 8px;
  color: #303133;
}

.stat-label {
  font-size: 14px;
  color: #909399;
}

.chart-row {
  margin-top: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.chart-container {
  min-height: 400px;
  width: 100%;
}

.no-chart {
  height: 300px;
  display: flex;
  align-items: center;
  justify-content: center;
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

/* 卡片样式优化 */
:deep(.el-card) {
  border-radius: 8px;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
}

:deep(.el-card__header) {
  background-color: #fafafa;
  border-bottom: 1px solid #ebeef5;
  font-weight: 600;
}

/* 个股对比分析样式 */
.stock-selection-area {
  margin-bottom: 20px;
  padding: 20px;
  background-color: #fafafa;
  border-radius: 8px;
}

.search-section {
  margin-bottom: 20px;
}

.search-section h4 {
  margin: 0 0 10px 0;
  color: #303133;
  font-size: 14px;
  font-weight: 600;
}

.search-controls {
  display: flex;
  align-items: center;
  margin-bottom: 15px;
}

.search-results {
  margin-top: 10px;
}

.search-results-header {
  font-size: 12px;
  color: #909399;
  margin-bottom: 8px;
}

.search-results-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.search-result-tag {
  cursor: pointer;
  transition: all 0.3s;
  display: flex;
  align-items: center;
  gap: 5px;
}

.search-result-tag:hover {
  transform: translateY(-1px);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

.stock-price {
  font-size: 11px;
  font-weight: bold;
}

.selected-section h4 {
  margin: 0 0 10px 0;
  color: #303133;
  font-size: 14px;
  font-weight: 600;
}

.selected-stocks {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.selected-stock-tag {
  font-size: 13px;
  padding: 8px 12px;
}

.no-stocks-selected {
  text-align: center;
  padding: 20px;
}

.comparison-chart-area {
  margin-top: 20px;
  border-top: 1px solid #ebeef5;
  padding-top: 20px;
}

.stock-comparison-controls {
  display: flex;
  align-items: flex-start;
  flex-wrap: wrap;
  gap: 15px;
}

.time-range-controls {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.preset-controls,
.custom-controls {
  display: flex;
  align-items: center;
  gap: 8px;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .sector-analysis {
    padding: 10px;
  }

  .stats-row .el-col {
    margin-bottom: 10px;
  }

  .stat-value {
    font-size: 24px;
  }

  .chart-container {
    min-height: 300px;
  }

  .search-controls {
    flex-direction: column;
    align-items: stretch;
    gap: 10px;
  }

  .stock-comparison-controls {
    flex-direction: column;
    align-items: stretch;
    gap: 10px;
  }

  .stock-selection-area {
    padding: 15px;
  }
}

/* 板块详情样式 */
.sector-info {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 15px;
  padding: 10px;
  background: #f8f9fa;
  border-radius: 4px;
}

.sector-change {
  font-weight: bold;
  font-size: 16px;
}

/* 图表容器样式 */
.chart-container {
  margin-bottom: 20px;
}

.chart-container h4 {
  margin: 0 0 10px 0;
  color: #303133;
  font-size: 14px;
  font-weight: 600;
  padding: 8px 12px;
  background: #f0f9ff;
  border-left: 4px solid #409eff;
  border-radius: 4px;
}

/* 选中行样式 */
.selected-row {
  background-color: #e6f7ff !important;
}

.selected-row:hover {
  background-color: #bae7ff !important;
}

/* 无数据样式 */
.no-data {
  padding: 40px;
  text-align: center;
}

.no-chart {
  padding: 40px;
  text-align: center;
}

/* 连阳天数样式 */
.consecutive-days-badge {
  display: inline-block;
  background-color: #f56c6c;
  color: white;
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: bold;
  min-width: 40px;
  text-align: center;
}

.no-consecutive-days {
  color: #909399;
  font-size: 12px;
}
</style>
