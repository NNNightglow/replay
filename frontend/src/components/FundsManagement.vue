<template>
  <div class="funds-management">
    <el-card class="fm-card">
      <template #header>
        <div class="fm-header">
          <span>💼 资金管理计算器</span>
          <el-tag size="small" type="info">Beta</el-tag>
        </div>
      </template>

      <el-form :model="form" label-width="110px" :inline="false" class="fm-form">
        <el-row :gutter="16">
          <el-col :xs="24" :md="12">
            <el-form-item label="资金管理方式">
              <el-select v-model="form.method" placeholder="选择策略">
                <el-option label="固定百分比 (Fixed Fractional)" value="fixed_fractional" />
                <el-option label="固定比例 (Fixed Ratio)" value="fixed_ratio" />
                <el-option label="固定波幅 (ATR/波动率)" value="fixed_volatility" />
                <el-option label="固定资金 (固定股数)" value="fixed_amount" />
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="12">
            <el-form-item label="账户本金">
              <el-input v-model.number="form.accountCapital" type="number" min="0" placeholder="单位：元" />
            </el-form-item>
          </el-col>
        </el-row>

        <el-row :gutter="16">
          <el-col :xs="24" :md="12">
            <el-form-item label="单笔风险(%)" v-if="form.method !== 'fixed_amount'">
              <el-input v-model.number="form.riskPercent" type="number" min="0" max="100" placeholder="如 1 表示1%" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="12" v-if="form.method === 'fixed_ratio'">
            <el-form-item label="利润增量Δ (元)">
              <el-input v-model.number="form.delta" type="number" min="0" placeholder="每盈利Δ元，增加1单位" />
            </el-form-item>
          </el-col>
        </el-row>

        <el-row :gutter="16">
          <el-col :xs="24" :md="12">
            <el-form-item label="标的现价">
              <el-input v-model.number="form.entryPrice" type="number" min="0" placeholder="当前或计划买入价" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="12">
            <el-form-item label="止损价格">
              <el-input v-model.number="form.stopPrice" type="number" min="0" placeholder="触发止损价" />
            </el-form-item>
          </el-col>
        </el-row>

        <el-row :gutter="16">
          <el-col :xs="24" :md="12" v-if="form.method === 'fixed_volatility'">
            <el-form-item label="ATR(或波动)值">
              <el-input v-model.number="form.atr" type="number" min="0" placeholder="如 0.50 元" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="12" v-if="form.method === 'fixed_amount'">
            <el-form-item label="固定股数">
              <el-input v-model.number="form.fixedShares" type="number" min="0" placeholder="每次固定买入股数" />
            </el-form-item>
          </el-col>
        </el-row>

        <el-row :gutter="16">
          <el-col :xs="24" :md="12">
            <el-form-item label="滑点+手续费(%)">
              <el-input v-model.number="form.costPercent" type="number" min="0" max="100" placeholder="可选，默认0" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="12">
            <el-form-item label="最小交易单位">
              <el-input v-model.number="form.lotSize" type="number" min="1" placeholder="A股通常为100股" />
            </el-form-item>
          </el-col>
        </el-row>

        <div class="fm-actions">
          <el-button type="primary" @click="recalculate">计算</el-button>
          <el-button @click="resetForm">重置</el-button>
          <div v-if="stockInfo" class="stock-info">当前票：{{ stockInfo.name }} ({{ stockInfo.code }})</div>
        </div>
      </el-form>

      <el-divider />

      <div class="fm-result" v-if="result">
        <el-row :gutter="16">
          <el-col :xs="24" :md="6">
            <el-statistic title="建议建仓股数" :value="result.shares" />
            <div class="helper">向下取整至手数：{{ lotAdjustedShares }}</div>
          </el-col>
          <el-col :xs="24" :md="6">
            <el-statistic title="建仓金额(元)" :value="formatNumber(result.positionAmount)" />
          </el-col>
          <el-col :xs="24" :md="6">
            <el-statistic title="单笔最大亏损(元)" :value="formatNumber(result.maxLoss)" />
            <div class="helper">占比：{{ formatPercent(effectiveRiskPct) }}</div>
          </el-col>
          <el-col :xs="24" :md="6">
            <el-statistic title="止损距离(元)" :value="formatNumber(result.stopDistance)" />
            <div class="helper">止损价：{{ formatNumber(form.stopPrice) }}</div>
          </el-col>
        </el-row>

        <el-alert
          v-if="warning"
          :title="warning"
          type="warning"
          show-icon
          class="mt-12"
        />
      </div>

      <div v-else class="fm-empty">
        <el-empty description="填写参数后点击计算，显示建议仓位与止损" />
      </div>

      <el-divider />

      <div class="fm-notes">
        <p><strong>说明：</strong></p>
        <ul>
          <li>固定百分比与固定风险同义：单笔风险 = 本金 × 风险%。</li>
          <li>固定波幅：股数 = (本金 × 风险%) / (ATR × 每点价值)。此处每点价值默认1元/股。</li>
          <li>固定比例：根据利润阶梯估算单位数，示例实现为简化版。</li>
          <li>结果包含滑点与手续费的影响：有效风险 = 风险% - 费用%。</li>
        </ul>
      </div>
    </el-card>
  </div>
</template>

<script>
import { utils } from '@/services/api'

export default {
  name: 'FundsManagement',
  props: {
    stockInfo: {
      type: Object,
      default: null // { name, code }
    },
    prefillPrice: {
      type: Number,
      default: null
    }
  },
  data() {
    return {
      form: {
        method: 'fixed_fractional',
        accountCapital: 100000,
        riskPercent: 1,
        entryPrice: this.prefillPrice || null,
        stopPrice: null,
        atr: null,
        fixedShares: null,
        delta: 1000,
        costPercent: 0,
        lotSize: 100
      },
      result: null,
      warning: ''
    }
  },
  watch: {
    prefillPrice: {
      immediate: true,
      handler(v) {
        if (v && !this.form.entryPrice) this.form.entryPrice = v
      }
    }
  },
  computed: {
    lotAdjustedShares() {
      if (!this.result) return 0
      const size = Math.max(1, Math.floor(this.form.lotSize || 100))
      return Math.floor(this.result.shares / size) * size
    },
    effectiveRiskPct() {
      const risk = Number(this.form.riskPercent || 0)
      const cost = Number(this.form.costPercent || 0)
      return Math.max(0, risk - cost)
    },
    formatNumber() {
      return utils.formatNumber
    },
    formatPercent() {
      return utils.formatPercent
    }
  },
  methods: {
    resetForm() {
      this.form = {
        method: 'fixed_fractional',
        accountCapital: 100000,
        riskPercent: 1,
        entryPrice: this.prefillPrice || null,
        stopPrice: null,
        atr: null,
        fixedShares: null,
        delta: 1000,
        costPercent: 0,
        lotSize: 100
      }
      this.result = null
      this.warning = ''
    },
    recalculate() {
      this.warning = ''
      const { method } = this.form
      if (method === 'fixed_amount') {
        this.calculateFixedAmount()
      } else if (method === 'fixed_ratio') {
        this.calculateFixedRatio()
      } else if (method === 'fixed_volatility') {
        this.calculateFixedVolatility()
      } else {
        this.calculateFixedFractional()
      }
    },
    getRiskMoney() {
      const capital = Number(this.form.accountCapital || 0)
      const riskPct = this.effectiveRiskPct / 100
      return Math.max(0, capital * riskPct)
    },
    getStopDistance() {
      const entry = Number(this.form.entryPrice || 0)
      const stop = Number(this.form.stopPrice || 0)
      if (entry <= 0 || stop <= 0) return 0
      return Math.max(0, entry - stop)
    },
    calculateFixedFractional() {
      const riskMoney = this.getRiskMoney()
      const stopDistance = this.getStopDistance()
      if (riskMoney <= 0 || stopDistance <= 0) {
        this.warning = '请填写有效的本金、风险%、买入价与止损价'
        this.result = null
        return
      }
      let shares = Math.floor(riskMoney / stopDistance)
      if (!isFinite(shares) || shares <= 0) {
        this.warning = '计算结果无效，请检查输入'
        this.result = null
        return
      }
      const positionAmount = shares * Number(this.form.entryPrice)
      this.result = {
        shares,
        positionAmount,
        maxLoss: shares * stopDistance,
        stopDistance
      }
    },
    calculateFixedVolatility() {
      const riskMoney = this.getRiskMoney()
      const atr = Number(this.form.atr || 0)
      if (riskMoney <= 0 || atr <= 0) {
        this.warning = '请填写有效的本金、风险%与ATR值'
        this.result = null
        return
      }
      // 每点价值默认1元/股
      let shares = Math.floor(riskMoney / atr)
      if (!isFinite(shares) || shares <= 0) {
        this.warning = '计算结果无效，请检查输入'
        this.result = null
        return
      }
      const positionAmount = shares * Number(this.form.entryPrice || 0)
      const stopDistance = this.getStopDistance() || atr
      this.result = {
        shares,
        positionAmount,
        maxLoss: shares * atr,
        stopDistance
      }
    },
    calculateFixedRatio() {
      // 简化实现：根据累计利润估计单位数
      // 单位数 = floor(累计利润 / Δ) + 1，建议股数 = 单位数 × 基础单位
      const delta = Number(this.form.delta || 0)
      if (delta <= 0) {
        this.warning = '请填写有效的Δ(利润增量)'
        this.result = null
        return
      }
      // 这里没有账户历史利润数据，使用账户本金 × 风险% 作为单位规模的参考
      const unitMoney = this.getRiskMoney()
      const unitShares = this.form.entryPrice > 0 ? Math.floor(unitMoney / this.form.entryPrice) : 0
      const assumedProfit = unitMoney // 可替换为外部传入的累计利润
      const units = Math.max(1, Math.floor(assumedProfit / delta) + 1)
      const shares = units * Math.max(1, unitShares)
      if (!isFinite(shares) || shares <= 0) {
        this.warning = '计算结果无效，请检查输入'
        this.result = null
        return
      }
      const stopDistance = this.getStopDistance()
      const positionAmount = shares * Number(this.form.entryPrice || 0)
      this.result = {
        shares,
        positionAmount,
        maxLoss: stopDistance > 0 ? shares * stopDistance : 0,
        stopDistance
      }
    },
    calculateFixedAmount() {
      const shares = Math.floor(Number(this.form.fixedShares || 0))
      if (!isFinite(shares) || shares <= 0) {
        this.warning = '请填写有效的固定股数'
        this.result = null
        return
      }
      const entry = Number(this.form.entryPrice || 0)
      if (entry <= 0) {
        this.warning = '请填写有效的买入价'
        this.result = null
        return
      }
      const stopDistance = this.getStopDistance()
      const positionAmount = shares * entry
      this.result = {
        shares,
        positionAmount,
        maxLoss: stopDistance > 0 ? shares * stopDistance : 0,
        stopDistance
      }
    }
  }
}
</script>

<style scoped>
.funds-management {
  margin-top: 20px;
}

.fm-card {
  margin-bottom: 20px;
}

.fm-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-weight: 600;
}

.fm-form {
  margin-bottom: 8px;
}

.fm-actions {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-top: 4px;
}

.stock-info {
  color: #909399;
  font-size: 13px;
}

.fm-result {
  margin-top: 8px;
}

.helper {
  color: #909399;
  font-size: 12px;
  margin-top: 4px;
}

.fm-empty {
  padding: 8px 0;
}

.fm-notes {
  color: #606266;
  font-size: 13px;
}

.mt-12 {
  margin-top: 12px;
}
</style>
