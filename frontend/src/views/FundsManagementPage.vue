<template>
  <div class="funds-management-page">
    <div class="page-header">
      <h1>💼 资金管理</h1>
      <p>选择策略，输入本金、价格与止损，计算建议仓位</p>
    </div>

    <funds-management />
  </div>
</template>

<script>
import FundsManagement from '@/components/FundsManagement.vue'

export default {
  name: 'FundsManagementPage',
  components: { FundsManagement }
}
</script>

<style scoped>
.funds-management-page {
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
</style>
