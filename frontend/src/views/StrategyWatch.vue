<template>
  <div class="strategy-watch-page">
    <div class="page-header">
      <div>
        <h2>策略看盘</h2>
        <p>上传资料并构建知识库，用策略对话快速分析与复盘。</p>
      </div>
      <div class="header-actions">
        <el-radio-group v-model="topMode" size="large" class="mode-switch">
          <el-radio-button label="dialog">对话</el-radio-button>
          <el-radio-button label="strategy_analysis">策略</el-radio-button>
        </el-radio-group>
        <el-tag class="runtime-tag" :type="runtime.api_key_configured ? 'success' : 'warning'" size="large">
          {{ runtime.api_key_configured ? `模型已连接：${runtime.model}` : '未配置模型 API Key，请先配置 .env' }}
        </el-tag>
      </div>
    </div>

    <div v-if="isReplayChatMode" class="page-body" :class="{ 'chat-sidebar-collapsed': chatSidebarCollapsed }">
      <aside class="left-panel" :class="{ 'chat-sidebar-collapsed': chatSidebarCollapsed }">
        <el-card class="panel-card conversation-card" :class="{ collapsed: chatSidebarCollapsed }" shadow="never">
          <template #header>
            <div class="card-header">
              <span>{{ chatSidebarCollapsed ? '侧栏' : '对话列表' }}</span>
              <div class="card-actions">
                <el-button v-if="!chatSidebarCollapsed" type="primary" link @click="createConversation">新建对话</el-button>
                <el-button link @click="toggleChatSidebarCollapse">
                  {{ chatSidebarCollapsed ? '▶' : '◀' }}
                </el-button>
              </div>
            </div>
          </template>

          <div v-if="!chatSidebarCollapsed" class="conversation-list">
            <div
              v-for="item in conversations"
              :key="item.id"
              class="conversation-item"
              :class="{ active: item.id === activeConversationId }"
              @click="switchConversation(item.id)"
            >
              <div class="conversation-main">
                <div class="title">{{ item.title }}</div>
                <div class="meta">{{ formatDateTime(item.updated_at) }} · {{ item.message_count }} 条消息</div>
              </div>
              <el-button type="danger" link @click.stop="deleteConversation(item.id)">删除</el-button>
            </div>
            <el-empty v-if="!conversations.length" description="暂无对话，点击“新建对话”开始" :image-size="64" />
          </div>
        </el-card>

        <el-card v-if="!chatSidebarCollapsed" class="panel-card" shadow="never">
          <template #header>
            <div class="card-header">
              <span>资料库</span>
              <div class="card-actions">
                <el-button link @click="openResourceManageDialog">管理</el-button>
                <el-button link @click="openJobsDialog">任务列表</el-button>
                <el-button link @click="loadResources">刷新</el-button>
              </div>
            </div>
          </template>

          <el-upload
            ref="uploadRef"
            class="upload-box"
            drag
            multiple
            :auto-upload="false"
            :on-change="handleFileChange"
            :show-file-list="true"
          >
            <el-icon class="el-icon--upload"><UploadFilled /></el-icon>
            <div class="upload-desc">拖拽文件到此处或点击上传，支持批量上传并自动转为 Markdown。</div>
            <template #tip>
              <div class="upload-tip">支持 pdf/doc/docx/png/jpg/mp3/mp4</div>
            </template>
          </el-upload>

          <div class="resource-actions">
            <el-input
              v-model="uploadGroupName"
              size="small"
              placeholder="分组名称（可选）"
              style="width: 140px"
            />
            <el-button type="primary" size="small" :loading="uploading" @click="uploadPendingFiles">
              上传并转 Markdown
            </el-button>
          </div>

          <div class="resource-actions">
            <el-select v-model="selectedGroupId" size="small" style="width: 180px" placeholder="所有分组">
              <el-option label="所有分组" value="" />
              <el-option
                v-for="grp in resourceGroups"
                :key="grp.group_id"
                :label="`${grp.group_name} (${grp.count})`"
                :value="grp.group_id"
              />
            </el-select>
          </div>

          <div class="resource-list">
            <div v-for="res in filteredResources" :key="res.id" class="resource-item">
              <div class="resource-main">
                <div class="name">{{ res.original_name }}</div>
                <div class="meta">
                  <el-tag
                    size="small"
                    :type="res.status === 'ok' ? 'success' : res.status === 'processing' ? 'warning' : 'danger'"
                  >
                    {{ res.status }}
                  </el-tag>
                  <el-tag size="small" type="info">{{ formatSourceType(res.source_type) }}</el-tag>
                  <el-tag size="small" type="info">{{ res.group_name || '未分组' }}</el-tag>
                  <el-tag v-if="res.id === activeResourceId" size="small" type="warning">当前</el-tag>
                  <span>{{ formatSize(res.size_bytes) }}</span>
                </div>
                <div v-if="res.status === 'processing'" class="resource-progress">
                  <el-progress :percentage="Number(res.progress || 0)" :stroke-width="10" />
                  <div class="progress-text">{{ res.progress_message || '处理中' }}</div>
                </div>
              </div>
              <div class="resource-buttons">
                <el-button link @click="downloadResourceMarkdown(res)">下载原MD</el-button>
                <el-button link @click="downloadResourceAiSummary(res)">下载AI总结</el-button>
                <el-button link @click="renameResource(res)">重命名</el-button>
                <el-button
                  link
                  :type="res.id === activeResourceId ? 'warning' : 'primary'"
                  @click="switchActiveResource(res.id)"
                >
                  {{ res.id === activeResourceId ? '已激活' : '设为当前' }}
                </el-button>
                <el-button type="danger" link @click="deleteResource(res.id)">删除</el-button>
              </div>
            </div>
            <el-empty v-if="!filteredResources.length" description="暂无资源" :image-size="56" />
          </div>
        </el-card>
      </aside>

      <section class="chat-panel">
        <el-card class="chat-card" shadow="never">
          <template #header>
            <div class="card-header">
              <span>{{ activeConversationTitle || '未选择对话' }}</span>
              <div class="card-actions">
                <el-button
                  v-if="activeConversationId"
                  link
                  :disabled="!messages.length"
                  @click="downloadConversation"
                >
                  下载对话
                </el-button>
                <el-button v-if="activeConversationId" link @click="renameConversation">重命名</el-button>
              </div>
            </div>
          </template>

          <div ref="messagesRef" class="messages-wrap">
            <div
              v-for="msg in messages"
              v-if="!shouldHideAssistantMessage(msg)"
              :key="msg.id"
              class="message-row"
              :class="msg.role"
            >
              <div class="message-stack">
                <div class="bubble">
                  <div class="role">
                    {{ msg.role === 'user' ? '我' : '助手' }}
                    <span v-if="msg.agent_name" class="agent-chip">{{ msg.agent_name }}</span>
                  </div>
                  <template v-if="msg.role === 'assistant'">
                    <div
                      class="content md-content"
                      v-html="renderMarkdown(getDisplayedAssistantContent(msg))"
                    ></div>
                    <div v-if="getCrawlResults(msg).length" class="crawl-result-list">
                      <div
                        v-for="(item, idx) in getCrawlResults(msg)"
                        :key="`${msg.id}-crawl-${idx}`"
                        class="crawl-result-card"
                      >
                        <div class="crawl-result-head">
                          <span class="crawl-result-title">{{ item.title || item.url || `抓取文章${idx + 1}` }}</span>
                          <el-tag size="small" :type="getCrawlStatusTag(item)">{{ getCrawlStatusText(item) }}</el-tag>
                        </div>
                        <div class="crawl-result-url">{{ item.url || '--' }}</div>
                        <div class="crawl-result-files">
                          <span class="crawl-result-file" :class="{ ok: !!item.markdown_relpath }">MD</span>
                          <span class="crawl-result-file" :class="{ ok: !!item.pdf_relpath }">PDF</span>
                          <span class="crawl-result-file" :class="{ ok: !!item.docx_relpath }">Word</span>
                        </div>
                        <div v-if="summarizeCrawlError(item)" class="crawl-result-error">{{ summarizeCrawlError(item) }}</div>
                      </div>
                    </div>
                  </template>
                  <div v-else class="content">{{ msg.content }}</div>
                  <div class="time">{{ formatDateTime(msg.created_at) }}</div>
                </div>
                <div class="message-actions">
                  <button class="copy-tag" type="button" @click.stop="copyMessage(msg)">复制</button>
                  <el-dropdown
                    v-if="msg.crawl_results?.length"
                    trigger="click"
                    @command="handleDownloadCommand"
                  >
                    <button class="copy-tag" type="button">下载</button>
                    <template #dropdown>
                      <el-dropdown-menu>
                        <el-dropdown-item
                          v-for="option in buildDownloadOptions(msg)"
                          :key="option.key"
                          :command="option"
                        >
                          {{ option.label }}
                        </el-dropdown-item>
                        <el-dropdown-item v-if="!buildDownloadOptions(msg).length" disabled>
                          暂无可下载格式
                        </el-dropdown-item>
                      </el-dropdown-menu>
                    </template>
                  </el-dropdown>
                </div>
              </div>
            </div>
            <div v-if="strategyEditRun.active" class="message-row assistant">
              <div class="message-stack">
                <div class="bubble strategy-edit-run-bubble">
                  <div class="role">
                    策略编辑师
                    <span v-if="selectedAgentName" class="agent-chip">{{ selectedAgentName }}</span>
                  </div>
                  <div class="strategy-edit-run-question">问题：{{ strategyEditRun.question }}</div>
                  <div class="strategy-edit-run-status">
                    <span class="strategy-edit-run-spinner"></span>
                    <span class="strategy-edit-run-title">{{ strategyEditRunCurrentTitle }}</span>
                  </div>
                  <div class="strategy-edit-run-steps">
                    <div v-for="(step, idx) in strategyEditRunRecentSteps" :key="`run-step-${idx}`">
                      {{ step }}
                    </div>
                  </div>
                </div>
              </div>
            </div>
            <el-empty v-if="!messages.length" description="暂无消息，开始对话吧" :image-size="80" />
          </div>

          <div class="composer">
            <div class="composer-tools">
              <el-select v-model="selectedModel" placeholder="选择模型" style="width: 180px">
                <el-option
                  v-for="model in modelOptions"
                  :key="model.value"
                  :label="model.label"
                  :value="model.value"
                />
              </el-select>
              <el-select v-model="activeMode" placeholder="模式" style="width: 140px">
                <el-option label="对话" value="dialog" />
                <el-option label="爬虫" value="crawler" />
                <el-option label="策略编辑" value="strategy_edit" />
              </el-select>
              <el-select v-model="activeMemoryProfileId" placeholder="长期记忆（人格）" style="width: 220px">
                <el-option label="不使用长期记忆" value="" />
                <el-option
                  v-for="profile in memoryProfiles"
                  :key="profile.id"
                  :label="`${profile.name} (${profile.linked_resource_count || 0})`"
                  :value="profile.id"
                />
              </el-select>
              <el-button size="small" @click="openMemoryManageDialog">记忆编辑</el-button>
              <el-popover
                placement="bottom-start"
                trigger="click"
                :width="700"
                v-model:visible="promptTemplatePopoverVisible"
                :teleported="false"
              >
                <template #reference>
                  <el-button size="small" class="prompt-edit-btn">提示词编辑</el-button>
                </template>
                <div class="prompt-template-panel">
                  <div class="prompt-template-list">
                    <div
                      v-for="tpl in promptTemplates"
                      :key="tpl.id"
                      class="prompt-template-item"
                      :class="{ active: tpl.id === selectedPromptTemplateId }"
                      @click="selectedPromptTemplateId = tpl.id"
                    >
                      {{ tpl.name }}
                    </div>
                  </div>
                  <div class="prompt-template-editor">
                    <div class="prompt-template-row">
                      <span class="prompt-template-label">提示词</span>
                      <el-input
                        v-model="promptTemplateNameInput"
                        size="small"
                        :disabled="selectedPromptTemplateId === 'none'"
                        placeholder="模板名称"
                        class="prompt-template-name-input"
                      />
                      <el-button size="small" @click="createPromptTemplate">新增模板</el-button>
                      <el-button
                        size="small"
                        type="danger"
                        plain
                        :disabled="selectedPromptTemplateId === 'none'"
                        @click="deletePromptTemplate"
                      >
                        删除模板
                      </el-button>
                    </div>
                    <el-input
                      v-model="promptTemplateContentInput"
                      type="textarea"
                      :rows="12"
                      :disabled="selectedPromptTemplateId === 'none'"
                      placeholder="编辑提示词模板内容；发送时会作为前置指令。"
                    />
                  </div>
                </div>
              </el-popover>
              <el-popover
                placement="bottom-start"
                trigger="click"
                :width="520"
                v-model:visible="groupPopoverVisible"
                :teleported="false"
              >
                <template #reference>
                  <el-button size="small" style="flex: 1">选择参考资料（按分组）</el-button>
                </template>
                <div class="group-picker">
                  <div class="group-list">
                    <div
                      class="group-item"
                      :class="{ active: hoverGroupId === '' }"
                      @mouseenter="hoverGroupId = ''"
                    >
                      全部
                    </div>
                    <div
                      v-for="grp in resourceGroups"
                      :key="`group_${grp.group_id}`"
                      class="group-item"
                      :class="{ active: hoverGroupId === `group:${grp.group_id}` }"
                      @mouseenter="hoverGroupId = `group:${grp.group_id}`"
                    >
                      {{ grp.group_name }} ({{ grp.count }})
                    </div>
                    <div
                      class="group-item"
                      :class="{ active: hoverGroupId === 'strategy:__all__' }"
                      @mouseenter="hoverGroupId = 'strategy:__all__'"
                    >
                      策略 ({{ strategyTaggedCount }})
                    </div>
                  </div>
                  <div class="group-resources">
                    <div
                      v-for="item in hoverReferenceItems"
                      :key="item.id"
                      class="group-resource-item"
                      @click="toggleResourceSelection(item.id)"
                    >
                      <el-checkbox :model-value="selectedResourceIds.includes(item.id)">
                        <div class="group-resource-title">{{ item.title }}</div>
                      </el-checkbox>
                    </div>
                    <el-empty v-if="!hoverReferenceItems.length" description="该分组暂无内容" :image-size="48" />
                  </div>
                </div>
              </el-popover>
            </div>

            <div class="composer-input">
              <el-input
                v-model="inputText"
                type="textarea"
                :rows="4"
                placeholder="输入问题或指令，Ctrl+Enter 发送。可在顶部切换对话/爬虫/策略分析模式。"
                @keydown.ctrl.enter.prevent="sendMessage"
              />
              <el-button type="primary" :loading="sending" @click="sendMessage">
                发送 (Ctrl+Enter)
              </el-button>
            </div>
          </div>
        </el-card>
      </section>
    </div>

    <div v-else class="page-body watch-body" :class="{ 'watch-left-collapsed': strategyListCollapsed }">
      <aside class="watch-left" :class="{ collapsed: strategyListCollapsed }">
        <el-card class="panel-card strategy-card" :class="{ collapsed: strategyListCollapsed }" shadow="never">
          <template #header>
            <div class="card-header">
              <span>{{ strategyListCollapsed ? '策略' : '策略管理' }}</span>
              <div class="card-actions">
                <template v-if="!strategyListCollapsed">
                  <el-button link @click="loadStrategies">刷新</el-button>
                  <el-button type="primary" link @click="createStrategy">新建策略</el-button>
                </template>
                <el-button link @click="toggleStrategyListCollapse">
                  {{ strategyListCollapsed ? '▶' : '◀' }}
                </el-button>
              </div>
            </div>
          </template>

          <div v-if="!strategyListCollapsed" class="strategy-list" v-loading="watchLoading">
            <div
              v-for="item in strategies"
              :key="item.id"
              class="strategy-item"
              :class="{ active: item.id === activeStrategyId }"
              @click="setActiveStrategy(item.id)"
            >
              <div class="strategy-main">
                <div class="title">{{ item.name }}</div>
                <div class="meta">
                  <span>{{ formatDateTime(item.updated_at) }}</span>
                </div>
              </div>
              <div class="strategy-buttons">
                <el-button link size="small" @click.stop="renameStrategy(item)">重命名</el-button>
                <el-button link size="small" type="danger" @click.stop="deleteStrategy(item.id)">
                  删除
                </el-button>
              </div>
            </div>
            <el-empty v-if="!strategies.length" description="暂无策略，点击“新建策略”创建" :image-size="64" />
          </div>
        </el-card>
      </aside>

      <section class="watch-panel">
        <el-card class="watch-card" shadow="never">
          <template #header>
            <div class="card-header">
              <span>{{ activeStrategy ? activeStrategy.name : '未选择策略' }}</span>
              <div class="card-actions">
                <el-button
                  v-if="activeStrategy && !isKeyLevelStrategy"
                  link
                  @click="generateStrategyView"
                >
                  Agent生成视图
                </el-button>
                <el-button
                  v-if="activeStrategy && !isKeyLevelStrategy"
                  link
                  @click="openWidgetEditor"
                >
                  新增图表
                </el-button>
                <el-switch
                  v-if="activeStrategy && !isKeyLevelStrategy"
                  v-model="watchLayoutEditMode"
                  inline-prompt
                  active-text="编辑"
                  inactive-text="浏览"
                />
                <el-button
                  v-if="activeStrategy && !isKeyLevelStrategy && watchLayoutEditMode"
                  link
                  @click="saveWatchWidgetConfig({ silent: false })"
                >
                  保存布局
                </el-button>
                <el-button link @click="switchToChat">进入对话设计</el-button>
              </div>
            </div>
          </template>

          <div v-if="activeStrategy" class="watch-content">
            <div v-if="isKeyLevelStrategy" class="key-board">
              <div class="key-toolbar">
                <div class="key-toolbar-left">
                  <el-autocomplete
                    v-model="keyStockSearchQuery"
                    :fetch-suggestions="queryKeyWatchStockSuggestions"
                    placeholder="输入股票代码或名称添加自选"
                    style="width: 300px"
                    clearable
                    @select="onWatchStockSuggestionSelect"
                    @keyup.enter="addWatchStockFromInput"
                  />
                  <div class="key-toolbar-actions">
                    <el-select v-model="keyLevelWindowDays" style="width: 138px" @change="onKeyLevelWindowChange">
                      <el-option :value="250" label="近1年关键位" />
                      <el-option :value="730" label="近2年关键位" />
                      <el-option :value="1825" label="近5年关键位" />
                      <el-option :value="3650" label="近10年关键位" />
                    </el-select>
                    <el-select v-model="keyChartMode" style="width: 114px">
                      <el-option
                        v-for="item in keyChartModeOptions"
                        :key="`key-chart-mode-${item.value}`"
                        :label="item.label"
                        :value="item.value"
                      />
                    </el-select>
                    <el-select v-model="keyAdjustMode" style="width: 114px">
                      <el-option
                        v-for="item in keyAdjustModeOptions"
                        :key="`key-adjust-mode-${item.value}`"
                        :label="item.label"
                        :value="item.value"
                      />
                    </el-select>
                    <el-select v-model="keySubIndicatorMode" style="width: 124px">
                      <el-option
                        v-for="item in keySubIndicatorModeOptions"
                        :key="`key-sub-indicator-${item.value}`"
                        :label="item.label"
                        :value="item.value"
                      />
                    </el-select>
                    <el-button :disabled="!keySelectedCode" :loading="keyKlineLoading" @click="loadKeyStockKline(keySelectedCode)">
                      刷新
                    </el-button>
                  </div>
                </div>
                <div class="key-toolbar-right">
                  <el-select v-model="keyWatchGroupBy" style="width: 108px">
                    <el-option
                      v-for="item in keyWatchGroupByOptions"
                      :key="`key-group-by-${item.value}`"
                      :label="item.label"
                      :value="item.value"
                    />
                  </el-select>
                  <el-select v-model="keyWatchFilterGroup" style="width: 138px">
                    <el-option
                      v-for="item in keyWatchGroupFilterOptions"
                      :key="`key-group-filter-${item.value}`"
                      :label="item.label"
                      :value="item.value"
                    />
                  </el-select>
                  <el-select v-model="keyWatchSortField" style="width: 128px">
                    <el-option
                      v-for="item in keyWatchSortFieldOptions"
                      :key="`key-sort-${item.value}`"
                      :label="item.label"
                      :value="item.value"
                    />
                  </el-select>
                  <el-button link @click="toggleKeyWatchSortOrder">
                    {{ keyWatchSortOrder === 'desc' ? '降序' : '升序' }}
                  </el-button>
                </div>
              </div>

              <div ref="keyBoardBodyRef" class="key-board-body">
                <div class="key-watchlist" :style="{ width: `${keyPaneWidthPercent}%` }">
                  <div class="key-watchlist-header">
                    <span>自选股 ({{ keyWatchlist.length }})</span>
                    <span class="key-watchlist-sub">虚拟表格 · {{ keyWatchGroupByLabel }}</span>
                  </div>
                  <div ref="keyWatchListRef" class="key-watchlist-list" @scroll="onKeyWatchListScroll">
                    <div :style="{ height: `${keyWatchVirtualPaddingTop}px` }"></div>
                    <template v-for="row in keyWatchVisibleRows" :key="row.id">
                      <div v-if="row.type === 'group'" class="key-watch-group-row">
                        <span>{{ row.label }}</span>
                        <span>{{ row.count }} 只</span>
                      </div>
                      <div
                        v-else
                        class="key-watch-item"
                        :class="{ active: row.item.code === keySelectedCode }"
                        @click="selectWatchStock(row.item.code)"
                      >
                        <div class="row-top">
                          <span class="name">{{ row.item.name }}</span>
                          <span class="code">{{ row.item.code }}</span>
                        </div>
                        <div class="row-mid">
                          <span class="price">{{ formatWatchPrice(row.item.latest_price) }}</span>
                          <span class="change" :class="watchChangeClass(row.item.change_pct)">
                            {{ formatWatchChange(row.item.change_pct) }}
                          </span>
                        </div>
                        <div class="row-bottom">
                          <span>成交额 {{ formatWatchAmount(row.item.amount) }}</span>
                          <span>成交量 {{ formatWatchVolume(row.item.volume) }}</span>
                        </div>
                        <div class="row-actions">
                          <el-tag size="small" effect="plain">{{ resolveWatchGroupLabel(row.item, 'custom') }}</el-tag>
                          <el-button link @click.stop="renameWatchStockGroup(row.item)">分组</el-button>
                          <el-button type="danger" link @click.stop="removeWatchStock(row.item.code)">删除</el-button>
                        </div>
                      </div>
                    </template>
                    <div :style="{ height: `${keyWatchVirtualPaddingBottom}px` }"></div>
                    <el-empty
                      v-if="!keyWatchFlatRows.length"
                      description="还没有自选股，先在上方搜索添加"
                      :image-size="72"
                    />
                  </div>
                </div>

                <div class="key-divider" @mousedown.prevent="startKeyPaneResize"></div>

                <div class="key-chart-panel">
                  <div class="key-chart-title">
                    <template v-if="keySelectedStock">
                      <span>{{ keySelectedStock.name }} ({{ keySelectedStock.code }})</span>
                      <el-tag size="small" effect="plain">{{ keyChartModeLabel }}</el-tag>
                      <el-tag size="small" effect="plain">{{ keyAdjustModeLabel }}</el-tag>
                      <el-tag size="small" type="info" effect="plain">{{ keySubIndicatorModeLabel }}</el-tag>
                    </template>
                    <span v-else>请选择左侧股票查看K线</span>
                  </div>
                  <div v-if="keyChartMode === 'time'" class="key-chart-hint">
                    当前无分钟数据，分时模式使用日线收盘走势替代显示。
                  </div>
                  <div v-if="keyAdjustMode !== 'none'" class="key-chart-hint">
                    当前数据源不区分复权口径，已保留复权切换状态位用于后续扩展。
                  </div>
                  <div v-if="keyKlineLoading" class="watch-placeholder">
                    <div class="placeholder-title">正在加载K线数据...</div>
                  </div>
                  <div v-else-if="keyWatchError" class="watch-placeholder">
                    <div class="placeholder-title">加载失败</div>
                    <div class="placeholder-desc">{{ keyWatchError }}</div>
                  </div>
                  <div v-else-if="keyKlineData.length" class="key-chart-wrap">
                    <VChart
                      :option="keyKlineOption"
                      :init-options="keyChartInitOptions"
                      autoresize
                      style="height: 100%; width: 100%"
                    />
                  </div>
                  <div v-else class="watch-placeholder">
                    <div class="placeholder-title">暂无K线数据</div>
                    <div class="placeholder-desc">请先从左侧选择或新增自选股。</div>
                  </div>
                </div>
              </div>
            </div>
            <div v-else-if="watchLoading" class="watch-placeholder">
              <div class="placeholder-title">正在加载图表...</div>
              <div class="placeholder-desc">根据策略配置拉取最新看盘图表。</div>
            </div>
            <div v-else-if="watchError" class="watch-placeholder">
              <div class="placeholder-title">图表加载失败</div>
              <div class="placeholder-desc">{{ watchError }}</div>
            </div>
            <div v-else class="watch-layout-shell" :class="{ 'tool-open': widgetEditorVisible }">
              <aside v-if="widgetEditorVisible" class="watch-widget-tool-panel">
                <div class="watch-widget-tool-head">
                  <span>图表组件库</span>
                  <el-button link @click="widgetEditorVisible = false">收起</el-button>
                </div>
                <el-collapse v-model="widgetToolExpandedCategoryIds" class="watch-widget-tool-collapse">
                  <el-collapse-item
                    v-for="category in widgetToolCategories"
                    :key="`tool-panel-${category.id}`"
                    :name="category.id"
                    :title="category.label"
                  >
                    <div class="watch-widget-tool-list">
                      <button
                        v-for="tpl in (category.templates || [])"
                        :key="`tool-item-${tpl.id}`"
                        type="button"
                        class="watch-widget-tool-item"
                        draggable="true"
                        @dragstart="onWidgetToolTemplateDragStart(tpl, $event)"
                        @dragend="onWidgetToolTemplateDragEnd"
                        @click="addWidgetFromToolTemplate(tpl)"
                      >
                        {{ tpl.title }}
                      </button>
                    </div>
                  </el-collapse-item>
                </el-collapse>
                <div class="watch-widget-tool-hint">点击快速添加，或拖动到右侧布局区域。</div>
              </aside>

              <div class="watch-layout">
                <div
                  ref="watchBoardRef"
                  class="watch-layout-board"
                  :class="{ editing: watchLayoutEditMode, 'palette-dragging': widgetPaletteDragging }"
                  :style="watchBoardStyle"
                  @dragover.prevent="onWatchBoardDragOver"
                  @drop.prevent="onWatchBoardDrop"
                >
                  <div
                    v-for="widget in watchWidgets"
                    :key="widget.id"
                    class="watch-widget"
                    :class="{
                      editable: watchLayoutEditMode,
                      moving: watchDraggingWidgetId === widget.id,
                      resizing: watchResizingWidgetId === widget.id
                    }"
                    :style="watchWidgetStyle(widget)"
                  >
                    <div
                      class="widget-header"
                      :class="{ draggable: watchLayoutEditMode }"
                      @mousedown.stop.prevent="startWatchWidgetDrag(widget, $event)"
                    >
                      <span>{{ widget.title }}</span>
                      <div class="widget-header-actions">
                        <el-dropdown
                          v-if="isAnalysisWidgetType(widget.type)"
                          trigger="click"
                          @command="(cmd) => setWidgetLinkGroup(widget, cmd)"
                        >
                          <el-button size="small" link>联动组{{ getWatchlistLinkGroupLabel(widget) }}</el-button>
                          <template #dropdown>
                            <el-dropdown-menu>
                              <el-dropdown-item command="">（无）</el-dropdown-item>
                              <el-dropdown-item
                                v-for="num in 9"
                                :key="`header-link-group-${widget.id}-${num}`"
                                :command="String(num)"
                              >
                                {{ num }}
                              </el-dropdown-item>
                            </el-dropdown-menu>
                          </template>
                        </el-dropdown>
                        <el-button
                          v-if="widget.type === 'daily_replay'"
                          size="small"
                          link
                          @click.stop="openDailyReplayRequirementDialog(widget)"
                        >
                          复盘要求
                        </el-button>
                        <el-button
                          v-if="widget.type === 'daily_replay'"
                          size="small"
                          link
                          :loading="dailyReplayRunningWidgetId === widget.id"
                          @click.stop="runDailyReplayWidget(widget)"
                        >
                          开始复盘
                        </el-button>
                        <el-button
                          v-if="watchLayoutEditMode"
                          type="danger"
                          link
                          @click.stop="removeWatchWidget(widget.id)"
                        >
                          删除
                        </el-button>
                      </div>
                    </div>
                    <div class="watch-widget-body">
                      <div v-if="widget.error" class="widget-error">{{ widget.error }}</div>
                      <div v-else-if="widget.type === 'watchlist_panel'" class="widget-watchlist">
                        <div class="widget-watchlist-tabs">
                          <button
                            v-for="group in (widget.watchlistGroups || [])"
                            :key="`widget-watchlist-tab-${widget.id}-${group.id}`"
                            type="button"
                            class="widget-watchlist-tab"
                            :class="{ active: group.id === widget.watchlistMeta?.group_id }"
                            @click="switchWatchlistPanelGroup(widget, group.id)"
                          >
                            {{ group.name }}
                          </button>
                          <div class="widget-watchlist-tabs-actions">
                            <button
                              type="button"
                              class="widget-watchlist-tab-icon"
                              title="新建分组"
                              @click="createWatchlistGroupForWidget(widget)"
                            >
                              +
                            </button>
                            <el-dropdown trigger="click" @command="(cmd) => setWatchlistPanelLinkGroup(widget, cmd)">
                              <button
                                type="button"
                                class="widget-watchlist-tab-icon"
                                title="设置联动分组"
                              >
                                {{ getWatchlistLinkGroupLabel(widget) }}
                              </button>
                              <template #dropdown>
                                <el-dropdown-menu>
                                  <el-dropdown-item command="">（无）</el-dropdown-item>
                                  <el-dropdown-item
                                    v-for="num in 9"
                                    :key="`watch-link-group-${widget.id}-${num}`"
                                    :command="String(num)"
                                  >
                                    {{ num }}
                                  </el-dropdown-item>
                                </el-dropdown-menu>
                              </template>
                            </el-dropdown>
                          </div>
                        </div>
                        <div class="widget-watchlist-list">
                          <div class="widget-watchlist-table-head" :style="getWatchlistGridStyle(widget.columns)">
                            <span class="seq-head">
                              <button
                                type="button"
                                class="watchlist-seq-setting-btn"
                                title="设置"
                                @click="openWatchlistPanelConfig(widget)"
                              >
                                设
                              </button>
                            </span>
                            <span v-for="col in (widget.columns || [])" :key="`watchlist-head-${widget.id}-${col}`">{{ getWatchlistColumnLabel(col) }}</span>
                          </div>
                          <div
                            v-for="(item, idx) in (widget.watchlistRows || [])"
                            :key="`widget-watch-${widget.id}-${item.type}-${item.code}-${item.name}-${idx}`"
                            class="widget-watchlist-item"
                            :style="getWatchlistGridStyle(widget.columns)"
                            @click="onWatchlistPanelRowClick(widget, item)"
                          >
                            <span class="seq-cell">{{ idx + 1 }}</span>
                            <span
                              v-for="col in (widget.columns || [])"
                              :key="`watchlist-cell-${widget.id}-${idx}-${col}`"
                              :class="getWatchlistCellClass(col, item)"
                            >
                              {{ formatWatchlistCell(item, col) }}
                            </span>
                          </div>
                          <el-empty v-if="!(widget.watchlistRows || []).length" description="自选股为空" :image-size="58" />
                        </div>
                      </div>
                      <div v-else-if="widget.type === 'daily_replay'" class="daily-replay-widget">
                        <div class="daily-replay-head">
                          <div class="daily-replay-title">今日复盘</div>
                          <div class="daily-replay-sub">调用策略编辑多 Agent 执行复盘</div>
                        </div>
                        <div class="daily-replay-content">
                          <div class="daily-replay-label">复盘要求</div>
                          <div class="daily-replay-requirement">{{ widget.replayRequirement || '未设置复盘要求，点击右上角“复盘要求”' }}</div>
                        </div>
                        <div class="daily-replay-actions">
                          <el-button size="small" @click="openDailyReplayRequirementDialog(widget)">编辑要求</el-button>
                          <el-button
                            size="small"
                            type="primary"
                            :loading="dailyReplayRunningWidgetId === widget.id"
                            @click="runDailyReplayWidget(widget)"
                          >
                            开始复盘
                          </el-button>
                        </div>
                      </div>
                      <EChartsRenderer
                        v-else
                        :key="getWidgetRenderKey(widget)"
                        :chart-html="widget.chartHtml"
                        :height="getWidgetChartHeight(widget)"
                      />
                    </div>
                    <div
                      v-if="watchLayoutEditMode"
                      class="widget-resize-handle"
                      @mousedown.stop.prevent="startWatchWidgetResize(widget, $event)"
                    ></div>
                  </div>
                </div>
                <el-empty
                  v-if="!watchWidgets.length && !watchLoading"
                  description="暂无图表配置，可通过“新增图表”或 Agent 生成视图补充"
                  :image-size="90"
                />
              </div>
            </div>
          </div>
          <el-empty v-else description="请选择左侧策略进入看盘" :image-size="90" />
        </el-card>
      </section>
    </div>

    <el-dialog v-model="jobsDialogVisible" title="任务列表" width="620px">
      <div class="job-list dialog-job-list">
        <div v-for="job in jobList" :key="job.job_id" class="job-item">
          <div class="job-main">
            <div class="job-title">{{ job.original_name || job.resource_id }}</div>
            <div class="job-meta">
              <el-tag
                size="small"
                :type="job.status === 'ok' ? 'success' : job.status === 'running' ? 'warning' : job.status === 'queued' ? 'info' : 'danger'"
              >
                {{ job.status }}
              </el-tag>
              <span>{{ formatDateTime(job.created_at) }}</span>
            </div>
            <el-progress :percentage="Number(job.progress || 0)" :stroke-width="10" />
            <div class="progress-text">{{ job.message || '处理中' }}</div>
          </div>
        </div>
        <el-empty v-if="!jobList.length" description="暂无任务" :image-size="56" />
      </div>
      <template #footer>
        <el-button @click="loadJobs">刷新</el-button>
        <el-button type="primary" @click="jobsDialogVisible = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="resourceManageVisible" title="资料库管理" width="980px">
      <div class="manage-toolbar">
        <el-select v-model="manageFilterGroupId" size="small" style="width: 200px" placeholder="筛选分组">
          <el-option label="所有分组" value="" />
          <el-option
            v-for="grp in resourceGroups"
            :key="grp.group_id"
            :label="`${grp.group_name} (${grp.count})`"
            :value="grp.group_id"
          />
        </el-select>
        <el-input
          v-model="manageKeyword"
          size="small"
          clearable
          placeholder="搜索资料名称"
          style="width: 220px"
        />
        <el-tag size="small" type="info">已选 {{ manageSelectedIds.length }} 条</el-tag>
      </div>
      <div class="manage-body">
        <div class="manage-list">
          <el-checkbox-group v-model="manageSelectedIds" class="manage-checkbox-group">
            <div
              v-for="res in manageFilteredResources"
              :key="res.id"
              class="manage-item"
              draggable="true"
              @dragstart="onResourceDragStart(res)"
              @dragend="onResourceDragEnd"
            >
              <el-checkbox :label="res.id">
                <div class="manage-info">
                  <div class="manage-name">{{ res.original_name }}</div>
                  <div class="manage-meta">
                    {{ res.group_name || '未分组' }} · {{ formatSize(res.size_bytes) }}
                  </div>
                </div>
              </el-checkbox>
              <div class="manage-actions">
                <el-button link size="small" @click="downloadResourceMarkdown(res)">下载原MD</el-button>
                <el-button link size="small" @click="downloadResourceAiSummary(res)">下载AI总结</el-button>
                <el-button link size="small" @click="renameResource(res)">重命名</el-button>
                <el-button link size="small" @click="quickTransferResource(res)">移动/复制</el-button>
                <el-button link size="small" type="danger" @click="deleteResource(res.id)">删除</el-button>
              </div>
            </div>
          </el-checkbox-group>
          <el-empty v-if="!manageFilteredResources.length" description="暂无资源" :image-size="56" />
        </div>

        <div class="manage-panel">
          <div class="manage-section">
            <div class="manage-section-title">批量操作</div>
            <div class="manage-inline-row">
              <el-radio-group v-model="manageMode" size="small">
                <el-radio-button label="move">移动</el-radio-button>
                <el-radio-button label="copy">复制</el-radio-button>
              </el-radio-group>
              <el-radio-group v-model="manageTargetType" size="small" class="manage-inline-radio">
                <el-radio-button label="existing">已有分组</el-radio-button>
                <el-radio-button label="new">新建分组</el-radio-button>
              </el-radio-group>
            </div>
            <el-select
              v-if="manageTargetType === 'existing'"
              v-model="manageTargetGroupId"
              size="small"
              placeholder="选择目标分组"
            >
              <el-option
                v-for="grp in manageTransferGroupOptions"
                :key="grp.group_id"
                :label="`${grp.group_name} (${grp.count})`"
                :value="grp.group_id"
              />
            </el-select>
            <el-input
              v-else
              v-model="manageTargetGroupName"
              size="small"
              placeholder="输入新分组名称"
            />
            <div class="manage-section-actions">
              <el-button
                type="primary"
                size="small"
                :disabled="!manageSelectedIds.length"
                @click="submitResourceTransfer"
              >
                应用到选中
              </el-button>
              <el-button size="small" :disabled="!manageSelectedIds.length" @click="manageSelectedIds = []">
                清空选择
              </el-button>
            </div>
            <el-button
              type="danger"
              size="small"
              plain
              :disabled="!manageSelectedIds.length"
              @click="batchDeleteResources"
            >
              删除选中资料
            </el-button>
          </div>

          <div class="manage-section">
            <div class="manage-section-title">分组管理</div>
            <div class="manage-section-subtitle">
              当前分组：{{ manageCurrentGroup ? `${manageCurrentGroup.group_name} (${manageCurrentGroup.count})` : '未选择' }}
            </div>
            <div class="manage-section-actions">
              <el-button size="small" type="primary" plain @click="createManageGroup">新建分组</el-button>
              <el-button size="small" :disabled="!manageFilterGroupId" @click="renameManageGroup">重命名</el-button>
              <el-button
                type="danger"
                size="small"
                :disabled="!manageFilterGroupId"
                @click="deleteManageGroup"
              >
                删除本分组
              </el-button>
            </div>
          </div>

          <el-collapse class="manage-advanced">
            <el-collapse-item title="高级：拖拽快速移动" name="drag-move">
              <div class="manage-hint">可将左侧单条资料拖拽到目标分组，默认执行移动。</div>
              <div class="manage-group-list">
                <div
                  v-for="grp in resourceGroups"
                  :key="grp.group_id"
                  class="manage-group-chip"
                  @dragover.prevent
                  @drop="handleDropToGroup(grp)"
                >
                  {{ grp.group_name }}
                </div>
              </div>
            </el-collapse-item>
          </el-collapse>
        </div>
      </div>
      <template #footer>
        <el-button @click="resourceManageVisible = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="groupTransferVisible" title="移动/复制分组" width="560px">
      <el-form label-width="90px">
        <el-form-item label="源分组">
          <el-select v-model="groupTransferForm.source_group_id" style="width: 100%">
            <el-option
              v-for="grp in resourceGroups"
              :key="grp.group_id"
              :label="`${grp.group_name} (${grp.count})`"
              :value="grp.group_id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="目标分组">
          <el-select
            v-model="groupTransferForm.target_group_id"
            style="width: 100%"
            clearable
            placeholder="选择已有分组（可选）"
          >
            <el-option
              v-for="grp in targetGroupOptions"
              :key="grp.group_id"
              :label="`${grp.group_name} (${grp.count})`"
              :value="grp.group_id"
            />
          </el-select>
          <el-input
            v-model="groupTransferForm.target_group_name"
            placeholder="或输入新分组名称"
            style="margin-top: 8px"
            :disabled="!!groupTransferForm.target_group_id"
          />
        </el-form-item>
        <el-form-item label="操作">
          <el-radio-group v-model="groupTransferForm.mode">
            <el-radio label="move">移动</el-radio>
            <el-radio label="copy">复制</el-radio>
          </el-radio-group>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="groupTransferVisible = false">取消</el-button>
        <el-button type="primary" @click="submitGroupTransfer">确认</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="watchlistPanelConfigVisible" title="自选股组件设置" width="920px">
      <div class="watchlist-config-layout">
        <div class="watchlist-config-menu">
          <button
            v-for="menu in watchlistConfigMenuOptions"
            :key="`watchlist-config-menu-${menu.value}`"
            type="button"
            class="watchlist-config-menu-item"
            :class="{ active: watchlistConfigActiveMenu === menu.value }"
            @click="watchlistConfigActiveMenu = menu.value"
          >
            {{ menu.label }}
          </button>
        </div>
        <div class="watchlist-config-main">
          <div v-if="watchlistConfigActiveMenu === 'add_stock'" class="watchlist-config-panel">
            <div class="watchlist-config-panel-head">
              <span>当前自选组：{{ watchlistPanelConfigGroupName }}</span>
            </div>
            <div class="watchlist-config-stock-search">
              <el-input
                v-model="watchlistConfigStockQuery"
                size="small"
                clearable
                placeholder="输入名称或部分代码（自动搜索股票/指数/板块）"
                @keydown.enter.prevent="searchWatchlistConfigStocks"
              />
            </div>
            <div class="watchlist-config-stock-list">
              <div
                v-for="row in watchlistConfigStockCandidates"
                :key="`watchlist-config-stock-${row.key}`"
                class="watchlist-config-stock-item"
              >
                <span class="stock-code">{{ row.display_code || '--' }}</span>
                <span class="stock-name">{{ row.name }}</span>
                <button
                  v-if="!row.in_group"
                  type="button"
                  class="stock-mark add"
                  @click="addStockFromWatchlistConfigRow(row)"
                >
                  +
                </button>
                <span v-else class="stock-mark exists">√</span>
              </div>
              <el-empty v-if="!watchlistConfigStockCandidates.length" description="暂无匹配股票" :image-size="52" />
            </div>
          </div>
          <div v-else class="watchlist-config-panel">
            <div class="watchlist-columns-layout">
              <div class="watchlist-columns-schemes">
                <div class="watchlist-columns-title">表头方案</div>
                <button
                  v-for="scheme in watchlistColumnSchemes"
                  :key="`watchlist-scheme-${scheme.id}`"
                  type="button"
                  class="watchlist-scheme-item"
                  :class="{ active: scheme.id === watchlistColumnSchemeId }"
                  @click="applyWatchlistColumnScheme(scheme.id)"
                >
                  {{ scheme.name }}
                </button>
              </div>
              <div class="watchlist-columns-selected">
                <div class="watchlist-columns-title">已选表头</div>
                <div class="watchlist-selected-list">
                  <div
                    v-for="col in watchlistPanelConfigForm.columns"
                    :key="`watchlist-selected-${col}`"
                    class="watchlist-selected-item"
                  >
                    <span>{{ getWatchlistColumnLabel(col) }}</span>
                    <button type="button" @click="removeWatchlistColumn(col)">×</button>
                  </div>
                </div>
                <div class="watchlist-header-search">
                  <el-input
                    v-model="watchlistHeaderSearch"
                    size="small"
                    clearable
                    placeholder="搜索表头库（表头数据）"
                  />
                  <div class="watchlist-header-search-results">
                    <button
                      v-for="col in watchlistHeaderSearchResults"
                      :key="`watchlist-header-result-${col.value}`"
                      type="button"
                      @click="addWatchlistColumn(col.value)"
                    >
                      + {{ col.label }}
                    </button>
                    <el-empty v-if="!watchlistHeaderSearchResults.length" description="无可添加表头" :image-size="44" />
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      <template #footer>
        <el-button @click="watchlistPanelConfigVisible = false">关闭</el-button>
        <el-button type="primary" @click="saveWatchlistPanelConfig">保存表头设置</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="dailyReplayConfigVisible" title="每日复盘要求" width="680px">
      <div class="daily-replay-config">
        <el-input
          v-model="dailyReplayRequirementDraft"
          type="textarea"
          :rows="12"
          placeholder="输入复盘要求，如：先总结市场主线，再复盘自选组中的强弱分化与明日观察点"
        />
      </div>
      <template #footer>
        <el-button @click="dailyReplayConfigVisible = false">取消</el-button>
        <el-button type="primary" @click="saveDailyReplayRequirement">保存</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="memoryManageVisible" title="长期记忆管理" width="980px">
      <div class="memory-toolbar">
        <el-select v-model="memoryManageProfileId" style="width: 260px" placeholder="选择人格">
          <el-option
            v-for="profile in memoryProfiles"
            :key="profile.id"
            :label="`${profile.name} (${profile.linked_resource_count || 0})`"
            :value="profile.id"
          />
        </el-select>
        <el-button size="small" @click="createMemoryProfile">新建人格</el-button>
        <el-button size="small" :disabled="!memoryManageProfileId" @click="renameMemoryProfile">重命名</el-button>
        <el-button
          size="small"
          type="danger"
          plain
          :disabled="!memoryManageProfileId"
          @click="deleteMemoryProfile"
        >
          删除人格
        </el-button>
        <el-button
          size="small"
          type="primary"
          plain
          :disabled="!memoryManageProfileId"
          @click="applyMemoryAsActive"
        >
          设为当前会话人格
        </el-button>
      </div>

      <el-tabs v-model="memoryActiveTab">
        <el-tab-pane label="资料绑定" name="binding">
          <div class="memory-bind-toolbar">
            <el-select
              v-model="memoryBindFilterGroupId"
              size="small"
              style="width: 180px"
              placeholder="筛选分组"
            >
              <el-option label="所有分组" value="" />
              <el-option
                v-for="grp in resourceGroups"
                :key="grp.group_id"
                :label="`${grp.group_name} (${grp.count})`"
                :value="grp.group_id"
              />
            </el-select>
            <el-button
              size="small"
              type="primary"
              :disabled="!memoryManageProfileId || !memoryBindSelectedIds.length"
              @click="bindSelectedResourcesToMemory"
            >
              添加选中文件到长期记忆
            </el-button>
            <el-select
              v-model="memoryBindGroupId"
              size="small"
              style="width: 220px"
              placeholder="选择分组"
            >
              <el-option
                v-for="grp in resourceGroups"
                :key="grp.group_id"
                :label="`${grp.group_name} (${grp.count})`"
                :value="grp.group_id"
              />
            </el-select>
            <el-button
              size="small"
              :disabled="!memoryManageProfileId || !memoryBindGroupId"
              @click="bindGroupToMemory"
            >
              首次纳入整组
            </el-button>
            <el-button
              size="small"
              :disabled="!memoryManageProfileId"
              @click="syncMemoryGroupIncremental"
            >
              手动同步分组新增
            </el-button>
          </div>

          <div class="memory-bind-list">
            <el-checkbox-group v-model="memoryBindSelectedIds" class="memory-checkbox-group">
              <div
                v-for="res in memoryBindResources"
                :key="res.id"
                class="memory-bind-item"
              >
                <el-checkbox :label="res.id">
                  <div class="memory-bind-name">{{ res.original_name }}</div>
                  <div class="memory-bind-meta">
                    {{ res.group_name || '未分组' }}
                    <span v-if="res.content_time"> · 内容时间 {{ res.content_time }}</span>
                  </div>
                </el-checkbox>
              </div>
            </el-checkbox-group>
            <el-empty v-if="!memoryBindResources.length" description="暂无可选资料" :image-size="56" />
          </div>
        </el-tab-pane>

        <el-tab-pane label="人物侧写" name="portrait">
          <div class="memory-portrait-actions">
            <el-select
              v-model="selectedPortraitModel"
              size="small"
              style="width: 240px"
              placeholder="选择侧写模型"
            >
              <el-option
                v-for="model in modelOptions"
                :key="`portrait_${model.value}`"
                :label="model.label"
                :value="model.value"
              />
            </el-select>
            <el-button
              size="small"
              type="primary"
              :disabled="!memoryManageProfileId"
              :loading="memoryDraftLoading"
              @click="generateMemoryPortraitDraft"
            >
              AI 生成初稿
            </el-button>
            <el-button
              size="small"
              :disabled="!memoryManageProfileId"
              @click="saveMemoryPortrait"
            >
              保存侧写
            </el-button>
            <el-dropdown
              trigger="click"
              :disabled="!memoryManageProfileId"
              @command="exportMemoryPortrait"
            >
              <el-button size="small">
                导出侧写
              </el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item command="md">导出 Markdown</el-dropdown-item>
                  <el-dropdown-item command="docx">导出 Word</el-dropdown-item>
                  <el-dropdown-item command="pdf">导出 PDF</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
            <el-button
              size="small"
              :disabled="!memoryManageProfileId"
              @click="loadMemoryPreviewContext"
            >
              刷新上下文预览
            </el-button>
          </div>
          <el-form label-width="110px">
            <el-form-item label="交易方法论">
              <el-input v-model="memoryPortraitForm.methodology" type="textarea" :rows="4" />
            </el-form-item>
            <el-form-item label="交易手法">
              <el-input v-model="memoryPortraitForm.tactics" type="textarea" :rows="4" />
            </el-form-item>
            <el-form-item label="观点">
              <el-input v-model="memoryPortraitForm.views" type="textarea" :rows="4" />
            </el-form-item>
            <el-form-item label="交易操作">
              <el-input v-model="memoryPortraitForm.operations" type="textarea" :rows="4" />
            </el-form-item>
            <el-form-item label="风控规则">
              <el-input v-model="memoryPortraitForm.risk_rules" type="textarea" :rows="3" />
            </el-form-item>
            <el-form-item label="风格约束">
              <el-input v-model="memoryPortraitForm.style_constraints" type="textarea" :rows="3" />
            </el-form-item>
          </el-form>
        </el-tab-pane>

        <el-tab-pane label="上下文预览" name="preview">
          <el-input v-model="memoryPreviewContext" type="textarea" :rows="18" readonly />
        </el-tab-pane>
      </el-tabs>

      <template #footer>
        <el-button @click="memoryManageVisible = false">关闭</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { CandlestickChart, LineChart, BarChart } from 'echarts/charts'
import { TitleComponent, TooltipComponent, LegendComponent, GridComponent, DataZoomComponent } from 'echarts/components'
import VChart from 'vue-echarts'
import ApiService from '../services/api'
import EChartsRenderer from '../components/EChartsRenderer.vue'
import { marked } from 'marked'
import DOMPurify from 'dompurify'
import {
  WATCHLIST_COLUMN_OPTIONS,
  WATCHLIST_DEFAULT_COLUMNS,
  buildWatchlistGridTemplate,
  filterWatchlistColumns,
  formatWatchlistCell as formatWatchlistCellUtil,
  getWatchlistCellClass as getWatchlistCellClassUtil,
  getWatchlistColumnLabel as getWatchlistColumnLabelUtil
} from '@visualizers/watchlistPanel'

use([
  CanvasRenderer,
  CandlestickChart,
  LineChart,
  BarChart,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent
])

const uploadRef = ref(null)
const messagesRef = ref(null)

const runtime = ref({
  api_key_configured: false,
  model: 'gpt-4o-mini',
  agents: []
})

const activeMode = ref('dialog')
const conversations = ref([])
const activeConversationId = ref('')
const messages = ref([])

const resources = ref([])
const selectedResourceIds = ref([])
const pendingUploadFiles = ref([])
const uploadGroupName = ref('')
const resourceGroups = ref([])
const selectedGroupId = ref('')
const hoverGroupId = ref('')
const groupPopoverVisible = ref(false)
const activeResourceId = ref('')
const jobList = ref([])
const jobsDialogVisible = ref(false)
const groupTransferVisible = ref(false)
const resourceManageVisible = ref(false)
const manageSelectedIds = ref([])
const manageFilterGroupId = ref('')
const manageKeyword = ref('')
const manageTargetGroupId = ref('')
const manageTargetGroupName = ref('')
const manageTargetType = ref('existing')
const manageMode = ref('move')
const dragResourceId = ref('')
const groupTransferForm = ref({
  source_group_id: '',
  target_group_id: '',
  target_group_name: '',
  mode: 'move'
})

const selectedAgentName = ref('dialog_agent')
const MODE_AGENT_MAP = Object.freeze({
  dialog: 'dialog_agent',
  crawler: 'engineer_agent',
  strategy_edit: 'architect_agent',
  strategy_analysis: 'analyst_agent'
})
const modeAgentSelection = ref({
  dialog: '',
  crawler: '',
  strategy_edit: '',
  strategy_analysis: ''
})
const isReplayChatMode = computed(() =>
  activeMode.value === 'dialog' || activeMode.value === 'crawler' || activeMode.value === 'strategy_edit'
)
const topMode = computed({
  get: () => (activeMode.value === 'strategy_analysis' ? 'strategy_analysis' : 'dialog'),
  set: (next) => {
    activeMode.value = next === 'strategy_analysis' ? 'strategy_analysis' : 'dialog'
  }
})
const DEFAULT_MODEL_OPTIONS = [
  { label: 'DeepSeek v3.2', value: 'DeepSeek3.2' },
  { label: 'DeepSeek v3.2 Thinking', value: 'deepseek-v3.2-thinking' },
  { label: '通义千问 qwen-max', value: 'qwen-max' },
]
const normalizeModelOptions = (rawOptions = []) => {
  const out = []
  const seen = new Set()
  const arr = Array.isArray(rawOptions) ? rawOptions : []
  for (const item of arr) {
    if (!item) continue
    if (typeof item === 'string') {
      const value = item.trim()
      if (!value || seen.has(value)) continue
      seen.add(value)
      out.push({ label: value, value })
      continue
    }
    const value = String(item.value || item.id || '').trim()
    if (!value || seen.has(value)) continue
    seen.add(value)
    const label = String(item.label || item.name || value).trim() || value
    out.push({ label, value })
  }
  return out
}
const modelOptions = ref(normalizeModelOptions(DEFAULT_MODEL_OPTIONS))
const selectedModel = ref('deepseek-v3.2')
const selectedPortraitModel = ref('deepseek-v3.2')
const PROMPT_TEMPLATE_STORAGE_KEY = 'strategy_watch_prompt_templates'
const LEGACY_PROMPT_TEMPLATE_STORAGE_KEY = 'strategy_watch_prompt_templates_v1'
const DEFAULT_PROMPT_TEMPLATES = [
  { id: 'none', name: '不使用模板', content: '' },
  {
    id: 'review_analysis',
    name: '复盘分析',
    content: [
      '你是A股复盘助手。',
      '请基于给定资料先提炼事实，再给结论。',
      '输出必须包含：',
      '1. 核心结论（3条以内）',
      '2. 关键信号（量价、情绪、板块联动）',
      '3. 风险点（至少2条）',
      '4. 下一交易日观察清单（可执行）'
    ].join('\n')
  },
  {
    id: 'trade_plan',
    name: '交易计划',
    content: [
      '你是交易计划助手。',
      '请将回答组织为“计划而非预测”，并明确触发条件。',
      '输出格式：',
      '- 交易假设',
      '- 入场条件',
      '- 加仓/减仓条件',
      '- 失效条件与止损',
      '- 仓位建议（百分比）'
    ].join('\n')
  },
  {
    id: 'risk_first',
    name: '风险优先',
    content: [
      '你是风控优先的交易顾问。',
      '先列风险，再列机会。',
      '所有结论给出置信度（高/中/低）与依据。',
      '若资料不足，明确写出“信息不足”并给补充数据清单。'
    ].join('\n')
  }
]
const clonePromptTemplates = () => DEFAULT_PROMPT_TEMPLATES.map(item => ({ ...item }))
const normalizePromptTemplates = (rawTemplates) => {
  const normalized = [{ id: 'none', name: '不使用模板', content: '' }]
  if (!Array.isArray(rawTemplates)) return normalized
  const usedIds = new Set(['none'])
  for (const item of rawTemplates) {
    const id = String(item?.id || '').trim()
    if (!id || id === 'none' || usedIds.has(id)) continue
    usedIds.add(id)
    normalized.push({
      id,
      name: String(item?.name || '').trim() || '未命名模板',
      content: String(item?.content || '')
    })
  }
  return normalized
}
const promptTemplates = ref(clonePromptTemplates())
const selectedPromptTemplateId = ref('none')
const promptTemplatePopoverVisible = ref(false)
const memoryProfiles = ref([])
const activeMemoryProfileId = ref('')
const memoryManageVisible = ref(false)
const memoryManageProfileId = ref('')
const memoryActiveTab = ref('binding')
const memoryBindSelectedIds = ref([])
const memoryBindGroupId = ref('')
const memoryBindFilterGroupId = ref('')
const memoryDraftLoading = ref(false)
const memoryPreviewContext = ref('')
const memoryPortraitForm = ref({
  methodology: '',
  tactics: '',
  views: '',
  operations: '',
  risk_rules: '',
  style_constraints: ''
})

const marketSentimentChartOptions = [
  { key: 'red_ratio_and_amount', title: '红盘率与成交额' },
  { key: 'limit_up_count', title: '涨停/跌停统计' },
  { key: 'ground_ceiling_count', title: '地天板/天地板' },
  { key: 'continuous_limit_up', title: '连板天梯' },
  { key: 'change_distribution', title: '涨跌幅分布' }
]
const widgetToolCategories = [
  {
    id: 'watchlist',
    label: '自选组合',
    templates: [
      {
        id: 'watchlist_panel',
        type: 'watchlist_panel',
        title: '自选股列表',
        desc: '展示自选分组与股票列表。',
        defaults: {
          group_id: '',
          max_count: 50
        },
        fields: [
          { key: 'max_count', label: '最大显示数', type: 'number', min: 1, max: 10000, step: 1 }
        ]
      }
    ]
  },
  {
    id: 'analysis',
    label: '分析图',
    templates: [
      {
        id: 'analysis_kline',
        type: 'analysis_kline',
        title: '分析图',
        desc: '自动识别股票/指数/板块并绘制K线。',
        defaults: {
          target_type: 'auto',
          target_value: '000001',
          days: 120
        },
        fields: [
          { key: 'target_value', label: '代码/名称', type: 'text', placeholder: '如 600519 / 上证指数 / 半导体' },
          { key: 'days', label: '回看天数', type: 'number', min: 20, max: 500, step: 5 }
        ]
      }
    ]
  },
  {
    id: 'review',
    label: '复盘',
    templates: [
      {
        id: 'daily_replay',
        type: 'daily_replay',
        title: '每日复盘',
        desc: '保存复盘要求并调用策略编辑多 Agent 执行复盘。',
        defaults: {
          requirement: '请按市场主线、强弱切换、风险点、明日计划四部分完成今日复盘。'
        },
        fields: []
      }
    ]
  },
  {
    id: 'market',
    label: '市场',
    templates: [
      ...marketSentimentChartOptions.map(item => ({
        id: `market_sentiment_${item.key}`,
        type: 'market_sentiment_chart',
        title: item.title,
        desc: '市场情绪图表。',
        defaults: {
          chart_key: item.key,
          days_back: 30
        },
        fields: [
          { key: 'days_back', label: '回看天数', type: 'number', min: 10, max: 240, step: 5 }
        ]
      })),
      {
        id: 'market_volume',
        type: 'market_volume',
        title: '市场量能对比',
        desc: '对比最近两个交易日成交额。',
        defaults: {},
        fields: []
      }
    ]
  }
]
const strategies = ref([])
const activeStrategyId = ref('')
const watchLoading = ref(false)
const watchWidgets = ref([])
const watchError = ref('')
const watchWidgetDefs = ref([])
const watchLayoutEditMode = ref(false)
const watchBoardRef = ref(null)
const watchDraggingWidgetId = ref('')
const watchResizingWidgetId = ref('')
const watchPersistTimer = ref(null)
const widgetEditorVisible = ref(false)
const widgetToolExpandedCategoryIds = ref([])
const widgetPaletteDragging = ref(false)
const widgetPaletteDragTemplateId = ref('')
const watchlistGroups = ref([])
const activeWatchlistGroupId = ref('')
const watchlistStockSearchQuery = ref('')
const watchlistSectorInput = ref('')
const watchlistSelectedIndexName = ref('')
const watchlistIndexOptions = ref(['上证指数', '深证成指', '创业板指', '科创50', '沪深300'])
const watchlistPanelConfigVisible = ref(false)
const watchlistPanelConfigWidgetId = ref('')
const dailyReplayConfigVisible = ref(false)
const dailyReplayConfigWidgetId = ref('')
const dailyReplayRequirementDraft = ref('')
const dailyReplayRunningWidgetId = ref('')
const watchlistPanelConfigForm = ref({
  group_id: '',
  max_count: 50,
  columns: [...WATCHLIST_DEFAULT_COLUMNS],
  linked_widget_id: '',
  stock_query: '',
  sector_input: '',
  index_name: ''
})
const watchlistConfigMenuOptions = [
  { value: 'add_stock', label: '添加股票' },
  { value: 'edit_columns', label: '编辑表头' }
]
const watchlistConfigActiveMenu = ref('add_stock')
const watchlistConfigStockQuery = ref('')
const watchlistConfigStockCandidates = ref([])
const watchlistConfigStockSearching = ref(false)
const watchlistConfigStockSearchTimer = ref(null)
const watchlistConfigStockSearchReqId = ref(0)
const watchlistConfigIndexCandidates = ref([])
const watchlistConfigSectorCandidates = ref([])
const watchlistConfigLookupLoaded = ref(false)
const watchlistHeaderSearch = ref('')
const watchlistColumnSchemeId = ref('classic')
const watchlistColumnSchemes = [
  { id: 'classic', name: '经典方案', columns: ['code', 'name', 'change_pct', 'amount'] },
  { id: 'trend', name: '趋势方案', columns: ['code', 'name', 'latest_price', 'change_pct', 'pct5', 'pct10'] },
  { id: 'compact', name: '精简方案', columns: ['code', 'name', 'change_pct'] }
]

const KEY_WATCHLIST_MAX = 10000
const KEY_WATCH_ROW_HEIGHT = 84
const KEY_WATCH_OVERSCAN = 6
const keyWatchlist = ref([])
const keySelectedCode = ref('')
const keyKlineData = ref([])
const keyLevels = ref([])
const keyKlineLoading = ref(false)
const keyWatchError = ref('')
const keyStockSearchQuery = ref('')
const keyPaneWidthPercent = ref(36)
const keyLevelWindowDays = ref(3650)
const keyBoardBodyRef = ref(null)
const keyPaneResizing = ref(false)
const keyWatchListRef = ref(null)
const keyWatchScrollTop = ref(0)
const keyWatchViewportHeight = ref(420)
const keyBoardHydrating = ref(false)
const keyWatchSortField = ref('change_pct')
const keyWatchSortOrder = ref('desc')
const keyWatchGroupBy = ref('custom')
const keyWatchFilterGroup = ref('__all__')
const keyChartMode = ref('kline')
const keyAdjustMode = ref('none')
const keySubIndicatorMode = ref('both')

const keyChartModeOptions = [
  { value: 'kline', label: 'K线' },
  { value: 'time', label: '分时' }
]
const keyAdjustModeOptions = [
  { value: 'none', label: '不复权' },
  { value: 'qfq', label: '前复权' },
  { value: 'hfq', label: '后复权' }
]
const keySubIndicatorModeOptions = [
  { value: 'both', label: '量+额' },
  { value: 'volume', label: '成交量' },
  { value: 'amount', label: '成交额' }
]
const keyWatchGroupByOptions = [
  { value: 'custom', label: '按分组' },
  { value: 'market', label: '按市场' },
  { value: 'change', label: '按涨跌' }
]
const keyWatchSortFieldOptions = [
  { value: 'code', label: '代码' },
  { value: 'name', label: '名称' },
  { value: 'latest_price', label: '最新价' },
  { value: 'change_pct', label: '涨跌幅' },
  { value: 'amount', label: '成交额' },
  { value: 'volume', label: '成交量' }
]
const watchlistColumnOptions = WATCHLIST_COLUMN_OPTIONS
const watchLinkGroupOptions = Array.from({ length: 9 }, (_, idx) => String(idx + 1))
const chatSidebarCollapsed = ref(false)
const strategyListCollapsed = ref(false)

const inputText = ref('')
const sending = ref(false)
const uploading = ref(false)
const uploadJobs = ref([])
const jobPollingTimer = ref(null)
const strategyEditRun = ref({
  active: false,
  question: '',
  conversationId: '',
  assistantMessageId: '',
  startedAt: '',
  bucket: '',
  steps: [],
  seenKeys: {}
})
const strategyEditRunPollTimer = ref(null)

const usableResources = computed(() => resources.value.filter(item => item.status === 'ok'))
const buildStrategyReferenceItem = (strategy) => {
  if (!strategy?.id) return null
  return {
    id: `strategy_ctx:${strategy.id}`,
    title: `[策略] ${strategy.name || strategy.id}`
  }
}
const hoverReferenceItems = computed(() => {
  const key = hoverGroupId.value || ''
  const mapResource = (item) => ({
    id: item.id,
    title: item.original_name || item.id
  })
  if (!key) return usableResources.value.map(mapResource)
  if (key.startsWith('group:')) {
    const gid = key.slice('group:'.length)
    return usableResources.value.filter(item => item.group_id === gid).map(mapResource)
  }
  if (key === 'strategy:__all__') {
    const strategyItems = strategies.value.map(buildStrategyReferenceItem).filter(Boolean)
    return strategyItems
  }
  if (key.startsWith('strategy:')) {
    const sid = key.slice('strategy:'.length)
    const strategy = strategies.value.find(item => item.id === sid)
    const strategyItem = buildStrategyReferenceItem(strategy)
    const strategyResources = usableResources.value
      .filter(item => (item.strategy_id || '') === sid)
      .map(mapResource)
    return strategyItem ? [strategyItem, ...strategyResources] : strategyResources
  }
  return usableResources.value.map(mapResource)
})
const filteredResources = computed(() => {
  if (!selectedGroupId.value) return resources.value
  return resources.value.filter(item => item.group_id === selectedGroupId.value)
})
const manageFilteredResources = computed(() => {
  const gid = String(manageFilterGroupId.value || '').trim()
  const kw = String(manageKeyword.value || '').trim().toLowerCase()
  return resources.value.filter(item => {
    if (gid && item.group_id !== gid) return false
    if (kw) {
      const name = String(item.original_name || item.id || '').toLowerCase()
      if (!name.includes(kw)) return false
    }
    return true
  })
})
const memoryBindResources = computed(() => {
  const base = resources.value.filter(item => item.status === 'ok')
  if (!memoryBindFilterGroupId.value) return base
  return base.filter(item => item.group_id === memoryBindFilterGroupId.value)
})
const currentMemoryProfile = computed(() => {
  return memoryProfiles.value.find(item => item.id === memoryManageProfileId.value) || null
})
const activeConversationTitle = computed(() => {
  const found = conversations.value.find(item => item.id === activeConversationId.value)
  return found ? found.title : ''
})
const activeStrategy = computed(() => {
  return strategies.value.find(item => item.id === activeStrategyId.value) || null
})
const watchlistPanelConfigWidget = computed(() => {
  const wid = String(watchlistPanelConfigWidgetId.value || '').trim()
  if (!wid) return null
  return watchWidgetDefs.value.find(item => item.id === wid) || null
})
const watchlistPanelConfigGroup = computed(() => {
  const gid = String(watchlistPanelConfigForm.value.group_id || '').trim()
  if (!gid) return null
  return watchlistGroups.value.find(item => item.id === gid) || null
})
const watchlistPanelConfigGroupName = computed(() => {
  return String(watchlistPanelConfigGroup.value?.name || '默认自选组')
})
const watchlistHeaderSearchResults = computed(() => {
  const keyword = String(watchlistHeaderSearch.value || '').trim().toLowerCase()
  const selected = new Set(Array.isArray(watchlistPanelConfigForm.value.columns) ? watchlistPanelConfigForm.value.columns : [])
  return watchlistColumnOptions.filter(col => {
    if (selected.has(col.value)) return false
    if (!keyword) return true
    return String(col.label || '').toLowerCase().includes(keyword) || String(col.value || '').toLowerCase().includes(keyword)
  })
})
const watchlistPanelLinkableWidgets = computed(() => {
  return watchWidgetDefs.value
    .filter(item => ['analysis_kline', 'stock_kline', 'sector_kline', 'index_kline'].includes(String(item?.type || '')))
    .map(item => ({
      id: item.id,
      label: `${item.title || item.type} (${item.id})`
    }))
})
const strategyTaggedCount = computed(() => {
  return (strategies.value || []).length
})
const watchBoardHeight = computed(() => {
  if (!watchWidgets.value.length) return 360
  const bottom = watchWidgets.value.reduce((max, item) => {
    const layout = item?.layout || {}
    const y = Number(layout.y || 0)
    const h = Number(layout.h || 360)
    return Math.max(max, y + h)
  }, 0)
  return Math.max(360, Math.ceil(bottom))
})
const watchBoardStyle = computed(() => {
  return { height: `${watchBoardHeight.value}px` }
})
const targetGroupOptions = computed(() => {
  const sourceId = groupTransferForm.value.source_group_id
  return resourceGroups.value.filter(item => item.group_id !== sourceId)
})
const manageCurrentGroup = computed(() => {
  const gid = String(manageFilterGroupId.value || '').trim()
  if (!gid) return null
  return resourceGroups.value.find(item => item.group_id === gid) || null
})
const manageTransferGroupOptions = computed(() => {
  const sourceId = String(manageFilterGroupId.value || '').trim()
  return resourceGroups.value.filter(item => item.group_id !== sourceId)
})
const selectedPromptTemplate = computed(() => {
  const found = promptTemplates.value.find(item => item.id === selectedPromptTemplateId.value)
  return found || promptTemplates.value[0] || { id: 'none', name: '不使用模板', content: '' }
})
const promptTemplateNameInput = computed({
  get: () => selectedPromptTemplate.value?.name || '',
  set: (value) => {
    if (selectedPromptTemplate.value.id === 'none') return
    selectedPromptTemplate.value.name = (String(value || '').trim() || '未命名模板').slice(0, 30)
  }
})
const promptTemplateContentInput = computed({
  get: () => selectedPromptTemplate.value?.content || '',
  set: (value) => {
    if (selectedPromptTemplate.value.id === 'none') return
    selectedPromptTemplate.value.content = String(value || '')
  }
})
const effectivePromptTemplate = computed(() => {
  if (selectedPromptTemplateId.value === 'none') return ''
  return (selectedPromptTemplate.value?.content || '').trim()
})
const strategyEditRunCurrentTitle = computed(() => {
  const steps = Array.isArray(strategyEditRun.value.steps) ? strategyEditRun.value.steps : []
  if (!steps.length) return '正在调用工具...'
  return String(steps[steps.length - 1] || '正在调用工具...')
})
const strategyEditRunRecentSteps = computed(() => {
  const steps = Array.isArray(strategyEditRun.value.steps) ? strategyEditRun.value.steps : []
  if (steps.length <= 4) return steps
  return steps.slice(steps.length - 4)
})

const normalizeStockCode = (value) => String(value || '').replace(/\D/g, '').padStart(6, '0').slice(-6)
const normalizeStockWidgetCode = (value) => {
  const digits = String(value || '').replace(/\D/g, '').slice(-6)
  if (!digits) return '000001'
  return digits.padStart(6, '0')
}
const INDEX_CODE_NAME_MAP = Object.freeze({
  '000001': '上证指数',
  '000016': '上证50',
  '000300': '沪深300',
  '000688': '科创50',
  '000852': '中证1000',
  '000905': '中证500',
  '399001': '深证成指',
  '399006': '创业板指',
  '800007': '微盘股',
  '899050': '北证50',
  '932000': '中证2000'
})
const INDEX_NAME_CODE_MAP = Object.freeze(
  Object.fromEntries(
    Object.entries(INDEX_CODE_NAME_MAP).map(([code, name]) => [name, code])
  )
)
const normalizeIndexCodeLike = (value) => {
  const text = String(value || '').trim().toUpperCase()
  if (!text) return ''
  if (/^(SH|SZ)\d{6}$/.test(text)) return text.slice(-6)
  if (/^1B\d{4,6}$/.test(text)) return text.slice(2).padStart(6, '0').slice(-6)
  if (/^\d{6}$/.test(text)) return text
  return ''
}
const normalizeAnalysisTargetType = (value) => {
  const kind = String(value || '').trim().toLowerCase()
  return ['auto', 'stock', 'index', 'sector'].includes(kind) ? kind : 'auto'
}
const isLikelyIndexName = (name) => {
  const text = String(name || '').trim()
  if (!text) return false
  if (watchlistIndexOptions.value.includes(text)) return true
  return /指数|上证|深证|沪深|中证|创业板|科创|恒生|纳指|道琼斯|标普|NYSE|NASDAQ/i.test(text)
}
const detectAnalysisTargetType = (value, preferred = 'auto') => {
  const targetValue = String(value || '').trim()
  const explicit = normalizeAnalysisTargetType(preferred)
  if (explicit !== 'auto') return explicit
  if (/^BK\d{4}$/i.test(targetValue) || /^88\d{4}$/.test(targetValue)) return 'sector'
  const indexCodeLike = normalizeIndexCodeLike(targetValue)
  if (indexCodeLike && INDEX_CODE_NAME_MAP[indexCodeLike]) return 'index'
  const digits = targetValue.replace(/\D/g, '')
  if (digits.length === 6) return 'stock'
  if (isLikelyIndexName(targetValue)) return 'index'
  return 'sector'
}
const normalizeAnalysisKlineParams = (source = {}) => {
  const days = clampNumber(Number(source.days || source.days_range || 120) || 120, 20, 500)
  const targetType = normalizeAnalysisTargetType(source.target_type || source.type_hint || 'auto')
  let targetName = String(source.target_name || source.name || '').trim()
  let targetValue = String(
    source.target_value
      || source.stock_code
      || source.index_name
      || source.sector_name
      || source.code
      || source.name
      || '000001'
  ).trim()
  const resolvedType = detectAnalysisTargetType(targetValue, targetType)
  if (resolvedType === 'stock') {
    targetValue = normalizeStockWidgetCode(targetValue)
  } else if (resolvedType === 'index') {
    const byName = INDEX_NAME_CODE_MAP[targetValue]
    if (byName) {
      if (!targetName) targetName = targetValue
      targetValue = byName
    } else {
      const byCodeLike = normalizeIndexCodeLike(targetValue)
      if (byCodeLike) {
        targetValue = byCodeLike
        if (!targetName) targetName = INDEX_CODE_NAME_MAP[byCodeLike] || ''
      }
    }
  } else if (!targetValue) {
    targetValue = resolvedType === 'index' ? '上证指数' : '半导体'
  }
  if (!targetName && resolvedType === 'index') {
    const codeLike = normalizeIndexCodeLike(targetValue)
    targetName = codeLike ? (INDEX_CODE_NAME_MAP[codeLike] || '') : ''
  }
  return {
    target_type: targetType,
    target_value: targetValue,
    target_name: targetName,
    days,
    link_group: watchLinkGroupOptions.includes(String(source.link_group || '').trim())
      ? String(source.link_group || '').trim()
      : ''
  }
}
const getAnalysisTarget = (params = {}) => {
  const normalized = normalizeAnalysisKlineParams(params)
  const resolvedType = detectAnalysisTargetType(normalized.target_value, normalized.target_type)
  const targetValue = resolvedType === 'stock'
    ? normalizeStockWidgetCode(normalized.target_value)
    : String(normalized.target_value || '').trim()
  return {
    type: resolvedType,
    value: targetValue,
    days: normalized.days
  }
}
const clampNumber = (value, min, max) => Math.max(min, Math.min(max, value))
const WATCHLIST_GROUP_ITEM_LIMIT = 10000
const genWatchlistGroupId = () => `wg_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`

const normalizeWatchlistGroups = (rawGroups = []) => {
  const groups = []
  const source = Array.isArray(rawGroups) ? rawGroups : []
  const used = new Set()
  source.forEach((item, idx) => {
    const raw = item && typeof item === 'object' ? item : {}
    let id = String(raw.id || '').trim()
    if (!id) id = `wg_auto_${idx + 1}`
    while (used.has(id)) id = `${id}_${used.size + 1}`
    used.add(id)
    const name = String(raw.name || '').trim() || `自选组${idx + 1}`
    const normalizedItems = []
    const rawItems = Array.isArray(raw.items) ? raw.items : []
    const itemUsed = new Set()
    rawItems.forEach(entry => {
      const one = entry && typeof entry === 'object' ? entry : {}
      const type = ['stock', 'sector', 'index'].includes(one.type) ? one.type : 'stock'
      const value = type === 'stock'
        ? normalizeStockCode(one.code || one.value || '')
        : String(one.name || one.code || one.value || '').trim()
      if (!value) return
      const dedupe = `${type}:${value}`
      if (itemUsed.has(dedupe)) return
      itemUsed.add(dedupe)
      normalizedItems.push(type === 'stock'
        ? { type, code: value, name: String(one.name || value).trim() || value }
        : { type, name: value }
      )
    })
    groups.push({
      id,
      name,
      items: normalizedItems.slice(0, WATCHLIST_GROUP_ITEM_LIMIT)
    })
  })
  if (!groups.length) {
    groups.push({ id: genWatchlistGroupId(), name: '默认自选组', items: [] })
  }
  return groups
}

const getStrategyWatchlistGroupsConfig = (strategy) => {
  const cfg = strategy?.config || {}
  return Array.isArray(cfg.watchlist_groups) ? cfg.watchlist_groups : []
}

const activeWatchlistGroup = computed(() => {
  return watchlistGroups.value.find(item => item.id === activeWatchlistGroupId.value) || watchlistGroups.value[0] || null
})

const ensureActiveWatchlistGroup = () => {
  const currentId = String(activeWatchlistGroupId.value || '')
  if (currentId && watchlistGroups.value.some(item => item.id === currentId)) return
  activeWatchlistGroupId.value = watchlistGroups.value[0]?.id || ''
}

const hydrateWatchlistGroupsFromStrategy = (strategy) => {
  const raw = getStrategyWatchlistGroupsConfig(strategy)
  watchlistGroups.value = normalizeWatchlistGroups(raw)
  ensureActiveWatchlistGroup()
  const indexSet = new Set(watchlistIndexOptions.value)
  watchlistGroups.value.forEach(group => {
    group.items.forEach(item => {
      if (item.type === 'index' && item.name) indexSet.add(item.name)
    })
  })
  watchlistIndexOptions.value = Array.from(indexSet)
}

const serializeWatchlistGroups = () => {
  return watchlistGroups.value.map(group => ({
    id: group.id,
    name: group.name,
    items: (Array.isArray(group.items) ? group.items : []).map(item => {
      if (item.type === 'stock') {
        return {
          type: 'stock',
          code: normalizeStockCode(item.code),
          name: String(item.name || normalizeStockCode(item.code)).trim() || normalizeStockCode(item.code)
        }
      }
      return {
        type: item.type === 'index' ? 'index' : 'sector',
        name: String(item.name || '').trim()
      }
    }).filter(item => (item.type === 'stock' ? !!item.code : !!item.name))
  }))
}
const WATCH_WIDGET_GAP = 0
const WATCH_WIDGET_MIN_WIDTH = 300
const WATCH_WIDGET_MIN_HEIGHT = 240
const WATCH_WIDGET_MAX_WIDTH = 1500
const WATCH_LAYOUT_SAVE_DEBOUNCE_MS = 700
const WATCH_WIDGET_SNAP_THRESHOLD = 8
const alignLayoutPixel = (value) => {
  const raw = Number(value || 0)
  if (!Number.isFinite(raw)) return 0
  if (typeof window === 'undefined') return Math.round(raw)
  const dpr = Number(window.devicePixelRatio || 1)
  if (!Number.isFinite(dpr) || dpr <= 0) return Math.round(raw)
  return Math.round(raw * dpr) / dpr
}
const snapCoordinate = (value, targets, threshold = WATCH_WIDGET_SNAP_THRESHOLD) => {
  const current = Number(value || 0)
  if (!Number.isFinite(current)) return 0
  const source = Array.isArray(targets) ? targets : []
  let best = current
  let bestDelta = Number.POSITIVE_INFINITY
  source.forEach((one) => {
    const candidate = Number(one)
    if (!Number.isFinite(candidate)) return
    const delta = Math.abs(candidate - current)
    if (delta <= threshold && delta < bestDelta) {
      bestDelta = delta
      best = candidate
    }
  })
  return best
}
const getSnapLayouts = (excludeWidgetId) => {
  const wid = String(excludeWidgetId || '').trim()
  return watchWidgetDefs.value
    .filter(item => String(item?.id || '').trim() && String(item?.id || '').trim() !== wid)
    .map(item => normalizeWidgetLayout(item?.layout || {}, 0, item?.type || 'market_sentiment_chart'))
}
const watchPointerState = {
  mode: '',
  widgetId: '',
  startX: 0,
  startY: 0,
  startLayout: { x: 0, y: 0, w: 0, h: 0 }
}

const getSentimentChartTitle = (chartKey) => {
  const found = marketSentimentChartOptions.find(item => item.key === chartKey)
  return found?.title || '市场情绪图'
}

const getAnalysisWidgetDisplayLabel = (params = {}) => {
  const target = getAnalysisTarget(params)
  const normalized = normalizeAnalysisKlineParams(params)
  const targetName = String(normalized.target_name || '').trim()
  if (target.type === 'stock') {
    const code = normalizeStockWidgetCode(target.value)
    if (targetName && targetName !== code) return `${code} ${targetName}`
    return code || '分析标的'
  }
  if (target.type === 'index') {
    const codeLike = normalizeIndexCodeLike(target.value)
    const inferredName = targetName || (codeLike ? (INDEX_CODE_NAME_MAP[codeLike] || '') : '')
    if (codeLike && inferredName && inferredName !== codeLike) return `${codeLike} ${inferredName}`
    return inferredName || target.value || '指数'
  }
  return targetName || target.value || '板块'
}

const getAnalysisWidgetTitle = (params = {}) => `${getAnalysisWidgetDisplayLabel(params)} K线`

const buildWidgetTitle = (type, params = {}) => {
  if (type === 'analysis_kline') {
    return getAnalysisWidgetTitle(params)
  }
  if (type === 'index_kline') return `${params.index_name || '上证指数'} K线`
  if (type === 'sector_kline') return `${params.sector_name || '板块'} K线`
  if (type === 'stock_kline') return `${params.stock_code || '000001'} K线`
  if (type === 'watchlist_panel') return '自选股列表'
  if (type === 'daily_replay') return '每日复盘'
  if (type === 'market_volume') return '市场量能对比'
  if (type === 'market_sentiment_chart') return getSentimentChartTitle(params.chart_key)
  return '自定义图表'
}

const genWidgetId = () => `widget_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`

const getDefaultWidgetSize = (type) => {
  if (type === 'analysis_kline') return { w: 620, h: 520 }
  if (type === 'index_kline' || type === 'sector_kline' || type === 'stock_kline') return { w: 620, h: 520 }
  if (type === 'watchlist_panel') return { w: 460, h: 520 }
  if (type === 'daily_replay') return { w: 520, h: 320 }
  if (type === 'market_volume') return { w: 620, h: 420 }
  return { w: 500, h: 420 }
}

const getDefaultWidgetLayout = (index, type) => {
  const size = getDefaultWidgetSize(type)
  const column = index % 2
  const row = Math.floor(index / 2)
  return {
    x: column * (size.w + WATCH_WIDGET_GAP),
    y: row * (size.h + WATCH_WIDGET_GAP),
    w: size.w,
    h: size.h
  }
}

const normalizeWidgetParams = (type, rawParams = {}) => {
  const source = rawParams && typeof rawParams === 'object' ? rawParams : {}
  if (type === 'analysis_kline') {
    return normalizeAnalysisKlineParams(source)
  }
  if (type === 'index_kline') {
    return {
      index_name: String(source.index_name || source.index || '上证指数').trim() || '上证指数',
      days_range: clampNumber(Number(source.days_range || source.days || 60) || 60, 20, 500),
      link_group: watchLinkGroupOptions.includes(String(source.link_group || '').trim())
        ? String(source.link_group || '').trim()
        : ''
    }
  }
  if (type === 'sector_kline') {
    return {
      sector_name: String(source.sector_name || source.sector || source.name || '半导体').trim() || '半导体',
      days_range: clampNumber(Number(source.days_range || source.days || 60) || 60, 20, 500),
      link_group: watchLinkGroupOptions.includes(String(source.link_group || '').trim())
        ? String(source.link_group || '').trim()
        : ''
    }
  }
  if (type === 'stock_kline') {
    return {
      stock_code: normalizeStockWidgetCode(source.stock_code || source.code || '000001'),
      days: clampNumber(Number(source.days || source.days_range || 120) || 120, 20, 500),
      link_group: watchLinkGroupOptions.includes(String(source.link_group || '').trim())
        ? String(source.link_group || '').trim()
        : ''
    }
  }
  if (type === 'watchlist_panel') {
    const validCols = filterWatchlistColumns(source.columns)
    return {
      group_id: String(source.group_id || '').trim(),
      max_count: clampNumber(Number(source.max_count || 50) || 50, 1, 10000),
      columns: validCols,
      linked_widget_id: String(source.linked_widget_id || '').trim(),
      link_group: watchLinkGroupOptions.includes(String(source.link_group || '').trim())
        ? String(source.link_group || '').trim()
        : ''
    }
  }
  if (type === 'daily_replay') {
    return {
      requirement: String(source.requirement || source.replay_requirement || '').trim()
        || '请按市场主线、强弱切换、风险点、明日计划四部分完成今日复盘。'
    }
  }
  if (type === 'market_sentiment_chart') {
    const chartKey = String(source.chart_key || source.key || 'red_ratio_and_amount').trim() || 'red_ratio_and_amount'
    const validChartKey = marketSentimentChartOptions.some(item => item.key === chartKey)
      ? chartKey
      : 'red_ratio_and_amount'
    return {
      chart_key: validChartKey,
      days_back: clampNumber(Number(source.days_back || source.days || 30) || 30, 10, 240)
    }
  }
  if (type === 'market_volume') {
    return {}
  }
  return { ...source }
}

const normalizeWidgetLayout = (rawLayout, index, type) => {
  const defaults = getDefaultWidgetLayout(index, type)
  const source = rawLayout && typeof rawLayout === 'object' ? rawLayout : {}
  const width = alignLayoutPixel(clampNumber(Number(source.w || defaults.w) || defaults.w, WATCH_WIDGET_MIN_WIDTH, WATCH_WIDGET_MAX_WIDTH))
  const height = alignLayoutPixel(clampNumber(Number(source.h || defaults.h) || defaults.h, WATCH_WIDGET_MIN_HEIGHT, 1800))
  return {
    x: alignLayoutPixel(Math.max(0, Number(source.x || defaults.x) || defaults.x)),
    y: alignLayoutPixel(Math.max(0, Number(source.y || defaults.y) || defaults.y)),
    w: width,
    h: height
  }
}

const expandLegacyBundleWidget = (widget) => {
  const source = widget && typeof widget === 'object' ? widget : {}
  if (source.type !== 'market_sentiment_bundle') return [source]
  const baseId = String(source.id || genWidgetId()).trim() || genWidgetId()
  const daysBack = clampNumber(Number(source?.params?.days_back || 30) || 30, 10, 240)
  return marketSentimentChartOptions.map((item, idx) => ({
    id: `${baseId}-${item.key}`,
    type: 'market_sentiment_chart',
    title: item.title,
    params: {
      chart_key: item.key,
      days_back: daysBack
    },
    layout: idx === 0 ? source.layout : null
  }))
}

const normalizeWidgetDefinitions = (rawWidgets, viewType = 'basic', useFallback = true) => {
  let source = []
  if (Array.isArray(rawWidgets)) {
    source = rawWidgets
  } else if (useFallback) {
    source = buildDefaultWidgets(viewType)
  }
  if (!source.length) return []
  const expanded = []
  source.forEach(item => {
    expandLegacyBundleWidget(item).forEach(one => expanded.push(one))
  })

  const usedIds = new Set()
  return expanded.map((item, index) => {
    const raw = item && typeof item === 'object' ? item : {}
    const type = String(raw.type || '').trim() || 'market_sentiment_chart'
    const params = normalizeWidgetParams(type, raw.params || {})
    let id = String(raw.id || '').trim()
    if (!id) id = genWidgetId()
    while (usedIds.has(id)) {
      id = `${id}_${usedIds.size + 1}`
    }
    usedIds.add(id)
    const title = String(raw.title || '').trim() || buildWidgetTitle(type, params)
    return {
      id,
      type,
      title,
      params,
      layout: normalizeWidgetLayout(raw.layout, index, type)
    }
  })
}

const serializeWidgetDef = (widget) => {
  const raw = widget && typeof widget === 'object' ? widget : {}
  const layout = normalizeWidgetLayout(raw.layout, 0, raw.type || 'market_sentiment_chart')
  return {
    id: String(raw.id || '').trim() || genWidgetId(),
    type: String(raw.type || '').trim() || 'market_sentiment_chart',
    title: String(raw.title || '').trim() || buildWidgetTitle(raw.type, raw.params || {}),
    params: normalizeWidgetParams(raw.type, raw.params || {}),
    layout: {
      x: Math.round(layout.x),
      y: Math.round(layout.y),
      w: Math.round(layout.w),
      h: Math.round(layout.h)
    }
  }
}

const getNextWidgetLayout = (type) => {
  const size = getDefaultWidgetSize(type)
  const maxBottom = watchWidgetDefs.value.reduce((max, item) => {
    const layout = item?.layout || {}
    return Math.max(max, Number(layout.y || 0) + Number(layout.h || 0))
  }, 0)
  return {
    x: 0,
    y: maxBottom ? maxBottom + WATCH_WIDGET_GAP : 0,
    w: size.w,
    h: size.h
  }
}

const getWidgetToolTemplateById = (templateId) => {
  const id = String(templateId || '').trim()
  if (!id) return null
  for (const category of widgetToolCategories) {
    const templates = Array.isArray(category?.templates) ? category.templates : []
    const found = templates.find(item => item?.id === id)
    if (found) return found
  }
  return null
}

const persistWatchlistGroups = async ({ silent = true } = {}) => {
  if (!activeStrategy.value?.id || isKeyLevelStrategyConfig(activeStrategy.value)) return
  await saveWatchWidgetConfig({ silent })
}

const setActiveWatchlistGroup = (groupId) => {
  const id = String(groupId || '').trim()
  if (!id) return
  if (!watchlistGroups.value.some(item => item.id === id)) return
  activeWatchlistGroupId.value = id
}

const createWatchlistGroup = async () => {
  const nextIndex = (watchlistGroups.value?.length || 0) + 1
  const defaultName = `自选组${nextIndex}`
  try {
    const { value } = await ElMessageBox.prompt('请输入组名', '新建自选组', {
      inputValue: defaultName,
      confirmButtonText: '创建',
      cancelButtonText: '取消'
    })
    const name = String(value || '').trim() || defaultName
    const group = { id: genWatchlistGroupId(), name, items: [] }
    watchlistGroups.value = [...watchlistGroups.value, group]
    activeWatchlistGroupId.value = group.id
    await persistWatchlistGroups()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const renameWatchlistGroup = async (group) => {
  const gid = String(group?.id || '').trim()
  if (!gid) return
  const current = watchlistGroups.value.find(item => item.id === gid)
  if (!current) return
  try {
    const { value } = await ElMessageBox.prompt('输入新的组名', '重命名自选组', {
      inputValue: current.name || '',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const name = String(value || '').trim()
    if (!name) return
    watchlistGroups.value = watchlistGroups.value.map(item => (item.id === gid ? { ...item, name } : item))
    await persistWatchlistGroups()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const addWatchlistItemToGroup = async (groupId, item) => {
  const gid = String(groupId || '').trim()
  const group = watchlistGroups.value.find(one => one.id === gid) || null
  if (!group) {
    ElMessage.warning('请先新建自选组')
    return
  }
  const raw = item && typeof item === 'object' ? item : {}
  const type = ['stock', 'sector', 'index'].includes(raw.type) ? raw.type : 'stock'
  const nextItem = type === 'stock'
    ? { type: 'stock', code: normalizeStockCode(raw.code || raw.value || ''), name: String(raw.name || '').trim() }
    : { type, name: String(raw.name || raw.value || '').trim() }
  if (type === 'stock' && !nextItem.code) return
  if (type !== 'stock' && !nextItem.name) return

  const existing = Array.isArray(group.items) ? group.items : []
  const exists = existing.some(one => (
    one?.type === type && (
      type === 'stock'
        ? normalizeStockCode(one.code) === nextItem.code
        : String(one.name || '').trim() === nextItem.name
    )
  ))
  if (exists) {
    ElMessage.warning('该标的已在当前组')
    return
  }
  if (existing.length >= WATCHLIST_GROUP_ITEM_LIMIT) {
    ElMessage.warning(`单组上限 ${WATCHLIST_GROUP_ITEM_LIMIT} 条`)
    return
  }
  const safeItem = type === 'stock'
    ? { type, code: nextItem.code, name: nextItem.name || nextItem.code }
    : { type, name: nextItem.name }
  watchlistGroups.value = watchlistGroups.value.map(one => (
    one.id === gid
      ? { ...one, items: [...existing, safeItem] }
      : one
  ))
  await persistWatchlistGroups()
}

const addWatchlistItemToActiveGroup = async (item) => {
  await addWatchlistItemToGroup(activeWatchlistGroup.value?.id || '', item)
}

const queryWatchlistStockSuggestions = async (queryString, cb) => {
  const term = (queryString || '').trim()
  if (!term) {
    cb([])
    return
  }
  try {
    const res = await ApiService.searchStocks(term)
    const rows = Array.isArray(res?.data) ? res.data : []
    cb(rows.slice(0, 20).map(item => ({
      value: `${item.代码} ${item.名称}`,
      type: 'stock',
      code: normalizeStockCode(item.代码),
      name: String(item.名称 || '').trim() || normalizeStockCode(item.代码)
    })))
  } catch (error) {
    console.error(error)
    cb([])
  }
}

const onWatchlistStockSuggestionSelect = async (item) => {
  watchlistStockSearchQuery.value = ''
  await addWatchlistItemToActiveGroup({
    type: 'stock',
    code: item?.code,
    name: item?.name
  })
}

const addSectorToActiveWatchlistGroup = async () => {
  const name = String(watchlistSectorInput.value || '').trim()
  if (!name) {
    ElMessage.warning('请输入板块名称')
    return
  }
  watchlistSectorInput.value = ''
  await addWatchlistItemToActiveGroup({ type: 'sector', name })
}

const addIndexToActiveWatchlistGroup = async () => {
  const name = String(watchlistSelectedIndexName.value || '').trim()
  if (!name) {
    ElMessage.warning('请选择指数')
    return
  }
  if (!watchlistIndexOptions.value.includes(name)) {
    watchlistIndexOptions.value = [...watchlistIndexOptions.value, name]
  }
  await addWatchlistItemToActiveGroup({ type: 'index', name })
}

const loadWatchlistIndexOptions = async () => {
  try {
    const res = await ApiService.getIndicesAvailable()
    const rows = Array.isArray(res?.data) ? res.data : []
    const names = rows
      .map(item => String(item?.name || item?.index_name || item || '').trim())
      .filter(Boolean)
    if (!names.length) return
    const merged = new Set(watchlistIndexOptions.value)
    names.forEach(name => merged.add(name))
    watchlistIndexOptions.value = Array.from(merged)
  } catch (error) {
    console.error(error)
  }
}

const getWatchlistColumnLabel = (col) => {
  return getWatchlistColumnLabelUtil(col)
}

const getWatchlistLinkGroupLabel = (widget) => {
  const group = String(widget?.params?.link_group || '').trim()
  return watchLinkGroupOptions.includes(group) ? group : '组'
}

const getWatchlistGridStyle = (columns) => {
  return {
    gridTemplateColumns: `48px ${buildWatchlistGridTemplate(columns)}`
  }
}

const getWatchlistCellClass = (col, item) => {
  return getWatchlistCellClassUtil(col, item, watchChangeClass)
}

const formatWatchlistCell = (item, col) => {
  return formatWatchlistCellUtil(item, col, {
    formatPrice: formatWatchPrice,
    formatChange: formatWatchChange,
    formatAmount: formatWatchAmount
  })
}

const openWatchlistPanelConfig = (widget) => {
  const source = widget && typeof widget === 'object' ? widget : {}
  const params = normalizeWidgetParams('watchlist_panel', source.params || {})
  const fallbackGroupId = watchlistGroups.value[0]?.id || ''
  watchlistPanelConfigWidgetId.value = String(source.id || '').trim()
  watchlistPanelConfigForm.value = {
    group_id: params.group_id || fallbackGroupId,
    max_count: params.max_count || 50,
    columns: filterWatchlistColumns(params.columns),
    linked_widget_id: String(params.linked_widget_id || '').trim(),
    stock_query: '',
    sector_input: '',
    index_name: watchlistIndexOptions.value[0] || ''
  }
  watchlistConfigActiveMenu.value = 'add_stock'
  watchlistConfigStockQuery.value = ''
  watchlistConfigStockCandidates.value = []
  watchlistHeaderSearch.value = ''
  watchlistColumnSchemeId.value = 'classic'
  watchlistPanelConfigVisible.value = true
  loadWatchlistConfigLookups()
}

const patchWidgetParamsById = async (widgetId, patch = {}) => {
  const target = String(widgetId || '').trim()
  if (!target) return false
  const idx = watchWidgetDefs.value.findIndex(item => item.id === target)
  if (idx < 0) return false
  const next = [...watchWidgetDefs.value]
  const current = next[idx]
  const type = String(current?.type || '').trim()
  if (!type) return false
  next[idx] = {
    ...current,
    params: normalizeWidgetParams(type, {
      ...(current.params || {}),
      ...(patch || {})
    })
  }
  watchWidgetDefs.value = next
  const saved = await saveWatchWidgetConfig({ silent: true })
  if (!saved) return false
  const rendered = await renderWatchWidget(next[idx])
  watchWidgets.value = patchWidgetById(watchWidgets.value, target, () => rendered)
  return true
}

const patchWatchlistWidgetParamsById = async (widgetId, patch = {}) => {
  return patchWidgetParamsById(widgetId, patch)
}

const openDailyReplayRequirementDialog = (widget) => {
  const wid = String(widget?.id || '').trim()
  if (!wid) return
  const def = watchWidgetDefs.value.find(item => item.id === wid)
  const params = normalizeWidgetParams('daily_replay', def?.params || widget?.params || {})
  dailyReplayConfigWidgetId.value = wid
  dailyReplayRequirementDraft.value = String(params.requirement || '').trim()
  dailyReplayConfigVisible.value = true
}

const saveDailyReplayRequirement = async () => {
  const wid = String(dailyReplayConfigWidgetId.value || '').trim()
  if (!wid) {
    dailyReplayConfigVisible.value = false
    return
  }
  const requirement = String(dailyReplayRequirementDraft.value || '').trim()
  if (!requirement) {
    ElMessage.warning('请先输入复盘要求')
    return
  }
  const saved = await patchWidgetParamsById(wid, { requirement })
  if (saved) {
    dailyReplayConfigVisible.value = false
    ElMessage.success('复盘要求已保存')
  }
}

const buildDailyReplayPrompt = (widget) => {
  const params = normalizeWidgetParams('daily_replay', widget?.params || {})
  const requirement = String(params.requirement || '').trim()
  const strategyName = String(activeStrategy.value?.name || activeStrategy.value?.strategy_name || '').trim() || '当前策略'
  const dateStr = dayjs().format('YYYY-MM-DD')
  return [
    `请按以下要求完成 ${dateStr} 的策略复盘。`,
    `策略：${strategyName}`,
    '',
    '【复盘要求】',
    requirement,
    '',
    '请输出结构化结论：',
    '1. 市场环境与主线',
    '2. 自选/关注标的强弱与分化',
    '3. 主要风险点与触发条件',
    '4. 明日计划（观察、应对、执行）'
  ].join('\n')
}

const runDailyReplayWidget = async (widget) => {
  const wid = String(widget?.id || '').trim()
  if (!wid) return
  const params = normalizeWidgetParams('daily_replay', widget?.params || {})
  if (!String(params.requirement || '').trim()) {
    openDailyReplayRequirementDialog(widget)
    return
  }
  if (sending.value || dailyReplayRunningWidgetId.value) {
    ElMessage.warning('当前有任务正在执行，请稍后再试')
    return
  }
  dailyReplayRunningWidgetId.value = wid
  try {
    await runStrategyEditPrompt(buildDailyReplayPrompt(widget))
  } finally {
    if (dailyReplayRunningWidgetId.value === wid) {
      dailyReplayRunningWidgetId.value = ''
    }
  }
}

const saveWatchlistPanelConfig = async () => {
  const widget = watchlistPanelConfigWidget.value
  if (!widget?.id) return
  const columns = filterWatchlistColumns(watchlistPanelConfigForm.value.columns)
  const nextGroupId = String(watchlistPanelConfigForm.value.group_id || '').trim() || watchlistGroups.value[0]?.id || ''
  await patchWatchlistWidgetParamsById(widget.id, {
    group_id: nextGroupId,
    max_count: clampNumber(Number(watchlistPanelConfigForm.value.max_count || 50) || 50, 1, 10000),
    columns,
    linked_widget_id: String(watchlistPanelConfigForm.value.linked_widget_id || '').trim()
  })
}

const loadWatchlistConfigLookups = async () => {
  if (watchlistConfigLookupLoaded.value) return
  try {
    const [indexRes, sectorRes] = await Promise.all([
      ApiService.getIndicesAvailable().catch(() => null),
      ApiService.getSectorNames('both').catch(() => null)
    ])
    const rawIndexRows = Array.isArray(indexRes?.data?.available_indices)
      ? indexRes.data.available_indices
      : (Array.isArray(indexRes?.data) ? indexRes.data : [])
    const indexRows = rawIndexRows.map(item => ({
      code: String(item?.code || item?.index_code || '').trim(),
      name: String(item?.name || item?.index_name || '').trim()
    })).filter(item => !!item.name)
    const mergedIndexMap = new Map()
    indexRows.forEach(item => {
      const key = `${item.code}|${item.name}`
      if (!mergedIndexMap.has(key)) mergedIndexMap.set(key, item)
    })
    watchlistIndexOptions.value.forEach(name => {
      const n = String(name || '').trim()
      if (!n) return
      const key = `|${n}`
      if (!mergedIndexMap.has(key)) mergedIndexMap.set(key, { code: '', name: n })
    })
    watchlistConfigIndexCandidates.value = Array.from(mergedIndexMap.values())

    const sectorData = sectorRes?.data || {}
    const sectorNames = [
      ...(Array.isArray(sectorData?.sector_names) ? sectorData.sector_names : []),
      ...(Array.isArray(sectorData?.concept_names) ? sectorData.concept_names : []),
      ...(Array.isArray(sectorData?.names) ? sectorData.names : [])
    ]
    const sectorSet = new Set(sectorNames.map(name => String(name || '').trim()).filter(Boolean))
    watchlistConfigSectorCandidates.value = Array.from(sectorSet)
    watchlistConfigLookupLoaded.value = true
  } catch (error) {
    watchlistConfigLookupLoaded.value = false
    console.error(error)
  }
}

const searchWatchlistConfigStocks = async () => {
  const keyword = String(watchlistConfigStockQuery.value || '').trim()
  if (!keyword) {
    watchlistConfigStockCandidates.value = []
    return
  }
  const reqId = watchlistConfigStockSearchReqId.value + 1
  watchlistConfigStockSearchReqId.value = reqId
  watchlistConfigStockSearching.value = true
  try {
    await loadWatchlistConfigLookups()
    const stockRes = await ApiService.searchStocks(keyword).catch(() => ({ data: [] }))
    if (reqId !== watchlistConfigStockSearchReqId.value) return
    const rows = Array.isArray(stockRes?.data) ? stockRes.data : []
    const gid = String(watchlistPanelConfigForm.value.group_id || '').trim()
    const group = watchlistGroups.value.find(one => one.id === gid) || null
    const items = Array.isArray(group?.items) ? group.items : []
    const existsStockSet = new Set(
      items
        .filter(one => one?.type === 'stock')
        .map(one => normalizeStockCode(one?.code || ''))
    )
    const existsIndexSet = new Set(
      items
        .filter(one => one?.type === 'index')
        .map(one => String(one?.name || '').trim())
        .filter(Boolean)
    )
    const existsSectorSet = new Set(
      items
        .filter(one => one?.type === 'sector')
        .map(one => String(one?.name || '').trim())
        .filter(Boolean)
    )
    const keywordLower = keyword.toLowerCase()
    const stockCandidates = rows.slice(0, 40).map(item => {
      const code = normalizeStockCode(item?.代码 || item?.code || '')
      const name = String(item?.名称 || item?.name || '').trim() || code
      return {
        key: `stock:${code}`,
        type: 'stock',
        code,
        display_code: code,
        name,
        in_group: existsStockSet.has(code)
      }
    }).filter(one => !!one.code)
    const indexCandidates = watchlistConfigIndexCandidates.value
      .filter(item => {
        const name = String(item?.name || '').trim()
        const code = String(item?.code || '').trim().toLowerCase()
        return name.toLowerCase().includes(keywordLower) || code.includes(keywordLower)
      })
      .slice(0, 15)
      .map(item => ({
        key: `index:${item.code || item.name}`,
        type: 'index',
        code: String(item?.code || '').trim(),
        display_code: String(item?.code || '').trim(),
        name: String(item?.name || '').trim(),
        in_group: existsIndexSet.has(String(item?.name || '').trim())
      }))
    const sectorCandidates = watchlistConfigSectorCandidates.value
      .filter(name => String(name || '').toLowerCase().includes(keywordLower))
      .slice(0, 15)
      .map(name => ({
        key: `sector:${name}`,
        type: 'sector',
        code: '',
        display_code: '--',
        name: String(name || '').trim(),
        in_group: existsSectorSet.has(String(name || '').trim())
      }))
    watchlistConfigStockCandidates.value = [...stockCandidates, ...indexCandidates, ...sectorCandidates].slice(0, 50)
  } catch (error) {
    console.error(error)
    watchlistConfigStockCandidates.value = []
  } finally {
    if (reqId === watchlistConfigStockSearchReqId.value) {
      watchlistConfigStockSearching.value = false
    }
  }
}

const addStockFromWatchlistConfigRow = async (row) => {
  const type = String(row?.type || 'stock').trim()
  const code = normalizeStockCode(row?.code || '')
  const name = String(row?.name || '').trim() || code
  const gid = String(watchlistPanelConfigForm.value.group_id || '').trim()
  if (!gid) return
  if (type === 'stock' && !code) return
  if (type === 'stock') {
    await addWatchlistItemToGroup(gid, { type: 'stock', code, name })
  } else if (type === 'index') {
    await addWatchlistItemToGroup(gid, { type: 'index', name })
  } else if (type === 'sector') {
    await addWatchlistItemToGroup(gid, { type: 'sector', name })
  } else {
    return
  }
  watchlistConfigStockCandidates.value = watchlistConfigStockCandidates.value.map(item => (
    item.key === row.key ? { ...item, in_group: true } : item
  ))
  const widget = watchlistPanelConfigWidget.value
  if (widget?.id) {
    const rendered = await renderWatchWidget(widget)
    watchWidgets.value = patchWidgetById(watchWidgets.value, widget.id, () => rendered)
  }
}

const applyWatchlistColumnScheme = (schemeId) => {
  const target = watchlistColumnSchemes.find(one => one.id === schemeId)
  if (!target) return
  watchlistColumnSchemeId.value = target.id
  watchlistPanelConfigForm.value.columns = filterWatchlistColumns(target.columns)
}

const addWatchlistColumn = (col) => {
  const value = String(col || '').trim()
  if (!value) return
  const current = Array.isArray(watchlistPanelConfigForm.value.columns) ? watchlistPanelConfigForm.value.columns : []
  if (current.includes(value)) return
  watchlistPanelConfigForm.value.columns = filterWatchlistColumns([...current, value])
}

const removeWatchlistColumn = (col) => {
  const value = String(col || '').trim()
  if (!value) return
  const current = Array.isArray(watchlistPanelConfigForm.value.columns) ? watchlistPanelConfigForm.value.columns : []
  watchlistPanelConfigForm.value.columns = filterWatchlistColumns(current.filter(item => item !== value))
}

const switchWatchlistPanelGroup = async (widget, groupId) => {
  const wid = String(widget?.id || '').trim()
  const gid = String(groupId || '').trim()
  if (!wid || !gid) return
  await patchWatchlistWidgetParamsById(wid, { group_id: gid })
}

const createWatchlistGroupForWidget = async (widget) => {
  await createWatchlistGroup()
  const wid = String(widget?.id || '').trim()
  const gid = String(activeWatchlistGroupId.value || '').trim()
  if (!wid || !gid) return
  await patchWatchlistWidgetParamsById(wid, { group_id: gid })
}

const setWatchlistPanelLinkGroup = async (widget, groupId) => {
  const wid = String(widget?.id || '').trim()
  if (!wid) return
  const linkGroup = String(groupId || '').trim()
  await patchWatchlistWidgetParamsById(wid, {
    link_group: watchLinkGroupOptions.includes(linkGroup) ? linkGroup : ''
  })
}

const setWidgetLinkGroup = async (widget, groupId) => {
  const wid = String(widget?.id || '').trim()
  const wtype = String(widget?.type || '').trim()
  if (!wid || !wtype) return
  const linkGroup = watchLinkGroupOptions.includes(String(groupId || '').trim()) ? String(groupId || '').trim() : ''
  if (wtype === 'watchlist_panel') {
    await setWatchlistPanelLinkGroup(widget, linkGroup)
    return
  }
  if (!isAnalysisWidgetType(wtype)) return
  const patched = watchWidgetDefs.value.map(one => (
    one.id === wid
      ? { ...one, params: normalizeWidgetParams(wtype, { ...(one.params || {}), link_group: linkGroup }) }
      : one
  ))
  watchWidgetDefs.value = patched
  const saved = await saveWatchWidgetConfig({ silent: true })
  if (!saved) return
  const latest = patched.find(one => one.id === wid)
  if (!latest) return
  const rendered = await renderWatchWidget(latest)
  watchWidgets.value = patchWidgetById(watchWidgets.value, wid, () => rendered)
}

const isAnalysisWidgetType = (type) => ['analysis_kline', 'stock_kline', 'sector_kline', 'index_kline'].includes(String(type || ''))
const canWidgetAcceptWatchlistSource = (widget, source) => {
  const wtype = String(widget?.type || '').trim()
  const stype = String(source?.type || '').trim()
  if (!wtype || !stype) return false
  if (wtype === 'analysis_kline') return ['stock', 'sector', 'index'].includes(stype)
  if (wtype === 'stock_kline') return stype === 'stock'
  if (wtype === 'sector_kline') return stype === 'sector'
  if (wtype === 'index_kline') return stype === 'index'
  return false
}

const findLinkedAnalysisWidgets = (widget) => {
  const candidates = watchWidgetDefs.value.filter(item => isAnalysisWidgetType(item.type))
  const wid = String(widget?.id || '').trim()
  const widgetDef = wid ? watchWidgetDefs.value.find(item => item.id === wid) : null
  const params = widgetDef?.params || widget?.params || {}
  const linkGroup = String(params.link_group || '').trim()
  // 严格按联动分组联动；不再使用 linked_widget_id，也不跨组兜底。
  if (watchLinkGroupOptions.includes(linkGroup)) {
    const byGroup = candidates.filter(item => String(item?.params?.link_group || '').trim() === linkGroup)
    return byGroup
  }
  if (candidates.length === 1) return [candidates[0]]
  return []
}

const onWatchlistPanelRowClick = async (widget, item) => {
  const source = item && typeof item === 'object' ? item : {}
  const widgetId = String(widget?.id || '').trim()
  const widgetDef = widgetId ? watchWidgetDefs.value.find(one => one.id === widgetId) : null
  const linkGroup = String(widgetDef?.params?.link_group || widget?.params?.link_group || '').trim()
  let targets = findLinkedAnalysisWidgets(widget)
  targets = targets.filter(target => canWidgetAcceptWatchlistSource(target, source))
  const hasExplicitLinkGroup = watchLinkGroupOptions.includes(linkGroup)
  if (!targets.length && !hasExplicitLinkGroup && source.type === 'stock' && source.code) {
    const fallback = watchWidgetDefs.value.find(item => item.type === 'analysis_kline')
      || watchWidgetDefs.value.find(item => item.type === 'stock_kline')
    if (fallback) targets = [fallback]
  }
  if (!targets.length && !hasExplicitLinkGroup) {
    const generic = watchWidgetDefs.value.find(item => item.type === 'analysis_kline')
    if (generic && canWidgetAcceptWatchlistSource(generic, source)) {
      targets = [generic]
    }
  }
  if (!targets.length) {
    console.log('[watchlist-link] row-click no-target', {
      source: {
        type: String(source.type || ''),
        code: String(source.code || ''),
        name: String(source.name || '')
      },
      watchlist_widget_id: widgetId,
      link_group: linkGroup
    })
    try {
      await ApiService.postStrategyWatchLinkDebug({
        status: 'no_target',
        watchlist_widget_id: widgetId,
        link_group: linkGroup,
        source: {
          type: String(source.type || ''),
          code: String(source.code || ''),
          name: String(source.name || '')
        },
        matched_targets: []
      })
    } catch (error) {
      console.error(error)
    }
    return
  }

  const patchLogs = []
  const patched = watchWidgetDefs.value.map((one) => {
    const target = targets.find(item => item.id === one.id)
    if (!target) return one
    const patch = {}
    if (target.type === 'analysis_kline') {
      if (source.type === 'stock' && source.code) {
        patch.target_type = 'stock'
        patch.target_value = normalizeStockWidgetCode(source.code)
        patch.target_name = String(source.name || '').trim()
      } else if (source.type === 'sector' && source.name) {
        patch.target_type = 'sector'
        patch.target_value = String(source.name || '').trim()
        patch.target_name = String(source.name || '').trim()
      } else if (source.type === 'index' && source.name) {
        patch.target_type = 'index'
        patch.target_value = String(source.name || '').trim()
        patch.target_name = String(source.name || '').trim()
      } else {
        return one
      }
    } else if (target.type === 'stock_kline' && source.type === 'stock' && source.code) {
      patch.stock_code = normalizeStockWidgetCode(source.code)
    } else if (target.type === 'sector_kline' && source.type === 'sector' && source.name) {
      patch.sector_name = String(source.name || '').trim()
    } else if (target.type === 'index_kline' && source.type === 'index' && source.name) {
      patch.index_name = String(source.name || '').trim()
    } else {
      return one
    }
    patchLogs.push({
      target_id: target.id,
      target_type: target.type,
      patch: { ...patch }
    })
    return { ...one, params: normalizeWidgetParams(target.type, { ...(one.params || {}), ...patch }) }
  })
  if (patchLogs.length) {
    console.log('[watchlist-link] row-click', {
      source: {
        type: String(source.type || ''),
        code: String(source.code || ''),
        name: String(source.name || '')
      },
      watchlist_widget_id: widgetId,
      link_group: linkGroup,
      matched_targets: patchLogs
    })
  } else {
    console.log('[watchlist-link] row-click no-patch', {
      source: {
        type: String(source.type || ''),
        code: String(source.code || ''),
        name: String(source.name || '')
      },
      watchlist_widget_id: widgetId,
      link_group: linkGroup
    })
  }
  try {
    await ApiService.postStrategyWatchLinkDebug({
      status: patchLogs.length ? 'patched' : 'no_patch',
      watchlist_widget_id: widgetId,
      link_group: linkGroup,
      source: {
        type: String(source.type || ''),
        code: String(source.code || ''),
        name: String(source.name || '')
      },
      matched_targets: patchLogs
    })
  } catch (error) {
    console.error(error)
  }
  watchWidgetDefs.value = patched
  const saved = await saveWatchWidgetConfig({ silent: true })
  if (!saved) {
    console.warn('[watchlist-link] saveWatchWidgetConfig failed, render locally only')
  }
  for (const target of targets) {
    const latest = patched.find(one => one.id === target.id)
    if (!latest) continue
    const rendered = await renderWatchWidget(latest)
    const chartHtml = String(rendered?.chartHtml || '')
    console.log('[watchlist-link] render-target', {
      target_id: String(target.id || ''),
      target_type: String(target.type || ''),
      params: latest.params || {},
      html_length: chartHtml.length,
      html_preview: chartHtml.slice(0, 120)
    })
    try {
      await ApiService.postStrategyWatchLinkDebug({
        status: 'rendered',
        watchlist_widget_id: widgetId,
        link_group: linkGroup,
        source: {
          type: String(source.type || ''),
          code: String(source.code || ''),
          name: String(source.name || '')
        },
        matched_targets: [{
          target_id: String(target.id || ''),
          target_type: String(target.type || ''),
          patch: latest.params || {},
          render_result: {
            html_length: chartHtml.length,
            error: String(rendered?.error || '')
          }
        }]
      })
    } catch (error) {
      console.error(error)
    }
    watchWidgets.value = patchWidgetById(watchWidgets.value, target.id, () => rendered)
  }
}

const getDroppedWidgetLayout = (type, event) => {
  const size = getDefaultWidgetSize(type)
  const boardEl = watchBoardRef.value
  if (!boardEl || !event) return getNextWidgetLayout(type)
  const rect = boardEl.getBoundingClientRect()
  const scrollLeft = Number(boardEl.scrollLeft || 0)
  const scrollTop = Number(boardEl.scrollTop || 0)
  const rawX = Number(event.clientX || 0) - rect.left + scrollLeft - size.w / 2
  const rawY = Number(event.clientY || 0) - rect.top + scrollTop - 28
  const maxX = Math.max(0, Math.max(Number(boardEl.scrollWidth || 0), Number(boardEl.clientWidth || 0)) - size.w)
  return normalizeWidgetLayout(
    {
      x: alignLayoutPixel(clampNumber(rawX, 0, maxX)),
      y: alignLayoutPixel(Math.max(0, rawY)),
      w: size.w,
      h: size.h
    },
    watchWidgetDefs.value.length,
    type
  )
}

const openWidgetEditor = () => {
  if (!activeStrategy.value?.id) return
  const willOpen = !widgetEditorVisible.value
  widgetEditorVisible.value = willOpen
  if (!willOpen) {
    widgetPaletteDragging.value = false
    widgetPaletteDragTemplateId.value = ''
    return
  }
  watchLayoutEditMode.value = true
  const validIds = widgetToolCategories.map(item => String(item?.id || '').trim()).filter(Boolean)
  widgetToolExpandedCategoryIds.value = validIds
}

const onWidgetToolTemplateDragStart = (template, event) => {
  if (!template?.id || !activeStrategy.value?.id) return
  const templateId = String(template.id || '').trim()
  if (!templateId) return
  watchLayoutEditMode.value = true
  widgetPaletteDragging.value = true
  widgetPaletteDragTemplateId.value = templateId
  if (event?.dataTransfer) {
    event.dataTransfer.effectAllowed = 'copy'
    event.dataTransfer.setData('text/plain', templateId)
    event.dataTransfer.setData('application/x-watch-widget-template', templateId)
  }
}

const onWidgetToolTemplateDragEnd = () => {
  widgetPaletteDragging.value = false
  widgetPaletteDragTemplateId.value = ''
}

const onWatchBoardDragOver = (event) => {
  if (!widgetPaletteDragTemplateId.value) return
  if (event?.dataTransfer) {
    event.dataTransfer.dropEffect = 'copy'
  }
}

const onWatchBoardDrop = async (event) => {
  const draggedId = (
    event?.dataTransfer?.getData('application/x-watch-widget-template')
    || event?.dataTransfer?.getData('text/plain')
    || widgetPaletteDragTemplateId.value
    || ''
  ).trim()
  const template = getWidgetToolTemplateById(draggedId)
  if (!template) {
    onWidgetToolTemplateDragEnd()
    return
  }
  const layout = getDroppedWidgetLayout(template.type, event)
  await addWidgetFromToolTemplate(template, { layout, silent: true })
  onWidgetToolTemplateDragEnd()
}

const getWidgetChartHeight = (widget) => {
  return '100%'
}

const getWidgetRenderKey = (widget) => {
  const wid = String(widget?.id || '')
  const wtype = String(widget?.type || '')
  const params = widget?.params && typeof widget.params === 'object'
    ? JSON.stringify(widget.params)
    : ''
  const htmlLen = Number(String(widget?.chartHtml || '').length || 0)
  const err = String(widget?.error || '')
  return `${wid}|${wtype}|${params}|${htmlLen}|${err}`
}

const watchWidgetStyle = (widget) => {
  const layout = widget?.layout || {}
  return {
    left: `${alignLayoutPixel(Math.max(0, Number(layout.x || 0)))}px`,
    top: `${alignLayoutPixel(Math.max(0, Number(layout.y || 0)))}px`,
    width: `${alignLayoutPixel(clampNumber(Number(layout.w || 500), WATCH_WIDGET_MIN_WIDTH, WATCH_WIDGET_MAX_WIDTH))}px`,
    height: `${alignLayoutPixel(clampNumber(Number(layout.h || 420), WATCH_WIDGET_MIN_HEIGHT, 1800))}px`
  }
}

const patchWidgetById = (targetList, widgetId, updater) => {
  const next = targetList.map(item => {
    if (!item || item.id !== widgetId) return item
    return updater(item)
  })
  return next
}

const patchWatchWidgetLayout = (widgetId, patch) => {
  watchWidgetDefs.value = patchWidgetById(watchWidgetDefs.value, widgetId, item => ({
    ...item,
    layout: {
      ...(item.layout || {}),
      ...patch
    }
  }))
  watchWidgets.value = patchWidgetById(watchWidgets.value, widgetId, item => ({
    ...item,
    layout: {
      ...(item.layout || {}),
      ...patch
    }
  }))
}

const loadPromptTemplatesFromStorage = () => {
  if (typeof window === 'undefined') return
  try {
    const raw = window.localStorage.getItem(PROMPT_TEMPLATE_STORAGE_KEY)
      || window.localStorage.getItem(LEGACY_PROMPT_TEMPLATE_STORAGE_KEY)
    if (!raw) {
      promptTemplates.value = clonePromptTemplates()
      return
    }
    const parsed = JSON.parse(raw)
    promptTemplates.value = normalizePromptTemplates(parsed)
    if (window.localStorage.getItem(LEGACY_PROMPT_TEMPLATE_STORAGE_KEY)) {
      window.localStorage.removeItem(LEGACY_PROMPT_TEMPLATE_STORAGE_KEY)
    }
  } catch (error) {
    console.error(error)
    promptTemplates.value = clonePromptTemplates()
  }
  if (!promptTemplates.value.some(item => item.id === selectedPromptTemplateId.value)) {
    selectedPromptTemplateId.value = 'none'
  }
}

const persistPromptTemplatesToStorage = () => {
  if (typeof window === 'undefined') return
  try {
    const payload = promptTemplates.value
      .filter(item => item.id !== 'none')
      .map(item => ({
        id: item.id,
        name: item.name,
        content: item.content
      }))
    window.localStorage.setItem(PROMPT_TEMPLATE_STORAGE_KEY, JSON.stringify(payload))
  } catch (error) {
    console.error(error)
  }
}

const createPromptTemplate = async () => {
  try {
    const { value } = await ElMessageBox.prompt('请输入模板名称', '新增提示词模板', {
      inputValue: '新模板',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const name = String(value || '').trim()
    if (!name) return
    const nextId = `tpl_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`
    promptTemplates.value.push({
      id: nextId,
      name: name.slice(0, 30),
      content: ''
    })
    selectedPromptTemplateId.value = nextId
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const deletePromptTemplate = async () => {
  if (selectedPromptTemplateId.value === 'none') return
  const target = selectedPromptTemplate.value
  try {
    await ElMessageBox.confirm(`确定删除模板“${target.name}”吗？`, '删除模板', {
      type: 'warning',
      confirmButtonText: '删除',
      cancelButtonText: '取消'
    })
    promptTemplates.value = promptTemplates.value.filter(item => item.id !== selectedPromptTemplateId.value)
    selectedPromptTemplateId.value = 'none'
    ElMessage.success('模板已删除')
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const resetMemoryPortraitForm = () => {
  memoryPortraitForm.value = {
    methodology: '',
    tactics: '',
    views: '',
    operations: '',
    risk_rules: '',
    style_constraints: ''
  }
}

const applyMemoryPortrait = (portrait) => {
  const source = portrait || {}
  memoryPortraitForm.value = {
    methodology: String(source.methodology || ''),
    tactics: String(source.tactics || ''),
    views: String(source.views || ''),
    operations: String(source.operations || ''),
    risk_rules: String(source.risk_rules || ''),
    style_constraints: String(source.style_constraints || '')
  }
}

const loadMemoryPortrait = async (profileId) => {
  const pid = String(profileId || '').trim()
  if (!pid) {
    resetMemoryPortraitForm()
    return
  }
  try {
    const res = await ApiService.getMemoryPortrait(pid)
    applyMemoryPortrait(res.data || {})
  } catch (error) {
    console.error(error)
    resetMemoryPortraitForm()
  }
}

const loadMemoryProfiles = async () => {
  try {
    const res = await ApiService.getMemoryProfiles()
    const payload = res.data || {}
    const profiles = payload.profiles || []
    const activeId = payload.active_profile_id || ''
    memoryProfiles.value = Array.isArray(profiles) ? profiles : []

    const validIds = new Set(memoryProfiles.value.map(item => item.id))
    if (activeMemoryProfileId.value && !validIds.has(activeMemoryProfileId.value)) {
      activeMemoryProfileId.value = ''
    }
    if (activeId && validIds.has(activeId)) {
      activeMemoryProfileId.value = activeId
    }
    if (!activeMemoryProfileId.value && memoryProfiles.value.length) {
      activeMemoryProfileId.value = memoryProfiles.value[0].id
    }

    if (memoryManageProfileId.value && !validIds.has(memoryManageProfileId.value)) {
      memoryManageProfileId.value = activeMemoryProfileId.value || (memoryProfiles.value[0]?.id || '')
    }
  } catch (error) {
    console.error(error)
  }
}

const loadMemoryPreviewContext = async () => {
  const pid = String(memoryManageProfileId.value || '').trim()
  if (!pid) {
    memoryPreviewContext.value = ''
    return
  }
  try {
    const res = await ApiService.previewMemoryProfileContext(pid)
    memoryPreviewContext.value = String(res.data?.context || '')
  } catch (error) {
    console.error(error)
    memoryPreviewContext.value = ''
  }
}

const openMemoryManageDialog = async () => {
  memoryManageVisible.value = true
  memoryActiveTab.value = 'binding'
  memoryBindSelectedIds.value = []
  await loadMemoryProfiles()
  if (!memoryManageProfileId.value) {
    memoryManageProfileId.value = activeMemoryProfileId.value || (memoryProfiles.value[0]?.id || '')
  }
  await loadMemoryPortrait(memoryManageProfileId.value)
  await loadMemoryPreviewContext()
}

const createMemoryProfile = async () => {
  try {
    const { value } = await ElMessageBox.prompt('请输入人格名称', '新建长期记忆人格', {
      inputValue: '新人格',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const name = String(value || '').trim()
    if (!name) return
    const res = await ApiService.createMemoryProfile({ name })
    const created = res.data
    await loadMemoryProfiles()
    if (created?.id) {
      memoryManageProfileId.value = created.id
      activeMemoryProfileId.value = created.id
      await ApiService.setActiveMemoryProfile(created.id)
      await loadMemoryPortrait(created.id)
      await loadMemoryPreviewContext()
    }
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const renameMemoryProfile = async () => {
  if (!memoryManageProfileId.value || !currentMemoryProfile.value) return
  try {
    const { value } = await ElMessageBox.prompt('请输入新的人格名称', '重命名人格', {
      inputValue: currentMemoryProfile.value.name || '',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const name = String(value || '').trim()
    if (!name) return
    await ApiService.updateMemoryProfile(memoryManageProfileId.value, { name })
    await loadMemoryProfiles()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const deleteMemoryProfile = async () => {
  if (!memoryManageProfileId.value || !currentMemoryProfile.value) return
  try {
    await ElMessageBox.confirm(
      `确定删除人格“${currentMemoryProfile.value.name}”吗？`,
      '删除人格',
      {
        type: 'warning',
        confirmButtonText: '删除',
        cancelButtonText: '取消'
      }
    )
    const removedId = memoryManageProfileId.value
    await ApiService.deleteMemoryProfile(removedId)
    if (activeMemoryProfileId.value === removedId) {
      activeMemoryProfileId.value = ''
      await ApiService.setActiveMemoryProfile('')
    }
    memoryManageProfileId.value = ''
    resetMemoryPortraitForm()
    memoryPreviewContext.value = ''
    await loadMemoryProfiles()
    if (!memoryManageProfileId.value) {
      memoryManageProfileId.value = activeMemoryProfileId.value || (memoryProfiles.value[0]?.id || '')
    }
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const applyMemoryAsActive = async () => {
  const pid = String(memoryManageProfileId.value || '').trim()
  activeMemoryProfileId.value = pid
  try {
    await ApiService.setActiveMemoryProfile(pid)
    ElMessage.success(pid ? '已设置当前人格' : '已取消当前人格')
  } catch (error) {
    console.error(error)
  }
}

const bindSelectedResourcesToMemory = async () => {
  const pid = String(memoryManageProfileId.value || '').trim()
  if (!pid || !memoryBindSelectedIds.value.length) return
  try {
    const res = await ApiService.bindMemoryProfileResources(pid, memoryBindSelectedIds.value)
    const added = Number(res.data?.added || 0)
    ElMessage.success(`已添加 ${added} 个资料到长期记忆`)
    memoryBindSelectedIds.value = []
    await loadMemoryProfiles()
    await loadMemoryPreviewContext()
  } catch (error) {
    console.error(error)
  }
}

const bindGroupToMemory = async () => {
  const pid = String(memoryManageProfileId.value || '').trim()
  const gid = String(memoryBindGroupId.value || '').trim()
  if (!pid || !gid) return
  try {
    const res = await ApiService.bindMemoryProfileGroup(pid, gid)
    const added = Number(res.data?.added || 0)
    ElMessage.success(`整组纳入完成，新增 ${added} 条`)
    await loadMemoryProfiles()
    await loadMemoryPreviewContext()
  } catch (error) {
    console.error(error)
  }
}

const syncMemoryGroupIncremental = async () => {
  const pid = String(memoryManageProfileId.value || '').trim()
  if (!pid) return
  try {
    const res = await ApiService.syncMemoryProfileGroup(pid, memoryBindGroupId.value || '')
    const added = Number(res.data?.added || 0)
    ElMessage.success(`同步完成，新增 ${added} 条`)
    await loadMemoryProfiles()
    await loadMemoryPreviewContext()
  } catch (error) {
    console.error(error)
  }
}

const generateMemoryPortraitDraft = async () => {
  const pid = String(memoryManageProfileId.value || '').trim()
  if (!pid) return
  memoryDraftLoading.value = true
  try {
    const res = await ApiService.extractMemoryPortraitDraft(pid, {
      model: selectedPortraitModel.value || ''
    })
    applyMemoryPortrait(res.data?.portrait || {})
    await loadMemoryProfiles()
    await loadMemoryPreviewContext()
    memoryActiveTab.value = 'portrait'
    ElMessage.success('已生成侧写初稿')
  } catch (error) {
    console.error(error)
  } finally {
    memoryDraftLoading.value = false
  }
}

const saveMemoryPortrait = async () => {
  const pid = String(memoryManageProfileId.value || '').trim()
  if (!pid) return
  try {
    await ApiService.updateMemoryPortrait(pid, {
      methodology: memoryPortraitForm.value.methodology,
      tactics: memoryPortraitForm.value.tactics,
      views: memoryPortraitForm.value.views,
      operations: memoryPortraitForm.value.operations,
      risk_rules: memoryPortraitForm.value.risk_rules,
      style_constraints: memoryPortraitForm.value.style_constraints
    })
    await loadMemoryProfiles()
    await loadMemoryPreviewContext()
    ElMessage.success('长期记忆侧写已保存')
  } catch (error) {
    console.error(error)
  }
}

const exportMemoryPortrait = async (format) => {
  const pid = String(memoryManageProfileId.value || '').trim()
  const fmt = String(format || 'md').trim().toLowerCase()
  if (!pid || !['md', 'docx', 'pdf'].includes(fmt)) return
  try {
    const blob = await ApiService.exportMemoryPortrait(pid, {
      format: fmt,
      portrait: {
        methodology: memoryPortraitForm.value.methodology,
        tactics: memoryPortraitForm.value.tactics,
        views: memoryPortraitForm.value.views,
        operations: memoryPortraitForm.value.operations,
        risk_rules: memoryPortraitForm.value.risk_rules,
        style_constraints: memoryPortraitForm.value.style_constraints
      }
    })
    const profileName = currentMemoryProfile.value?.name || 'memory_portrait'
    const filename = buildSafeFilename(`${profileName}_人物侧写`, fmt)
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  } catch (error) {
    console.error(error)
  }
}

const isKeyLevelStrategyConfig = (strategy) => {
  if (!strategy) return false
  const name = String(strategy.name || '')
  const cfg = strategy.config || {}
  return (
    strategy.view_type === 'key_levels' ||
    cfg.dashboard_type === 'key_levels_watchlist' ||
    name.includes('关键位') ||
    name.includes('鍏抽敭') ||
    name.toLowerCase().includes('key level')
  )
}

const isKeyLevelStrategy = computed(() => isKeyLevelStrategyConfig(activeStrategy.value))

const keySelectedStock = computed(() => {
  const code = normalizeStockCode(keySelectedCode.value)
  return keyWatchlist.value.find(item => item.code === code) || null
})

const keyChartModeLabel = computed(() => {
  const found = keyChartModeOptions.find(item => item.value === keyChartMode.value)
  return found?.label || 'K线'
})

const keyAdjustModeLabel = computed(() => {
  const found = keyAdjustModeOptions.find(item => item.value === keyAdjustMode.value)
  return found?.label || '不复权'
})

const keySubIndicatorModeLabel = computed(() => {
  const found = keySubIndicatorModeOptions.find(item => item.value === keySubIndicatorMode.value)
  return found?.label || '量+额'
})

const keyWatchGroupByLabel = computed(() => {
  const found = keyWatchGroupByOptions.find(item => item.value === keyWatchGroupBy.value)
  return found?.label || '按分组'
})

const resolveMarketGroupByCode = (code) => {
  const normalized = normalizeStockCode(code)
  if (normalized.startsWith('60') || normalized.startsWith('68')) return '沪市'
  if (normalized.startsWith('00') || normalized.startsWith('30')) return '深市'
  if (normalized.startsWith('43') || normalized.startsWith('83') || normalized.startsWith('87')) return '北交'
  return '其他'
}

const resolveWatchGroupLabel = (item, groupBy = keyWatchGroupBy.value) => {
  const source = item && typeof item === 'object' ? item : {}
  if (groupBy === 'market') return resolveMarketGroupByCode(source.code)
  if (groupBy === 'change') {
    const pct = Number(source.change_pct || 0)
    if (pct > 0) return '上涨'
    if (pct < 0) return '下跌'
    return '平盘'
  }
  return String(source.group || '').trim() || '默认分组'
}

const compareWatchSortValue = (a, b, field, order = 'desc') => {
  const f = String(field || 'change_pct')
  let result = 0
  if (f === 'name') {
    result = String(a.name || '').localeCompare(String(b.name || ''), 'zh-Hans-CN')
  } else if (f === 'code') {
    result = String(a.code || '').localeCompare(String(b.code || ''), 'zh-Hans-CN')
  } else {
    result = Number(a[f] || 0) - Number(b[f] || 0)
  }
  return order === 'asc' ? result : -result
}

const keySortedWatchlist = computed(() => {
  const list = Array.isArray(keyWatchlist.value) ? [...keyWatchlist.value] : []
  list.sort((a, b) => compareWatchSortValue(a, b, keyWatchSortField.value, keyWatchSortOrder.value))
  return list
})

const sortWatchGroupKeys = (keys = [], groupBy = keyWatchGroupBy.value) => {
  if (groupBy === 'market') {
    const rank = { 沪市: 1, 深市: 2, 北交: 3, 其他: 4 }
    return [...keys].sort((a, b) => (rank[a] || 99) - (rank[b] || 99))
  }
  if (groupBy === 'change') {
    const rank = { 上涨: 1, 平盘: 2, 下跌: 3 }
    return [...keys].sort((a, b) => (rank[a] || 99) - (rank[b] || 99))
  }
  return [...keys].sort((a, b) => String(a).localeCompare(String(b), 'zh-Hans-CN'))
}

const keyGroupedWatchRows = computed(() => {
  const groupBy = keyWatchGroupBy.value
  const grouped = new Map()
  keySortedWatchlist.value.forEach(item => {
    const label = resolveWatchGroupLabel(item, groupBy)
    const arr = grouped.get(label) || []
    arr.push(item)
    grouped.set(label, arr)
  })
  return sortWatchGroupKeys(Array.from(grouped.keys()), groupBy).map(label => ({
    label,
    items: grouped.get(label) || []
  }))
})

const keyWatchGroupFilterOptions = computed(() => {
  const options = [{ value: '__all__', label: '全部分组' }]
  keyGroupedWatchRows.value.forEach(group => {
    options.push({
      value: group.label,
      label: `${group.label} (${group.items.length})`
    })
  })
  return options
})

const keyWatchFlatRows = computed(() => {
  const selectedGroup = String(keyWatchFilterGroup.value || '__all__')
  const rows = []
  keyGroupedWatchRows.value.forEach(group => {
    if (selectedGroup !== '__all__' && selectedGroup !== group.label) return
    rows.push({
      id: `group::${group.label}`,
      type: 'group',
      label: group.label,
      count: group.items.length
    })
    group.items.forEach(item => {
      rows.push({
        id: `stock::${item.code}`,
        type: 'stock',
        item
      })
    })
  })
  return rows
})

const keyWatchVirtualMeta = computed(() => {
  const rows = keyWatchFlatRows.value
  const viewport = Math.max(1, Number(keyWatchViewportHeight.value || 0))
  const visibleCount = Math.ceil(viewport / KEY_WATCH_ROW_HEIGHT) + KEY_WATCH_OVERSCAN * 2
  const start = Math.max(0, Math.floor(keyWatchScrollTop.value / KEY_WATCH_ROW_HEIGHT) - KEY_WATCH_OVERSCAN)
  const end = Math.min(rows.length, start + visibleCount)
  return {
    rows: rows.slice(start, end),
    paddingTop: start * KEY_WATCH_ROW_HEIGHT,
    paddingBottom: Math.max(0, (rows.length - end) * KEY_WATCH_ROW_HEIGHT)
  }
})

const keyWatchVisibleRows = computed(() => keyWatchVirtualMeta.value.rows)
const keyWatchVirtualPaddingTop = computed(() => keyWatchVirtualMeta.value.paddingTop)
const keyWatchVirtualPaddingBottom = computed(() => keyWatchVirtualMeta.value.paddingBottom)

const onKeyWatchListScroll = (event) => {
  keyWatchScrollTop.value = Number(event?.target?.scrollTop || 0)
}

const updateKeyWatchViewport = () => {
  const height = Number(keyWatchListRef.value?.clientHeight || 0)
  if (height > 0) keyWatchViewportHeight.value = height
}

const handleKeyWatchWindowResize = () => {
  updateKeyWatchViewport()
}

const resetKeyWatchListScroll = () => {
  keyWatchScrollTop.value = 0
  if (keyWatchListRef.value) {
    keyWatchListRef.value.scrollTop = 0
  }
}

const toggleKeyWatchSortOrder = () => {
  keyWatchSortOrder.value = keyWatchSortOrder.value === 'desc' ? 'asc' : 'desc'
}

const resolveDevicePixelRatio = () => {
  if (typeof window === 'undefined') return 1
  const dpr = Number(window.devicePixelRatio || 1)
  if (!Number.isFinite(dpr) || dpr <= 0) return 1
  return dpr
}

const alignToDevicePixel = (value) => {
  const dpr = resolveDevicePixelRatio()
  return Math.round(Number(value || 0) * dpr) / dpr
}

const keyChartInitOptions = computed(() => ({
  renderer: 'canvas',
  devicePixelRatio: resolveDevicePixelRatio(),
  useDirtyRect: true
}))

const keyKlineOption = computed(() => {
  if (!keyKlineData.value.length) return {}

  const rows = keyKlineData.value
  const dates = rows.map(item => item.date)
  const candlestickData = rows.map(item => [item.open, item.close, item.low, item.high])
  const closeData = rows.map(item => item.close)
  const ma5Data = rows.map(item => item.ma5)
  const ma10Data = rows.map(item => item.ma10)
  const ma20Data = rows.map(item => item.ma20)
  const volumeData = rows.map(item => ({
    value: item.volume || 0,
    itemStyle: {
      color: Number(item.close) >= Number(item.open) ? '#ef232a' : '#14b143'
    }
  }))
  const amountData = rows.map(item => ({
    value: item.amount || 0,
    itemStyle: {
      color: Number(item.close) >= Number(item.open) ? '#ef232a' : '#14b143'
    }
  }))

  const levelMarkLine = keyLevels.value.length
    ? {
        symbol: 'none',
        silent: true,
        lineStyle: { color: '#d4a100', type: 'dashed', width: 1.2 },
        label: {
          show: true,
          position: 'insideEndTop',
          color: '#8a6a00',
          formatter: (p) => {
            const n = Number(p.value)
            return Number.isFinite(n) ? n.toFixed(2) : ''
          }
        },
        data: keyLevels.value.map(level => ({ yAxis: Number(level) }))
      }
    : undefined

  const subPanels = []
  if (keySubIndicatorMode.value === 'volume' || keySubIndicatorMode.value === 'both') {
    subPanels.push({ key: 'volume', name: '成交量', data: volumeData, color: '#5ca6ff' })
  }
  if (keySubIndicatorMode.value === 'amount' || keySubIndicatorMode.value === 'both') {
    subPanels.push({ key: 'amount', name: '成交额', data: amountData, color: '#8a7dff' })
  }

  const grid = []
  const xAxis = []
  const yAxis = []
  const series = []
  const axisLineWidth = alignToDevicePixel(1)
  const pct = (value) => `${Math.max(0, Number(Number(value).toFixed(4)))}%`

  // 主图和副图使用连续网格，避免中间露出背景缝隙。
  const mainTopPct = 8
  const bottomReservedPct = 10
  const usablePct = 100 - mainTopPct - bottomReservedPct
  const mainRatio = subPanels.length >= 2 ? 0.68 : 0.76
  const mainHeightPct = subPanels.length ? usablePct * mainRatio : usablePct
  const subHeightPct = subPanels.length ? (usablePct - mainHeightPct) / subPanels.length : 0
  let cursorPct = mainTopPct

  grid.push({
    left: alignToDevicePixel(56),
    right: alignToDevicePixel(36),
    top: pct(cursorPct),
    height: pct(mainHeightPct),
    containLabel: false
  })
  xAxis.push({
    type: 'category',
    data: dates,
    boundaryGap: false,
    scale: true,
    min: 'dataMin',
    max: 'dataMax',
    axisLine: { lineStyle: { width: axisLineWidth, color: '#d7e0ea' } },
    axisLabel: { show: subPanels.length === 0 },
    axisTick: { show: subPanels.length === 0 },
    splitLine: { show: false }
  })
  yAxis.push({
    scale: true,
    axisLine: { lineStyle: { width: axisLineWidth, color: '#d7e0ea' } },
    splitLine: { lineStyle: { width: axisLineWidth, color: '#eef3f8' } }
  })

  if (keyChartMode.value === 'time') {
    series.push({
      name: '分时(收盘)',
      type: 'line',
      data: closeData,
      smooth: true,
      symbol: 'none',
      lineStyle: { color: '#2f8cb7', width: alignToDevicePixel(1.5) },
      markLine: levelMarkLine
    })
  } else {
    series.push({
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
    })
    series.push(
      {
        name: 'MA5',
        type: 'line',
        data: ma5Data,
        smooth: true,
        symbol: 'none',
        lineStyle: { color: '#4ECDC4', opacity: 0.9 }
      },
      {
        name: 'MA10',
        type: 'line',
        data: ma10Data,
        smooth: true,
        symbol: 'none',
        lineStyle: { color: '#ffbf00', opacity: 0.9 }
      },
      {
        name: 'MA20',
        type: 'line',
        data: ma20Data,
        smooth: true,
        symbol: 'none',
        lineStyle: { color: '#f92672', opacity: 0.9 }
      }
    )
  }

  cursorPct += mainHeightPct
  subPanels.forEach((panel, idx) => {
    grid.push({
      left: alignToDevicePixel(56),
      right: alignToDevicePixel(36),
      top: pct(cursorPct),
      height: pct(subHeightPct),
      containLabel: false
    })
    xAxis.push({
      type: 'category',
      gridIndex: idx + 1,
      data: dates,
      boundaryGap: false,
      scale: true,
      min: 'dataMin',
      max: 'dataMax',
      axisLine: { lineStyle: { width: axisLineWidth, color: '#d7e0ea' } },
      axisLabel: { show: idx === subPanels.length - 1 },
      axisTick: { show: idx === subPanels.length - 1 },
      splitLine: { show: false }
    })
    yAxis.push({
      scale: true,
      gridIndex: idx + 1,
      splitNumber: 2,
      axisLine: { lineStyle: { width: axisLineWidth, color: '#d7e0ea' } },
      axisLabel: { show: true },
      axisTick: { show: false },
      splitLine: { show: false }
    })
    series.push({
      name: panel.name,
      type: 'bar',
      xAxisIndex: idx + 1,
      yAxisIndex: idx + 1,
      data: panel.data,
      itemStyle: { opacity: 0.86 }
    })
    cursorPct += subHeightPct
  })

  const dataZoomAxisIndexes = xAxis.map((_, idx) => idx)
  const legendItems = series
    .map(item => item.name)
    .filter(name => String(name || '').trim() !== '')

  return {
    animation: false,
    color: ['#4ECDC4', '#ffbf00', '#f92672'],
    axisPointer: {
      link: [{ xAxisIndex: 'all' }],
      snap: true
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: {
        type: 'cross',
        lineStyle: { width: axisLineWidth, color: '#6f7f90' }
      }
    },
    legend: {
      data: legendItems,
      top: 24,
      right: 10
    },
    grid,
    xAxis,
    yAxis,
    dataZoom: [
      { type: 'inside', xAxisIndex: dataZoomAxisIndexes, start: 70, end: 100 },
      { type: 'slider', xAxisIndex: dataZoomAxisIndexes, top: '92%', start: 70, end: 100 }
    ],
    series
  }
})

const formatWatchPrice = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num) || num === 0) return '--'
  return num.toFixed(2)
}

const formatWatchChange = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '--'
  return `${num.toFixed(2)}%`
}

const formatWatchAmount = (value) => {
  const num = Number(value || 0)
  if (!Number.isFinite(num) || num <= 0) return '--'
  if (num >= 100000000) return `${(num / 100000000).toFixed(2)}亿`
  if (num >= 10000) return `${(num / 10000).toFixed(2)}万`
  return num.toFixed(0)
}

const formatWatchVolume = (value) => {
  const num = Number(value || 0)
  if (!Number.isFinite(num) || num <= 0) return '--'
  if (num >= 100000000) return `${(num / 100000000).toFixed(2)}亿`
  if (num >= 10000) return `${(num / 10000).toFixed(2)}万`
  return num.toFixed(0)
}

const watchChangeClass = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return 'neutral'
  if (num > 0) return 'up'
  if (num < 0) return 'down'
  return 'neutral'
}

const toggleChatSidebarCollapse = () => {
  chatSidebarCollapsed.value = !chatSidebarCollapsed.value
}

const toggleStrategyListCollapse = () => {
  strategyListCollapsed.value = !strategyListCollapsed.value
}

const scrollMessagesToBottom = async () => {
  await nextTick()
  const el = messagesRef.value
  if (el) el.scrollTop = el.scrollHeight
}

const formatDateTime = (value) => {
  if (!value) return '--'
  const dt = new Date(value)
  if (Number.isNaN(dt.getTime())) return value
  return dt.toLocaleString()
}

const formatSize = (bytes) => {
  const num = Number(bytes || 0)
  if (!num) return '--'
  if (num < 1024) return `${num} B`
  if (num < 1024 * 1024) return `${(num / 1024).toFixed(1)} KB`
  if (num < 1024 * 1024 * 1024) return `${(num / (1024 * 1024)).toFixed(1)} MB`
  return `${(num / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

const formatSourceType = (type) => {
  if (type === 'url') return '爬虫抓取'
  if (type === 'file') return '本地上传'
  return '其他来源'
}

const getCrawlResults = (msg) => {
  return Array.isArray(msg?.crawl_results) ? msg.crawl_results : []
}

const compactError = (text, limit = 120) => {
  const value = String(text || '').trim()
  if (!value) return ''
  return value.length > limit ? `${value.slice(0, limit - 3)}...` : value
}

const summarizeCrawlError = (item) => {
  if (!item) return ''
  const parts = []
  if (item.error) parts.push(compactError(item.error))
  if (item.pdf_error) parts.push(`PDF: ${compactError(item.pdf_error)}`)
  if (item.docx_error) parts.push(`Word: ${compactError(item.docx_error)}`)
  return parts.join(' | ')
}

const getCrawlStatusText = (item) => {
  if ((item?.status || '') !== 'ok') return '抓取失败'
  if (item?.pdf_error || item?.docx_error) return '抓取成功（部分格式失败）'
  return '抓取成功'
}

const getCrawlStatusTag = (item) => {
  if ((item?.status || '') !== 'ok') return 'danger'
  if (item?.pdf_error || item?.docx_error) return 'warning'
  return 'success'
}

const buildCrawlerAgentSummary = (msg) => {
  const items = getCrawlResults(msg)
  if (!items.length) return (msg?.content || '')
  const total = items.length
  const okCount = items.filter(item => (item?.status || '') === 'ok').length
  const failCount = total - okCount
  const lines = [
    `抓取任务完成：共 ${total} 条，成功 ${okCount} 条，失败 ${failCount} 条。`,
    ''
  ]
  items.forEach((item, idx) => {
    const title = item?.title || item?.url || `抓取文章${idx + 1}`
    const status = getCrawlStatusText(item)
    const formatState = [
      `MD:${item?.markdown_relpath ? '可下载' : '不可用'}`,
      `PDF:${item?.pdf_relpath ? '可下载' : '不可用'}`,
      `Word:${item?.docx_relpath ? '可下载' : '不可用'}`
    ].join('，')
    lines.push(`${idx + 1}. ${title}`)
    lines.push(`状态：${status}`)
    lines.push(`格式：${formatState}`)
    if (item?.url) lines.push(`链接：${item.url}`)
    const err = summarizeCrawlError(item)
    if (err) lines.push(`备注：${err}`)
    lines.push('')
  })
  return lines.join('\n').trim()
}

const buildOrchestrationPmSummary = (msg) => {
  if ((msg?.conversation_mode || '') !== 'strategy_edit') return ''
  const meta = msg?.orchestration_meta
  if (!meta || typeof meta !== 'object') return ''
  if (meta.requirements_ready !== false) return ''
  const req = meta.pm_requirements && typeof meta.pm_requirements === 'object'
    ? meta.pm_requirements
    : {}
  const reason = String(req.block_reason || '').trim() || '关键约束信息不完整。'
  const questions = Array.isArray(req.next_questions)
    ? req.next_questions.map(item => String(item || '').trim()).filter(Boolean).slice(0, 3)
    : []
  const lines = [
    '【PM需求澄清】',
    `当前无法进入架构拆解：${reason}`,
    '',
    '请补充以下最小必要信息：'
  ]
  if (questions.length) {
    questions.forEach((q, idx) => lines.push(`${idx + 1}. ${q}`))
  } else {
    lines.push('1. 请补充目标范围、输入输出和验收标准。')
  }
  lines.push('')
  lines.push('补充后我会自动转交 architect_agent 进行任务分解与执行编排。')
  return lines.join('\n').trim()
}

const getDisplayedAssistantContent = (msg) => {
  const orchestrationPmSummary = buildOrchestrationPmSummary(msg)
  if (orchestrationPmSummary) return orchestrationPmSummary
  if ((msg?.conversation_mode === 'crawler' || msg?.provider === 'crawler') && getCrawlResults(msg).length) {
    return buildCrawlerAgentSummary(msg)
  }
  return msg?.content || ''
}

const replaceStrategyLocal = (updated) => {
  if (!updated?.id) return
  const idx = strategies.value.findIndex(item => item.id === updated.id)
  if (idx >= 0) {
    strategies.value[idx] = updated
  } else {
    strategies.value.unshift(updated)
  }
}

const buildKeyWatchStock = (item = {}) => {
  const code = normalizeStockCode(item.code || item.代码)
  if (!code) return null
  return {
    code,
    name: (item.name || item.名称 || '').trim() || code,
    latest_price: Number(item.latest_price ?? item.最新价 ?? 0) || 0,
    change_pct: Number(item.change_pct ?? item.涨跌幅 ?? 0) || 0,
    amount: Number(item.amount ?? item.成交额 ?? 0) || 0,
    volume: Number(item.volume ?? item.成交量 ?? 0) || 0,
    group: String(item.group || item.group_name || '').trim() || '默认分组'
  }
}

const getKeyBoardConfig = (strategy) => {
  const cfg = strategy?.config || {}
  const board = cfg.key_levels_board
  return board && typeof board === 'object' ? board : {}
}

const hydrateKeyBoardFromStrategy = (strategy) => {
  keyBoardHydrating.value = true
  const board = getKeyBoardConfig(strategy)
  const rawWatchlist = Array.isArray(board.watchlist) ? board.watchlist : []
  const nextWatchlist = []
  const seen = new Set()
  rawWatchlist.forEach(item => {
    const built = buildKeyWatchStock(item)
    if (!built || seen.has(built.code)) return
    seen.add(built.code)
    nextWatchlist.push(built)
  })
  keyWatchlist.value = nextWatchlist.slice(0, KEY_WATCHLIST_MAX)

  const selectedCode = normalizeStockCode(board.selected_code || '')
  if (selectedCode && keyWatchlist.value.some(item => item.code === selectedCode)) {
    keySelectedCode.value = selectedCode
  } else {
    keySelectedCode.value = keyWatchlist.value[0]?.code || ''
  }

  keyPaneWidthPercent.value = clampNumber(
    Number(board.pane_width_percent || 36) || 36,
    0,
    64
  )
  keyLevelWindowDays.value = Number(board.window_days || 3650) || 3650
  keyChartMode.value = ['kline', 'time'].includes(String(board.chart_mode || ''))
    ? String(board.chart_mode)
    : 'kline'
  keyAdjustMode.value = ['none', 'qfq', 'hfq'].includes(String(board.adjust_mode || ''))
    ? String(board.adjust_mode)
    : 'none'
  keySubIndicatorMode.value = ['both', 'volume', 'amount'].includes(String(board.sub_indicator_mode || ''))
    ? String(board.sub_indicator_mode)
    : 'both'
  keyWatchGroupBy.value = ['custom', 'market', 'change'].includes(String(board.group_by || ''))
    ? String(board.group_by)
    : 'custom'
  keyWatchSortField.value = keyWatchSortFieldOptions.some(item => item.value === board.sort_field)
    ? board.sort_field
    : 'change_pct'
  keyWatchSortOrder.value = board.sort_order === 'asc' ? 'asc' : 'desc'
  keyWatchFilterGroup.value = '__all__'
  nextTick(() => {
    resetKeyWatchListScroll()
    updateKeyWatchViewport()
    keyBoardHydrating.value = false
  })
}

const persistKeyBoardState = async () => {
  const strategy = activeStrategy.value
  if (!strategy?.id || !isKeyLevelStrategyConfig(strategy)) return

  const currentConfig = strategy.config && typeof strategy.config === 'object'
    ? { ...strategy.config }
    : {}
  currentConfig.dashboard_type = 'key_levels_watchlist'
  currentConfig.key_levels_board = {
    watchlist: keyWatchlist.value.map(item => ({
      code: item.code,
      name: item.name,
      latest_price: item.latest_price,
      change_pct: item.change_pct,
      amount: item.amount,
      volume: item.volume,
      group: item.group || '默认分组'
    })),
    selected_code: keySelectedCode.value || '',
    pane_width_percent: keyPaneWidthPercent.value,
    window_days: keyLevelWindowDays.value,
    chart_mode: keyChartMode.value,
    adjust_mode: keyAdjustMode.value,
    sub_indicator_mode: keySubIndicatorMode.value,
    group_by: keyWatchGroupBy.value,
    sort_field: keyWatchSortField.value,
    sort_order: keyWatchSortOrder.value
  }

  try {
    const res = await ApiService.updateStrategyWatchStrategy(strategy.id, {
      view_type: 'key_levels',
      config: currentConfig
    })
    if (res?.data?.id) replaceStrategyLocal(res.data)
  } catch (error) {
    console.error(error)
  }
}

const queryKeyWatchStockSuggestions = async (queryString, cb) => {
  const term = (queryString || '').trim()
  if (!term) {
    cb([])
    return
  }
  try {
    const res = await ApiService.searchStocks(term)
    const rows = Array.isArray(res?.data) ? res.data : []
    const suggestions = rows.slice(0, 20).map(item => ({
      value: `${item.代码} ${item.名称}`,
      code: normalizeStockCode(item.代码),
      name: item.名称,
      latest_price: Number(item.最新价 || 0),
      change_pct: Number(item.涨跌幅 || 0),
      amount: Number(item.成交额 || 0),
      volume: Number(item.成交量 || 0),
      group: '默认分组'
    }))
    cb(suggestions)
  } catch (error) {
    console.error(error)
    cb([])
  }
}

const addWatchStock = async (candidate) => {
  const stock = buildKeyWatchStock(candidate)
  if (!stock?.code) {
    ElMessage.warning('请输入有效的股票代码')
    return
  }
  if (keyWatchlist.value.some(item => item.code === stock.code)) {
    ElMessage.warning('该股票已在自选列表')
    return
  }
  if (keyWatchlist.value.length >= KEY_WATCHLIST_MAX) {
    ElMessage.warning(`自选股上限为 ${KEY_WATCHLIST_MAX} 只`)
    return
  }
  keyWatchlist.value.push(stock)
  keyStockSearchQuery.value = ''
  if (!keySelectedCode.value) keySelectedCode.value = stock.code
  nextTick(() => updateKeyWatchViewport())
  await persistKeyBoardState()
  await loadKeyStockKline(keySelectedCode.value)
}

const addWatchStockFromInput = async () => {
  const term = (keyStockSearchQuery.value || '').trim()
  if (!term) return
  try {
    const res = await ApiService.searchStocks(term)
    const rows = Array.isArray(res?.data) ? res.data : []
    if (!rows.length) {
      ElMessage.warning('未找到匹配股票')
      return
    }
    await addWatchStock(rows[0])
  } catch (error) {
    console.error(error)
  }
}

const onWatchStockSuggestionSelect = async (item) => {
  await addWatchStock(item)
}

const onKeyLevelWindowChange = async () => {
  await persistKeyBoardState()
  if (keySelectedCode.value) await loadKeyStockKline(keySelectedCode.value)
}

const renameWatchStockGroup = async (stock) => {
  const code = normalizeStockCode(stock?.code || '')
  if (!code) return
  const current = keyWatchlist.value.find(item => item.code === code)
  if (!current) return
  try {
    const { value } = await ElMessageBox.prompt('请输入分组名称', '设置分组', {
      inputValue: String(current.group || '默认分组'),
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const nextGroup = String(value || '').trim() || '默认分组'
    keyWatchlist.value = keyWatchlist.value.map(item => (
      item.code === code
        ? { ...item, group: nextGroup }
        : item
    ))
    await persistKeyBoardState()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const removeWatchStock = async (code) => {
  const normalized = normalizeStockCode(code)
  const next = keyWatchlist.value.filter(item => item.code !== normalized)
  if (next.length === keyWatchlist.value.length) return
  keyWatchlist.value = next
  if (keySelectedCode.value === normalized) {
    keySelectedCode.value = keyWatchlist.value[0]?.code || ''
    if (!keySelectedCode.value) {
      keyKlineData.value = []
      keyLevels.value = []
    }
  }
  nextTick(() => updateKeyWatchViewport())
  await persistKeyBoardState()
  if (keySelectedCode.value) await loadKeyStockKline(keySelectedCode.value)
}

const selectWatchStock = async (code) => {
  const normalized = normalizeStockCode(code)
  if (!normalized) return
  keySelectedCode.value = normalized
  await persistKeyBoardState()
  await loadKeyStockKline(normalized)
}

const loadKeyStockKline = async (stockCode) => {
  const code = normalizeStockCode(stockCode)
  if (!code) return
  keyKlineLoading.value = true
  keyWatchError.value = ''
  try {
    const [klineResp, levelsResp] = await Promise.all([
      ApiService.getStockKline(code, 3650, null, 'data'),
      ApiService.getStockLevels(code, keyLevelWindowDays.value, null)
    ])

    const rows = klineResp?.data?.data?.kline_data || []
    keyKlineData.value = Array.isArray(rows) ? rows : []

    const lv = levelsResp?.data?.levels || []
    keyLevels.value = Array.isArray(lv) ? lv.map(v => Number(v)).filter(Number.isFinite) : []

    if (!keyKlineData.value.length) {
      keyWatchError.value = `股票 ${code} 暂无K线数据`
      return
    }

    const latest = keyKlineData.value[keyKlineData.value.length - 1]
    const prev = keyKlineData.value[keyKlineData.value.length - 2]
    const latestPrice = Number(latest?.close || 0)
    const prevClose = Number(prev?.close || latest?.open || 0)
    const changePct = prevClose ? ((latestPrice - prevClose) / prevClose) * 100 : 0
    const amount = Number(latest?.amount || 0)
    const volume = Number(latest?.volume || 0)
    const idx = keyWatchlist.value.findIndex(item => item.code === code)
    if (idx >= 0) {
      keyWatchlist.value[idx] = {
        ...keyWatchlist.value[idx],
        latest_price: latestPrice,
        change_pct: changePct,
        amount,
        volume
      }
    }
  } catch (error) {
    console.error(error)
    keyKlineData.value = []
    keyLevels.value = []
    keyWatchError.value = 'K线加载失败，请稍后重试'
  } finally {
    keyKlineLoading.value = false
  }
}

const keyResizeState = { startX: 0, startWidth: 36 }

const stopKeyPaneResize = async () => {
  if (!keyPaneResizing.value) return
  keyPaneResizing.value = false
  document.removeEventListener('mousemove', onKeyPaneResizeMove)
  document.removeEventListener('mouseup', stopKeyPaneResize)
  await persistKeyBoardState()
}

const onKeyPaneResizeMove = (event) => {
  if (!keyPaneResizing.value || !keyBoardBodyRef.value) return
  const rect = keyBoardBodyRef.value.getBoundingClientRect()
  if (!rect.width) return
  const delta = ((event.clientX - keyResizeState.startX) / rect.width) * 100
  keyPaneWidthPercent.value = clampNumber(keyResizeState.startWidth + delta, 0, 64)
}

const startKeyPaneResize = (event) => {
  if (!keyBoardBodyRef.value) return
  keyPaneResizing.value = true
  keyResizeState.startX = event.clientX
  keyResizeState.startWidth = keyPaneWidthPercent.value
  document.addEventListener('mousemove', onKeyPaneResizeMove)
  document.addEventListener('mouseup', stopKeyPaneResize)
}

const resolveModeAgentName = (mode) => {
  const mapped = MODE_AGENT_MAP[mode] || MODE_AGENT_MAP.dialog
  const agents = Array.isArray(runtime.value?.agents) ? runtime.value.agents : []
  if (agents.some(item => item?.name === mapped)) return mapped
  return agents[0]?.name || mapped
}

const syncAgentByMode = (force = false) => {
  const mode = activeMode.value
  const remembered = modeAgentSelection.value[mode]
  if (!force && remembered) {
    selectedAgentName.value = remembered
    return
  }
  selectedAgentName.value = resolveModeAgentName(mode)
}

const loadRuntime = async () => {
  const res = await ApiService.getStrategyRuntime()
  runtime.value = res.data || runtime.value

  const runtimeOptions = normalizeModelOptions(runtime.value.model_options || [])
  if (runtimeOptions.length) {
    modelOptions.value = runtimeOptions
  }

  const candidateModel = runtime.value.model
  if (candidateModel) {
    if (!modelOptions.value.some(item => item.value === candidateModel)) {
      modelOptions.value.push({ label: candidateModel, value: candidateModel })
    }
    selectedModel.value = candidateModel
  }

  const candidatePortraitModel = runtime.value.portrait_model || candidateModel
  if (candidatePortraitModel) {
    if (!modelOptions.value.some(item => item.value === candidatePortraitModel)) {
      modelOptions.value.push({ label: candidatePortraitModel, value: candidatePortraitModel })
    }
    selectedPortraitModel.value = candidatePortraitModel
  }

  if (!selectedPortraitModel.value) {
    selectedPortraitModel.value = selectedModel.value || (modelOptions.value[0]?.value || '')
  }
  syncAgentByMode()
}

const loadConversations = async () => {
  const res = await ApiService.getStrategyConversations()
  conversations.value = res.data || []
  if (!activeConversationId.value && conversations.value.length) {
    activeConversationId.value = conversations.value[0].id
  }
  if (activeConversationId.value) {
    await loadMessages(activeConversationId.value)
  }
}

const createConversation = async () => {
  const res = await ApiService.createStrategyConversation({})
  const conv = res.data
  conversations.value.unshift({
    id: conv.id,
    title: conv.title,
    created_at: conv.created_at,
    updated_at: conv.updated_at,
    message_count: 0,
    last_message_preview: ''
  })
  activeConversationId.value = conv.id
  messages.value = []
}

const switchConversation = async (id) => {
  stopStrategyEditRun()
  activeConversationId.value = id
  await loadMessages(id)
}

const loadMessages = async (conversationId) => {
  const res = await ApiService.getStrategyMessages(conversationId)
  messages.value = res.data?.messages || []
  await scrollMessagesToBottom()
}

const renameConversation = async () => {
  if (!activeConversationId.value) return
  try {
    const { value } = await ElMessageBox.prompt('请输入对话名称', '重命名对话', {
      inputValue: activeConversationTitle.value || '',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const name = (value || '').trim()
    if (!name) return
    await ApiService.renameStrategyConversation(activeConversationId.value, name)
    await loadConversations()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const deleteConversation = async (conversationId) => {
  try {
    await ElMessageBox.confirm('确定删除该对话吗？删除后无法恢复。', '删除对话', {
      type: 'warning',
      confirmButtonText: '删除',
      cancelButtonText: '取消'
    })
    await ApiService.deleteStrategyConversation(conversationId)
    if (activeConversationId.value === conversationId) {
      activeConversationId.value = ''
      messages.value = []
    }
    await loadConversations()
    if (!conversations.value.length) await createConversation()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const loadResources = async (silent = false) => {
  const config = silent ? { hideLoading: true } : undefined
  const res = await ApiService.getStrategyResources(config)
  const payload = res.data || {}
  resources.value = Array.isArray(payload) ? payload : (payload.resources || [])
  resourceGroups.value = Array.isArray(payload) ? [] : (payload.groups || [])
  activeResourceId.value = Array.isArray(payload) ? '' : (payload.active_resource_id || '')
  const exist = new Set(usableResources.value.map(item => item.id))
  selectedResourceIds.value = selectedResourceIds.value.filter(id => exist.has(id))
  if (manageSelectedIds.value.length) {
    const exist = new Set(resources.value.map(item => item.id))
    manageSelectedIds.value = manageSelectedIds.value.filter(id => exist.has(id))
  }
  if (selectedGroupId.value && !resourceGroups.value.some(g => g.group_id === selectedGroupId.value)) {
    selectedGroupId.value = ''
  }
  if (manageFilterGroupId.value && !resourceGroups.value.some(g => g.group_id === manageFilterGroupId.value)) {
    manageFilterGroupId.value = ''
  }
  if (manageTargetGroupId.value && !resourceGroups.value.some(g => g.group_id === manageTargetGroupId.value)) {
    manageTargetGroupId.value = ''
  }
  if (hoverGroupId.value) {
    const key = hoverGroupId.value
    if (key.startsWith('group:')) {
      const gid = key.slice('group:'.length)
      if (!resourceGroups.value.some(g => g.group_id === gid)) hoverGroupId.value = ''
    } else if (key === 'strategy:__all__') {
      if (!strategies.value.length) hoverGroupId.value = ''
    } else if (key.startsWith('strategy:')) {
      const sid = key.slice('strategy:'.length)
      const hasStrategy = strategies.value.some(item => item.id === sid)
      if (!hasStrategy) hoverGroupId.value = ''
    }
  }
}

const loadJobs = async (silent = false) => {
  try {
    const config = silent ? { hideLoading: true } : undefined
    const res = await ApiService.listStrategyResourceJobs(config)
    jobList.value = Array.isArray(res.data) ? res.data : (res.data || [])
  } catch (error) {
    console.error(error)
  }
}

const openJobsDialog = async () => {
  jobsDialogVisible.value = true
  await loadJobs()
}

const stopJobPolling = () => {
  if (jobPollingTimer.value) {
    clearInterval(jobPollingTimer.value)
    jobPollingTimer.value = null
  }
}

const pollJobsOnce = async () => {
  if (!uploadJobs.value.length) {
    stopJobPolling()
    return
  }
  const jobIds = [...uploadJobs.value]
  try {
  const results = await Promise.all(
    jobIds.map(async id => {
      try {
        const res = await ApiService.getStrategyResourceJob(id, { hideLoading: true })
        return { id, job: res.data }
      } catch (error) {
        const status = error?.response?.status
        if (status === 404) {
          return { id, missing: true }
        }
        return { id, error: true }
      }
    })
  )
  const done = new Set()
  results.forEach(item => {
    if (!item) return
    if (item.missing) {
      done.add(item.id)
      return
    }
    const status = item.job?.status
    if (!status || (status !== 'queued' && status !== 'running')) {
      done.add(item.id)
    }
  })
    await loadResources(true)
    await loadJobs(true)
    uploadJobs.value = uploadJobs.value.filter(id => !done.has(id))
    if (!uploadJobs.value.length) stopJobPolling()
  } catch (error) {
    console.error(error)
  }
}

const startJobPolling = (jobIds) => {
  const ids = Array.isArray(jobIds) ? jobIds.filter(Boolean) : []
  if (!ids.length) return
  const merged = new Set(uploadJobs.value)
  ids.forEach(id => merged.add(id))
  uploadJobs.value = Array.from(merged)
  if (!jobPollingTimer.value) {
    jobPollingTimer.value = setInterval(pollJobsOnce, 2000)
  }
  pollJobsOnce()
}

const loadStrategies = async () => {
  watchLoading.value = true
  try {
    const res = await ApiService.getStrategyWatchStrategies()
    const payload = res.data || {}
    strategies.value = Array.isArray(payload) ? payload : (payload.strategies || [])
    activeStrategyId.value = Array.isArray(payload) ? '' : (payload.active_strategy_id || '')
    if (!activeStrategyId.value && strategies.value.length) {
      activeStrategyId.value = strategies.value[0].id
    }
  } catch (error) {
    console.error(error)
  } finally {
    watchLoading.value = false
  }
}

const createStrategy = async () => {
  try {
    const { value } = await ElMessageBox.prompt('请输入策略名称', '新建策略', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      inputPlaceholder: '例如：趋势突破策略'
    })
    const name = (value || '').trim()
    if (!name) return
    const res = await ApiService.createStrategyWatchStrategy({
      name,
      view_type: 'basic'
    })
    const created = res.data
    if (created) {
      strategies.value.unshift(created)
      activeStrategyId.value = created.id
    } else {
      await loadStrategies()
    }
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const renameStrategy = async (strategy) => {
  if (!strategy?.id) return
  try {
    const { value } = await ElMessageBox.prompt('请输入策略名称', '重命名策略', {
      inputValue: strategy.name || '',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const name = (value || '').trim()
    if (!name) return
    await ApiService.updateStrategyWatchStrategy(strategy.id, { name })
    await loadStrategies()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const deleteStrategy = async (strategyId) => {
  try {
    await ApiService.deleteStrategyWatchStrategy(strategyId)
    if (activeStrategyId.value === strategyId) {
      activeStrategyId.value = ''
    }
    await loadStrategies()
  } catch (error) {
    console.error(error)
  }
}

const setActiveStrategy = async (strategyId) => {
  activeStrategyId.value = strategyId
  try {
    await ApiService.setActiveStrategyWatchStrategy(strategyId)
  } catch (error) {
    console.error(error)
  }
}

const buildDefaultWidgets = (viewType) => {
  if (viewType === 'key_levels') return []
  return [
    {
      id: 'market-sentiment-red-ratio',
      type: 'market_sentiment_chart',
      title: '红盘率与成交额',
      params: { chart_key: 'red_ratio_and_amount', days_back: 30 },
      layout: { x: 0, y: 0, w: 640, h: 420 }
    },
    {
      id: 'analysis-kline',
      type: 'analysis_kline',
      title: '上证指数 K线',
      params: { target_type: 'index', target_value: '上证指数', days: 60 },
      layout: { x: 0, y: 434, w: 720, h: 520 }
    }
  ]
}

const buildWidgetsFromStrategy = (strategy) => {
  if (!strategy) return []
  const config = strategy.config || {}
  if (Array.isArray(config.widgets)) {
    return normalizeWidgetDefinitions(config.widgets, strategy.view_type || 'basic', false)
  }
  return normalizeWidgetDefinitions(null, strategy.view_type || 'basic', true)
}

const saveWatchWidgetConfig = async ({ silent = true } = {}) => {
  const strategy = activeStrategy.value
  if (!strategy?.id || isKeyLevelStrategyConfig(strategy)) return false

  const normalizedDefs = normalizeWidgetDefinitions(
    watchWidgetDefs.value,
    strategy.view_type || 'basic',
    false
  )
  watchWidgetDefs.value = normalizedDefs

  const currentConfig = strategy.config && typeof strategy.config === 'object'
    ? { ...strategy.config }
    : {}
  currentConfig.widgets = normalizedDefs.map(item => serializeWidgetDef(item))
  currentConfig.watchlist_groups = serializeWatchlistGroups()

  try {
    const res = await ApiService.updateStrategyWatchStrategy(strategy.id, {
      view_type: strategy.view_type || 'basic',
      config: currentConfig
    })
    if (res?.data?.id) {
      replaceStrategyLocal(res.data)
    }
    if (!silent) {
      ElMessage.success('图表配置已保存')
    }
    return true
  } catch (error) {
    console.error(error)
    if (!silent) {
      ElMessage.error('保存图表配置失败')
    }
    return false
  }
}

const queueSaveWatchWidgetConfig = () => {
  if (watchPersistTimer.value) {
    clearTimeout(watchPersistTimer.value)
    watchPersistTimer.value = null
  }
  watchPersistTimer.value = setTimeout(() => {
    saveWatchWidgetConfig({ silent: true })
    watchPersistTimer.value = null
  }, WATCH_LAYOUT_SAVE_DEBOUNCE_MS)
}

const stopWatchWidgetInteract = () => {
  if (!watchPointerState.mode) return
  const changedWidgetId = watchPointerState.widgetId
  watchPointerState.mode = ''
  watchPointerState.widgetId = ''
  watchDraggingWidgetId.value = ''
  watchResizingWidgetId.value = ''
  document.removeEventListener('mousemove', onWatchWidgetInteractMove)
  document.removeEventListener('mouseup', stopWatchWidgetInteract)
  if (changedWidgetId) {
    queueSaveWatchWidgetConfig()
  }
}

const onWatchWidgetInteractMove = (event) => {
  const widgetId = watchPointerState.widgetId
  if (!watchPointerState.mode || !widgetId) return

  const dx = event.clientX - watchPointerState.startX
  const dy = event.clientY - watchPointerState.startY
  const start = watchPointerState.startLayout || { x: 0, y: 0, w: 0, h: 0 }
  const boardRect = watchBoardRef.value?.getBoundingClientRect()
  const boardWidth = Math.max(0, Number(boardRect?.width || 0))
  const snapLayouts = getSnapLayouts(widgetId)

  if (watchPointerState.mode === 'drag') {
    const maxX = Math.max(0, boardWidth - start.w)
    const xTargets = [0, maxX]
    const yTargets = [0]
    snapLayouts.forEach((layout) => {
      const left = Number(layout.x || 0)
      const right = Number(layout.x || 0) + Number(layout.w || 0)
      const top = Number(layout.y || 0)
      const bottom = Number(layout.y || 0) + Number(layout.h || 0)
      xTargets.push(left, right, left - start.w, right - start.w)
      yTargets.push(top, bottom, top - start.h, bottom - start.h)
    })
    const rawX = clampNumber(start.x + dx, 0, maxX)
    const rawY = Math.max(0, start.y + dy)
    patchWatchWidgetLayout(widgetId, {
      x: alignLayoutPixel(clampNumber(snapCoordinate(rawX, xTargets), 0, maxX)),
      y: alignLayoutPixel(Math.max(0, snapCoordinate(rawY, yTargets)))
    })
    return
  }

  if (watchPointerState.mode === 'resize') {
    const maxWidth = Math.max(WATCH_WIDGET_MIN_WIDTH, boardWidth - start.x)
    const rightTargets = [Number(start.x || 0) + Number(maxWidth || 0)]
    const bottomTargets = []
    snapLayouts.forEach((layout) => {
      const left = Number(layout.x || 0)
      const right = Number(layout.x || 0) + Number(layout.w || 0)
      const top = Number(layout.y || 0)
      const bottom = Number(layout.y || 0) + Number(layout.h || 0)
      rightTargets.push(left, right)
      bottomTargets.push(top, bottom)
    })
    const rawWidth = clampNumber(start.w + dx, WATCH_WIDGET_MIN_WIDTH, maxWidth)
    const rawHeight = clampNumber(start.h + dy, WATCH_WIDGET_MIN_HEIGHT, 1800)
    const rawRight = Number(start.x || 0) + Number(rawWidth || 0)
    const rawBottom = Number(start.y || 0) + Number(rawHeight || 0)
    const snappedRight = snapCoordinate(rawRight, rightTargets)
    const snappedBottom = snapCoordinate(rawBottom, bottomTargets)
    patchWatchWidgetLayout(widgetId, {
      w: alignLayoutPixel(clampNumber(snappedRight - start.x, WATCH_WIDGET_MIN_WIDTH, maxWidth)),
      h: alignLayoutPixel(clampNumber(snappedBottom - start.y, WATCH_WIDGET_MIN_HEIGHT, 1800))
    })
  }
}

const startWatchWidgetDrag = (widget, event) => {
  if (!watchLayoutEditMode.value || !widget?.id) return
  if (event?.target?.closest && event.target.closest('button')) return
  const layout = normalizeWidgetLayout(widget.layout, 0, widget.type || '')
  watchPointerState.mode = 'drag'
  watchPointerState.widgetId = widget.id
  watchPointerState.startX = event.clientX
  watchPointerState.startY = event.clientY
  watchPointerState.startLayout = { ...layout }
  watchDraggingWidgetId.value = widget.id
  watchResizingWidgetId.value = ''
  document.addEventListener('mousemove', onWatchWidgetInteractMove)
  document.addEventListener('mouseup', stopWatchWidgetInteract)
}

const startWatchWidgetResize = (widget, event) => {
  if (!watchLayoutEditMode.value || !widget?.id) return
  const layout = normalizeWidgetLayout(widget.layout, 0, widget.type || '')
  watchPointerState.mode = 'resize'
  watchPointerState.widgetId = widget.id
  watchPointerState.startX = event.clientX
  watchPointerState.startY = event.clientY
  watchPointerState.startLayout = { ...layout }
  watchResizingWidgetId.value = widget.id
  watchDraggingWidgetId.value = ''
  document.addEventListener('mousemove', onWatchWidgetInteractMove)
  document.addEventListener('mouseup', stopWatchWidgetInteract)
}

const removeWatchWidget = async (widgetId) => {
  if (!watchLayoutEditMode.value || !widgetId) return
  watchWidgetDefs.value = watchWidgetDefs.value.filter(item => item.id !== widgetId)
  watchWidgets.value = watchWidgets.value.filter(item => item.id !== widgetId)
  await saveWatchWidgetConfig({ silent: false })
}

const loadWatchlistWidgetRows = async (stockItems = [], maxCount = 50) => {
  const normalizedStockItems = Array.isArray(stockItems)
    ? stockItems
      .map(item => ({
        code: normalizeStockCode(item?.code || ''),
        name: String(item?.name || '').trim()
      }))
      .filter(item => !!item.code)
    : []
  const normalizedCodes = [...new Set(normalizedStockItems.map(item => item.code))]
  if (!normalizedCodes.length) return []

  const selectedCodes = normalizedCodes.slice(0, clampNumber(Number(maxCount || 50) || 50, 1, 200))
  const rows = await Promise.all(selectedCodes.map(async (code) => {
    try {
      const res = await ApiService.getStockKline(code, 5, null, 'data')
      const klineRows = res?.data?.data?.kline_data || []
      if (!Array.isArray(klineRows) || !klineRows.length) {
        return {
          code,
          name: code,
          latest_price: 0,
          change_pct: 0,
          pct5: 0,
          pct10: 0,
          amount: 0
        }
      }
      const latest = klineRows[klineRows.length - 1] || {}
      const prev = klineRows[klineRows.length - 2] || {}
      const back5 = klineRows[Math.max(0, klineRows.length - 6)] || {}
      const back10 = klineRows[Math.max(0, klineRows.length - 11)] || {}
      const latestPrice = Number(latest.close || 0) || 0
      const prevClose = Number(prev.close || latest.open || 0) || 0
      const base5 = Number(back5.close || back5.open || 0) || 0
      const base10 = Number(back10.close || back10.open || 0) || 0
      const changePct = prevClose ? ((latestPrice - prevClose) / prevClose) * 100 : 0
      const pct5 = base5 ? ((latestPrice - base5) / base5) * 100 : 0
      const pct10 = base10 ? ((latestPrice - base10) / base10) * 100 : 0
      const amount = Number(latest.amount || 0) || 0
      const named = normalizedStockItems.find(item => item.code === code)
      const cached = keyWatchlist.value.find(item => item.code === code)
      return {
        code,
        name: named?.name || cached?.name || code,
        latest_price: latestPrice,
        change_pct: changePct,
        pct5,
        pct10,
        amount
      }
    } catch (error) {
      return {
        code,
        name: code,
        latest_price: 0,
        change_pct: 0,
        pct5: 0,
        pct10: 0,
        amount: 0
      }
    }
  }))
  return rows
}

const pickChartHtml = (res, key = '') => {
  const data = res?.data || {}
  if (typeof data?.chart_html === 'string' && data.chart_html) return data.chart_html
  if (typeof data?.data?.chart_html === 'string' && data.data.chart_html) return data.data.chart_html
  if (key) {
    const top = data?.[key]?.chart_html
    if (typeof top === 'string' && top) return top
    const nested = data?.data?.[key]?.chart_html
    if (typeof nested === 'string' && nested) return nested
  }
  return ''
}

const renderWatchWidget = async (widget, sentimentCache = null) => {
  const source = widget && typeof widget === 'object' ? widget : {}
  try {
    if (source.type === 'market_sentiment_chart') {
      const daysBack = clampNumber(Number(source.params?.days_back || 30) || 30, 10, 240)
      const chartKey = String(source.params?.chart_key || 'red_ratio_and_amount').trim() || 'red_ratio_and_amount'
      let charts = sentimentCache && sentimentCache.get(daysBack)
      if (!charts) {
        const res = await ApiService.getMarketSentimentCharts(null, daysBack)
        charts = res.data?.charts || {}
        if (sentimentCache) sentimentCache.set(daysBack, charts)
      }
      const chartHtml = charts?.[chartKey] || ''
      return {
        ...source,
        title: source.title || getSentimentChartTitle(chartKey),
        chartHtml: chartHtml || '<div>暂无情绪图表</div>',
        error: chartHtml ? '' : `未找到图表: ${chartKey}`
      }
    }

    if (source.type === 'analysis_kline') {
      const target = getAnalysisTarget(source.params || {})
      const analysisTitle = getAnalysisWidgetTitle(source.params || {})
      if (target.type === 'stock') {
        const res = await ApiService.getStockKline(target.value, target.days, null, 'chart')
        const chartHtml = pickChartHtml(res, target.value)
        return {
          ...source,
          title: analysisTitle,
          chartHtml: chartHtml || '<div>暂无个股图表</div>',
          error: chartHtml ? '' : '个股图表为空'
        }
      }
      if (target.type === 'index') {
        const res = await ApiService.getIndexKlineChart(target.value, target.days)
        const chartHtml = pickChartHtml(res, target.value)
        return {
          ...source,
          title: analysisTitle,
          chartHtml: chartHtml || '<div>暂无指数图表</div>',
          error: chartHtml ? '' : '指数图表为空'
        }
      }
      const res = await ApiService.getSingleSectorKline(target.value, {
        days_range: target.days,
        format: 'chart'
      })
      const chartHtml = pickChartHtml(res, target.value)
      return {
        ...source,
        title: analysisTitle,
        chartHtml: chartHtml || '<div>暂无板块图表</div>',
        error: chartHtml ? '' : '板块图表为空'
      }
    }

    if (source.type === 'index_kline') {
      const indexName = source.params?.index_name || '上证指数'
      const daysRange = source.params?.days_range || 60
      const res = await ApiService.getIndexKlineChart(indexName, daysRange)
      const chartHtml = pickChartHtml(res, indexName)
      return {
        ...source,
        title: source.title || `${indexName} K线`,
        chartHtml: chartHtml || '<div>暂无指数图表</div>',
        error: chartHtml ? '' : '指数图表为空'
      }
    }

    if (source.type === 'sector_kline') {
      const sectorName = String(source.params?.sector_name || '半导体').trim() || '半导体'
      const daysRange = clampNumber(Number(source.params?.days_range || 60) || 60, 20, 500)
      const res = await ApiService.getSingleSectorKline(sectorName, {
        days_range: daysRange,
        format: 'chart'
      })
      const chartHtml = pickChartHtml(res, sectorName)
      return {
        ...source,
        title: source.title || `${sectorName} K线`,
        chartHtml: chartHtml || '<div>暂无板块图表</div>',
        error: chartHtml ? '' : '板块图表为空'
      }
    }

    if (source.type === 'stock_kline') {
      const stockCode = normalizeStockWidgetCode(source.params?.stock_code || '000001')
      const days = clampNumber(Number(source.params?.days || 120) || 120, 20, 500)
      const res = await ApiService.getStockKline(stockCode, days, null, 'chart')
      const chartHtml = pickChartHtml(res, stockCode)
      return {
        ...source,
        title: source.title || `${stockCode} K线`,
        chartHtml: chartHtml || '<div>暂无个股图表</div>',
        error: chartHtml ? '' : '个股图表为空'
      }
    }

    if (source.type === 'watchlist_panel') {
      const groupId = String(source.params?.group_id || '').trim()
      const maxCount = clampNumber(Number(source.params?.max_count || 50) || 50, 1, 10000)
      const columns = normalizeWidgetParams('watchlist_panel', source.params || {}).columns
      const targetGroup = groupId
        ? watchlistGroups.value.find(item => item.id === groupId)
        : (watchlistGroups.value[0] || null)
      const groupItems = Array.isArray(targetGroup?.items) ? targetGroup.items : []
      const stockItems = groupItems.filter(item => item?.type === 'stock')
      const stockRows = await loadWatchlistWidgetRows(stockItems, maxCount)
      const stockMap = new Map(stockRows.map(item => [item.code, item]))
      const watchlistRows = groupItems.slice(0, maxCount).map(item => {
        if (item.type === 'stock') {
          const code = normalizeStockCode(item.code)
          const enriched = stockMap.get(code)
          return {
            type: 'stock',
            code,
            name: item.name || enriched?.name || code,
            latest_price: enriched?.latest_price || 0,
            change_pct: enriched?.change_pct || 0,
            pct5: enriched?.pct5 || 0,
            pct10: enriched?.pct10 || 0,
            amount: enriched?.amount || 0
          }
        }
        return {
          type: item.type === 'index' ? 'index' : 'sector',
          code: '--',
          name: item.name || '--',
          latest_price: 0,
          change_pct: 0,
          pct5: 0,
          pct10: 0,
          amount: 0
        }
      })
      return {
        ...source,
        title: source.title || `${targetGroup?.name || '自选股列表'}`,
        chartHtml: '',
        columns,
        watchlistGroups: watchlistGroups.value.map(group => ({ id: group.id, name: group.name })),
        watchlistRows,
        watchlistMeta: {
          group_id: String(targetGroup?.id || ''),
          group_name: String(targetGroup?.name || '默认自选组'),
          total: groupItems.length,
          loaded: watchlistRows.length
        },
        error: ''
      }
    }

    if (source.type === 'daily_replay') {
      const params = normalizeWidgetParams('daily_replay', source.params || {})
      return {
        ...source,
        title: source.title || '每日复盘',
        chartHtml: '',
        replayRequirement: String(params.requirement || '').trim(),
        error: ''
      }
    }

    if (source.type === 'market_volume') {
      const res = await ApiService.getMarketVolume()
      const chartHtml = res?.data?.chart_html || res?.data?.data?.chart_html
      return {
        ...source,
        title: source.title || '市场量能对比',
        chartHtml: chartHtml || '<div>暂无量能图表</div>',
        error: chartHtml ? '' : '量能图表为空'
      }
    }

    return {
      ...source,
      title: source.title || '未识别图表',
      chartHtml: '<div>暂不支持该图表类型</div>',
      error: source.type ? `未支持的类型: ${source.type}` : '未支持的图表类型'
    }
  } catch (error) {
    console.error(error)
    return {
      ...source,
      title: source.title || '图表加载失败',
      chartHtml: '<div>图表加载失败</div>',
      error: '图表加载失败'
    }
  }
}

const addWidgetFromToolTemplate = async (template, options = {}) => {
  const strategy = activeStrategy.value
  if (!strategy?.id || !template?.id) return

  const type = String(template.type || '').trim() || 'market_sentiment_chart'
  const draftDefaults = { ...(template.defaults || {}) }
  if (type === 'watchlist_panel') {
    draftDefaults.group_id = String(activeWatchlistGroupId.value || watchlistGroups.value[0]?.id || '').trim()
  }
  const baseParams = normalizeWidgetParams(type, draftDefaults)
  const title = buildWidgetTitle(type, baseParams)
  const preferredLayout = options?.layout && typeof options.layout === 'object'
    ? normalizeWidgetLayout(options.layout, watchWidgetDefs.value.length, type)
    : getNextWidgetLayout(type)
  const nextWidget = {
    id: genWidgetId(),
    type,
    title,
    params: baseParams,
    layout: preferredLayout
  }

  const previousDefs = watchWidgetDefs.value
  watchWidgetDefs.value = [...previousDefs, nextWidget]
  const shouldSilent = options?.silent === true
  const saved = await saveWatchWidgetConfig({ silent: shouldSilent })
  if (!saved) {
    watchWidgetDefs.value = previousDefs
    return
  }

  const rendered = await renderWatchWidget(nextWidget)
  watchWidgets.value = [...watchWidgets.value, rendered]
}

const loadWatchWidgets = async () => {
  const strategy = activeStrategy.value
  watchError.value = ''
  watchWidgets.value = []
  watchWidgetDefs.value = []
  if (!strategy || !activeMode.value || activeMode.value !== 'strategy_analysis') {
    widgetEditorVisible.value = false
    watchlistGroups.value = normalizeWatchlistGroups([])
    ensureActiveWatchlistGroup()
    return
  }
  hydrateWatchlistGroupsFromStrategy(strategy)

  if (isKeyLevelStrategyConfig(strategy)) {
    widgetEditorVisible.value = false
    watchLayoutEditMode.value = false
    watchLoading.value = false
    hydrateKeyBoardFromStrategy(strategy)
    if (keySelectedCode.value) {
      await loadKeyStockKline(keySelectedCode.value)
    } else {
      keyKlineData.value = []
      keyLevels.value = []
      keyWatchError.value = ''
    }
    return
  }

  const widgetDefs = buildWidgetsFromStrategy(strategy)
  watchWidgetDefs.value = widgetDefs.map(item => ({
    ...item,
    params: { ...(item.params || {}) },
    layout: { ...(item.layout || {}) }
  }))
  if (!widgetDefs.length) return

  watchLoading.value = true
  try {
    const results = []
    const sentimentCache = new Map()
    for (const widget of widgetDefs) {
      const rendered = await renderWatchWidget(widget, sentimentCache)
      results.push(rendered)
    }
    watchWidgets.value = results
  } catch (error) {
    console.error(error)
    watchError.value = '图表加载失败，请稍后重试或检查后端日志。'
  } finally {
    watchLoading.value = false
  }
}

const generateStrategyView = async () => {
  if (!activeStrategy.value) return
  try {
    const { value } = await ElMessageBox.prompt('描述你要的看盘视图目标', 'Agent 生成视图', {
      confirmButtonText: '生成',
      cancelButtonText: '取消',
      inputPlaceholder: '例如：重点观察市场情绪、连板梯队与指数趋势'
    })
    const goal = (value || '').trim()
    if (!goal) return
    await ApiService.generateStrategyWatchView(activeStrategy.value.id, { goal })
    await loadStrategies()
    await loadWatchWidgets()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const switchToChat = () => {
  activeMode.value = 'dialog'
}

const handleFileChange = (_file, fileList) => {
  pendingUploadFiles.value = fileList || []
}

const uploadPendingFiles = async () => {
  if (!pendingUploadFiles.value.length) {
    ElMessage.warning('请先选择要上传的文件')
    return
  }

  const formData = new FormData()
  pendingUploadFiles.value.forEach(item => {
    if (item.raw) formData.append('files', item.raw)
  })
  formData.append('whisper_model', 'base')
  formData.append('group_name', (uploadGroupName.value || '').trim())
  formData.append('strategy_id', (activeStrategyId.value || '').trim())

  uploading.value = true
  try {
    const res = await ApiService.uploadStrategyResources(formData)
    const uploadedCount = res.data?.uploaded?.length || 0
    const rejectedCount = res.data?.rejected?.length || 0
    const jobs = res.data?.jobs || []
    if (jobs.length) {
      ElMessage.success(`上传成功：${uploadedCount} 个进入后台处理，失败 ${rejectedCount} 个。`)
      startJobPolling(jobs.map(item => item.job_id))
    } else {
      ElMessage.success(`上传完成：成功 ${uploadedCount} 个，失败 ${rejectedCount} 个。`)
    }
    pendingUploadFiles.value = []
    uploadGroupName.value = ''
    if (uploadRef.value) uploadRef.value.clearFiles()
    await loadResources()
  } catch (error) {
    console.error(error)
  } finally {
    uploading.value = false
  }
}

const renameSelectedGroup = async () => {
  if (!selectedGroupId.value) return
  const current = resourceGroups.value.find(g => g.group_id === selectedGroupId.value)
  if (!current) return
  try {
    const { value } = await ElMessageBox.prompt('请输入新的分组名称', '重命名分组', {
      inputValue: current.group_name || '',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const name = (value || '').trim()
    if (!name) return
    await ApiService.renameStrategyResourceGroup(selectedGroupId.value, name)
    await loadResources()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const renameManageGroup = async () => {
  if (!manageFilterGroupId.value) return
  const current = resourceGroups.value.find(g => g.group_id === manageFilterGroupId.value)
  if (!current) return
  try {
    const { value } = await ElMessageBox.prompt('请输入新的分组名称', '重命名分组', {
      inputValue: current.group_name || '',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const name = (value || '').trim()
    if (!name) return
    await ApiService.renameStrategyResourceGroup(manageFilterGroupId.value, name)
    await loadResources()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const createManageGroup = async () => {
  try {
    const { value } = await ElMessageBox.prompt('请输入新分组名称', '新建分组', {
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const name = (value || '').trim()
    if (!name) return
    await ApiService.createStrategyResourceGroup(name)
    await loadResources()
    const created = resourceGroups.value.find(g => g.group_name === name)
    manageFilterGroupId.value = created?.group_id || ''
    ElMessage.success('分组已创建')
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const openGroupTransferDialog = () => {
  if (!selectedGroupId.value) return
  groupTransferForm.value = {
    source_group_id: selectedGroupId.value,
    target_group_id: '',
    target_group_name: '',
    mode: 'move'
  }
  groupTransferVisible.value = true
}

const submitGroupTransfer = async () => {
  const payload = {
    source_group_id: groupTransferForm.value.source_group_id,
    target_group_id: groupTransferForm.value.target_group_id || '',
    target_group_name: (groupTransferForm.value.target_group_name || '').trim(),
    mode: groupTransferForm.value.mode
  }
  if (!payload.source_group_id) {
    ElMessage.warning('请选择源分组')
    return
  }
  if (!payload.target_group_id && !payload.target_group_name) {
    ElMessage.warning('请选择或输入目标分组')
    return
  }
  try {
    await ApiService.transferStrategyResourceGroup(payload)
    ElMessage.success('分组操作已完成')
    groupTransferVisible.value = false
    await loadResources()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const openResourceManageDialog = () => {
  manageFilterGroupId.value = selectedGroupId.value || ''
  manageKeyword.value = ''
  manageTargetType.value = 'existing'
  manageTargetGroupId.value = ''
  manageTargetGroupName.value = ''
  manageMode.value = 'move'
  manageSelectedIds.value = []
  resourceManageVisible.value = true
}

const resolveManageTransferTarget = () => {
  if (manageTargetType.value === 'existing') {
    const gid = String(manageTargetGroupId.value || '').trim()
    if (!gid) {
      ElMessage.warning('请选择目标分组')
      return null
    }
    return { target_group_id: gid, target_group_name: '' }
  }
  const name = String(manageTargetGroupName.value || '').trim()
  if (!name) {
    ElMessage.warning('请输入新分组名称')
    return null
  }
  return { target_group_id: '', target_group_name: name }
}

const transferResources = async (resourceIds, targetGroupId, targetGroupName, mode = 'move') => {
  const payload = {
    resource_ids: resourceIds,
    target_group_id: targetGroupId || '',
    target_group_name: (targetGroupName || '').trim(),
    mode
  }
  if (!payload.resource_ids?.length) {
    ElMessage.warning('请选择要操作的资料')
    return false
  }
  if (!payload.target_group_id && !payload.target_group_name) {
    ElMessage.warning('请选择或输入目标分组')
    return false
  }
  try {
    await ApiService.transferStrategyResources(payload)
    await loadResources()
    return true
  } catch (error) {
    if (error !== 'cancel') console.error(error)
    return false
  }
}

const renameResource = async (resource) => {
  if (!resource?.id) return
  try {
    const { value } = await ElMessageBox.prompt('请输入新的资料名称', '重命名资料', {
      inputValue: resource.original_name || '',
      confirmButtonText: '确定',
      cancelButtonText: '取消'
    })
    const name = (value || '').trim()
    if (!name) return
    await ApiService.renameStrategyResource(resource.id, name)
    await loadResources()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const submitResourceTransfer = async () => {
  const target = resolveManageTransferTarget()
  if (!target) return
  const ok = await transferResources(
    manageSelectedIds.value,
    target.target_group_id,
    target.target_group_name,
    manageMode.value
  )
  if (ok) {
    ElMessage.success('资料库操作已完成')
    manageSelectedIds.value = []
  }
}

const quickTransferResource = async (resource) => {
  if (!resource?.id) return
  const target = resolveManageTransferTarget()
  if (!target) return
  const ok = await transferResources(
    [resource.id],
    target.target_group_id,
    target.target_group_name,
    manageMode.value
  )
  if (ok) ElMessage.success('资料已更新')
}

const deleteManageGroup = async () => {
  const gid = String(manageFilterGroupId.value || '').trim()
  if (!gid) {
    ElMessage.warning('请先选择分组')
    return
  }

  const current = manageCurrentGroup.value
  const groupName = current?.group_name || gid
  let impactText = ''
  try {
    const impactRes = await ApiService.getStrategyResourceGroupImpact(gid)
    const impact = impactRes.data || {}
    impactText = `资料 ${Number(impact.resource_count || 0)} 条，关联策略 ${Number(impact.strategy_count || 0)} 个，长期记忆订阅 ${Number(impact.memory_subscription_profile_count || 0)} 个。`
  } catch (error) {
    console.error(error)
  }

  try {
    await ElMessageBox.confirm(
      `确定删除分组“${groupName}”？\n处理方式：资料转入“未分组”后删除该分组。\n${impactText}`.trim(),
      '删除分组',
      {
        type: 'warning',
        confirmButtonText: '确认删除',
        cancelButtonText: '取消'
      }
    )
    await ApiService.deleteStrategyResourceGroup(gid, {
      action: 'move_to_default',
      remove_subscriptions: false
    })
    if (selectedGroupId.value === gid) selectedGroupId.value = ''
    manageFilterGroupId.value = ''
    manageSelectedIds.value = []
    await loadResources()
    ElMessage.success('分组删除完成')
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const batchDeleteResources = async () => {
  if (!manageSelectedIds.value.length) return
  try {
    await ElMessageBox.confirm(
      `确定删除选中的 ${manageSelectedIds.value.length} 条资料吗？删除后无法恢复。`,
      '批量删除',
      {
        type: 'warning',
        confirmButtonText: '删除',
        cancelButtonText: '取消'
      }
    )
    const ids = [...manageSelectedIds.value]
    for (const id of ids) {
      await ApiService.deleteStrategyResource(id)
    }
    if (ids.includes(activeResourceId.value)) {
      activeResourceId.value = ''
    }
    manageSelectedIds.value = []
    await loadResources()
    ElMessage.success('删除完成')
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const onResourceDragStart = (resource) => {
  dragResourceId.value = resource?.id || ''
}

const onResourceDragEnd = () => {
  dragResourceId.value = ''
}

const handleDropToGroup = async (group) => {
  if (!dragResourceId.value || !group?.group_id) return
  const ok = await transferResources([dragResourceId.value], group.group_id, group.group_name, 'move')
  if (ok) ElMessage.success(`已移动到 ${group.group_name}`)
  dragResourceId.value = ''
}

const switchActiveResource = async (resourceId) => {
  await ApiService.setActiveStrategyResource(resourceId)
  activeResourceId.value = resourceId
}

const toggleResourceSelection = (resourceId) => {
  const idx = selectedResourceIds.value.indexOf(resourceId)
  if (idx >= 0) {
    selectedResourceIds.value.splice(idx, 1)
  } else {
    selectedResourceIds.value.push(resourceId)
  }
}

const deleteResource = async (resourceId) => {
  try {
    await ApiService.deleteStrategyResource(resourceId)
    if (activeResourceId.value === resourceId) {
      activeResourceId.value = ''
    }
    await loadResources()
  } catch (error) {
    if (error !== 'cancel') console.error(error)
  }
}

const extractApiErrorMessage = async (error, fallback = '操作失败') => {
  const respData = error?.response?.data
  if (respData) {
    if (typeof respData === 'object' && !(respData instanceof Blob)) {
      return respData.message || respData.error || fallback
    }
    if (typeof Blob !== 'undefined' && respData instanceof Blob) {
      try {
        const text = await respData.text()
        if (text) {
          try {
            const parsed = JSON.parse(text)
            return parsed?.message || parsed?.error || text || fallback
          } catch {
            return text || fallback
          }
        }
      } catch {
        // ignore blob parse errors and fallback below
      }
    }
    if (typeof respData === 'string') {
      try {
        const parsed = JSON.parse(respData)
        return parsed?.message || parsed?.error || respData || fallback
      } catch {
        return respData || fallback
      }
    }
  }
  return error?.message || fallback
}

const downloadResourceMarkdown = async (resource) => {
  if (!resource?.id) return
  try {
    const blob = await ApiService.downloadStrategyResourceFile(resource.id, 'markdown')
    const baseName = (resource.original_name || resource.id)
      .replace(/[\\/:*?"<>|]+/g, '_')
      .replace(/\s+/g, ' ')
      .trim()
    const filename = `${baseName || resource.id}.md`
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  } catch (error) {
    console.error(error)
  }
}

const downloadResourceAiSummary = async (resource) => {
  if (!resource?.id) return
  try {
    const blob = await ApiService.downloadStrategyResourceAiSummary(resource.id)
    const baseName = (resource.original_name || resource.id)
      .replace(/[\\/:*?"<>|]+/g, '_')
      .replace(/\s+/g, ' ')
      .trim()
    const filename = `${baseName || resource.id}__ai_summary.md`
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  } catch (error) {
    console.error(error)
    ElMessage.error(await extractApiErrorMessage(error, '下载原始 Markdown 失败'))
  }
}

const buildSafeFilename = (name, ext) => {
  const base = (name || 'strategy_chat')
    .replace(/[\\/:*?"<>|]+/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
  return `${base || 'strategy_chat'}.${ext}`
}

const downloadCrawl = async (item, format) => {
  const relpath = format === 'pdf'
    ? item.pdf_relpath
    : format === 'docx'
      ? item.docx_relpath
      : item.markdown_relpath
  if (!relpath) {
    ElMessage.warning('该格式文件不存在')
    return
  }
  try {
    const blob = await ApiService.downloadCrawledFile(relpath)
    const filename = buildSafeFilename(item.title || item.url || 'wechat_article', format)
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  } catch (error) {
    console.error(error)
    ElMessage.error(await extractApiErrorMessage(error, '下载 AI 总结失败'))
  }
}

const buildDownloadOptions = (msg) => {
  const options = []
  const items = getCrawlResults(msg)
  items.forEach((item, idx) => {
    const title = item.title || item.url || `抓取文章${idx + 1}`
    if (item.markdown_relpath) {
      options.push({ key: `${idx}-md`, label: `${title} - MD`, item, format: 'md' })
    }
    if (item.pdf_relpath) {
      options.push({ key: `${idx}-pdf`, label: `${title} - PDF`, item, format: 'pdf' })
    }
    if (item.docx_relpath) {
      options.push({ key: `${idx}-docx`, label: `${title} - Word`, item, format: 'docx' })
    }
  })
  return options
}

const handleDownloadCommand = (option) => {
  if (!option?.item || !option?.format) return
  downloadCrawl(option.item, option.format)
}

const copyTextToClipboard = async (text) => {
  if (!text) {
    ElMessage.warning('没有可复制的内容')
    return
  }
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text)
    } else {
      const textarea = document.createElement('textarea')
      textarea.value = text
      textarea.setAttribute('readonly', 'true')
      textarea.style.position = 'absolute'
      textarea.style.left = '-9999px'
      document.body.appendChild(textarea)
      textarea.select()
      document.execCommand('copy')
      document.body.removeChild(textarea)
    }
    ElMessage.success('已复制到剪贴板')
  } catch (error) {
    console.error(error)
    ElMessage.error('复制失败，请手动复制')
  }
}

const copyMessage = async (msg) => {
  const roleLabel = msg?.role === 'user' ? '我' : '助手'
  const time = formatDateTime(msg?.created_at)
  const content = (msg?.content || '').trim()
  const text = `[${roleLabel} ${time}]\n${content}`
  await copyTextToClipboard(text)
}

const renderMarkdown = (content) => {
  const normalized = String(content || '')
    .split('\n')
    .filter(line => line.trim() !== '')
    .join('\n')

  marked.setOptions({
    gfm: true,
    breaks: false,
    pedantic: false,
    smartLists: true,
    smartypants: false
  })

  const renderer = new marked.Renderer()

  renderer.listitem = (text) => {
    if (String(text || '').includes('</p>')) {
      return `<li>${text}</li>`
    }
    return `<li>${String(text || '').trim()}</li>\n`
  }

  renderer.paragraph = (text) => `<p>${text}</p>`

  let html = marked.parse(normalized, { renderer })

  html = DOMPurify.sanitize(html, {
    ALLOWED_TAGS: ['ul', 'ol', 'li', 'p', 'br', 'strong', 'em', 'code', 'pre'],
    ALLOWED_ATTR: ['class', 'id']
  })

  return html
}

const downloadConversation = async () => {
  if (!messages.value.length) {
    ElMessage.warning('当前对话没有内容可下载')
    return
  }
  const title = (activeConversationTitle.value || '策略对话')
    .replace(/[\\/:*?"<>|]+/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
  const lines = messages.value.map(item => {
    const roleLabel = item.role === 'user' ? '我' : '助手'
    const time = formatDateTime(item.created_at)
    const content = (item.content || '').trim()
    return `【${roleLabel} ${time}】\n${content}`
  })
  const content = lines.join('\n\n')
  const filename = `${title || '策略对话'}_${new Date().toISOString().slice(0, 10)}.txt`
  const blob = new Blob([content], { type: 'text/plain;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
  URL.revokeObjectURL(url)
}

const prettifyEventType = (eventType) => {
  const text = String(eventType || '').trim()
  if (!text) return '处理中'
  return text.replace(/_/g, ' ')
}

const formatStrategyEditProgressTitle = (item) => {
  const eventType = String(item?.event_type || '').trim()
  const payload = item?.payload && typeof item.payload === 'object' ? item.payload : {}
  const agentName = String(payload.agent_name || '').trim()
  const stageName = String(payload.stage_name || '').trim()
  const taskId = String(payload.task_id || '').trim()
  const taskTitle = String(payload.task_title || '').trim()
  if (eventType === 'agent_tool_start') {
    return `调用工具：${payload.tool_name || 'unknown'}${agentName ? `（${agentName}）` : ''}`
  }
  if (eventType === 'agent_tool_done') {
    return `工具完成：${payload.tool_name || 'unknown'}${agentName ? `（${agentName}）` : ''}`
  }
  if (eventType === 'agent_invoke_start') return '开始推理'
  if (eventType === 'agent_invoke_done') return '推理完成'
  if (eventType === 'agent_request_start') return '发起模型请求'
  if (eventType === 'agent_tools_bound') return '已绑定工具'
  if (eventType === 'orchestration_stage_start') {
    const suffix = [agentName, stageName, taskId].filter(Boolean).join(' · ')
    return `子代理启动${suffix ? `：${suffix}` : ''}`
  }
  if (eventType === 'orchestration_stage_done') {
    const suffix = [agentName, stageName, taskId].filter(Boolean).join(' · ')
    return `子代理完成${suffix ? `：${suffix}` : ''}`
  }
  if (eventType === 'orchestration_stage_error') {
    const suffix = [agentName, stageName, taskId].filter(Boolean).join(' · ')
    return `子代理异常${suffix ? `：${suffix}` : ''}`
  }
  if (eventType === 'task_execution_start') {
    const suffix = [agentName, taskId, taskTitle].filter(Boolean).join(' · ')
    return `执行任务${suffix ? `：${suffix}` : ''}`
  }
  if (eventType === 'task_execution_done') {
    const suffix = [agentName, taskId, taskTitle].filter(Boolean).join(' · ')
    return `任务完成${suffix ? `：${suffix}` : ''}`
  }
  if (eventType === 'conversation_agent_done') return '结果生成完成'
  return `进度：${prettifyEventType(eventType)}`
}

const appendStrategyEditProgress = (title, key = '') => {
  const text = String(title || '').trim()
  if (!text || !strategyEditRun.value.active) return
  const dedupeKey = key || text
  if (strategyEditRun.value.seenKeys?.[dedupeKey]) return
  strategyEditRun.value.seenKeys = {
    ...(strategyEditRun.value.seenKeys || {}),
    [dedupeKey]: true
  }
  strategyEditRun.value.steps = [...(strategyEditRun.value.steps || []), text]
}

const stopStrategyEditRunPoll = () => {
  if (strategyEditRunPollTimer.value) {
    clearInterval(strategyEditRunPollTimer.value)
    strategyEditRunPollTimer.value = null
  }
}

const stopStrategyEditRun = () => {
  stopStrategyEditRunPoll()
  strategyEditRun.value = {
    active: false,
    question: '',
    conversationId: '',
    assistantMessageId: '',
    startedAt: '',
    bucket: '',
    steps: [],
    seenKeys: {}
  }
}

const pollStrategyEditRunProgress = async () => {
  if (!strategyEditRun.value.active) return
  try {
    const params = {
      limit: 120
    }
    const bucket = strategyEditRun.value.bucket || activeConversationTitle.value
    if (bucket) params.bucket = bucket
    const res = await ApiService.getStrategyAgentLogs(params)
    const rows = Array.isArray(res?.data) ? [...res.data].reverse() : []
    rows.forEach(item => {
      const payload = item?.payload && typeof item.payload === 'object' ? item.payload : {}
      const itemConvId = String(payload.conversation_id || '').trim()
      if (strategyEditRun.value.conversationId && itemConvId && itemConvId !== strategyEditRun.value.conversationId) return
      const key = `${item?.timestamp || ''}|${item?.event_type || ''}|${payload.tool_name || ''}`
      appendStrategyEditProgress(formatStrategyEditProgressTitle(item), key)
    })
    if (strategyEditRun.value.steps?.length) {
      scrollMessagesToBottom()
    }
  } catch (error) {
    console.error(error)
  }
}

const startStrategyEditRun = (questionText) => {
  stopStrategyEditRun()
  strategyEditRun.value = {
    active: true,
    question: String(questionText || '').trim(),
    conversationId: activeConversationId.value || '',
    assistantMessageId: '',
    startedAt: new Date().toISOString(),
    bucket: activeConversationTitle.value || '',
    steps: ['策略编辑师已接收问题', '正在调用工具...'],
    seenKeys: {
      init_accepted: true,
      init_running: true
    }
  }
  pollStrategyEditRunProgress()
  strategyEditRunPollTimer.value = setInterval(pollStrategyEditRunProgress, 1200)
}

const shouldHideAssistantMessage = (msg) => {
  if (!msg || msg.role !== 'assistant') return false
  if (!strategyEditRun.value.active) return false
  if (String(activeMode.value || '') !== 'strategy_edit') return false
  const runAssistantId = String(strategyEditRun.value.assistantMessageId || '').trim()
  if (!runAssistantId || runAssistantId !== String(msg.id || '').trim()) return false
  return !String(msg.content || '').trim()
}

const runStrategyEditPrompt = async (text) => {
  const content = String(text || '').trim()
  if (!content) return false
  if (!activeConversationId.value) await createConversation()
  const payload = {
    content,
    resource_ids: selectedResourceIds.value,
    conversation_mode: 'strategy_edit',
    agent_name: MODE_AGENT_MAP.strategy_edit,
    model: selectedModel.value,
    strategy_id: activeStrategyId.value || '',
    memory_profile_id: activeMemoryProfileId.value || '',
    prompt_template_id: selectedPromptTemplateId.value,
    prompt_template: effectivePromptTemplate.value
  }

  sending.value = true
  try {
    startStrategyEditRun(content)
    let assistantRef = null
    await ApiService.streamStrategyMessage(activeConversationId.value, payload, {
      onMeta: (evt) => {
        strategyEditRun.value.conversationId = String(evt?.conversation_id || strategyEditRun.value.conversationId || '')
        strategyEditRun.value.bucket = activeConversationTitle.value || strategyEditRun.value.bucket || ''
        const userMessage = evt.user_message
        const assistantMessage = evt.assistant_message
        if (userMessage) messages.value.push(userMessage)
        if (assistantMessage) {
          messages.value.push(assistantMessage)
          assistantRef = messages.value[messages.value.length - 1]
          strategyEditRun.value.assistantMessageId = String(assistantMessage.id || '')
        }
        scrollMessagesToBottom()
      },
      onDelta: (evt) => {
        if (!assistantRef) return
        const textChunk = evt.text || ''
        if (textChunk) {
          assistantRef.content = (assistantRef.content || '') + textChunk
          stopStrategyEditRun()
        }
        scrollMessagesToBottom()
      },
      onDone: async () => {
        stopStrategyEditRun()
        await loadConversations()
        await loadResources()
        await scrollMessagesToBottom()
      },
      onError: (err) => {
        stopStrategyEditRun()
        if (err?.message) ElMessage.error(err.message)
      }
    })
    return true
  } catch (error) {
    console.error(error)
    ElMessage.error('复盘执行失败')
    return false
  } finally {
    sending.value = false
  }
}

const sendMessage = async () => {
  const text = (inputText.value || '').trim()
  if (!text) {
    ElMessage.warning('请输入内容后再发送')
    return
  }
  if (!activeConversationId.value) await createConversation()
  const isStrategyEditMode = activeMode.value === 'strategy_edit'

  const payload = {
    content: text,
    resource_ids: selectedResourceIds.value,
    conversation_mode: activeMode.value,
    agent_name: selectedAgentName.value,
    model: selectedModel.value,
    strategy_id: activeStrategyId.value || '',
    memory_profile_id: activeMemoryProfileId.value || '',
    prompt_template_id: selectedPromptTemplateId.value,
    prompt_template: effectivePromptTemplate.value
  }

  sending.value = true
  try {
    inputText.value = ''
    if (isStrategyEditMode) {
      startStrategyEditRun(text)
    }
    let assistantRef = null
    await ApiService.streamStrategyMessage(activeConversationId.value, payload, {
      onMeta: (evt) => {
        if (isStrategyEditMode) {
          strategyEditRun.value.conversationId = String(evt?.conversation_id || strategyEditRun.value.conversationId || '')
          strategyEditRun.value.bucket = activeConversationTitle.value || strategyEditRun.value.bucket || ''
        }
        const userMessage = evt.user_message
        const assistantMessage = evt.assistant_message
        if (userMessage) messages.value.push(userMessage)
        if (assistantMessage) {
          messages.value.push(assistantMessage)
          assistantRef = messages.value[messages.value.length - 1]
          if (isStrategyEditMode) {
            strategyEditRun.value.assistantMessageId = String(assistantMessage.id || '')
          }
        }
        scrollMessagesToBottom()
      },
      onDelta: (evt) => {
        if (!assistantRef) return
        const textChunk = evt.text || ''
        if (textChunk) {
          assistantRef.content = (assistantRef.content || '') + textChunk
          if (isStrategyEditMode) {
            stopStrategyEditRun()
          }
        }
        scrollMessagesToBottom()
      },
      onDone: async () => {
        if (isStrategyEditMode) {
          stopStrategyEditRun()
        }
        await loadConversations()
        await loadResources()
        await scrollMessagesToBottom()
      },
      onError: (err) => {
        if (isStrategyEditMode) {
          stopStrategyEditRun()
        }
        const message = typeof err === 'string' ? err : (err?.message || err?.error || '')
        if (assistantRef) {
          const suffix = message ? `\n\n${message}` : '\n\n流式输出失败'
          assistantRef.content = (assistantRef.content || '') + suffix
        } else {
          ElMessage.error(message || '流式输出失败')
        }
      }
    })
  } catch (error) {
    console.error(error)
    if (isStrategyEditMode) {
      stopStrategyEditRun()
    }
    const message = error?.message || error?.error || '发送失败，请检查网络与后端服务'
    ElMessage.error(message)
  } finally {
    sending.value = false
  }
}

onMounted(async () => {
  window.addEventListener('resize', handleKeyWatchWindowResize)
  loadPromptTemplatesFromStorage()
  try {
    await loadRuntime()
    await loadResources()
    await loadMemoryProfiles()
    await loadWatchlistIndexOptions()
    await loadConversations()
    if (!conversations.value.length) await createConversation()
    await loadStrategies()
    await loadWatchWidgets()
    await nextTick()
    updateKeyWatchViewport()
  } catch (error) {
    console.error(error)
  }
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleKeyWatchWindowResize)
  groupPopoverVisible.value = false
  promptTemplatePopoverVisible.value = false
  memoryManageVisible.value = false
  stopJobPolling()
  stopKeyPaneResize()
  stopWatchWidgetInteract()
  stopStrategyEditRun()
  if (watchPersistTimer.value) {
    clearTimeout(watchPersistTimer.value)
    watchPersistTimer.value = null
  }
  if (watchlistConfigStockSearchTimer.value) {
    clearTimeout(watchlistConfigStockSearchTimer.value)
    watchlistConfigStockSearchTimer.value = null
  }
})

watch(promptTemplates, () => {
  persistPromptTemplatesToStorage()
  if (!promptTemplates.value.some(item => item.id === selectedPromptTemplateId.value)) {
    selectedPromptTemplateId.value = 'none'
  }
}, { deep: true })

watch([activeStrategyId, activeMode], () => {
  syncAgentByMode()
  if (!isReplayChatMode.value) groupPopoverVisible.value = false
  if (!isReplayChatMode.value) promptTemplatePopoverVisible.value = false
  if (activeMode.value !== 'strategy_edit') {
    stopStrategyEditRun()
  }
  if (activeMode.value !== 'strategy_analysis') {
    watchLayoutEditMode.value = false
    stopWatchWidgetInteract()
  }
  loadWatchWidgets()
})

watch(watchLayoutEditMode, (nextVal) => {
  if (!nextVal) {
    stopWatchWidgetInteract()
  }
})

watch(watchlistGroups, () => {
  ensureActiveWatchlistGroup()
}, { deep: true })

watch(watchlistConfigStockQuery, (nextVal) => {
  if (!watchlistPanelConfigVisible.value || watchlistConfigActiveMenu.value !== 'add_stock') return
  if (watchlistConfigStockSearchTimer.value) {
    clearTimeout(watchlistConfigStockSearchTimer.value)
    watchlistConfigStockSearchTimer.value = null
  }
  const keyword = String(nextVal || '').trim()
  if (!keyword) {
    watchlistConfigStockCandidates.value = []
    watchlistConfigStockSearching.value = false
    return
  }
  watchlistConfigStockSearchTimer.value = setTimeout(() => {
    searchWatchlistConfigStocks()
  }, 180)
})

watch(keyWatchGroupFilterOptions, (options) => {
  if (Array.isArray(options) && options.some(item => item.value === keyWatchFilterGroup.value)) return
  keyWatchFilterGroup.value = '__all__'
})

watch([keyWatchGroupBy, keyWatchSortField, keyWatchSortOrder, keyWatchFilterGroup], () => {
  resetKeyWatchListScroll()
  nextTick(() => updateKeyWatchViewport())
})

watch(keyWatchFlatRows, () => {
  nextTick(() => updateKeyWatchViewport())
})

watch(
  [keyChartMode, keyAdjustMode, keySubIndicatorMode, keyWatchGroupBy, keyWatchSortField, keyWatchSortOrder],
  () => {
    if (keyBoardHydrating.value) return
    persistKeyBoardState()
  }
)

watch(memoryManageProfileId, async (nextId) => {
  memoryBindSelectedIds.value = []
  if (!nextId) {
    resetMemoryPortraitForm()
    memoryPreviewContext.value = ''
    return
  }
  await loadMemoryPortrait(nextId)
  await loadMemoryPreviewContext()
})

watch(memoryBindResources, () => {
  const valid = new Set(memoryBindResources.value.map(item => item.id))
  memoryBindSelectedIds.value = memoryBindSelectedIds.value.filter(id => valid.has(id))
})

watch(selectedModel, (nextVal) => {
  if (!selectedPortraitModel.value) {
    selectedPortraitModel.value = nextVal || ''
  }
})

watch(selectedAgentName, (nextVal) => {
  const mode = activeMode.value
  if (!mode || !nextVal) return
  modeAgentSelection.value[mode] = nextVal
})

watch(manageTargetType, (next) => {
  if (next === 'existing') {
    manageTargetGroupName.value = ''
  } else {
    manageTargetGroupId.value = ''
  }
})

watch(activeMemoryProfileId, async (nextId, prevId) => {
  if (nextId === prevId) return
  try {
    await ApiService.setActiveMemoryProfile(nextId || '')
  } catch (error) {
    console.error(error)
  }
})
</script>

<style scoped>
.strategy-watch-page {
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: 14px;
  overflow-y: auto;
  overflow-x: hidden;
}

.page-header {
  background: linear-gradient(120deg, #0f5f9c 0%, #2c8ca8 45%, #6db2a2 100%);
  color: #fff;
  border-radius: 12px;
  padding: 16px 18px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.page-header h2 {
  margin: 0 0 6px;
}

.page-header p {
  margin: 0;
  opacity: 0.9;
  font-size: 13px;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  justify-content: flex-end;
}

.mode-switch {
  display: flex;
  flex-wrap: wrap;
}

.mode-switch :deep(.el-radio-button__inner) {
  padding: 8px 16px;
}

.runtime-tag {
  white-space: nowrap;
}

.page-body {
  flex: 1;
  display: grid;
  grid-template-columns: 340px 1fr;
  gap: 14px;
  min-height: 0;
  overflow: hidden;
}

.left-panel {
  display: grid;
  grid-template-rows: 1fr 1fr;
  gap: 14px;
  min-height: 0;
}

.page-body.chat-sidebar-collapsed {
  grid-template-columns: 84px 1fr;
}

.left-panel.chat-sidebar-collapsed {
  grid-template-rows: auto;
}

.watch-body {
  grid-template-columns: 320px 1fr;
  overflow: visible;
}

.watch-body.watch-left-collapsed {
  grid-template-columns: 84px 1fr;
}

.watch-left {
  min-height: 0;
}

.panel-card {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.conversation-card.collapsed,
.strategy-card.collapsed {
  height: auto;
}

.panel-card :deep(.el-card__body) {
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 10px;
  overflow: hidden;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.card-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.conversation-list,
.resource-list,
.job-list {
  flex: 1;
  overflow: auto;
  min-height: 0;
}

.conversation-item,
.resource-item {
  border: 1px solid #e7eaf0;
  border-radius: 10px;
  padding: 10px;
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 8px;
  margin-bottom: 8px;
  background: #fff;
}

.conversation-item {
  cursor: pointer;
}

.conversation-item.active {
  border-color: #2f8cb7;
  background: #f2fbff;
}

.conversation-main,
.resource-main {
  min-width: 0;
}

.title,
.name {
  font-size: 13px;
  font-weight: 600;
  color: #243447;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.meta {
  margin-top: 2px;
  color: #7b8694;
  font-size: 12px;
  display: flex;
  gap: 8px;
  align-items: center;
}

.upload-tip {
  font-size: 12px;
  color: #7b8694;
}

.upload-box :deep(.el-upload-dragger) {
  padding: 12px 10px;
  min-height: 120px;
}

.upload-box :deep(.el-icon--upload) {
  font-size: 28px;
  margin-bottom: 6px;
}

.upload-desc {
  font-size: 12px;
  color: #5d6b7a;
}

.resource-actions {
  display: flex;
  justify-content: space-between;
  gap: 8px;
}

.resource-buttons {
  display: flex;
  align-items: flex-start;
  gap: 6px;
  padding-top: 2px;
  flex-wrap: wrap;
  align-self: flex-start;
}

.job-item {
  border: 1px solid #e7eaf0;
  border-radius: 10px;
  padding: 10px;
  display: flex;
  flex-direction: column;
  gap: 6px;
  margin-bottom: 8px;
  background: #fff;
}

.job-title {
  font-size: 13px;
  font-weight: 600;
  color: #243447;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.job-meta {
  display: flex;
  gap: 8px;
  align-items: center;
  color: #7b8694;
  font-size: 12px;
}

.dialog-job-list {
  max-height: 420px;
  overflow: auto;
}

.resource-progress {
  margin-top: 6px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.progress-text {
  font-size: 12px;
  color: #5d6b7a;
}

.strategy-list {
  overflow: auto;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.strategy-item {
  border: 1px solid #e7eaf0;
  border-radius: 10px;
  padding: 10px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
  background: #fff;
  cursor: pointer;
}

.strategy-item.active {
  border-color: #2f8cb7;
  background: #f2fbff;
}

.strategy-main {
  min-width: 0;
}

.strategy-buttons {
  display: flex;
  align-items: center;
  gap: 4px;
}

.chat-panel,
.chat-card {
  height: 100%;
  min-height: 0;
}

.chat-card {
  display: flex;
  flex-direction: column;
}

.chat-card :deep(.el-card__header) {
  flex-shrink: 0;
}

.chat-card :deep(.el-card__body) {
  flex: 1;
  height: auto;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 12px;
  overflow: hidden;
}

.messages-wrap {
  flex: 1;
  overflow: auto;
  min-height: 0;
  border: 1px solid #ebedf1;
  border-radius: 12px;
  background: #fafcff;
  padding: 10px;
}

.message-row {
  display: flex;
  margin-bottom: 6px;
}

.message-row.user {
  justify-content: flex-end;
}

.message-row.assistant {
  justify-content: flex-start;
}

.message-stack {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  max-width: 82%;
}

.message-row.user .message-stack {
  align-items: flex-end;
}

.bubble {
  border-radius: 12px;
  padding: 8px 10px;
  line-height: 1.2;
  white-space: pre-wrap;
  word-break: break-word;
}

.message-row.user .bubble {
  background: #1f93b8;
  color: #fff;
}

.message-row.assistant .bubble {
  background: #ffffff;
  border: 1px solid #e5e9f0;
  color: #27364a;
}

.bubble .role {
  font-size: 12px;
  opacity: 0.86;
  margin-bottom: 2px;
}

.agent-chip {
  margin-left: 6px;
  color: #8aa5b5;
}

.bubble .content {
  font-size: 20px;
}

.md-content :deep(p) {
  margin: 0;
}

.md-content :deep(ul),
.md-content :deep(ol) {
  margin: 0;
  padding-left: 16px;
}

.md-content :deep(li) {
  margin: 0;
  padding: 0;
  line-height: 1.2;
}

.md-content :deep(li > p) {
  margin: 0;
}

.md-content :deep(code) {
  background: #f3f5f8;
  padding: 2px 4px;
  border-radius: 4px;
  font-family: "Consolas", "Courier New", monospace;
}

.md-content :deep(pre) {
  background: #f3f5f8;
  padding: 6px 8px;
  border-radius: 6px;
  overflow: auto;
}

.md-content :deep(pre code) {
  background: transparent;
  padding: 0;
}

.crawl-result-list {
  margin-top: 8px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.crawl-result-card {
  border: 1px solid #e7edf3;
  border-radius: 8px;
  padding: 8px;
  background: #f8fbff;
}

.crawl-result-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.crawl-result-title {
  font-size: 13px;
  color: #243447;
  font-weight: 600;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.crawl-result-url {
  margin-top: 4px;
  font-size: 11px;
  color: #6c7a89;
  word-break: break-all;
}

.crawl-result-files {
  margin-top: 6px;
  display: flex;
  gap: 6px;
}

.crawl-result-file {
  font-size: 11px;
  line-height: 1;
  padding: 3px 6px;
  border-radius: 10px;
  border: 1px solid #d0d8e3;
  color: #7a8796;
  background: #f2f5f9;
}

.crawl-result-file.ok {
  border-color: #69b882;
  color: #2f8f50;
  background: #e9f7ef;
}

.crawl-result-error {
  margin-top: 6px;
  font-size: 11px;
  color: #b43d30;
  word-break: break-word;
}

.bubble .time {
  margin-top: 4px;
  font-size: 11px;
  opacity: 0.7;
}

.strategy-edit-run-bubble {
  min-width: 360px;
  max-width: 780px;
  background: #f7fbff !important;
  border-color: #cfe2f3 !important;
}

.strategy-edit-run-question {
  font-size: 13px;
  color: #2f3e52;
  margin-bottom: 8px;
}

.strategy-edit-run-status {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  color: #1f6f98;
  margin-bottom: 6px;
}

.strategy-edit-run-spinner {
  width: 14px;
  height: 14px;
  border: 2px solid #b8d2e6;
  border-top-color: #2f8cb7;
  border-radius: 50%;
  animation: strategy-edit-spin 0.9s linear infinite;
  flex-shrink: 0;
}

.strategy-edit-run-title {
  font-weight: 600;
}

.strategy-edit-run-steps {
  font-size: 12px;
  color: #62768b;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

@keyframes strategy-edit-spin {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}

.copy-tag {
  margin-top: 4px;
  padding: 4px 10px;
  border-radius: 10px;
  border: 1px solid #d6dde8;
  background: #fff;
  color: #526171;
  font-size: 12px;
  line-height: 1;
  cursor: pointer;
}

.copy-tag:hover {
  border-color: #2f8cb7;
  color: #1f7aa2;
}

.message-actions {
  margin-top: 4px;
  display: flex;
  gap: 8px;
  align-items: center;
}

.message-row.user .copy-tag {
  background: #f1f7fa;
}

.message-row.assistant .copy-tag {
  background: #ffffff;
}

.composer {
  display: flex;
  flex-direction: column;
  gap: 8px;
  flex-shrink: 0;
  padding-top: 6px;
  border-top: 1px solid #ebedf1;
}

.composer-tools {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  align-items: center;
}

.composer-input {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 10px;
  align-items: start;
}

.composer-input .el-button {
  align-self: start;
}

.prompt-edit-btn {
  min-width: 72px;
}

.prompt-template-panel {
  display: grid;
  grid-template-columns: 170px 1fr;
  gap: 8px;
  min-height: 340px;
}

.prompt-template-list {
  border-right: 1px solid #edf0f5;
  padding-right: 8px;
  overflow-y: auto;
  max-height: 360px;
}

.prompt-template-item {
  padding: 6px 8px;
  border-radius: 6px;
  font-size: 12px;
  color: #425466;
  cursor: pointer;
  margin-bottom: 4px;
}

.prompt-template-item:hover,
.prompt-template-item.active {
  background: #eef7fb;
  color: #1f7aa2;
}

.prompt-template-editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.prompt-template-row {
  display: flex;
  align-items: center;
  gap: 8px;
}

.prompt-template-label {
  font-size: 12px;
  color: #3b4a5e;
  min-width: 36px;
}

.prompt-template-name-input {
  flex: 1;
}

.group-picker {
  display: grid;
  grid-template-columns: 160px 1fr;
  gap: 8px;
  height: 320px;
}

.group-list {
  border-right: 1px solid #edf0f5;
  padding-right: 8px;
  height: 100%;
  overflow-y: auto;
}

.group-item {
  padding: 6px 8px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 12px;
  color: #425466;
}
.group-item:hover,
.group-item.active {
  background: #eef7fb;
  color: #1f7aa2;
}

.group-resources {
  height: 100%;
  overflow-y: auto;
  padding-right: 4px;
}

.group-resource-item {
  padding: 4px 6px;
  border-radius: 6px;
  cursor: pointer;
}
.group-resource-title {
  font-size: 12px;
  color: #243447;
}

.group-resource-item:hover {
  background: #f6f9fc;
}

.watch-panel {
  height: auto;
  min-height: 0;
  overflow: visible;
}

.watch-card {
  height: auto;
  min-height: 0;
}

.watch-card :deep(.el-card__body) {
  height: auto;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 0;
  overflow: hidden;
}

.watch-content {
  display: flex;
  flex-direction: column;
  gap: 0;
  flex: 1 0 auto;
  min-height: auto;
  overflow: hidden;
}

.watch-placeholder {
  border: 1px dashed #b8c6d4;
  border-radius: 12px;
  padding: 16px;
  background: #f7fbff;
}

.placeholder-title {
  font-weight: 600;
  color: #2a3f55;
  margin-bottom: 8px;
}

.placeholder-desc {
  font-size: 13px;
  color: #5d6b7a;
  line-height: 1.6;
}

.watch-layout {
  display: flex;
  flex-direction: column;
  flex: 1 0 auto;
  min-height: auto;
  overflow: hidden;
}

.watch-layout-shell {
  display: grid;
  grid-template-columns: 1fr;
  gap: 0;
  flex: 1;
  min-height: 0;
}

.watch-layout-shell.tool-open {
  grid-template-columns: 260px 1fr;
}

.watch-widget-tool-panel {
  border: 1px solid #e5e9f0;
  border-radius: 12px;
  background: #f8fbff;
  min-height: 0;
  overflow: auto;
  display: flex;
  flex-direction: column;
}

.watch-widget-tool-head {
  position: sticky;
  top: 0;
  z-index: 2;
  background: #f2f8ff;
  border-bottom: 1px solid #e5edf6;
  padding: 10px 12px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 13px;
  font-weight: 600;
  color: #2a3f55;
}

.watchlist-group-manager {
  border-bottom: 1px solid #e5edf6;
  padding: 8px 10px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  background: #f8fbff;
}

.watchlist-group-row {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  align-items: center;
}

.watchlist-group-chip {
  border: 1px solid #d7e2ee;
  background: #fff;
  color: #2f4358;
  font-size: 12px;
  line-height: 1;
  padding: 6px 8px;
  cursor: pointer;
}

.watchlist-group-chip.active {
  border-color: #2f8cb7;
  color: #1f7aa2;
  background: #ecf6fb;
}

.watchlist-add-row {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  align-items: center;
}

.watch-widget-tool-collapse {
  padding: 8px 10px 4px;
}

.watch-widget-tool-collapse :deep(.el-collapse-item__header) {
  font-size: 13px;
  color: #2a3f55;
}

.watch-widget-tool-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 2px 0 8px;
}

.watch-widget-tool-item {
  border: 1px solid #dce6f1;
  background: #fff;
  color: #2f4358;
  border-radius: 8px;
  font-size: 12px;
  line-height: 1.4;
  padding: 8px 10px;
  text-align: left;
  cursor: grab;
}

.watch-widget-tool-item:hover {
  border-color: #2f8cb7;
  color: #1f7aa2;
}

.watch-widget-tool-item:active {
  cursor: grabbing;
}

.watch-widget-tool-hint {
  margin-top: auto;
  border-top: 1px dashed #d5dfeb;
  padding: 10px 12px;
  font-size: 12px;
  color: #6d7f92;
}

.watch-layout-board {
  position: relative;
  border: 0;
  border-radius: 0;
  background: #e5e9f0;
  overflow: hidden;
  min-height: 360px;
}

.watch-layout-board.editing {
  background-image:
    linear-gradient(to right, rgba(47, 140, 183, 0.08) 1px, transparent 1px),
    linear-gradient(to bottom, rgba(47, 140, 183, 0.08) 1px, transparent 1px);
  background-size: 16px 16px;
}

.watch-layout-board.palette-dragging {
  border-color: #2f8cb7;
  box-shadow: none;
}

.key-board {
  display: flex;
  flex-direction: column;
  gap: 10px;
  flex: 1;
  min-height: 0;
  overflow: hidden;
}

.key-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  flex-wrap: wrap;
}

.key-toolbar-left {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.key-toolbar-right {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.key-toolbar-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.key-board-body {
  display: flex;
  align-items: stretch;
  flex: 1;
  min-height: 0;
  border: 1px solid #e5e9f0;
  border-radius: 0;
  overflow: hidden;
  background: #fff;
}

.key-watchlist {
  display: flex;
  flex-direction: column;
  max-width: 64%;
  background: #f7fbff;
  border-right: 1px solid #e5e9f0;
  min-height: 0;
}

.key-watchlist-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  font-size: 13px;
  font-weight: 600;
  color: #2a3f55;
  padding: 10px 12px 8px;
  border-bottom: 1px solid #e5e9f0;
}

.key-watchlist-sub {
  font-size: 12px;
  font-weight: 500;
  color: #6d7f92;
}

.key-watchlist-list {
  flex: 1;
  min-height: 0;
  overflow: auto;
  padding: 8px;
  position: relative;
}

.key-watch-group-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 84px;
  padding: 0 10px;
  border: 1px dashed #cfdae7;
  border-radius: 0;
  background: #f2f7fd;
  color: #44566b;
  font-size: 12px;
  font-weight: 600;
}

.key-watch-item {
  height: 84px;
  border: 1px solid #dce6f0;
  background: #fff;
  border-radius: 0;
  padding: 6px 10px;
  cursor: pointer;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 2px;
}

.key-watch-item.active {
  border-color: #2f8cb7;
  background: #f0f9ff;
}

.key-watch-item .row-top,
.key-watch-item .row-mid,
.key-watch-item .row-bottom {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.key-watch-item .row-top {
  margin-bottom: 1px;
}

.key-watch-item .row-mid {
  margin-bottom: 1px;
}

.key-watch-item .name {
  font-size: 13px;
  font-weight: 600;
  color: #243447;
}

.key-watch-item .code {
  font-size: 12px;
  color: #66788a;
}

.key-watch-item .price {
  font-size: 14px;
  font-weight: 600;
  color: #22313f;
}

.key-watch-item .change {
  font-size: 12px;
  font-weight: 600;
}

.key-watch-item .change.up {
  color: #d43834;
}

.key-watch-item .change.down {
  color: #1d9a57;
}

.key-watch-item .change.neutral {
  color: #7b8694;
}

.key-watch-item .row-bottom {
  font-size: 11px;
  color: #66788a;
}

.key-watch-item .row-actions {
  margin-top: 1px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
}

.key-divider {
  width: 8px;
  cursor: col-resize;
  background: linear-gradient(180deg, #f0f4f9 0%, #e6edf5 100%);
  border-left: 1px solid #d5dde8;
  border-right: 1px solid #d5dde8;
}

.key-chart-panel {
  flex: 1;
  min-width: 340px;
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}

.key-chart-title {
  padding: 10px 12px;
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  font-size: 13px;
  font-weight: 600;
  color: #2a3f55;
  border-bottom: 1px solid #e5e9f0;
}

.key-chart-hint {
  font-size: 12px;
  color: #6d7f92;
  padding: 6px 12px 0;
}

.key-chart-wrap {
  flex: 1;
  min-height: 0;
  min-height: 480px;
  padding: 0;
  background: #fff;
}

.watch-widget {
  position: absolute;
  background: #fff;
  border: 1px solid #e5e9f0;
  border-radius: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  box-shadow: none;
  box-sizing: border-box;
}

.watch-widget.editable {
  user-select: none;
}

.watch-widget.moving,
.watch-widget.resizing {
  border-color: #2f8cb7;
  box-shadow: none;
  z-index: 20;
}

.widget-header {
  padding: 10px 12px;
  font-size: 13px;
  font-weight: 600;
  color: #2a3f55;
  border-bottom: 1px solid #e8edf4;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
  background: #f9fcff;
}

.widget-header.draggable {
  cursor: move;
}

.widget-header-actions {
  display: flex;
  align-items: center;
  gap: 4px;
}

.watch-widget-body {
  flex: 1;
  min-height: 0;
  padding: 0;
  overflow: hidden;
  background: #fff;
}

.daily-replay-widget {
  height: 100%;
  min-height: 0;
  display: flex;
  flex-direction: column;
  background: #fff;
}

.daily-replay-head {
  border-bottom: 1px solid #e5e9f0;
  padding: 10px 12px;
}

.daily-replay-title {
  font-size: 14px;
  font-weight: 600;
  color: #2a3f55;
}

.daily-replay-sub {
  margin-top: 4px;
  font-size: 12px;
  color: #5f7388;
}

.daily-replay-content {
  flex: 1;
  min-height: 0;
  padding: 10px 12px;
  overflow: auto;
}

.daily-replay-label {
  font-size: 12px;
  font-weight: 600;
  color: #44566b;
  margin-bottom: 6px;
}

.daily-replay-requirement {
  font-size: 12px;
  color: #2f3f52;
  line-height: 1.7;
  white-space: pre-wrap;
  word-break: break-word;
}

.daily-replay-actions {
  padding: 10px 12px;
  border-top: 1px solid #e5e9f0;
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

.daily-replay-config {
  padding: 2px 0 4px;
}

.widget-watchlist {
  height: 100%;
  min-height: 0;
  display: flex;
  flex-direction: column;
  border: 0;
  background: #fff;
}

.widget-watchlist-head {
  padding: 8px 10px;
  border-bottom: 1px solid #e5e9f0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 12px;
  color: #44566b;
  font-weight: 600;
  background: #fff;
}

.widget-watchlist-tabs {
  display: flex;
  align-items: stretch;
  overflow-x: auto;
  overflow-y: hidden;
  border-bottom: 1px solid #e5e9f0;
  background: #f8fbff;
}

.widget-watchlist-tabs-actions {
  margin-left: auto;
  display: flex;
  align-items: center;
  border-left: 1px solid #e5e9f0;
  background: #f8fbff;
  position: sticky;
  right: 0;
  z-index: 2;
}

.widget-watchlist-tab {
  border: 0;
  border-right: 1px solid #e5e9f0;
  background: #f8fbff;
  color: #5f7388;
  font-size: 12px;
  line-height: 1;
  padding: 8px 10px;
  white-space: nowrap;
  cursor: pointer;
}

.widget-watchlist-tab.active {
  background: #ecf6fb;
  color: #1f7aa2;
}

.widget-watchlist-tab-icon {
  width: 28px;
  height: 28px;
  border: 0;
  border-right: 1px solid #e5e9f0;
  background: #f8fbff;
  color: #5f7388;
  font-size: 12px;
  line-height: 1;
  cursor: pointer;
}

.widget-watchlist-tab-icon:hover {
  color: #1f7aa2;
  background: #ecf6fb;
}

.widget-watchlist-list {
  flex: 1;
  min-height: 0;
  overflow: auto;
  background: #fff;
}

.widget-watchlist-table-head {
  display: grid;
  gap: 0;
  font-size: 12px;
  color: #5f7388;
  border-bottom: 1px solid #e5e9f0;
  background: #f8fbff;
}

.widget-watchlist-table-head > span {
  padding: 8px 10px;
  border-right: 1px solid #e5e9f0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.widget-watchlist-table-head > .seq-head,
.widget-watchlist-item > .seq-cell {
  text-align: center;
  padding: 0;
}

.watchlist-seq-setting-btn {
  width: 100%;
  height: 100%;
  min-height: 30px;
  border: 0;
  background: transparent;
  color: #5f7388;
  font-size: 12px;
  cursor: pointer;
}

.watchlist-seq-setting-btn:hover {
  color: #1f7aa2;
  background: #ecf6fb;
}

.widget-watchlist-item {
  display: grid;
  gap: 0;
  align-items: center;
  font-size: 12px;
  color: #44566b;
  border-bottom: 1px solid #eef2f7;
  cursor: pointer;
}

.widget-watchlist-item:hover {
  background: #f4f9fd;
}

.widget-watchlist-item > span {
  padding: 8px 10px;
  border-right: 1px solid #eef2f7;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.widget-watchlist-item .name {
  color: #2a3f55;
  font-weight: 600;
}

.widget-watchlist-item .code {
  color: #718194;
}

.widget-watchlist-item .change.up {
  color: #d43834;
}

.widget-watchlist-item .change.down {
  color: #1d9a57;
}

.widget-watchlist-item .change.neutral {
  color: #7b8694;
}

.watchlist-config-layout {
  display: grid;
  grid-template-columns: 150px 1fr;
  gap: 12px;
  min-height: 420px;
}

.watchlist-config-menu {
  border: 1px solid #e5e9f0;
  background: #f8fbff;
  display: flex;
  flex-direction: column;
}

.watchlist-config-menu-item {
  border: 0;
  border-bottom: 1px solid #e5e9f0;
  background: #f8fbff;
  color: #4a5f78;
  font-size: 12px;
  text-align: left;
  padding: 10px 12px;
  cursor: pointer;
}

.watchlist-config-menu-item.active {
  background: #ecf6fb;
  color: #1f7aa2;
  font-weight: 600;
}

.watchlist-config-main {
  border: 1px solid #e5e9f0;
  background: #fff;
  min-height: 0;
}

.watchlist-config-panel {
  height: 100%;
  padding: 10px 12px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.watchlist-config-panel-head,
.watchlist-config-group-line {
  font-size: 12px;
  color: #5f7388;
}

.watchlist-config-stock-search {
  display: flex;
  align-items: center;
  gap: 8px;
}

.watchlist-config-stock-list {
  border: 1px solid #e5e9f0;
  min-height: 280px;
  max-height: 340px;
  overflow: auto;
}

.watchlist-config-stock-item {
  display: grid;
  grid-template-columns: 96px 1fr 40px;
  align-items: center;
  border-bottom: 1px solid #eef2f7;
  font-size: 12px;
}

.watchlist-config-stock-item .stock-code {
  padding: 8px 10px;
  color: #6b7f94;
}

.watchlist-config-stock-item .stock-name {
  padding: 8px 10px;
  color: #2a3f55;
}

.stock-mark {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  margin: 0 auto;
  border: 0;
  background: transparent;
  font-size: 14px;
}

.stock-mark.add {
  cursor: pointer;
  color: #1f7aa2;
}

.stock-mark.exists {
  color: #1d9a57;
  font-weight: 600;
}

.watchlist-columns-layout {
  display: grid;
  grid-template-columns: 180px 1fr;
  gap: 10px;
  min-height: 360px;
}

.watchlist-columns-schemes,
.watchlist-columns-selected {
  border: 1px solid #e5e9f0;
  padding: 8px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 0;
}

.watchlist-columns-title {
  font-size: 12px;
  color: #4a5f78;
  font-weight: 600;
}

.watchlist-scheme-item {
  border: 1px solid #d7e2ee;
  background: #fff;
  color: #4a5f78;
  font-size: 12px;
  text-align: left;
  padding: 6px 8px;
  cursor: pointer;
}

.watchlist-scheme-item.active {
  border-color: #2f8cb7;
  background: #ecf6fb;
  color: #1f7aa2;
}

.watchlist-selected-list {
  border: 1px solid #e5e9f0;
  min-height: 120px;
  max-height: 160px;
  overflow: auto;
}

.watchlist-selected-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 6px 8px;
  border-bottom: 1px solid #eef2f7;
  font-size: 12px;
  color: #2a3f55;
}

.watchlist-selected-item button {
  border: 0;
  background: transparent;
  color: #a14d4d;
  cursor: pointer;
}

.watchlist-header-search {
  margin-top: auto;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.watchlist-header-search-results {
  border: 1px solid #e5e9f0;
  min-height: 140px;
  max-height: 180px;
  overflow: auto;
  padding: 6px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.watchlist-header-search-results button {
  border: 1px solid #d7e2ee;
  background: #fff;
  color: #4a5f78;
  font-size: 12px;
  text-align: left;
  padding: 6px 8px;
  cursor: pointer;
}

.widget-error {
  font-size: 12px;
  color: #c45656;
  background: #fdecec;
  border-radius: 0;
  padding: 8px;
}

.widget-resize-handle {
  position: absolute;
  right: 1px;
  bottom: 1px;
  width: 14px;
  height: 14px;
  cursor: nwse-resize;
  border-right: 2px solid #6fa6c0;
  border-bottom: 2px solid #6fa6c0;
  border-bottom-right-radius: 0;
}

.watch-widget-body :deep(.chart-wrapper) {
  height: 100% !important;
  margin: 0 !important;
  background: #fff !important;
  border-radius: 0 !important;
  box-shadow: none !important;
}

.widget-tool-layout {
  min-height: 480px;
  display: grid;
  grid-template-columns: 180px 1fr;
  gap: 12px;
}

.widget-tool-sidebar {
  border: 1px solid #e8edf4;
  border-radius: 10px;
  padding: 8px;
  background: #f8fbff;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.widget-tool-category {
  border: 1px solid #dce6f1;
  border-radius: 8px;
  padding: 8px 10px;
  font-size: 13px;
  color: #33485e;
  cursor: pointer;
  user-select: none;
}

.widget-tool-category:hover {
  border-color: #2f8cb7;
  color: #1f7aa2;
}

.widget-tool-category.active {
  background: #eaf6ff;
  border-color: #2f8cb7;
  color: #1f7aa2;
  font-weight: 600;
}

.widget-tool-main {
  border: 1px solid #e8edf4;
  border-radius: 10px;
  padding: 10px 12px;
  background: #fff;
  min-height: 0;
  overflow: auto;
}

.widget-tool-main-title {
  font-size: 14px;
  font-weight: 600;
  color: #2a3f55;
  margin-bottom: 8px;
}

.widget-tool-collapse :deep(.el-collapse-item__header) {
  font-size: 13px;
  color: #2a3f55;
}

.widget-tool-item-desc {
  font-size: 12px;
  color: #6f7f90;
  margin-bottom: 8px;
}

.widget-tool-item-form {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.widget-tool-item-actions {
  display: flex;
  justify-content: flex-end;
}

.manage-toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
  margin-bottom: 10px;
}

.manage-hint {
  margin: 6px 0 10px;
  font-size: 12px;
  color: #7b8694;
}

.manage-body {
  display: grid;
  grid-template-columns: 1fr 320px;
  gap: 12px;
  min-height: 320px;
}

.manage-list {
  border: 1px solid #e7eaf0;
  border-radius: 10px;
  padding: 8px;
  max-height: 420px;
  overflow: auto;
  background: #fff;
}

.manage-panel {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.manage-section {
  border: 1px solid #e7eaf0;
  border-radius: 10px;
  background: #fff;
  padding: 10px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.manage-section-title {
  font-size: 13px;
  font-weight: 600;
  color: #2a3f55;
}

.manage-section-subtitle {
  font-size: 12px;
  color: #667789;
}

.manage-inline-row {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.manage-inline-radio {
  flex-wrap: wrap;
}

.manage-section-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.manage-advanced :deep(.el-collapse-item__header) {
  font-size: 12px;
  color: #5d6b7a;
}

.manage-checkbox-group {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.manage-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
  padding: 8px;
  border: 1px solid #eef1f6;
  border-radius: 10px;
  background: #fff;
}

.manage-item:hover {
  border-color: #2f8cb7;
}

.manage-info {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.manage-name {
  font-size: 13px;
  font-weight: 600;
  color: #243447;
  max-width: 360px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.manage-meta {
  font-size: 12px;
  color: #7b8694;
}

.manage-actions {
  display: flex;
  align-items: center;
  gap: 6px;
}

.manage-group-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.manage-group-chip {
  border: 1px dashed #b8c6d4;
  border-radius: 8px;
  padding: 6px 8px;
  font-size: 12px;
  color: #2a3f55;
  background: #f7fbff;
  cursor: pointer;
  user-select: none;
}

.manage-group-chip:hover {
  border-color: #2f8cb7;
  color: #1f7aa2;
}

.memory-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 10px;
}

.memory-bind-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 10px;
  flex-wrap: wrap;
}

.memory-bind-list {
  border: 1px solid #e9eef3;
  border-radius: 10px;
  padding: 10px;
  max-height: 420px;
  overflow: auto;
  background: #fff;
}

.memory-checkbox-group {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.memory-bind-item {
  border: 1px solid #edf1f7;
  border-radius: 10px;
  padding: 8px;
}

.memory-bind-name {
  font-size: 13px;
  font-weight: 600;
  color: #243447;
}

.memory-bind-meta {
  font-size: 12px;
  color: #7c8796;
}

.memory-portrait-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 10px;
}

@media (max-width: 1180px) {
  .page-header {
    flex-direction: column;
    align-items: flex-start;
  }

  .page-body {
    grid-template-columns: 1fr;
    grid-template-rows: auto 1fr;
  }

  .left-panel {
    grid-template-columns: 1fr;
    grid-template-rows: auto auto;
  }

  .composer-tools {
    flex-wrap: wrap;
  }

  .header-actions {
    width: 100%;
    justify-content: flex-start;
  }

  .manage-body {
    grid-template-columns: 1fr;
  }

  .manage-group-list {
    flex-direction: row;
    flex-wrap: wrap;
  }

  .memory-toolbar {
    flex-wrap: wrap;
  }

  .key-board-body {
    flex-direction: column;
    min-height: 520px;
  }

  .key-watchlist {
    width: 100% !important;
    max-width: 100%;
    min-width: 0;
    max-height: 260px;
    border-right: none;
    border-bottom: 1px solid #e5e9f0;
  }

  .key-divider {
    width: 100%;
    height: 8px;
    cursor: row-resize;
  }

  .key-chart-panel {
    min-width: 0;
  }

  .watch-layout-shell.tool-open {
    grid-template-columns: 1fr;
    grid-template-rows: auto 1fr;
  }

  .watch-widget-tool-panel {
    max-height: 260px;
  }

  .widget-tool-layout {
    grid-template-columns: 1fr;
  }

  .widget-tool-sidebar {
    flex-direction: row;
    overflow-x: auto;
  }
}

@media (max-width: 860px) {
  .composer-input {
    grid-template-columns: 1fr;
  }

  .composer-input .el-button {
    width: 100%;
  }
}
</style>
