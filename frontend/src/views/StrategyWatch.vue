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
                <el-button link @click="downloadResource(res)">下载</el-button>
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
                <el-autocomplete
                  v-model="keyStockSearchQuery"
                  :fetch-suggestions="queryKeyWatchStockSuggestions"
                  placeholder="输入股票代码或名称添加自选"
                  style="width: 320px"
                  clearable
                  @select="onWatchStockSuggestionSelect"
                  @keyup.enter="addWatchStockFromInput"
                />
                <div class="key-toolbar-actions">
                  <el-select v-model="keyLevelWindowDays" style="width: 140px" @change="onKeyLevelWindowChange">
                    <el-option :value="250" label="近1年关键位" />
                    <el-option :value="730" label="近2年关键位" />
                    <el-option :value="1825" label="近5年关键位" />
                    <el-option :value="3650" label="近10年关键位" />
                  </el-select>
                  <el-button :disabled="!keySelectedCode" :loading="keyKlineLoading" @click="loadKeyStockKline(keySelectedCode)">
                    刷新K线
                  </el-button>
                </div>
              </div>

              <div ref="keyBoardBodyRef" class="key-board-body">
                <div class="key-watchlist" :style="{ width: `${keyPaneWidthPercent}%` }">
                  <div class="key-watchlist-header">自选股 ({{ keyWatchlist.length }})</div>
                  <div class="key-watchlist-list">
                    <div
                      v-for="item in keyWatchlist"
                      :key="item.code"
                      class="key-watch-item"
                      :class="{ active: item.code === keySelectedCode }"
                      @click="selectWatchStock(item.code)"
                    >
                      <div class="row-top">
                        <span class="name">{{ item.name }}</span>
                        <span class="code">{{ item.code }}</span>
                      </div>
                      <div class="row-mid">
                        <span class="price">{{ formatWatchPrice(item.latest_price) }}</span>
                        <span class="change" :class="watchChangeClass(item.change_pct)">
                          {{ formatWatchChange(item.change_pct) }}
                        </span>
                      </div>
                      <div class="row-bottom">
                        <span>成交额 {{ formatWatchAmount(item.amount) }}</span>
                        <el-button type="danger" link @click.stop="removeWatchStock(item.code)">删除</el-button>
                      </div>
                    </div>
                    <el-empty
                      v-if="!keyWatchlist.length"
                      description="还没有自选股，先在上方搜索添加"
                      :image-size="72"
                    />
                  </div>
                </div>

                <div class="key-divider" @mousedown.prevent="startKeyPaneResize"></div>

                <div class="key-chart-panel">
                  <div class="key-chart-title">
                    <span v-if="keySelectedStock">
                      {{ keySelectedStock.name }} ({{ keySelectedStock.code }}) K线 + 关键位
                    </span>
                    <span v-else>请选择左侧股票查看K线</span>
                  </div>
                  <div v-if="keyKlineLoading" class="watch-placeholder">
                    <div class="placeholder-title">正在加载K线数据...</div>
                  </div>
                  <div v-else-if="keyWatchError" class="watch-placeholder">
                    <div class="placeholder-title">加载失败</div>
                    <div class="placeholder-desc">{{ keyWatchError }}</div>
                  </div>
                  <div v-else-if="keyKlineData.length" class="key-chart-wrap">
                    <VChart :option="keyKlineOption" autoresize style="height: 100%; width: 100%" />
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
                      <EChartsRenderer
                        v-else
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

    <el-dialog v-model="resourceManageVisible" title="资料库管理" width="820px">
      <div class="manage-toolbar">
        <el-select v-model="manageFilterGroupId" size="small" style="width: 160px" placeholder="筛选分组">
          <el-option label="所有分组" value="" />
          <el-option
            v-for="grp in resourceGroups"
            :key="grp.group_id"
            :label="`${grp.group_name} (${grp.count})`"
            :value="grp.group_id"
          />
        </el-select>
        <el-button size="small" type="primary" plain @click="createManageGroup">新建分组</el-button>
        <el-button size="small" :disabled="!manageFilterGroupId" @click="renameManageGroup">重命名分组</el-button>
        <el-select v-model="manageTargetGroupId" size="small" style="width: 160px" placeholder="目标分组">
          <el-option
            v-for="grp in resourceGroups"
            :key="grp.group_id"
            :label="`${grp.group_name} (${grp.count})`"
            :value="grp.group_id"
          />
        </el-select>
        <el-input
          v-model="manageTargetGroupName"
          size="small"
          placeholder="或输入新分组名称"
          style="width: 180px"
          :disabled="!!manageTargetGroupId"
        />
        <el-radio-group v-model="manageMode" size="small">
          <el-radio-button label="move">移动</el-radio-button>
          <el-radio-button label="copy">复制</el-radio-button>
        </el-radio-group>
        <el-button
          type="primary"
          size="small"
          :disabled="!manageSelectedIds.length"
          @click="submitResourceTransfer"
        >
          应用到选中
        </el-button>
        <el-button
          type="danger"
          size="small"
          :disabled="!manageSelectedIds.length"
          @click="batchDeleteResources"
        >
          删除选中
        </el-button>
      </div>
      <div class="manage-hint">支持单篇移动/复制，拖动到右侧分组默认移动。</div>
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
                <el-button link size="small" @click="renameResource(res)">重命名</el-button>
                <el-button link size="small" @click="quickTransferResource(res)">移动/复制</el-button>
                <el-button link size="small" type="danger" @click="deleteResource(res.id)">删除</el-button>
              </div>
            </div>
          </el-checkbox-group>
          <el-empty v-if="!manageFilteredResources.length" description="暂无资源" :image-size="56" />
        </div>
        <div class="manage-groups">
          <div class="manage-group-title">拖到分组</div>
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
const manageTargetGroupId = ref('')
const manageTargetGroupName = ref('')
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
    id: 'index',
    label: '指数',
    templates: [
      {
        id: 'index_kline',
        type: 'index_kline',
        title: '指数K线',
        desc: '查看指数趋势与成交量。',
        defaults: {
          index_name: '上证指数',
          days_range: 60
        },
        fields: [
          { key: 'index_name', label: '指数名称', type: 'text', placeholder: '如：上证指数、创业板指' },
          { key: 'days_range', label: '回看天数', type: 'number', min: 20, max: 500, step: 5 }
        ]
      }
    ]
  },
  {
    id: 'sector',
    label: '板块',
    templates: [
      {
        id: 'sector_kline',
        type: 'sector_kline',
        title: '板块K线',
        desc: '查看行业/概念板块走势。',
        defaults: {
          sector_name: '半导体',
          days_range: 60
        },
        fields: [
          { key: 'sector_name', label: '板块名称', type: 'text', placeholder: '如：半导体、证券' },
          { key: 'days_range', label: '回看天数', type: 'number', min: 20, max: 500, step: 5 }
        ]
      }
    ]
  },
  {
    id: 'stock',
    label: '个股',
    templates: [
      {
        id: 'stock_kline',
        type: 'stock_kline',
        title: '个股K线',
        desc: '查看个股K线与均线。',
        defaults: {
          stock_code: '000001',
          days: 120
        },
        fields: [
          { key: 'stock_code', label: '股票代码', type: 'text', placeholder: '6位代码，如 600519' },
          { key: 'days', label: '回看天数', type: 'number', min: 20, max: 500, step: 5 }
        ]
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
  if (!manageFilterGroupId.value) return resources.value
  return resources.value.filter(item => item.group_id === manageFilterGroupId.value)
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
  return Math.max(360, Math.ceil(bottom + 24))
})
const watchBoardStyle = computed(() => {
  return { height: `${watchBoardHeight.value}px` }
})
const targetGroupOptions = computed(() => {
  const sourceId = groupTransferForm.value.source_group_id
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
const clampNumber = (value, min, max) => Math.max(min, Math.min(max, value))
const WATCH_WIDGET_GAP = 14
const WATCH_WIDGET_MIN_WIDTH = 300
const WATCH_WIDGET_MIN_HEIGHT = 240
const WATCH_WIDGET_MAX_WIDTH = 1500
const WATCH_LAYOUT_SAVE_DEBOUNCE_MS = 700
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

const buildWidgetTitle = (type, params = {}) => {
  if (type === 'index_kline') return `${params.index_name || '上证指数'} K线`
  if (type === 'sector_kline') return `${params.sector_name || '板块'} K线`
  if (type === 'stock_kline') return `${params.stock_code || '000001'} K线`
  if (type === 'market_volume') return '市场量能对比'
  if (type === 'market_sentiment_chart') return getSentimentChartTitle(params.chart_key)
  return '自定义图表'
}

const genWidgetId = () => `widget_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`

const getDefaultWidgetSize = (type) => {
  if (type === 'index_kline' || type === 'sector_kline' || type === 'stock_kline') return { w: 620, h: 520 }
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
  if (type === 'index_kline') {
    return {
      index_name: String(source.index_name || source.index || '上证指数').trim() || '上证指数',
      days_range: clampNumber(Number(source.days_range || source.days || 60) || 60, 20, 500)
    }
  }
  if (type === 'sector_kline') {
    return {
      sector_name: String(source.sector_name || source.sector || source.name || '半导体').trim() || '半导体',
      days_range: clampNumber(Number(source.days_range || source.days || 60) || 60, 20, 500)
    }
  }
  if (type === 'stock_kline') {
    return {
      stock_code: normalizeStockWidgetCode(source.stock_code || source.code || '000001'),
      days: clampNumber(Number(source.days || source.days_range || 120) || 120, 20, 500)
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
  const width = clampNumber(Number(source.w || defaults.w) || defaults.w, WATCH_WIDGET_MIN_WIDTH, WATCH_WIDGET_MAX_WIDTH)
  const height = clampNumber(Number(source.h || defaults.h) || defaults.h, WATCH_WIDGET_MIN_HEIGHT, 1800)
  return {
    x: Math.max(0, Number(source.x || defaults.x) || defaults.x),
    y: Math.max(0, Number(source.y || defaults.y) || defaults.y),
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
      x: clampNumber(rawX, 0, maxX),
      y: Math.max(0, rawY),
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
  if (!widgetToolExpandedCategoryIds.value.length) {
    const firstCategoryId = widgetToolCategories[0]?.id || ''
    widgetToolExpandedCategoryIds.value = firstCategoryId ? [firstCategoryId] : []
  }
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
  const raw = Number(widget?.layout?.h || 360) - 58
  return `${Math.max(180, Math.floor(raw))}px`
}

const watchWidgetStyle = (widget) => {
  const layout = widget?.layout || {}
  return {
    left: `${Math.max(0, Number(layout.x || 0))}px`,
    top: `${Math.max(0, Number(layout.y || 0))}px`,
    width: `${clampNumber(Number(layout.w || 500), WATCH_WIDGET_MIN_WIDTH, WATCH_WIDGET_MAX_WIDTH)}px`,
    height: `${clampNumber(Number(layout.h || 420), WATCH_WIDGET_MIN_HEIGHT, 1800)}px`
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

const keyKlineOption = computed(() => {
  if (!keyKlineData.value.length) return {}
  const dates = keyKlineData.value.map(item => item.date)
  const candlestickData = keyKlineData.value.map(item => [item.open, item.close, item.low, item.high])
  const ma5Data = keyKlineData.value.map(item => item.ma5)
  const ma10Data = keyKlineData.value.map(item => item.ma10)
  const ma20Data = keyKlineData.value.map(item => item.ma20)
  const volumeData = keyKlineData.value.map(item => ({
    value: item.amount || item.volume || 0,
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

  return {
    animation: false,
    color: ['#4ECDC4', '#ffbf00', '#f92672'],
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' }
    },
    legend: {
      data: ['MA5', 'MA10', 'MA20'],
      top: 24,
      right: 10
    },
    grid: [
      { left: '8%', right: '6%', height: '56%' },
      { left: '8%', right: '6%', top: '74%', height: '16%' }
    ],
    xAxis: [
      {
        type: 'category',
        data: dates,
        boundaryGap: false,
        scale: true,
        min: 'dataMin',
        max: 'dataMax',
        splitLine: { show: false }
      },
      {
        type: 'category',
        gridIndex: 1,
        data: dates,
        boundaryGap: false,
        scale: true,
        min: 'dataMin',
        max: 'dataMax',
        axisLabel: { show: false },
        axisTick: { show: false },
        splitLine: { show: false }
      }
    ],
    yAxis: [
      { scale: true, splitArea: { show: true } },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 2,
        axisLabel: { show: false },
        axisTick: { show: false },
        splitLine: { show: false }
      }
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 70, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], top: '92%', start: 70, end: 100 }
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
        },
        markLine: levelMarkLine
      },
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
      },
      {
        name: '成交额',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumeData
      }
    ]
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
    amount: Number(item.amount ?? item.成交额 ?? 0) || 0
  }
}

const getKeyBoardConfig = (strategy) => {
  const cfg = strategy?.config || {}
  const board = cfg.key_levels_board
  return board && typeof board === 'object' ? board : {}
}

const hydrateKeyBoardFromStrategy = (strategy) => {
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
  keyWatchlist.value = nextWatchlist.slice(0, 30)

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
      amount: item.amount
    })),
    selected_code: keySelectedCode.value || '',
    pane_width_percent: keyPaneWidthPercent.value,
    window_days: keyLevelWindowDays.value
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
      amount: Number(item.成交额 || 0)
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
  if (keyWatchlist.value.length >= 30) {
    ElMessage.warning('自选股上限为 30 只')
    return
  }
  keyWatchlist.value.push(stock)
  keyStockSearchQuery.value = ''
  if (!keySelectedCode.value) keySelectedCode.value = stock.code
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
    const amount = Number(latest?.amount || latest?.volume || 0)
    const idx = keyWatchlist.value.findIndex(item => item.code === code)
    if (idx >= 0) {
      keyWatchlist.value[idx] = {
        ...keyWatchlist.value[idx],
        latest_price: latestPrice,
        change_pct: changePct,
        amount
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
      id: 'index-kline',
      type: 'index_kline',
      title: '上证指数 K线',
      params: { index_name: '上证指数', days_range: 60 },
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

  if (watchPointerState.mode === 'drag') {
    const maxX = Math.max(0, boardWidth - start.w)
    patchWatchWidgetLayout(widgetId, {
      x: clampNumber(start.x + dx, 0, maxX),
      y: Math.max(0, start.y + dy)
    })
    return
  }

  if (watchPointerState.mode === 'resize') {
    const maxWidth = Math.max(WATCH_WIDGET_MIN_WIDTH, boardWidth - start.x)
    patchWatchWidgetLayout(widgetId, {
      w: clampNumber(start.w + dx, WATCH_WIDGET_MIN_WIDTH, maxWidth),
      h: clampNumber(start.h + dy, WATCH_WIDGET_MIN_HEIGHT, 1800)
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

    if (source.type === 'index_kline') {
      const indexName = source.params?.index_name || '上证指数'
      const daysRange = source.params?.days_range || 60
      const res = await ApiService.getIndexKlineChart(indexName, daysRange)
      const chartHtml = res?.data?.chart_html || res?.data?.data?.chart_html || res?.data?.[indexName]?.chart_html
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
      const chartHtml = res?.data?.chart_html || res?.data?.data?.chart_html
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
      const chartHtml = res?.data?.chart_html || res?.data?.data?.chart_html
      return {
        ...source,
        title: source.title || `${stockCode} K线`,
        chartHtml: chartHtml || '<div>暂无个股图表</div>',
        error: chartHtml ? '' : '个股图表为空'
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
  const baseParams = normalizeWidgetParams(type, template.defaults || {})
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
    return
  }

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
  manageTargetGroupId.value = ''
  manageTargetGroupName.value = ''
  manageMode.value = 'move'
  manageSelectedIds.value = []
  resourceManageVisible.value = true
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
  const ok = await transferResources(
    manageSelectedIds.value,
    manageTargetGroupId.value,
    manageTargetGroupName.value,
    manageMode.value
  )
  if (ok) {
    ElMessage.success('资料库操作已完成')
    manageSelectedIds.value = []
  }
}

const quickTransferResource = async (resource) => {
  if (!resource?.id) return
  const ok = await transferResources(
    [resource.id],
    manageTargetGroupId.value,
    manageTargetGroupName.value,
    manageMode.value
  )
  if (ok) ElMessage.success('资料已更新')
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

const downloadResource = async (resource) => {
  if (!resource?.id) return
  try {
    const res = await ApiService.getStrategyResourceMarkdown(resource.id)
    const content = res.data?.content || ''
    const baseName = (resource.original_name || resource.id)
      .replace(/[\\/:*?"<>|]+/g, '_')
      .replace(/\s+/g, ' ')
      .trim()
    const filename = `${baseName || resource.id}.md`
    const blob = new Blob([content], { type: 'text/markdown;charset=utf-8' })
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
  } finally {
    sending.value = false
  }
}

onMounted(async () => {
  loadPromptTemplatesFromStorage()
  try {
    await loadRuntime()
    await loadResources()
    await loadMemoryProfiles()
    await loadConversations()
    if (!conversations.value.length) await createConversation()
    await loadStrategies()
    await loadWatchWidgets()
  } catch (error) {
    console.error(error)
  }
})

onBeforeUnmount(() => {
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
  gap: 12px;
  overflow: visible;
}

.watch-content {
  display: flex;
  flex-direction: column;
  gap: 16px;
  flex: 1 0 auto;
  min-height: auto;
  overflow: visible;
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
  overflow: visible;
}

.watch-layout-shell {
  display: grid;
  grid-template-columns: 1fr;
  gap: 12px;
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
  border: 1px solid #e5e9f0;
  border-radius: 12px;
  background: linear-gradient(180deg, #f7fbff 0%, #fdfefe 100%);
  overflow: visible;
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
  box-shadow: inset 0 0 0 1px rgba(47, 140, 183, 0.32);
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

.key-toolbar-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.key-board-body {
  display: flex;
  align-items: stretch;
  flex: 1;
  min-height: 0;
  border: 1px solid #e5e9f0;
  border-radius: 10px;
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
  font-size: 13px;
  font-weight: 600;
  color: #2a3f55;
  padding: 10px 12px 8px;
  border-bottom: 1px solid #e5e9f0;
}

.key-watchlist-list {
  flex: 1;
  min-height: 0;
  overflow: auto;
  padding: 8px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.key-watch-item {
  border: 1px solid #dce6f0;
  background: #fff;
  border-radius: 8px;
  padding: 8px 10px;
  cursor: pointer;
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
  margin-bottom: 4px;
}

.key-watch-item .row-mid {
  margin-bottom: 4px;
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
  font-size: 12px;
  color: #66788a;
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
  font-size: 13px;
  font-weight: 600;
  color: #2a3f55;
  border-bottom: 1px solid #e5e9f0;
}

.key-chart-wrap {
  flex: 1;
  min-height: 0;
  min-height: 480px;
  padding: 8px;
}

.watch-widget {
  position: absolute;
  background: #fff;
  border: 1px solid #e5e9f0;
  border-radius: 12px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  box-shadow: 0 2px 8px rgba(19, 55, 82, 0.08);
}

.watch-widget.editable {
  user-select: none;
}

.watch-widget.moving,
.watch-widget.resizing {
  border-color: #2f8cb7;
  box-shadow: 0 10px 28px rgba(31, 122, 162, 0.22);
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
}

.watch-widget-body {
  flex: 1;
  min-height: 0;
  padding: 8px;
  overflow: hidden;
}

.widget-error {
  font-size: 12px;
  color: #c45656;
  background: #fdecec;
  border-radius: 8px;
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
  border-bottom-right-radius: 8px;
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
  gap: 8px;
  align-items: center;
}

.manage-hint {
  margin: 6px 0 10px;
  font-size: 12px;
  color: #7b8694;
}

.manage-body {
  display: grid;
  grid-template-columns: 1fr 180px;
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

.manage-groups {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.manage-group-title {
  font-size: 12px;
  color: #5d6b7a;
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
