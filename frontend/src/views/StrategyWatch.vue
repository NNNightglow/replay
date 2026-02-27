<template>
  <div class="strategy-watch-page">
    <div class="page-header">
      <div>
        <h2>策略看盘</h2>
        <p>上传资料并构建知识库，用策略对话快速分析与复盘。</p>
      </div>
      <div class="header-actions">
        <el-radio-group v-model="activeMode" size="large" class="mode-switch">
          <el-radio-button label="chat">对话</el-radio-button>
          <el-radio-button label="watch">看盘</el-radio-button>
        </el-radio-group>
        <el-tag :type="runtime.api_key_configured ? 'success' : 'warning'" size="large">
          {{ runtime.api_key_configured ? `模型已连接：${runtime.model}` : '未配置模型 API Key，请先配置 .env' }}
        </el-tag>
      </div>
    </div>

    <div v-if="activeMode === 'chat'" class="page-body" :class="{ 'chat-sidebar-collapsed': chatSidebarCollapsed }">
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
            <div v-for="msg in messages" :key="msg.id" class="message-row" :class="msg.role">
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
              <el-select v-model="selectedAgentName" placeholder="选择 Agent" style="width: 260px">
                <el-option
                  v-for="agent in runtime.agents || []"
                  :key="agent.name"
                  :label="`${agent.display_name} (${agent.name})`"
                  :value="agent.name"
                />
              </el-select>
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
                placeholder="输入问题或指令，Ctrl+Enter 发送。支持 @agent 指定角色。"
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
                  <el-tag size="small" type="info">{{ getStrategyViewLabel(item.view_type) }}</el-tag>
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
            <div v-else class="watch-grid">
              <div v-for="widget in watchWidgets" :key="widget.id" class="watch-widget">
                <div class="widget-header">
                  <span>{{ widget.title }}</span>
                </div>
                <div v-if="widget.error" class="widget-error">{{ widget.error }}</div>
                <EChartsRenderer
                  v-else
                  :chart-html="widget.chartHtml"
                  :height="widget.height || '360px'"
                />
              </div>
              <el-empty
                v-if="!watchWidgets.length && !watchLoading"
                description="暂无图表配置，可通过 Agent 生成视图"
                :image-size="90"
              />
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

const activeMode = ref('chat')
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

const selectedAgentName = ref('planner_agent')
const modelOptions = [
  { label: 'v3.2 思考', value: 'deepseek-v3.2-thinking' },
  { label: 'v3.2', value: 'deepseek-v3.2' }
]
const selectedModel = ref('deepseek-v3.2')
const PROMPT_TEMPLATE_STORAGE_KEY = 'strategy_watch_prompt_templates_v1'
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

const strategyViewOptions = [
  { label: '基础看盘', value: 'basic' },
  { label: '趋势跟踪', value: 'trend' },
  { label: '突破监控', value: 'breakout' },
  { label: '量能观察', value: 'volume' },
  { label: '关键位看板', value: 'key_levels' }
]
const strategies = ref([])
const activeStrategyId = ref('')
const watchLoading = ref(false)
const watchWidgets = ref([])
const watchError = ref('')

const keyWatchlist = ref([])
const keySelectedCode = ref('')
const keyKlineData = ref([])
const keyLevels = ref([])
const keyKlineLoading = ref(false)
const keyWatchError = ref('')
const keyStockSearchQuery = ref('')
const keyPaneWidthPercent = ref(36)
const keyLevelWindowDays = ref(3650)
const keyLevelMethodVer = ref('v1')
const keyBoardBodyRef = ref(null)
const keyPaneResizing = ref(false)
const chatSidebarCollapsed = ref(false)
const strategyListCollapsed = ref(false)

const inputText = ref('')
const sending = ref(false)
const uploading = ref(false)
const uploadJobs = ref([])
const jobPollingTimer = ref(null)

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

const normalizeStockCode = (value) => String(value || '').replace(/\D/g, '').padStart(6, '0').slice(-6)
const clampNumber = (value, min, max) => Math.max(min, Math.min(max, value))

const loadPromptTemplatesFromStorage = () => {
  if (typeof window === 'undefined') return
  try {
    const raw = window.localStorage.getItem(PROMPT_TEMPLATE_STORAGE_KEY)
    if (!raw) {
      promptTemplates.value = clonePromptTemplates()
      return
    }
    const parsed = JSON.parse(raw)
    promptTemplates.value = normalizePromptTemplates(parsed)
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

const getDisplayedAssistantContent = (msg) => {
  if (msg?.agent_name === 'crawler_agent' && getCrawlResults(msg).length) {
    return buildCrawlerAgentSummary(msg)
  }
  return msg?.content || ''
}

const getStrategyViewLabel = (type) => {
  const found = strategyViewOptions.find(item => item.value === type)
  return found ? found.label : '自定义视图'
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
  keyLevelMethodVer.value = (board.method_ver || 'v1').trim() || 'v1'
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
    window_days: keyLevelWindowDays.value,
    method_ver: keyLevelMethodVer.value
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
      ApiService.getStockLevels(code, keyLevelWindowDays.value, null, keyLevelMethodVer.value)
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

const loadRuntime = async () => {
  const res = await ApiService.getStrategyRuntime()
  runtime.value = res.data || runtime.value
  const candidateModel = runtime.value.model
  if (candidateModel && modelOptions.some(item => item.value === candidateModel)) {
    selectedModel.value = candidateModel
  }
  if (!selectedAgentName.value && runtime.value.agents?.length) {
    selectedAgentName.value = runtime.value.agents[0].name
  }
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

const loadResources = async () => {
  const res = await ApiService.getStrategyResources()
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

const loadJobs = async () => {
  try {
    const res = await ApiService.listStrategyResourceJobs()
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
      jobIds.map(id =>
        ApiService.getStrategyResourceJob(id)
          .then(res => ({ id, job: res.data }))
          .catch(() => null)
      )
    )
    const done = new Set()
    results.forEach(item => {
      if (!item?.job) return
      const status = item.job.status
      if (status && status !== 'queued' && status !== 'running') {
        done.add(item.id)
      }
    })
    await loadResources()
    await loadJobs()
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

const updateActiveStrategyView = async (viewType) => {
  if (!activeStrategyId.value) return
  try {
    await ApiService.updateStrategyWatchStrategy(activeStrategyId.value, { view_type: viewType })
    await loadStrategies()
  } catch (error) {
    console.error(error)
  }
}

const buildDefaultWidgets = (viewType) => {
  if (viewType === 'key_levels') return []
  if (viewType === 'trend') {
    return [
      {
        id: 'index-kline',
        type: 'index_kline',
        title: '主板指数趋势',
        params: { index_name: '上证指数', days_range: 60 }
      }
    ]
  }
  if (viewType === 'breakout') {
    return [
      {
        id: 'market-sentiment',
        type: 'market_sentiment_bundle',
        title: '连板与情绪概览',
        params: { days_back: 60 }
      }
    ]
  }
  if (viewType === 'volume') {
    return [
      {
        id: 'market-volume',
        type: 'market_volume',
        title: '市场量能对比',
        params: {}
      }
    ]
  }
  return [
    {
      id: 'market-sentiment',
      type: 'market_sentiment_bundle',
      title: '市场情绪组合',
      params: { days_back: 30 }
    }
  ]
}

const buildWidgetsFromStrategy = (strategy) => {
  if (!strategy) return []
  const config = strategy.config || {}
  if (Array.isArray(config.widgets) && config.widgets.length) {
    return config.widgets
  }
  return buildDefaultWidgets(strategy.view_type || 'basic')
}

const loadWatchWidgets = async () => {
  const strategy = activeStrategy.value
  watchError.value = ''
  watchWidgets.value = []
  if (!strategy || !activeMode.value || activeMode.value !== 'watch') return

  if (isKeyLevelStrategyConfig(strategy)) {
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
  if (!widgetDefs.length) return

  watchLoading.value = true
  try {
    const results = []
    for (const widget of widgetDefs) {
      if (widget.type === 'market_sentiment_bundle') {
        const res = await ApiService.getMarketSentimentCharts(null, widget.params?.days_back || 30)
        const charts = res.data?.charts || {}
        const mapping = [
          { key: 'red_ratio_and_amount', title: '红盘率与成交额' },
          { key: 'limit_up_count', title: '涨停/跌停统计' },
          { key: 'ground_ceiling_count', title: '地天板/天地板' },
          { key: 'continuous_limit_up', title: '连板天梯' },
          { key: 'change_distribution', title: '涨跌幅分布' }
        ]
        mapping.forEach(item => {
          if (charts[item.key]) {
            results.push({
              id: `${widget.id}-${item.key}`,
              title: item.title,
              chartHtml: charts[item.key],
              height: '420px'
            })
          }
        })
        continue
      }

      if (widget.type === 'index_kline') {
        const indexName = widget.params?.index_name || '上证指数'
        const daysRange = widget.params?.days_range || 60
        const res = await ApiService.getIndexKlineChart(indexName, daysRange)
        const chartHtml = res.data?.chart_html || res.data?.[indexName]?.chart_html
        results.push({
          id: widget.id,
          title: widget.title || `${indexName} K线`,
          chartHtml: chartHtml || '<div>暂无指数图表</div>',
          height: '520px'
        })
        continue
      }

      if (widget.type === 'market_volume') {
        const res = await ApiService.getMarketVolume()
        const chartHtml = res.data?.chart_html
        results.push({
          id: widget.id,
          title: widget.title || '市场量能对比',
          chartHtml: chartHtml || '<div>暂无量能图表</div>',
          height: '420px'
        })
        continue
      }

      results.push({
        id: widget.id,
        title: widget.title || '未识别图表',
        chartHtml: '<div>暂不支持该图表类型</div>',
        height: '240px',
        error: widget.type ? `未支持的类型: ${widget.type}` : '未支持的图表类型'
      })
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
  activeMode.value = 'chat'
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

const sendMessage = async () => {
  const text = (inputText.value || '').trim()
  if (!text) {
    ElMessage.warning('请输入内容后再发送')
    return
  }
  if (!activeConversationId.value) await createConversation()

  const payload = {
    content: text,
    resource_ids: selectedResourceIds.value,
    agent_name: selectedAgentName.value,
    model: selectedModel.value,
    strategy_id: activeStrategyId.value || '',
    prompt_template_id: selectedPromptTemplateId.value,
    prompt_template: effectivePromptTemplate.value
  }

  sending.value = true
  try {
    inputText.value = ''
    let assistantRef = null
    await ApiService.streamStrategyMessage(activeConversationId.value, payload, {
      onMeta: (evt) => {
        const userMessage = evt.user_message
        const assistantMessage = evt.assistant_message
        if (userMessage) messages.value.push(userMessage)
        if (assistantMessage) {
          messages.value.push(assistantMessage)
          assistantRef = messages.value[messages.value.length - 1]
        }
        scrollMessagesToBottom()
      },
      onDelta: (evt) => {
        if (!assistantRef) return
        const textChunk = evt.text || ''
        if (textChunk) assistantRef.content = (assistantRef.content || '') + textChunk
        scrollMessagesToBottom()
      },
      onDone: async () => {
        await loadConversations()
        await loadResources()
        await scrollMessagesToBottom()
      },
      onError: (err) => {
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
  } finally {
    sending.value = false
  }
}

onMounted(async () => {
  loadPromptTemplatesFromStorage()
  try {
    await loadRuntime()
    await loadResources()
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
  stopJobPolling()
  stopKeyPaneResize()
})

watch(promptTemplates, () => {
  persistPromptTemplatesToStorage()
  if (!promptTemplates.value.some(item => item.id === selectedPromptTemplateId.value)) {
    selectedPromptTemplateId.value = 'none'
  }
}, { deep: true })

watch([activeStrategyId, activeMode], () => {
  if (activeMode.value !== 'chat') groupPopoverVisible.value = false
  if (activeMode.value !== 'chat') promptTemplatePopoverVisible.value = false
  loadWatchWidgets()
})
</script>

<style scoped>
.strategy-watch-page {
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: 14px;
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
}

.mode-switch :deep(.el-radio-button__inner) {
  padding: 8px 16px;
}

.page-body {
  flex: 1;
  display: grid;
  grid-template-columns: 340px 1fr;
  gap: 14px;
  min-height: 0;
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
}

.chat-card :deep(.el-card__body) {
  height: calc(100% - 16px);
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.messages-wrap {
  flex: 1;
  overflow: auto;
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
}

.composer-tools {
  display: flex;
  gap: 10px;
}

.composer-input {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 10px;
  align-items: end;
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

.watch-panel,
.watch-card {
  height: 100%;
}

.watch-card :deep(.el-card__body) {
  height: calc(100% - 16px);
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.watch-content {
  display: flex;
  flex-direction: column;
  gap: 16px;
  flex: 1;
  min-height: 0;
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

.watch-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 14px;
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
  background: #fff;
  border: 1px solid #e5e9f0;
  border-radius: 12px;
  padding: 10px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.widget-header {
  font-size: 13px;
  font-weight: 600;
  color: #2a3f55;
}

.widget-error {
  font-size: 12px;
  color: #c45656;
  background: #fdecec;
  border-radius: 8px;
  padding: 8px;
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

@media (max-width: 1180px) {
  .page-body {
    grid-template-columns: 1fr;
    grid-template-rows: auto 1fr;
  }

  .left-panel {
    grid-template-columns: 1fr;
    grid-template-rows: auto auto;
  }

  .composer-tools {
    flex-direction: column;
  }

  .header-actions {
    width: 100%;
    justify-content: space-between;
  }

  .manage-body {
    grid-template-columns: 1fr;
  }

  .manage-group-list {
    flex-direction: row;
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
}
</style>
