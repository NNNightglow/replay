<template>
  <div class="chart-wrapper" :style="{ width: '100%', height: height }">
    <!-- 主要渲染方式：直接插入HTML -->
    <div
      v-if="useDirectRender"
      ref="directContainer"
      :style="{ width: '100%', height: '100%' }"
      v-html="processedHtml">
    </div>

    <!-- 备用渲染方式：iframe -->
    <iframe
      v-else
      ref="chartFrame"
      :srcdoc="iframeHtml"
      :style="{ width: '100%', height: '100%', border: 'none' }"
      @load="onIframeLoad"
      sandbox="allow-scripts allow-same-origin allow-downloads allow-popups">
    </iframe>

    <!-- 调试按钮 -->
    <div class="debug-controls" v-if="showDebug">
      <button @click="toggleRenderMode" class="debug-btn">
        切换到{{ useDirectRender ? 'iframe' : '直接' }}渲染
      </button>
      <button @click="showHtmlContent = !showHtmlContent" class="debug-btn">
        {{ showHtmlContent ? '隐藏' : '显示' }}HTML内容
      </button>
    </div>

    <!-- HTML内容显示 -->
    <div v-if="showHtmlContent" class="html-content">
      <pre>{{ chartHtml.substring(0, 1000) }}...</pre>
    </div>
  </div>
</template>

<script>
import { ref, computed, onMounted, nextTick, watch } from 'vue'

export default {
  name: 'EChartsRenderer',
  props: {
    chartHtml: {
      type: String,
      default: ''
    },
    height: {
      type: String,
      default: '400px'
    }
  },
  setup(props) {
    const chartFrame = ref(null)
    const directContainer = ref(null)
    const useDirectRender = ref(false) // 默认使用iframe渲染，更稳妥
    const showDebug = ref(false) // 关闭调试模式
    const showHtmlContent = ref(false)

    // 处理HTML内容
    const processedHtml = computed(() => {
      if (!props.chartHtml) {
        return ''
      }

      let htmlContent = props.chartHtml

      // 替换可能有问题的CDN为更可靠的CDN
      const cdnReplacements = [
        {
          old: 'https://assets.pyecharts.org/assets/v5/echarts.min.js',
          new: 'https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js'
        },
        {
          old: 'https://assets.pyecharts.org/assets/v5/',
          new: 'https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/'
        },
        {
          old: 'https://assets.pyecharts.org/assets/',
          new: 'https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/'
        }
      ]

      // 执行CDN替换
      cdnReplacements.forEach(replacement => {
        const escapedOld = replacement.old.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        const regex = new RegExp(escapedOld, 'g')
        htmlContent = htmlContent.replace(regex, replacement.new)
      })

      // 确保HTML内容包含必要的样式
      if (!htmlContent.includes('<style>')) {
        htmlContent = `
          <style>
            body { margin: 0; padding: 0; }
            #chart-container { width: 100%; height: 100%; }
          </style>
          ${htmlContent}
        `
      }

      return htmlContent
    })

    // 专门为iframe处理的HTML
    const iframeHtml = computed(() => {
      if (!props.chartHtml) {
        return ''
      }

      let htmlContent = props.chartHtml

      // 替换CDN
      const cdnReplacements = [
        {
          old: 'https://assets.pyecharts.org/assets/v5/echarts.min.js',
          new: 'https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js'
        },
        {
          old: 'https://assets.pyecharts.org/assets/v5/',
          new: 'https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/'
        },
        {
          old: 'https://assets.pyecharts.org/assets/',
          new: 'https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/'
        }
      ]

      cdnReplacements.forEach(replacement => {
        const escapedOld = replacement.old.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        const regex = new RegExp(escapedOld, 'g')
        htmlContent = htmlContent.replace(regex, replacement.new)
      })

      // 确保iframe内容是完整的HTML文档
      if (!htmlContent.includes('<!DOCTYPE html>')) {
        // 如果不是完整文档，包装成完整文档
        const docType = '<!DOCTYPE html>'
        const htmlOpen = '<html>'
        const headOpen = '<head>'
        const metaCharset = '<meta charset="UTF-8">'
        const metaViewport = '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
        const title = '<title>ECharts</' + 'title>'
        const scriptTag = '<script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></' + 'script>'
        const styleOpen = '<style>'
        const bodyStyles = `
          body { 
            margin: 0; 
            padding: 0; 
            width: 100%; 
            height: 100vh; 
            display: flex; 
            align-items: center; 
            justify-content: center;
          }
          .chart-container { 
            width: 100%; 
            height: 100%; 
          }
        `
        const styleClose = '</' + 'style>'
        const headClose = '</' + 'head>'
        const bodyOpen = '<body>'
        const bodyClose = '</' + 'body>'
        const htmlClose = '</' + 'html>'
        
        htmlContent = [
          docType,
          htmlOpen,
          headOpen,
          metaCharset,
          metaViewport,
          title,
          scriptTag,
          styleOpen,
          bodyStyles,
          styleClose,
          headClose,
          bodyOpen,
          htmlContent,
          bodyClose,
          htmlClose
        ].join('\n')
      } else {
        // 如果已经是完整文档，确保样式正确
        const headCloseTag = '</' + 'head>'
        const additionalStyles = `<style>
          body { margin: 0; padding: 0; width: 100%; height: 100vh; }
          .chart-container { width: 100%; height: 100%; }
        </style>`
        
        htmlContent = htmlContent.replace(headCloseTag, additionalStyles + headCloseTag)
      }

      return htmlContent
    })

    const toggleRenderMode = () => {
      useDirectRender.value = !useDirectRender.value
      console.log('🔄 切换渲染模式:', useDirectRender.value ? '直接渲染' : 'iframe渲染')
      
      // 重新渲染图表
      nextTick(() => {
        if (useDirectRender.value && directContainer.value) {
          executeChartScripts(directContainer.value)
        }
      })
    }

    const onIframeLoad = () => {
      console.log('📊 图表iframe加载完成')
      
      // 检查iframe中的图表是否正确渲染
      if (chartFrame.value) {
        try {
          const iframeDoc = chartFrame.value.contentDocument || chartFrame.value.contentWindow.document
          const chartContainers = iframeDoc.querySelectorAll('div[id]')
          
          console.log(`📊 iframe中找到 ${chartContainers.length} 个图表容器`)
          
          chartContainers.forEach((container, index) => {
            console.log(`📊 图表容器 ${index + 1}: ID=${container.id}, 尺寸=${container.offsetWidth}x${container.offsetHeight}`)
          })
          
          // 检查ECharts是否在iframe中正确加载
          const iframeWindow = chartFrame.value.contentWindow
          if (iframeWindow && iframeWindow.echarts) {
            console.log('✅ iframe中ECharts库已加载')
          } else {
            console.warn('⚠️ iframe中ECharts库未找到')
          }
        } catch (error) {
          console.warn('⚠️ 无法访问iframe内容:', error.message)
        }
      }
    }

    // 验证JavaScript代码语法
    const validateJavaScript = (code) => {
      try {
        // 使用Function构造函数来验证JavaScript语法
        new Function(code)
        return true
      } catch (error) {
        console.warn('⚠️ JavaScript语法验证失败:', error.message)
        return false
      }
    }

    // 清理和修复JavaScript代码
    const cleanScriptContent = (content) => {
      let cleaned = content.trim()
      
      // 移除可能的BOM字符
      cleaned = cleaned.replace(/^\uFEFF/, '')
      
      // 修复常见的语法错误
      // 1. 修复多余的右括号
      cleaned = cleaned.replace(/\]\s*\]/g, ']')
      
      // 2. 修复多余的右大括号
      cleaned = cleaned.replace(/\}\s*\}/g, '}')
      
      // 3. 修复多余的右小括号
      cleaned = cleaned.replace(/\)\s*\)/g, ')')
      
      // 4. 移除重复的分号
      cleaned = cleaned.replace(/;+/g, ';')
      
      // 5. 确保代码以分号结尾（如果不是以}结尾）
      if (cleaned && !cleaned.endsWith(';') && !cleaned.endsWith('}') && !cleaned.endsWith(')')) {
        cleaned += ';'
      }
      
      return cleaned
    }

    // 执行图表脚本
    const executeChartScripts = (container) => {
      if (!container) {
        console.warn('⚠️ 容器不存在，无法执行脚本')
        return
      }
      
      console.log('📊 开始执行图表脚本...')
      
      try {
        // 确保echarts库已加载
        if (typeof window.echarts === 'undefined') {
          console.warn('⚠️ ECharts库未加载，等待库加载完成...')
          // 动态加载echarts
          const script = document.createElement('script')
          script.src = 'https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js'
          script.onload = () => {
            console.log('✅ ECharts库加载完成，重试执行脚本')
            setTimeout(() => executeChartScripts(container), 100)
          }
          document.head.appendChild(script)
          return
        }
        
        // 查找并执行script标签
        const scripts = container.querySelectorAll('script')
        console.log(`📝 找到 ${scripts.length} 个脚本标签`)
        
        scripts.forEach((script, index) => {
          if (script.textContent && script.textContent.trim()) {
            try {
              let scriptContent = script.textContent.trim()
              
              // 检查脚本内容是否为空
              if (!scriptContent || scriptContent.length === 0) {
                return
              }
              
              console.log(`📊 处理脚本 ${index + 1}/${scripts.length}`)
              
              // 清理脚本内容
              scriptContent = cleanScriptContent(scriptContent)
              
              // 验证JavaScript语法
              if (!validateJavaScript(scriptContent)) {
                console.warn('⚠️ 跳过语法错误的脚本:', scriptContent.substring(0, 100) + '...')
                return
              }
              
              // 确保图表容器存在于DOM中
              const chartIdMatch = scriptContent.match(/document\.getElementById\(['"`]([^'"`]+)['"`]\)/)
              if (chartIdMatch) {
                const chartId = chartIdMatch[1]
                const chartContainer = document.getElementById(chartId)
                if (!chartContainer) {
                  console.warn(`⚠️ 图表容器不存在: ${chartId}`)
                  return
                }
                console.log(`✅ 找到图表容器: ${chartId}`)
              }
              
              // 使用eval执行脚本（在受控环境中）
              eval(scriptContent)
              console.log(`✅ 脚本 ${index + 1} 执行成功`)
              
            } catch (error) {
              console.error(`❌ 脚本 ${index + 1} 执行失败:`, error)
              console.error('❌ 错误脚本内容:', script.textContent.substring(0, 200))
            }
          }
        })

        // 查找并执行外部脚本
        const externalScripts = container.querySelectorAll('script[src]')
        externalScripts.forEach((script, index) => {
          if (script.src) {
            try {
              const newScript = document.createElement('script')
              newScript.src = script.src
              newScript.id = `chart-external-script-${Date.now()}-${index}`
              
              // 使用更安全的方式添加外部脚本
              if (container.appendChild) {
                container.appendChild(newScript)
                console.log('📊 加载外部脚本成功:', script.src)
              }
            } catch (error) {
              console.error('❌ 加载外部脚本失败:', error)
            }
          }
        })
      } catch (error) {
        console.error('❌ 执行图表脚本时发生错误:', error)
      }
    }

    // 监听chartHtml变化
    const handleChartHtmlChange = () => {
      if (props.chartHtml && useDirectRender.value) {
        nextTick(() => {
          if (directContainer.value) {
            console.log('📊 Chart HTML changed, re-executing scripts...')
            
            // 清除之前的脚本和图表
            const oldScripts = directContainer.value.querySelectorAll('script[id^="chart-script-"], script[id^="chart-external-script-"]')
            oldScripts.forEach(script => script.remove())
            
            // 清除现有的图表容器
            const existingCharts = directContainer.value.querySelectorAll('div[id]')
            existingCharts.forEach(chart => {
              const chartId = chart.id
              if (chartId && window.echarts) {
                const chartInstance = window.echarts.getInstanceByDom(chart)
                if (chartInstance) {
                  chartInstance.dispose()
                  console.log(`🗑️ 已销毁图表实例: ${chartId}`)
                }
              }
            })
            
            // 等待DOM更新后执行脚本
            setTimeout(() => {
              executeChartScripts(directContainer.value)
            }, 100)
          }
        })
      }
    }

    onMounted(() => {
      console.log('📊 EChartsRenderer mounted, chartHtml length:', props.chartHtml?.length || 0)
      // 初始加载时执行脚本
      if (props.chartHtml && useDirectRender.value) {
        console.log('📊 Executing initial scripts on mount...')
        nextTick(() => {
          if (directContainer.value) {
            setTimeout(() => {
              executeChartScripts(directContainer.value)
            }, 200) // 给更多时间确保DOM完全渲染
          }
        })
      }
    })

    // 监听chartHtml变化
    watch(() => props.chartHtml, () => {
      handleChartHtmlChange()
    }, { immediate: true })

    return {
      chartFrame,
      directContainer,
      processedHtml,
      iframeHtml,
      useDirectRender,
      showDebug,
      showHtmlContent,
      onIframeLoad,
      toggleRenderMode,
      handleChartHtmlChange
    }
  }
}
</script>

<style scoped>
.chart-wrapper {
  border-radius: 4px;
  overflow: hidden;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  position: relative;
}

.debug-controls {
  position: absolute;
  top: 10px;
  right: 10px;
  z-index: 1000;
  display: flex;
  gap: 5px;
}

.debug-btn {
  padding: 4px 8px;
  font-size: 12px;
  background: rgba(0, 0, 0, 0.7);
  color: white;
  border: none;
  border-radius: 3px;
  cursor: pointer;
}

.debug-btn:hover {
  background: rgba(0, 0, 0, 0.9);
}

.html-content {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  max-height: 200px;
  overflow: auto;
  background: rgba(0, 0, 0, 0.8);
  color: white;
  font-size: 10px;
  padding: 10px;
  z-index: 999;
}

.html-content pre {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
