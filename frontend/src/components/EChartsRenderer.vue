<template>
  <div class="chart-wrapper" :style="{ width: '100%', height }">
    <div ref="hostRef" class="chart-host" :style="{ width: '100%', height: '100%' }"></div>
    <div v-if="diagMessages.length" class="chart-error-overlay">
      <div class="overlay-title">渲染诊断</div>
      <div
        v-for="(msg, idx) in diagMessages"
        :key="`diag-${idx}`"
        class="overlay-line"
      >
        {{ msg }}
      </div>
    </div>
  </div>
</template>

<script>
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echartsLib from 'echarts'

const ECHARTS_CDN = 'https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js'
const loadedExternalScripts = new Set()

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
    const hostRef = ref(null)
    const diagMessages = ref([])
    let renderToken = 0
    const MAX_DIAG_LINES = 8
    let resizeObserver = null
    let resizeRaf = 0

    if (typeof window !== 'undefined' && !window.echarts) {
      window.echarts = echartsLib
    }

    const clearDiag = () => {
      diagMessages.value = []
    }

    const pushDiag = (message) => {
      const text = String(message || '').trim()
      if (!text) return
      const next = [...diagMessages.value, text]
      diagMessages.value = next.slice(-MAX_DIAG_LINES)
    }

    const replaceCdn = (html) => {
      let next = String(html || '')
      const replacements = [
        'https://assets.pyecharts.org/assets/v5/echarts.min.js',
        'https://assets.pyecharts.org/assets/v5/',
        'https://assets.pyecharts.org/assets/'
      ]
      replacements.forEach((oldUrl) => {
        const escaped = oldUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        next = next.replace(new RegExp(escaped, 'g'), ECHARTS_CDN)
      })
      return next
    }

    const extractBodyContent = (html) => {
      const raw = String(html || '')
      if (!raw) return ''
      if (!/<!doctype\s+html>/i.test(raw)) return raw
      const bodyMatch = raw.match(/<body[^>]*>([\s\S]*?)<\/body>/i)
      if (bodyMatch && bodyMatch[1]) return bodyMatch[1]
      return raw
    }

    const disposeChartsInHost = (host) => {
      if (!host || !window.echarts) return
      const nodes = host.querySelectorAll('div[id], .chart-container')
      nodes.forEach((node) => {
        try {
          const inst = window.echarts.getInstanceByDom(node)
          if (inst) inst.dispose()
        } catch (error) {
          console.error(error)
        }
      })
    }

    const resizeChartsInHost = () => {
      const host = hostRef.value
      if (!host || !window.echarts) return
      const nodes = host.querySelectorAll('div[id], .chart-container')
      nodes.forEach((node) => {
        try {
          const inst = window.echarts.getInstanceByDom(node)
          if (inst) {
            inst.resize({
              animation: false
            })
          }
        } catch (error) {
          console.error(error)
        }
      })
    }

    const scheduleResize = () => {
      if (resizeRaf) cancelAnimationFrame(resizeRaf)
      resizeRaf = requestAnimationFrame(() => {
        resizeRaf = 0
        resizeChartsInHost()
      })
    }

    const ensureExternalScript = (src) => {
      const url = String(src || '').trim()
      if (!url) return Promise.resolve()
      if (/echarts(\.min)?\.js/i.test(url)) return Promise.resolve()
      if (loadedExternalScripts.has(url)) return Promise.resolve()

      return new Promise((resolve, reject) => {
        const attr = 'data-echarts-renderer-src'
        const exist = document.querySelector(`script[${attr}="${url}"]`)
        if (exist) {
          const loaded = exist.getAttribute('data-loaded') === '1'
          if (loaded) {
            loadedExternalScripts.add(url)
            resolve()
            return
          }
          exist.addEventListener('load', () => {
            loadedExternalScripts.add(url)
            resolve()
          }, { once: true })
          exist.addEventListener('error', () => {
            reject(new Error(`load script failed: ${url}`))
          }, { once: true })
          return
        }

        const script = document.createElement('script')
        script.src = url
        script.async = false
        script.setAttribute(attr, url)
        script.addEventListener('load', () => {
          script.setAttribute('data-loaded', '1')
          loadedExternalScripts.add(url)
          resolve()
        }, { once: true })
        script.addEventListener('error', () => {
          reject(new Error(`load script failed: ${url}`))
        }, { once: true })
        document.head.appendChild(script)
      })
    }

    const renderChart = async () => {
      clearDiag()
      renderToken += 1
      const token = renderToken
      await nextTick()

      const host = hostRef.value
      if (!host) {
        pushDiag('宿主容器不存在')
        return
      }

      disposeChartsInHost(host)
      host.innerHTML = ''

      const raw = replaceCdn(props.chartHtml)
      if (!raw) {
        pushDiag('chartHtml 为空')
        return
      }

      const html = extractBodyContent(raw)
      const temp = document.createElement('div')
      temp.innerHTML = html

      const scripts = Array.from(temp.querySelectorAll('script'))
      scripts.forEach(node => node.remove())

      while (temp.firstChild) {
        host.appendChild(temp.firstChild)
      }

      for (const script of scripts) {
        if (token !== renderToken) return
        const src = script.getAttribute('src')
        if (src) {
          try {
            await ensureExternalScript(src)
          } catch (error) {
            console.error(error)
            pushDiag(`外部脚本加载失败: ${src}`)
          }
          continue
        }
        const code = String(script.textContent || '')
        if (!code.trim()) continue
        try {
          const runner = new Function(code)
          runner()
        } catch (error) {
          console.error(error)
          const summary = error && error.message ? error.message : String(error)
          pushDiag(`内联脚本执行失败: ${summary}`)
        }
      }

      if (window.echarts) {
        const containers = host.querySelectorAll('div[id], .chart-container')
        let instanceCount = 0
        containers.forEach((node) => {
          try {
            const inst = window.echarts.getInstanceByDom(node)
            if (inst) {
              instanceCount += 1
              inst.resize({
                animation: false
              })
            }
          } catch (error) {
            console.error(error)
            pushDiag(`实例 resize 失败: ${error?.message || error}`)
          }
        })
        if (!instanceCount) {
          pushDiag(`未创建 ECharts 实例（容器=${containers.length}，脚本=${scripts.length}）`)
        }
      } else {
        pushDiag('window.echarts 未挂载')
      }

      scheduleResize()
    }

    const bindHostResize = () => {
      const host = hostRef.value
      if (!host || typeof window === 'undefined') return
      if (typeof ResizeObserver !== 'undefined') {
        resizeObserver = new ResizeObserver(() => {
          scheduleResize()
        })
        resizeObserver.observe(host)
        if (host.parentElement) {
          resizeObserver.observe(host.parentElement)
        }
      }
      window.addEventListener('resize', scheduleResize, { passive: true })
    }

    const unbindHostResize = () => {
      if (resizeObserver) {
        resizeObserver.disconnect()
        resizeObserver = null
      }
      if (typeof window !== 'undefined') {
        window.removeEventListener('resize', scheduleResize)
      }
      if (resizeRaf) {
        cancelAnimationFrame(resizeRaf)
        resizeRaf = 0
      }
    }

    onMounted(() => {
      bindHostResize()
      renderChart()
    })

    watch(() => props.chartHtml, () => {
      renderChart()
    })

    watch(() => props.height, () => {
      scheduleResize()
    })

    onBeforeUnmount(() => {
      unbindHostResize()
      const host = hostRef.value
      if (host) disposeChartsInHost(host)
    })

    return {
      hostRef,
      diagMessages
    }
  }
}
</script>

<style scoped>
.chart-wrapper {
  overflow: hidden;
  position: relative;
}

.chart-host {
  overflow: hidden;
}

.chart-error-overlay {
  position: absolute;
  right: 8px;
  bottom: 8px;
  max-width: 80%;
  max-height: 45%;
  overflow: auto;
  background: rgba(0, 0, 0, 0.78);
  color: #ffb4b4;
  border: 1px solid rgba(255, 120, 120, 0.5);
  font-size: 12px;
  line-height: 1.4;
  padding: 8px 10px;
  z-index: 5;
}

.overlay-title {
  color: #ffd5d5;
  font-weight: 600;
  margin-bottom: 4px;
}

.overlay-line {
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
