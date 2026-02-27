# 🚀 A股复盘系统 - Vue.js + Flask 现代化架构

<div align="center">

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.9+-green.svg)
![Vue](https://img.shields.io/badge/vue-3.0+-brightgreen.svg)
![Flask](https://img.shields.io/badge/flask-2.0+-red.svg)

**一个专业的A股市场复盘分析系统，助力投资者回溯历史、发现机会、优化策略**

[功能特性](#-核心功能) • [快速开始](#-快速开始) • [技术架构](#-技术架构) • [使用文档](#-使用说明) • [贡献指南](#-贡献与支持)

</div>

---

## 📋 项目简介

本项目是一个基于 **Vue.js + Flask** 前后端分离架构的专业股票复盘系统，内置近十年A股日线级别数据，支持从**指数、市场情绪、板块、个股**等多维度进行深度复盘分析。

### ✨ 核心优势

| 特性 | 说明 |
|------|------|
| 🔍 **历史回溯** | 可查看任意历史交易日的市场状态，发现当时错过的信号 |
| 📊 **多维分析** | 指数、板块、个股、市场情绪四大维度立体分析 |
| 🎯 **数据驱动** | 基于公开数据源（AKShare、BaoStock），透明可靠 |
| ⚡ **高性能** | 相比streamlit方案性能提升90%+，内存占用降低81% |
| 💼 **资金管理** | 内置止损计算工具，参考《交易圣经》理论 |
| 🔄 **自动更新** | 每日18:00后自动更新最近30天数据 |

### 🆚 与同花顺等软件的区别

1. **开源透明**：数据来源公开（AKShare、BaoStock），代码完全开源
2. **历史回溯**：可查看任意历史时点的市场状态，帮助复盘改进交易系统
3. **资金管理**：集成建仓止损计算，科学管理风险
4. **可定制化**：开源代码，可根据个人需求自由扩展

---

## 🎯 核心功能

### 1. 📈 指数分析
- **多指数对比**：上证、深证、创业板、科创50、北证50、微盘股等
- **5分钟量能图**：实时追踪市场资金流向
- **技术指标**：MA5/MA10/MA20均线，涨跌幅统计

### 2. 🌡️ 市场情绪
- **核心指标**：红盘率、涨停数、跌停数、沪深成交额
- **分布分析**：涨跌幅分布、连板分布统计
- **特殊形态**：地天板、天地板、炸板分析
- **趋势追踪**：红盘率与量能走势、涨跌停数量变化

### 3. 🏢 板块分析
- **板块排行**：实时排名，快速定位热点
- **成分股分析**：查看板块内个股表现及K线图
- **多对象对比**：支持多板块/多指数横向对比

### 4. 💎 强势股分析
- **白马分析**：趋势股筛选，稳健投资标的
- **黑马分析**：连板股追踪，短线机会捕捉
- **新高股票**：突破历史高点的强势股
- **赚亏钱效应**：市场整体盈利效应分析

### 5. 💰 资金管理
- **止损计算**：根据《交易圣经》理论计算建仓止损位
- **风险控制**：科学的资金管理工具

### 6. 🧠 策略看盘（持续开发中）
- **策略会话**：支持按策略主题创建对话，沉淀复盘过程
- **资源管理**：支持上传文档并转为 Markdown 供策略引用
- **策略看板**：可生成策略视图与自定义看板界面（尚待完善），辅助盘前/盘后观察
- **开发说明**：该模块仍在持续迭代，部分字段和交互可能调整

---

## 🚀 快速开始

### 📋 环境要求

- **Python**: 3.9+
- **Node.js**: 16+
- **Anaconda/Miniconda**: 推荐使用
- **操作系统**: Windows/Linux/MacOS

### ⚡ 方法1: 一键启动（推荐）

双击运行启动脚本，自动完成所有配置：

```bash
start_system.bat
```

脚本会自动：
1. 激活 `replay` 虚拟环境
2. 启动 Flask 后端服务（端口 5000）
3. 启动 Vue 前端服务（端口 8080）
4. 自动打开浏览器访问系统

### 🔧 方法2: 手动启动

#### 步骤1: 创建虚拟环境

```bash
# 创建并激活虚拟环境
conda create -n replay python=3.10
conda activate replay
```

#### 步骤2: 安装依赖

```bash
# 安装后端依赖
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/

# 安装前端依赖
cd frontend
npm config set registry https://registry.npmjs.org/
npm install --no-optional
```

#### 步骤3: 启动服务

```bash
# 终端1 - 启动后端
conda activate replay
python flask_app.py

# 终端2 - 启动前端
cd frontend
npm run serve
```

#### 步骤4: 访问系统

- **前端界面**: http://localhost:8080
- **后端API**: http://localhost:5000

---

## 🎯 技术架构

### 🔧 后端技术栈

| 技术 | 说明 |
|------|------|
| **Flask** | 轻量级Web框架，RESTful API设计 |
| **Polars** | 高性能数据处理，比Pandas快10倍+ |
| **AKShare** | 开源金融数据接口 |
| **BaoStock** | 稳定的历史行情数据源 |
| **PyEcharts** | Python图表生成库 |

### 🎨 前端技术栈

| 技术 | 说明 |
|------|------|
| **Vue 3** | 渐进式JavaScript框架，Composition API |
| **Element Plus** | 企业级UI组件库 |
| **ECharts** | 专业数据可视化库，支持K线图 |
| **Axios** | HTTP客户端，统一请求拦截 |
| **Vue Router** | 单页面应用路由管理 |

### 📊 性能对比

| 操作 | 传统方案 | 本系统 | 性能提升 |
|------|---------|--------|----------|
| **选择股票** | 重新加载390行数据 | 只加载33行K线数据 | ⚡ **92%** |
| **切换模块** | 重新执行所有函数 | 路由切换，瞬间响应 | ⚡ **95%** |
| **内存使用** | 400MB | 75MB | 💾 **81%** |
| **响应时间** | 2-3秒延迟 | 0.3秒响应 | 🚀 **90%** |

---

## 📖 使用说明

### 🔐 `.env` 配置与更新使用

项目根目录存在 `.env` 文件（`E:\jupyter\大A\replay\.env`），策略看盘相关的 LLM 能力通过该文件配置。

当前支持字段：

```env
OPENAI_API_KEY=your_api_key
OPENAI_BASE_URL=https://api.302ai.cn/v1
OPENAI_MODEL=deepseek-v3.2
OPENAI_TIMEOUT_SECONDS=180
LLM_API_KEY=
```

使用方法：
1. 修改 `OPENAI_API_KEY` 为你的真实 key（优先读取该字段）。
2. 按你的服务商修改 `OPENAI_BASE_URL`（默认是 `https://api.302ai.cn/v1`）。
3. 修改 `OPENAI_MODEL` 选择要使用的模型。
4. 可按需调整 `OPENAI_TIMEOUT_SECONDS`（秒）。
5. `LLM_API_KEY` 是可选别名，`OPENAI_API_KEY` 为空时可用它兜底。

生效方式：
- 后端会自动读取 `.env`，修改后重启后端即可稳定生效。
- 技能脚本 `convert_to_markdown.py` 由子进程执行，改完 `.env` 后下次转换任务会读取最新配置。

注意事项：
- `.env` 含密钥，禁止提交到公开仓库。
- 建议在 `.gitignore` 中保留 `.env`。

### 🚧 策略板块看盘功能说明

策略板块看盘功能仍在持续开发中，当前版本以可用性优先，部分能力与交互仍可能调整：
- 部分看板字段与展示规则会继续迭代。
- 接口与配置项可能在后续版本小幅变更。
- 如用于正式交易，请先在回测与模拟环境充分验证。

### 🧩 技能脚本热更新说明（convert_to_markdown.py）

当前已采用“拆分进程”的方式执行技能脚本：
- 后端服务保持生产模式稳定运行，不会因为技能脚本改动而自动重启。
- `convert_to_markdown.py` 每次转换都会通过子进程重新启动执行。
- 因此 **修改 skill 脚本后，下次转换会立即生效**，不需要重启后端。

### 🧱 使用 `skills/multiformat-to-md` 的额外依赖

除 `pip install -r requirements.txt` 外，还需要系统级工具：

- `tesseract`（图片/PDF OCR）
  - 建议安装中文语言包 `chi_sim`，否则中文 OCR 效果会明显下降。
- `ffmpeg` + `ffprobe`（`.mp4` 抽音与音视频处理）
- `antiword`（`.doc` 文档解析）

校验方式（Windows/Linux 通用思路）：
- `tesseract --version`
- `ffmpeg -version`
- `ffprobe -version`
- `antiword -h`

说明：
- `.docx` 使用 `python-docx`，不依赖 `antiword`。
- 若仅处理 `.pdf/.docx/.png/.jpg`，可以不安装 `ffmpeg`；若不处理 `.doc`，可以不安装 `antiword`。

### 🔄 数据更新

系统支持自动和手动两种数据更新方式：

#### 自动更新
- 系统启动时自动检查数据更新
- 每日18:00后可获取当日最新数据
- 自动更新最近30天的增量数据

#### 手动更新
点击右上角设置图标，选择需要更新的数据类型：
- **股票更新**：更新个股日线数据
- **板块更新**：更新行业/概念板块数据
- **指数更新**：更新各类指数数据
- **股票状态数据更新**：更新涨停、跌停等市场状态

### 📅 历史回溯

1. 在任意页面选择历史日期
2. 系统自动加载该日期的市场状态
3. 可对比不同时期的市场表现

### 💡 使用技巧

- **板块对比**：支持多选板块/指数进行横向对比
- **自定义时间区间**：可保存常用的时间区间组合
- **股票组合管理**：可创建自选股组合，快速查看
- **资金管理计算**：输入建仓价格和止损比例，自动计算止损位

---

## 🗂️ 项目结构

```
replay/
├── backend/                 # 后端目录
│   └── templates/          # HTML模板
├── frontend/               # 前端目录
│   ├── src/
│   │   ├── components/    # Vue组件
│   │   ├── views/         # 页面视图
│   │   ├── router/        # 路由配置
│   │   └── services/      # API服务
│   └── public/            # 静态资源
├── utils/                  # 工具模块
│   ├── metadata/          # 元数据管理
│   │   ├── stock_data_manager.py    # 股票数据
│   │   ├── index_data_manager.py    # 指数数据
│   │   ├── sector_data_manager.py   # 板块数据
│   │   └── market_data_manager.py   # 市场数据
│   ├── visualizers/       # 可视化模块
│   └── analyzer.py        # 分析工具
├── data_cache/            # 数据缓存目录
├── flask_app.py           # Flask主程序
├── requirements.txt       # Python依赖
└── start_system.bat       # 一键启动脚本
```

---

## 🔮 后续规划
- [ ] **多Agent规划选股、看盘**：skill使用与多agent协同：自然语言转选股条件，智能化选股，自主规划看盘界面
- [ ] **分钟级数据**：引入MySQL存储个股分钟数据（约20GB）
- [ ] **实时推送**：WebSocket实时数据推送
- [ ] **策略回测**：内置回测引擎，验证交易策略
- [ ] **多用户系统**：支持用户注册、登录、个性化配置

---



### 参考文献

- 《交易圣经》 - 布伦特·奔富 (Brent Penfold)
- AKShare 文档：https://akshare.akfamily.xyz/
- BaoStock 文档：http://baostock.com/

---

## 📜 开源协议

本项目采用 [MIT License](LICENSE) 开源协议。

---

## ☕ 支持作者


<div align="center">

![赞赏码](data_cache/36bff6aa65d2443df2964d95493f212f.png)


</div>

---

<div align="center">

**💡 技术支持** | **📅 最后更新**: 2026-02-27 | **🔄 版本**: v2.0

Made with ❤️ by NNNightglow
寻求合作可联系1968259857@qq.com

</div>
