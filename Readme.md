# A股实时监控与AI分析系统 (Stock Monitor TYI)

这是一个基于 Rust (Axum) 开发的A股实时行情监控服务，支持自定义股票/指数列表、实时数据刷新、分时/K线图表展示，并集成了大模型（LLM）进行智能趋势研判、择时信号分析和拐点检测。

## 🚀 功能特性

1. **实时行情监控**：对接财经接口，实时获取A股指数和个股的开盘、收盘、最高、最低、成交量等数据。
2. **自定义配置**：支持通过前端或配置文件动态添加/删除关注的股票和指数，支持调整数据刷新频率。
3. **多维度图表**：
   - **分时图**：展示当日实时价格走势及均价线。
   - **K线图**：支持日K、周K、月K切换，包含成交量柱状图。
4. **资金流向**：对接东方财富接口，展示主力、超大单、大单、中单、小单的资金净流入情况。
5. **AI 智能分析**：
   - 支持配置多个 AI 提供商（兼容 OpenAI 格式，如 OpenRouter, Ollama, DeepSeek 等）。
   - 支持主备模型自动切换。
   - 提供三种分析维度：**趋势研判**、**择时信号**、**拐点检测**。
   - 前端可视化展示核心观点（看多/看空/震荡）及详细理由。

## 🛠️ 技术栈

- **后端**: Rust, Axum, Tokio, Reqwest, Serde
- **前端**: HTML5, CSS3, JavaScript (Vanilla), ECharts
- **数据源**: 新浪财经 (HQ), 东方财富 (EastMoney)

## ⚙️ 配置说明

项目启动时会自动读取 `setting/config.json` 文件。如果文件不存在，将使用默认配置并自动生成该文件。

## 配置文件 (`setting/config.json`)


## ⚙️  运行指南
1. 环境要求
Rust 工具链 (Nightly 或 Stable)
Cargo

2. 编译与运行

## ⚙️  克隆项目,运行项目

git clone https://github.com/liypsky/Stock_Monitor_AI.git
cd stock-monitor-tyi
cargo run 

浏览器打开 http://localhost:9527 即可看到监控首页。
点击个股可进入详情页查看图表和 AI 分析。

## ⚙️  非编译直接运行指南

进入仓库 Releases 页面，直接下载对应系统压缩包，解压运行即可，无需装 Rust 环境、无需编译.
1. Windows → 下载Windows 版
2. Linux → 下载 Linux 版
3. macOS → 下载 macOS 版

操作步骤:
1. 下载对应系统压缩包
2. 解压缩包
3. 运行程序
    Windows：双击 stock-monitor.exe
    Linux/macOS：./stock-monitor
4. 浏览器打开：http://localhost:9527

# 目录结构
参考DOC目录下文件结构文档.


⚠️ 免责声明
本项目仅供学习和技术研究使用。股市有风险，投资需谨慎。AI 分析结果仅供参考，不构成任何投资建议。使用者需自行承担因使用本软件产生的所有风险。