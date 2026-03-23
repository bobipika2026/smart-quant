# Smart Quant

> 让每个人都能进行策略交易的开源工具

## 简介

Smart Quant 是一个面向个人投资者的开源策略交易平台，提供策略管理、历史回测、智能选股、实时监控和信号通知等功能，让普通人也能用上专业级的交易策略。

## 功能特性

- 🎯 **策略库** - 内置28种成熟策略，开箱即用
- 📊 **回测系统** - 历史数据回测，完整评价体系
- 🔍 **智能选股** - 条件选股 + 策略选股
- 📡 **实时监控** - 7x24小时行情监控
- 🔔 **信号通知** - 多渠道推送，不错过交易机会
- 🧮 **因子矩阵** - 93万种组合回测，寻找最优策略组合

## 技术栈

- **后端**: Python 3.10+ / FastAPI / SQLite
- **前端**: Vue 3 / Vite / Naive UI / ECharts
- **数据源**: AkShare / TuShare

## 快速开始

### 环境要求

- Python 3.10+
- Node.js 18+
- Docker (可选)

### 本地开发

```bash
# 克隆项目
git clone https://github.com/bobipika2026/smart-quant.git
cd smart-quant

# 后端启动
cd backend
pip install -r requirements.txt
python -m uvicorn app.main:app --reload

# 前端启动
cd frontend
npm install
npm run dev
```

### Docker 部署

```bash
docker-compose up -d
```

## 项目结构

```
smart-quant/
├── backend/                # Python 后端
│   ├── app/
│   │   ├── api/           # API 路由
│   │   ├── services/      # 业务逻辑
│   │   ├── models/        # 数据模型
│   │   └── main.py        # 入口文件
│   └── requirements.txt
├── frontend/              # Vue 前端
│   ├── src/
│   │   ├── views/        # 页面
│   │   ├── components/   # 组件
│   │   └── api/          # API 调用
│   └── package.json
├── docker-compose.yml
└── README.md
```

## 文档

- [策略说明](docs/STRATEGIES.md) - 28种内置策略详细说明
- [因子矩阵](docs/FACTOR_MATRIX.md) - 因子组合、回测逻辑、评价体系
- [开发计划](docs/DEV_PLAN.md) - 版本规划和开发进度

## 内置策略（28种）

| 类型 | 策略 |
|------|------|
| 趋势策略 | MA金叉、MACD、布林带、ATR、SAR、Aroon、DMI、唐奇安、肯特纳、一目均衡、ZigZag、DEMA、EXPMA |
| 震荡策略 | KDJ、RSI、CCI、WR、BIAS、Stochastic、MFI |
| 量价策略 | OBV、VWAP、量价策略 |
| 动量策略 | MOM、ROC、ADX |

详细说明请查看 [策略文档](docs/STRATEGIES.md)

## 免责声明

本项目仅供学习和研究使用，所有策略信号仅供参考，不构成投资建议。投资有风险，入市需谨慎。

## License

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

---

**让量化交易变得简单** 🚀