# BSC 区块链扫描工具

一个高性能的 BSC（Binance Smart Chain）区块链数据扫描和分析工具，用于提取和过滤 Token 转账事件。

## ✨ 特性

- 🚀 **一键全链扫描**：从创世区块扫描到指定区块
- 🔄 **断点续传**：支持中断后从上次位置继续
- 🎯 **精确过滤**：使用 Bloom Filter 高效过滤目标用户
- 💰 **USD 价值计算**：自动计算转账的美元价值
- 📊 **分片输出**：大文件自动分片，便于处理
- 🛡️ **健壮性强**：网络重试、错误恢复、优雅退出
- ⚡ **流式处理**：内存友好，支持大规模数据处理

## 🚀 快速开始

### 1. 安装依赖
```bash
pnpm install
```

### 2. 构建 Bloom Filter
```bash
node build_bloom.mjs
```

### 3. 开始扫描
```bash
# Windows 用户
start_scan.bat

# Linux/Mac 用户
./start_scan.sh

# 或手动运行
RPC_HTTP=https://bsc-rpc.publicnode.com node scan_bloom.mjs
```

## 📁 项目结构

```
bsc-scan/
├── scan_bloom.mjs          # 主扫描脚本
├── build_bloom.mjs         # Bloom Filter 构建脚本
├── start_scan.bat          # Windows 启动脚本
├── start_scan.sh           # Linux/Mac 启动脚本
├── token_withbalance.csv   # Token 价格数据
├── final-results/          # 用户地址数据（26个分片）
├── output/                 # 扫描结果输出目录
├── package.json            # 项目配置
├── 使用说明.md             # 详细使用说明
└── README.md               # 项目说明
```

## 🔧 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `START_BLOCK` | `0` | 扫描起始区块号 |
| `TARGET_END` | `latest` | 目标终点区块号 |
| `FOLLOW_LATEST` | `1` | 是否持续追踪新区块 |
| `BLOCK_STEP` | `5000` | 每次查询的区块范围 |
| `ADDRESS_CHUNK` | `10` | 每次查询的地址数量 |
| `RPC_HTTP` | - | BSC 节点 RPC 地址 |

## 📊 输出格式

扫描结果保存为 CSV 文件，包含以下字段：

```csv
tx_hash,log_index,block_number,timestamp,token_address,raw_amount,usd_value,from,to
```

## 🛠️ 技术栈

- **Node.js** - 运行环境
- **viem** - 以太坊/BSC 交互库
- **bloom-filters** - 高效地址过滤
- **fast-csv** - CSV 文件处理
- **csv-parse** - CSV 数据解析

## 📖 详细文档

查看 [使用说明.md](使用说明.md) 获取完整的使用指南和配置说明。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

MIT License
