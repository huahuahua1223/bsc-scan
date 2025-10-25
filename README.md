# BSC 区块链扫描工具

一个高性能的 BSC（Binance Smart Chain）区块链数据扫描和分析工具，用于提取和过滤 Token 转账事件。

## ✨ 特性

- 🚀 **一键全链扫描**：从创世区块扫描到指定区块
- 🔄 **断点续传**：支持中断后从上次位置继续
- 🎯 **精确过滤**：使用 PartitionedBloomFilter 高效过滤目标用户
- 👤 **EOA 识别**：智能过滤合约地址，仅保留 EOA 到 EOA 的转账
- 💰 **USD 价值计算**：自动计算转账的美元价值
- 📊 **分片输出**：大文件自动分片（500万行/文件），便于处理
- 🛡️ **健壮性强**：网络重试、错误恢复、优雅退出
- ⚡ **流式处理**：内存友好，支持大规模数据处理
- 🎛️ **固定步长扫描**：稳定高效的区块扫描策略
- 📦 **批量传输**：HTTP 批量请求，降低网络延迟
- 👥 **用户视角输出**：从用户角度记录转入/转出/自转账

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
├── scan_bloom.mjs          # 主扫描脚本（带 EOA 过滤和提速优化）
├── eoa.mjs                 # EOA/合约地址判定模块
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
| `START_BLOCK` | `6000000` | 扫描起始区块号 |
| `TARGET_END` | `6244000` | 目标终点区块号（或 "latest"） |
| `FOLLOW_LATEST` | `0` | 是否持续追踪新区块 |
| `BLOCK_STEP` | `2000` | 固定区块步长 |
| `ADDRESS_CHUNK` | `10` | 每次查询的地址数量 |
| `USD_THRESHOLD` | `100` | USD 过滤阈值（小于此值丢弃） |
| `EOA_ONLY` | `1` | 只保留 EOA 到 EOA 的转账 |
| `INCLUDE_BLOCK_TS` | `0` | 是否包含区块时间戳 |
| `ROW_LIMIT` | `5000000` | 每个输出文件的最大行数 |
| `CONFIRMATIONS` | `40` | 区块确认深度 |
| `RPC_HTTP` | `http://127.0.0.1:8545` | BSC 节点 RPC 地址 |

## 📊 输出格式

扫描结果保存为 CSV 文件（`output/edge_events_part*.csv`），包含以下字段：

| 字段 | 说明 |
|------|------|
| `tx_hash` | 交易哈希 |
| `log_index` | 日志索引 |
| `block_number` | 区块号 |
| `timestamp` | 区块时间戳（可选） |
| `token_address` | Token 合约地址（小写） |
| `raw_amount` | 原始转账数量 |
| `usd_value` | USD 价值 |
| `from` | 发送方地址（小写） |
| `to` | 接收方地址（小写） |
| `user_address` | 用户地址（命中 Bloom 的一方） |
| `counterparty_address` | 对手方地址 |
| `direction_flag` | 方向标识：0=in, 1=out, 2=self |

### 方向标识说明

- **`0` (in)**：转入用户，`user_address` 是接收方
- **`1` (out)**：从用户转出，`user_address` 是发送方
- **`2` (self)**：自转账，`from == to`

### 唯一键

每条记录的唯一键为：`(tx_hash, log_index, user_address)`

### 特殊情况处理

- **单边命中**：只输出一行（from 或 to 其中一个在用户列表中）
- **双边命中**：输出两行（from 和 to 都在用户列表中，表示用户间转账）
- **自转账**：只输出一行，`direction_flag = 2`

## 🛠️ 技术栈

- **Node.js** - 运行环境
- **viem** - 以太坊/BSC 交互库
- **bloom-filters** - 高效地址过滤（PartitionedBloomFilter）
- **fast-csv** - CSV 文件处理
- **csv-parse** - CSV 数据解析

## 🔍 核心优化

### 1. PartitionedBloomFilter
使用分区布隆过滤器替代标准布隆过滤器，提供更好的性能和内存效率。

### 2. 地址规范化
全链路地址小写和 trim 处理，确保：
- Bloom Filter 构建时统一格式
- 查询时统一格式
- 输出时统一格式
- 避免大小写不匹配导致的遗漏

### 3. 用户视角输出
从用户角度记录每笔交易：
- 明确标识转入/转出/自转账
- 保持唯一键约束
- 便于后续分析和聚合

### 4. 断点续传
- 每 50 个批次自动保存进度
- 支持中断后从断点继续
- 失败批次单独记录，可后续重试

### 5. 流式处理
- 内存友好的流式 CSV 读写
- 高水位缓冲（32MB）
- 自动分片输出（500万行/文件）

## 📖 详细文档

查看 [使用说明.md](使用说明.md) 获取完整的使用指南和配置说明。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

MIT License
