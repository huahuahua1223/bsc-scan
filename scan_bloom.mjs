// scan_bloom.mjs
// 运行：START_BLOCK=0 END_BLOCK=500000 RPC_HTTP=http://127.0.0.1:8545 node scan_bloom.mjs

import { createPublicClient, http, parseAbiItem } from "viem";
import { defineChain } from "viem/utils";
import fs from "fs";
import path from "path";
import { parse } from "csv-parse";
import csv from "fast-csv";
import bloomPkg from "bloom-filters";
const { BloomFilter } = bloomPkg;

// ==== 参数 ====
const RPC_HTTP = process.env.RPC_HTTP || "http://127.0.0.1:8545";
const START_BLOCK = Number(process.env.START_BLOCK ?? 0);
const END_BLOCK   = Number(process.env.END_BLOCK   ?? 500_000);
const BLOCK_STEP  = Number(process.env.BLOCK_STEP  || 5000); // 默认5000个区块一次
const ADDRESS_CHUNK = Number(process.env.ADDRESS_CHUNK || 10);
const CONFIRMATIONS = Number(process.env.CONFIRMATIONS || 40);
const USD_THRESHOLD = Number(process.env.USD_THRESHOLD || 100);
const ROW_LIMIT = Number(process.env.ROW_LIMIT || 5_000_000);

// === 新增：追块&终点 ===
const FOLLOW_LATEST = String(process.env.FOLLOW_LATEST || "0") === "1"; // 扫完一轮后是否继续追新块
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 15000); // 追块时每轮睡眠（15秒）
// TARGET_END 可以是整数区块号，或 "latest"（默认）
const TARGET_END_RAW = process.env.TARGET_END || "62530100";

const TOKENS_CSV = process.env.TOKENS_CSV || "./token_withbalance.csv";
const BLOOM_JSON = process.env.BLOOM_JSON || "./userlist.bloom.json";
const PROGRESS_FILE = process.env.PROGRESS_FILE || "./scan_progress.json";
const CHECKPOINT_INTERVAL = Number(process.env.CHECKPOINT_INTERVAL || 50); // 每100个窗口保存一次进度
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 5); // 最大重试次数
const RETRY_DELAY_BASE = Number(process.env.RETRY_DELAY_BASE || 1000); // 重试延迟基数（毫秒）

// ==== 流式读取 Token 数据 ====
const TOKENS = {};
const TOKEN_ADDRESSES = [];

async function loadTokenData() {
  return new Promise((resolve, reject) => {
    const results = [];
    const stream = fs.createReadStream(TOKENS_CSV, { encoding: 'utf8' });
    
    stream
      .pipe(parse({ columns: true, trim: true, skip_empty_lines: true }))
      .on('data', (row) => {
        TOKENS[row.token_address] = { 
          decimals: Number(row.decimals), 
          price: Number(row.usd_price) 
        };
        TOKEN_ADDRESSES.push(row.token_address);
      })
      .on('end', () => {
        console.log(`[数据] 已加载 ${TOKEN_ADDRESSES.length} 个Token信息`);
        resolve();
      })
      .on('error', reject);
  });
}

// ==== 读 Bloom ====
if (!fs.existsSync(BLOOM_JSON)) {
  console.error(`Bloom not found: ${BLOOM_JSON}`);
  process.exit(1);
}
const bloom = BloomFilter.fromJSON(JSON.parse(fs.readFileSync(BLOOM_JSON, "utf8")));

// ==== viem client ====
const bsc = defineChain({
  id: 56, name: "BSC",
  nativeCurrency: { name: "BNB", symbol: "BNB", decimals: 18 },
  rpcUrls: { default: { http: [RPC_HTTP] } },
});
const client = createPublicClient({ chain: bsc, transport: http(RPC_HTTP) });
const transferEvent = parseAbiItem(
  "event Transfer(address indexed from, address indexed to, uint256 value)"
);

// ==== 输出：按 5,000,000 行/文件 自动切分 ====
const outDir = "./output";
fs.mkdirSync(outDir, { recursive: true });

let part = 1;
let rowsInPart = 0;
let current; // { path, stream, writer }

function openNewPart() {
  const filePath = path.join(outDir, `edge_events_part${part}.csv`);
  const stream = fs.createWriteStream(filePath, { flags: "w" }); // 新建并覆盖
  const writer = csv.format({ headers: true });                 // 每个分片都写表头
  writer.pipe(stream);
  return { path: filePath, stream, writer };
}
function closePart(obj) {
  return new Promise((resolve) => {
    obj.writer.end();
    obj.stream.end(resolve); // 确保 flush 完成再继续，避免 0 字节文件
  });
}
async function rotateIfNeeded() {
  if (rowsInPart >= ROW_LIMIT) {
    await closePart(current);
    part += 1;
    rowsInPart = 0;
    current = openNewPart();
  }
}
async function writeRow(row) {
  current.writer.write(row);
  rowsInPart += 1;
  await rotateIfNeeded();
}

// ==== 工具 ====
function chunks(arr, n) { const o = []; for (let i = 0; i < arr.length; i += n) o.push(arr.slice(i, i + n)); return o; }

// 解析目标终点区块号
function resolveTargetEnd(head) {
  if (TARGET_END_RAW.toLowerCase?.() === "latest") return head;
  const n = Number(TARGET_END_RAW);
  return Number.isFinite(n) && n > 0 ? BigInt(n) : head;
}
function usd(token, rawHex) {
  const m = TOKENS[token]; if (!m) return null;
  const v = BigInt(rawHex);
  return (Number(v) / 10 ** m.decimals) * m.price;
}

const tsCache = new Map();
async function ts(bn) {
  const k = Number(bn);
  if (tsCache.has(k)) return tsCache.get(k);
  const blk = await client.getBlock({ blockNumber: bn });
  const t = Number(blk.timestamp);
  tsCache.set(k, t);
  if (tsCache.size > 2000) {
    const first = tsCache.keys().next().value;
    tsCache.delete(first);
  }
  return t;
}

// ==== 断点续传相关函数 ====
function saveProgress(nextStart, currentBatch, totalWindows, targetEnd) {
  const progress = {
    nextStart: nextStart.toString(), // BigInt 转字符串保存
    currentBatch,
    totalWindows,
    timestamp: Date.now(),
    startBlock: START_BLOCK,
    targetEnd: targetEnd.toString(),
    part,
    rowsInPart
  };
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
  const currentWindow = Math.floor(Number(nextStart - BigInt(START_BLOCK)) / BLOCK_STEP) + 1;
  console.log(`[进度保存] 区块 ${nextStart}, 窗口 ${currentWindow}/${totalWindows}, 批次 ${currentBatch}`);
}

function loadProgress() {
  if (!fs.existsSync(PROGRESS_FILE)) {
    console.log(`[进度] 未找到进度文件，从头开始扫描`);
    return null;
  }
  
  try {
    const progress = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
    if (progress.nextStart) {
      const nextStart = BigInt(progress.nextStart);
      const currentWindow = Math.floor(Number(nextStart - BigInt(START_BLOCK)) / BLOCK_STEP) + 1;
      console.log(`[进度恢复] 从区块 ${nextStart} (窗口 ${currentWindow}) 继续`);
      return { ...progress, nextStart };
    }
    return progress;
  } catch (e) {
    console.warn(`[进度] 进度文件损坏，从头开始: ${e.message}`);
    return null;
  }
}

function cleanupProgress() {
  if (fs.existsSync(PROGRESS_FILE)) {
    fs.unlinkSync(PROGRESS_FILE);
    console.log(`[进度] 扫描完成，已清理进度文件`);
  }
}

// ==== 增强的重试机制 ====
async function withRetry(fn, label, max = MAX_RETRIES) {
  let err;
  for (let i = 0; i < max; i++) {
    try { 
      return await fn(); 
    } catch (e) {
      err = e;
      const delay = RETRY_DELAY_BASE * Math.pow(2, i); // 指数退避
      const jitter = Math.random() * 1000; // 添加随机抖动
      const totalDelay = delay + jitter;
      
      console.warn(`[重试 ${i + 1}/${max}] ${label}: ${e?.message || e}; 等待 ${Math.round(totalDelay)}ms`);
      
      // 特殊错误处理
      if (e?.message?.includes('rate limit') || e?.code === 429) {
        await new Promise(r => setTimeout(r, totalDelay * 2)); // 速率限制时等待更长时间
      } else if (e?.message?.includes('timeout')) {
        await new Promise(r => setTimeout(r, totalDelay));
      } else if (e?.message?.includes('connection')) {
        await new Promise(r => setTimeout(r, totalDelay * 1.5)); // 连接问题时等待稍长时间
      } else {
        await new Promise(r => setTimeout(r, totalDelay));
      }
    }
  }
  
  console.error(`[失败] ${label} 在 ${max} 次重试后仍然失败`);
  throw err;
}

// ==== 健康检查和自动恢复 ====
async function healthCheck() {
  try {
    const head = await withRetry(() => client.getBlockNumber(), 'health check', 3);
    return true;
  } catch (e) {
    console.error(`[健康检查] RPC连接失败: ${e.message}`);
    return false;
  }
}

// ==== 优雅退出处理 ====
let isShuttingDown = false;
let nextStart = BigInt(START_BLOCK);
let totalWindows = 0;
let currentBatch = 1;
let targetEnd = BigInt(0);

process.on('SIGINT', async () => {
  if (isShuttingDown) {
    console.log('\n[强制退出] 再次收到中断信号，强制退出');
    process.exit(1);
  }
  
  isShuttingDown = true;
  console.log('\n[优雅退出] 收到中断信号，正在保存进度并退出...');
  
  try {
    if (current) {
      await closePart(current);
    }
    saveProgress(nextStart, currentBatch, totalWindows, targetEnd);
    console.log('[退出] 进度已保存，可使用相同参数重新运行以继续');
  } catch (e) {
    console.error('[退出错误]', e.message);
  }
  
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('[退出] 收到终止信号');
  if (current) {
    await closePart(current);
  }
  saveProgress(nextStart, currentBatch, totalWindows, targetEnd);
  process.exit(0);
});

// ==== 内存监控 ====
function logMemoryUsage() {
  const usage = process.memoryUsage();
  const mb = (bytes) => Math.round(bytes / 1024 / 1024 * 100) / 100;
  console.log(`[内存] RSS: ${mb(usage.rss)}MB, Heap: ${mb(usage.heapUsed)}/${mb(usage.heapTotal)}MB, External: ${mb(usage.external)}MB`);
}

// 每5分钟输出一次内存使用情况
setInterval(logMemoryUsage, 5 * 60 * 1000);

// ==== 追块主流程 ====
(async () => {
  try {
    // 流式加载Token数据
    console.log(`[初始化] 正在加载Token数据...`);
    await loadTokenData();
    
    // 健康检查
    console.log(`[初始化] 正在进行健康检查...`);
    if (!(await healthCheck())) {
      throw new Error('RPC节点健康检查失败');
    }
    
    // 初始化输出文件
    current = openNewPart();
    
    // 加载断点续传进度
    const savedProgress = loadProgress();
    let resumeFromBatch = 1;
    
    if (savedProgress && savedProgress.startBlock === START_BLOCK && savedProgress.nextStart) {
      nextStart = savedProgress.nextStart;
      resumeFromBatch = savedProgress.currentBatch || 1;
      part = savedProgress.part || 1;
      rowsInPart = savedProgress.rowsInPart || 0;
      
      // 如果有现有文件，以追加模式打开
      if (rowsInPart > 0) {
        await closePart(current);
        const filePath = path.join(outDir, `edge_events_part${part}.csv`);
        if (fs.existsSync(filePath)) {
          const stream = fs.createWriteStream(filePath, { flags: "a" }); // 追加模式
          const writer = csv.format({ headers: false }); // 不重复写表头
          writer.pipe(stream);
          current = { path: filePath, stream, writer };
          console.log(`[恢复] 继续写入文件 ${filePath}, 当前行数: ${rowsInPart}`);
        } else {
          current = openNewPart();
        }
      }
    }

    console.log(`[配置] START_BLOCK=${START_BLOCK}, TARGET_END=${TARGET_END_RAW}, FOLLOW_LATEST=${FOLLOW_LATEST}`);
    console.log(`[配置] BLOCK_STEP=${BLOCK_STEP}, ADDRESS_CHUNK=${ADDRESS_CHUNK}, POLL_INTERVAL=${POLL_INTERVAL_MS}ms`);

    // === 追块主循环：一口气从 START_BLOCK 扫到 TARGET_END（或 latest），然后持续追新块 ===
    while (true) {
      const head = await client.getBlockNumber();                  // 最新块
      const tip = head - BigInt(CONFIRMATIONS);                    // 留确认深度
      targetEnd = resolveTargetEnd(head);                          // 解析 TARGET_END
      if (targetEnd > tip) targetEnd = tip;                        // 不能超出可确认的末尾

      // 计算总窗口数（用于进度显示）
      if (targetEnd > nextStart) {
        totalWindows = Math.ceil(Number(targetEnd - nextStart + 1n) / BLOCK_STEP);
      } else {
        totalWindows = 0;
      }

      if (nextStart > targetEnd) {
        if (!FOLLOW_LATEST) {
          console.log(`[完成] 已扫描到目标区块 ${targetEnd}，扫描结束`);
          break;                                 // 不追块 → 结束
        }
        // 追块模式：睡一会儿，等新区块，再继续
        console.log(`[追块] 等待新区块... 当前头部: ${head}, 下次扫描: ${nextStart}`);
        await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
        continue;
      }

      const windowEnd = nextStart + BigInt(BLOCK_STEP - 1) <= targetEnd
        ? nextStart + BigInt(BLOCK_STEP - 1)
        : targetEnd;

      const currentWindow = Math.floor(Number(nextStart - BigInt(START_BLOCK)) / BLOCK_STEP) + 1;
      console.log(`[扫描] 窗口 ${currentWindow}/${totalWindows} - 区块 ${nextStart}-${windowEnd} (head=${head})`);

      // === 按地址分片 getLogs → 写 CSV ===
      const addrBatches = chunks(TOKEN_ADDRESSES, ADDRESS_CHUNK);
      let startBatch = (nextStart === savedProgress?.nextStart) ? resumeFromBatch : 1;
      
      for (let batchIndex = startBatch - 1; batchIndex < addrBatches.length; batchIndex++) {
        const addrBatch = addrBatches[batchIndex];
        const batchCount = batchIndex + 1;
        currentBatch = batchCount; // 更新全局批次变量
        
        console.log(`  [批次] ${batchCount}/${addrBatches.length} - 查询 ${addrBatch.length} 个Token地址`);
        
        try {
          const logs = await withRetry(() => client.getLogs({
            address: addrBatch,
            event: transferEvent,
            fromBlock: nextStart,
            toBlock: windowEnd,
          }), `getLogs ${nextStart}-${windowEnd} (${addrBatch.length} addrs)`);

          let validLogs = 0;
          let bloomHits = 0;
          let usdFiltered = 0;
          
          // 流式处理日志
          for (const log of logs) {
            const { from, to, value } = log.args;
            // 只要 from/to 命中 user_list（Bloom），就保留
            if (!bloom.has(String(from).toLowerCase()) && !bloom.has(String(to).toLowerCase())) continue;
            bloomHits++;

            const u = usd(log.address, value);
            if (u == null || u < USD_THRESHOLD) continue;
            usdFiltered++;

            await writeRow({
              tx_hash: log.transactionHash,
              log_index: Number(log.logIndex),
              block_number: Number(log.blockNumber),
              timestamp: await ts(log.blockNumber),
              token_address: log.address,
              raw_amount: BigInt(value).toString(10),
              usd_value: u.toFixed(6),
              from, to,
            });
            validLogs++;
          }
          
          if (logs.length > 0) {
            console.log(`    [结果] 原始日志: ${logs.length}, Bloom命中: ${bloomHits}, USD过滤后: ${usdFiltered}, 有效记录: ${validLogs}`);
          }
          
          // 定期保存进度
          if (batchCount % CHECKPOINT_INTERVAL === 0) {
            saveProgress(nextStart, batchCount, totalWindows, targetEnd);
          }
          
        } catch (error) {
          console.error(`[错误] 批次 ${batchCount} 处理失败: ${error.message}`);
          saveProgress(nextStart, batchCount, totalWindows, targetEnd);
          throw error;
        }
      }
      
      console.log(`[完成] 区块 ${nextStart}-${windowEnd} 处理完毕`);
      
      // 移动窗口起点
      nextStart = windowEnd + 1n;
      currentBatch = 1; // 重置批次计数
      
      // 保存进度（记录下一个要处理的区块）
      saveProgress(nextStart, 1, totalWindows, targetEnd);
      
      // 重置断点续传标记
      if (savedProgress?.nextStart) {
        savedProgress.nextStart = null; // 确保只在第一个窗口使用断点续传
      }
    }

    await closePart(current);
    cleanupProgress();
    console.log(`[完成] 扫描完成，共生成 ${part} 个文件`);
    
  } catch (error) {
    console.error(`[致命错误] ${error.message}`);
    console.error(error.stack);
    
    // 保存当前进度
    if (typeof nextStart !== 'undefined') {
      saveProgress(nextStart, currentBatch, totalWindows || 0, targetEnd || BigInt(0));
    }
    
    process.exit(1);
  }
})();
