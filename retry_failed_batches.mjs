// retry_failed_batches.mjs
// 重试失败批次的脚本
// 运行：node retry_failed_batches.mjs

import { createPublicClient, http, parseAbiItem } from "viem";
import { defineChain } from "viem/utils";
import fs from "fs";
import path from "path";
import { fileURLToPath } from 'url';
import { parse } from "csv-parse";
import csv from "fast-csv";
import bloomPkg from "bloom-filters";
import { makeEOAChecker } from './eoa.mjs';

const { BloomFilter } = bloomPkg;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ==== 参数 ====
const RPC_HTTP = process.env.RPC_HTTP || "http://127.0.0.1:8545";
const FAILED_BATCHES_FILE = process.env.FAILED_BATCHES_FILE || "./failed_batches.json";
const OUTPUT_FILE = process.env.OUTPUT_FILE || "./retry_results.csv";
const TOKENS_CSV = process.env.TOKENS_CSV || "./token_withbalance.csv";
const BLOOM_JSON = process.env.BLOOM_JSON || "./userlist.bloom.json";

// 更保守的重试参数
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 5);
const RETRY_DELAY_BASE = Number(process.env.RETRY_DELAY_BASE || 3000); // 3秒基础延迟
const USD_THRESHOLD = Number(process.env.USD_THRESHOLD || 100);
const EOA_ONLY = String(process.env.EOA_ONLY || "1") === "1";
const SMALLER_CHUNK = Number(process.env.SMALLER_CHUNK || 3); // 每次只查3个地址

// ==== 加载数据 ====
const TOKENS = {};
const bloom = BloomFilter.fromJSON(JSON.parse(fs.readFileSync(BLOOM_JSON, "utf8")));

async function loadTokenData() {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(TOKENS_CSV, { encoding: 'utf8' });
    stream
      .pipe(parse({ columns: true, trim: true, skip_empty_lines: true }))
      .on('data', (row) => {
        TOKENS[row.token_address] = { 
          decimals: Number(row.decimals), 
          price: Number(row.usd_price) 
        };
      })
      .on('end', resolve)
      .on('error', reject);
  });
}

// ==== viem client ====
const bsc = defineChain({
  id: 56, name: "BSC",
  nativeCurrency: { name: "BNB", symbol: "BNB", decimals: 18 },
  rpcUrls: { default: { http: [RPC_HTTP] } },
});

const client = createPublicClient({ 
  chain: bsc, 
  transport: http(RPC_HTTP, {
    batch: true,
    timeout: 120_000, // 2分钟超时
    retryCount: 3,
    retryDelay: 3000
  })
});

const transferEvent = parseAbiItem(
  "event Transfer(address indexed from, address indexed to, uint256 value)"
);

const { isEOAAtBlock } = makeEOAChecker(client, {
  blockBucket: 50_000,
  maxCache: 200_000,
});

// ==== 工具函数 ====
function usd(token, rawHex) {
  const m = TOKENS[token]; 
  if (!m) return null;
  const v = BigInt(rawHex);
  return (Number(v) / 10 ** m.decimals) * m.price;
}

function chunks(arr, n) { 
  const o = []; 
  for (let i = 0; i < arr.length; i += n) o.push(arr.slice(i, i + n)); 
  return o; 
}

async function withRetry(fn, label, max = MAX_RETRIES) {
  let err;
  for (let i = 0; i < max; i++) {
    try { 
      const result = await fn();
      return result;
    } catch (e) {
      err = e;
      const delay = RETRY_DELAY_BASE * Math.pow(2, i);
      const jitter = Math.random() * 1000;
      const totalDelay = delay + jitter;
      
      console.warn(`  [重试 ${i + 1}/${max}] ${label}: ${e?.message?.substring(0, 100) || e}`);
      
      if (e?.message?.includes('timeout')) {
        await new Promise(r => setTimeout(r, totalDelay * 3)); // 超时等待更长
      } else {
        await new Promise(r => setTimeout(r, totalDelay));
      }
    }
  }
  
  console.error(`  [失败] ${label} 在 ${max} 次重试后仍然失败`);
  throw err;
}

// ==== 主流程 ====
(async () => {
  try {
    console.log(`[初始化] 加载Token数据...`);
    await loadTokenData();
    console.log(`[初始化] 已加载 ${Object.keys(TOKENS).length} 个Token`);
    
    // 读取失败批次
    if (!fs.existsSync(FAILED_BATCHES_FILE)) {
      console.log(`[错误] 未找到失败批次文件: ${FAILED_BATCHES_FILE}`);
      process.exit(1);
    }
    
    const failedBatches = JSON.parse(fs.readFileSync(FAILED_BATCHES_FILE, 'utf8'));
    console.log(`[加载] 找到 ${failedBatches.length} 个失败批次\n`);
    
    // 创建输出文件
    const outputStream = fs.createWriteStream(OUTPUT_FILE, { flags: "w" });
    const writer = csv.format({ headers: true });
    writer.pipe(outputStream);
    
    let totalSuccess = 0;
    let totalFailed = 0;
    let totalRecords = 0;
    
    // 逐个重试失败批次
    for (let i = 0; i < failedBatches.length; i++) {
      const batch = failedBatches[i];
      console.log(`\n[批次 ${i + 1}/${failedBatches.length}] 重试: ${batch.blockRange}`);
      console.log(`  原因: ${batch.error.substring(0, 100)}...`);
      
      // 从错误信息中提取地址列表
      const addressMatch = batch.error.match(/"address":\[(.*?)\]/);
      if (!addressMatch) {
        console.error(`  [跳过] 无法从错误信息中提取地址列表`);
        totalFailed++;
        continue;
      }
      
      const addressesStr = addressMatch[1];
      const addresses = addressesStr.split(',').map(addr => 
        addr.trim().replace(/"/g, '')
      );
      
      // 解析区块范围
      const [fromBlock, toBlock] = batch.blockRange.split('-').map(b => BigInt(b));
      
      console.log(`  地址数: ${addresses.length}, 区块: ${fromBlock}-${toBlock}`);
      
      // 将地址分成更小的批次（每次3个）
      const smallChunks = chunks(addresses, SMALLER_CHUNK);
      let batchSuccess = true;
      let batchRecords = 0;
      
      for (let j = 0; j < smallChunks.length; j++) {
        const addrChunk = smallChunks[j];
        console.log(`  [子批次 ${j + 1}/${smallChunks.length}] 查询 ${addrChunk.length} 个地址...`);
        
        try {
          const logs = await withRetry(() => client.getLogs({
            address: addrChunk,
            event: transferEvent,
            fromBlock,
            toBlock,
          }), `getLogs ${fromBlock}-${toBlock} (${addrChunk.length} addrs)`);
          
          console.log(`    获取到 ${logs.length} 条日志`);
          
          // 处理日志
          for (const log of logs) {
            const { from, to, value } = log.args;
            
            // Bloom过滤
            if (!bloom.has(String(from).toLowerCase()) && !bloom.has(String(to).toLowerCase())) {
              continue;
            }
            
            // USD过滤
            const u = usd(log.address, value);
            if (u == null || u < USD_THRESHOLD) continue;
            
            // EOA过滤
            if (EOA_ONLY) {
              const bn = log.blockNumber;
              const [fromIsEOA, toIsEOA] = await Promise.all([
                isEOAAtBlock(from, bn),
                isEOAAtBlock(to, bn),
              ]);
              if (!fromIsEOA || !toIsEOA) continue;
            }
            
            // 写入结果
            writer.write({
              tx_hash: log.transactionHash,
              log_index: Number(log.logIndex),
              block_number: Number(log.blockNumber),
              timestamp: 0,
              token_address: log.address,
              raw_amount: BigInt(value).toString(10),
              usd_value: u.toFixed(6),
              from, 
              to,
            });
            
            batchRecords++;
            totalRecords++;
          }
          
          // 等待一下，避免请求过快
          await new Promise(r => setTimeout(r, 500));
          
        } catch (error) {
          console.error(`    [失败] 子批次失败: ${error.message.substring(0, 100)}`);
          batchSuccess = false;
        }
      }
      
      if (batchSuccess) {
        console.log(`  ✅ 批次成功，获得 ${batchRecords} 条有效记录`);
        totalSuccess++;
      } else {
        console.log(`  ❌ 批次部分失败`);
        totalFailed++;
      }
    }
    
    // 关闭输出文件
    writer.end();
    await new Promise(resolve => outputStream.end(resolve));
    
    console.log(`\n========== 重试完成 ==========`);
    console.log(`总批次: ${failedBatches.length}`);
    console.log(`成功: ${totalSuccess}`);
    console.log(`失败: ${totalFailed}`);
    console.log(`有效记录: ${totalRecords}`);
    console.log(`结果已保存到: ${OUTPUT_FILE}`);
    console.log(`==============================\n`);
    
  } catch (error) {
    console.error(`[致命错误] ${error.message}`);
    console.error(error.stack);
    process.exit(1);
  }
})();

