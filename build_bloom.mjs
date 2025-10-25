// build_bloom.mjs
// 构建 PartitionedBloomFilter 用于高效用户地址过滤
// 运行：node build_bloom.mjs

import fs from "fs";
import { parse } from "csv-parse";
import pkg from "bloom-filters"; // https://www.npmjs.com/package/bloom-filters
const { PartitionedBloomFilter } = pkg;

// ---- 参数 ----
const INPUT_DIR = "./final-results"; // 用户地址 CSV 文件目录（26个分片）
const GLOB = /^merged_cleaned_part\d+_of_26\.csv$/i; // 文件名正则匹配
const ADDR_COL = "address";           // CSV 中的地址列名
const N = 12_880_000;                 // 预计地址数量
const FP = 0.000001;                  // 假阳率：1e-6 (百万分之一)

// 初始化 PartitionedBloomFilter（库会根据 N 和 FP 自动计算 bit 数和 hash 函数个数）
const bloom = PartitionedBloomFilter.create(N, FP);

const files = fs.readdirSync(INPUT_DIR).filter(f => GLOB.test(f));
console.log(`[Bloom] 找到 ${files.length} 个文件, 预计地址数=${N.toLocaleString()}, 假阳率=${FP}`);

// 流式读取所有 CSV 文件，将地址添加到 Bloom Filter
for (const f of files) {
  console.log(`[Bloom] 正在处理 ${f}...`);
  await new Promise((resolve, reject) => {
    fs.createReadStream(`${INPUT_DIR}/${f}`)
      .pipe(parse({ columns: true, trim: true }))
      .on("data", row => {
        // 全链路地址规范化：trim + 小写
        const addr = (row[ADDR_COL] || "").trim().toLowerCase();
        // 验证地址格式：0x 开头，42 字符长度
        if (addr.startsWith("0x") && addr.length === 42) {
          bloom.add(addr);
        }
      })
      .on("end", resolve)
      .on("error", reject);
  });
}

// 保存为 JSON 文件
const json = bloom.saveAsJSON();
fs.writeFileSync("./userlist.bloom.json", JSON.stringify(json));
console.log(`[Bloom] ✓ 已保存 userlist.bloom.json`);
console.log(`[Bloom] 统计信息: 位数=${json._size.toLocaleString()}, 哈希函数数=${json._nbHashes}`);
