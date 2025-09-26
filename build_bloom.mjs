import fs from "fs";
import { parse } from "csv-parse";
import pkg from "bloom-filters"; // https://www.npmjs.com/package/bloom-filters
const { BloomFilter } = pkg;

// ---- 参数 ----
const INPUT_DIR = "./final-results"; // 放你 26 个 csv 的目录
const GLOB = /^merged_cleaned_part\d+_of_26\.csv$/i; // 文件名正则
const ADDR_COL = "address";           // 你的列名，必要时改成实际列名
const N = 10_000_000;                 // 预计条数
const FP = 0.001;                     // 0.1% 假阳率

// 初始化 Bloom（库会按 N 和 FP 算 bit 数 & hash 个数）
const bloom = BloomFilter.create(N, FP);

const files = fs.readdirSync(INPUT_DIR).filter(f => GLOB.test(f));
console.log(`[bloom] files=${files.length}, target n=${N}, fp=${FP}`);

for (const f of files) {
  console.log(`[bloom] ingest ${f}`);
  await new Promise((resolve, reject) => {
    fs.createReadStream(`${INPUT_DIR}/${f}`)
      .pipe(parse({ columns: true, trim: true }))
      .on("data", row => {
        const a = (row[ADDR_COL] || "").toLowerCase();
        if (a.startsWith("0x") && a.length === 42) bloom.add(a);
      })
      .on("end", resolve)
      .on("error", reject);
  });
}

const json = bloom.saveAsJSON();
fs.writeFileSync("./userlist.bloom.json", JSON.stringify(json));
console.log(`[bloom] saved userlist.bloom.json (m=${json._size} bits, k=${json._nbHashes})`);
