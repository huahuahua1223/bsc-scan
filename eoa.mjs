// eoa.mjs
// EOA / Contract 判定（带区块高度 + 批量 + 本地缓存）
// 依赖：viem

import { getAddress } from 'viem';

const ZERO = '0x0000000000000000000000000000000000000000';
const PRECOMPILES = new Set([
  '0x0000000000000000000000000000000000000001',
  '0x0000000000000000000000000000000000000002',
  '0x0000000000000000000000000000000000000003',
  '0x0000000000000000000000000000000000000004',
  '0x0000000000000000000000000000000000000005',
  '0x0000000000000000000000000000000000000006',
  '0x0000000000000000000000000000000000000007',
  '0x0000000000000000000000000000000000000008',
  '0x0000000000000000000000000000000000000009',
]);

/**
 * 生成一个“在指定 block 判定是否合约地址”的 helper
 * - 使用本地 Map 缓存，按“区块桶”聚合，降低历史 getCode 次数
 * - 若 RPC 不支持历史 state，会自动降级到 latest
 */
export function makeEOAChecker(publicClient, { blockBucket = 50_000, maxCache = 200_000 } = {}) {
  const cache = new Map(); // key: `${addr}:${bucketStart}` -> boolean(isContract)

  function remember(key, val) {
    cache.set(key, val);
    if (cache.size > maxCache) {
      // 粗暴 LRU：随机删 10% 以控内存（足够好用）
      let i = 0;
      for (const k of cache.keys()) {
        cache.delete(k);
        if (++i > Math.ceil(maxCache * 0.1)) break;
      }
    }
  }

  function normalize(addr) {
    try { return getAddress(addr); } catch { return addr.toLowerCase(); }
  }

  async function isContractAtBlock(addr, blockNumber) {
    const address = normalize(addr);
    if (address === ZERO) return true;           // 铸造/销毁等非 EOA
    if (PRECOMPILES.has(address.toLowerCase())) return true; // 预编译也非 EOA

    // 把区块号按桶聚合，减少历史判定的粒度（几乎不影响你的通讯录画像）
    const bn = typeof blockNumber === 'bigint' ? blockNumber : BigInt(blockNumber);
    const bucketStart = (bn / BigInt(blockBucket)) * BigInt(blockBucket);
    const key = `${address}:${bucketStart.toString()}`;

    if (cache.has(key)) return cache.get(key);

    // 优先做“历史时点”的 getCode（需要归档/历史 state）
    let bytecode;
    try {
      bytecode = await publicClient.getCode({ address, blockNumber: bn });
    } catch (_) {
      // 降级到 latest（非归档节点常见）
      bytecode = await publicClient.getCode({ address });
    }
    const isContract = !!(bytecode && bytecode !== '0x');
    remember(key, isContract);
    return isContract;
  }

  async function isEOAAtBlock(addr, blockNumber) {
    return !(await isContractAtBlock(addr, blockNumber));
  }

  return { isContractAtBlock, isEOAAtBlock };
}
