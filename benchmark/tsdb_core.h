#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib> // aligned_alloc, free
#include <cstring> // memcpy
#include <deque>
#include <functional>
#include <limits> // for std::numeric_limits
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

// =========================
// Configurable Parameters
// =========================

static constexpr uint32_t SLOT_SIZE_BYTES = 16 * 1024; // 16KB
static constexpr uint32_t RECORD_SIZE = 24;            // typical
static constexpr uint32_t MAX_SLOTS = 1 << 20;         // configurable

// ===== Consistency / Performance Switch =====
// 1  = 强一致（用于单测 / 生产逻辑）
// 0  = 偏性能（默认，允许 benchmark 里关闭重栅栏）
#ifndef TSDB_STRICT_CONSISTENCY
#define TSDB_STRICT_CONSISTENCY 0
#endif

#if TSDB_STRICT_CONSISTENCY
#define TSDB_FENCE_BEFORE_SEAL()                                               \
  std::atomic_thread_fence(std::memory_order_seq_cst)
#else
#define TSDB_FENCE_BEFORE_SEAL()                                               \
  do { /* no-op in perf mode */                                                \
  } while (0)
#endif

// =========================
// Record
// =========================

struct Record {
  uint64_t key;   // timestamp or logical key
  uint64_t value; // payload
  uint32_t epoch; // optional
};

// =========================
// DataBlock (SB-Tree style)
// =========================

struct DataBlock {
  static constexpr size_t kBlockBytes = 4096;
  static constexpr size_t kEntryBytes = sizeof(uint64_t) * 2; // key + value
  static constexpr size_t kMaxEntries = kBlockBytes / kEntryBytes;
  static constexpr size_t kBucketSize = 64;
  static constexpr size_t kBucketCount =
      (kMaxEntries + kBucketSize - 1) / kBucketSize;

  uint32_t count;
  uint64_t min_key;
  uint64_t max_key;

  std::array<uint64_t, kMaxEntries> keys;
  std::array<uint64_t, kMaxEntries> values;

  std::array<uint64_t, kBucketCount> bucket_min_keys;
  std::array<uint16_t, kBucketCount> bucket_offsets;
  uint16_t bucket_count;

  DataBlock()
      : count(0), min_key(std::numeric_limits<uint64_t>::max()), max_key(0),
        bucket_count(0) {}

  inline void clear() {
    count = 0;
    min_key = std::numeric_limits<uint64_t>::max();
    max_key = 0;
    bucket_count = 0;
  }

  // Fill block from run[start:], returns next position in run
  inline size_t fill_from_run(const std::vector<Record> &run, size_t start) {
    clear();
    if (start >= run.size()) {
      return start;
    }
    size_t idx = 0;
    min_key = run[start].key;
    while (start < run.size() && idx < kMaxEntries) {
      keys[idx] = run[start].key;
      values[idx] = run[start].value;
      ++idx;
      ++start;
    }
    count = static_cast<uint32_t>(idx);
    max_key = keys[count - 1];
    build_index();
    return start;
  }

  inline void build_index() {
    if (count == 0) {
      bucket_count = 0;
      return;
    }
    bucket_count =
        static_cast<uint16_t>((count + kBucketSize - 1) / kBucketSize);
    for (uint16_t b = 0; b < bucket_count; ++b) {
      size_t offset = static_cast<size_t>(b) * kBucketSize;
      if (offset >= count) {
        offset = count - 1;
      }
      bucket_offsets[b] = static_cast<uint16_t>(offset);
      bucket_min_keys[b] = keys[offset];
    }
  }

  inline bool lookup(uint64_t key, Record &out) const {
    if (count == 0 || key < min_key || key > max_key) {
      return false;
    }
    uint16_t b = 0;
    if (bucket_count > 0) {
      auto it = std::upper_bound(bucket_min_keys.begin(),
                                 bucket_min_keys.begin() + bucket_count, key);
      if (it == bucket_min_keys.begin()) {
        b = 0;
      } else {
        b = static_cast<uint16_t>((it - bucket_min_keys.begin()) - 1);
      }
    }
    size_t start = bucket_offsets[b];
    size_t end = std::min(static_cast<size_t>(count), start + kBucketSize);
    for (size_t i = start; i < end; ++i) {
      if (keys[i] == key) {
        out.key = keys[i];
        out.value = values[i];
        out.epoch = 0;
        return true;
      }
      if (keys[i] > key) {
        break;
      }
    }
    return false;
  }

  inline void scan_range(uint64_t key_lo, uint64_t key_hi,
                         std::vector<Record> &out) const {
    if (count == 0 || key_hi < min_key || key_lo > max_key) {
      return;
    }

    const uint64_t *begin = keys.data();
    const uint64_t *end = begin + count;

    // 1) 在整个 block 内二分找到第一个 >= key_lo 的位置
    const uint64_t *it = std::lower_bound(begin, end, key_lo);

    // 2) 从这里开始顺序扫，直到 > key_hi 或 block 结束
    for (; it != end && *it <= key_hi; ++it) {
      size_t idx = static_cast<size_t>(it - begin);
      out.emplace_back(Record{keys[idx], values[idx], 0});
    }
  }

  // 返回本 block 实际扫描出的条数
  inline size_t scan_from(uint64_t start_key, size_t max_count,
                          std::vector<Record> &out) const {
    if (count == 0 || start_key > max_key) {
      return 0;
    }

    const uint64_t *ks = keys.data();
    const uint64_t *vs = values.data();

    // 1) 在 bucket_min_keys 里找 bucket
    uint32_t b = 0;
    if (bucket_count > 0) {
      // 线性扫描 bucket_min_keys（bucket_count 通常很小）
      while (b + 1 < bucket_count && bucket_min_keys[b + 1] <= start_key) {
        ++b;
      }
    }
    uint32_t idx = bucket_offsets[b];

    // 2) 在这个 bucket 内线性前进到 >= start_key
    const uint32_t end =
        std::min(count, static_cast<uint32_t>(idx + kBucketSize));
    while (idx < end && ks[idx] < start_key) {
      ++idx;
    }
    if (idx >= count)
      return 0;

    // 3) 按之前的方式一次性写出
    size_t n = std::min(max_count, static_cast<size_t>(count - idx));
    if (n == 0)
      return 0;

    size_t old_size = out.size();
    out.resize(old_size + n);
    Record *dst = out.data() + old_size;
    for (size_t i = 0; i < n; ++i) {
      dst[i].key = ks[idx + i];
      dst[i].value = vs[idx + i];
      dst[i].epoch = 0;
    }
    return n;
  }

  // Visitor 版本：不构造 Record，只调用 visitor 函数
  template <class Func>
  inline size_t scan_from_visit(uint64_t start_key, size_t max_count,
                                Func &&f) const {
    if (count == 0 || start_key > max_key) {
      return 0;
    }

    // 1) 用 N-ary 找 bucket
    uint16_t b = 0;
    if (bucket_count > 0) {
      auto it =
          std::upper_bound(bucket_min_keys.begin(),
                           bucket_min_keys.begin() + bucket_count, start_key);
      if (it == bucket_min_keys.begin()) {
        b = 0;
      } else {
        b = static_cast<uint16_t>((it - bucket_min_keys.begin()) - 1);
      }
    }

    size_t start = bucket_offsets[b];
    size_t end = std::min(static_cast<size_t>(count), start + kBucketSize);

    // 2) bucket 内 lower_bound 精确对齐到 start_key
    const uint64_t *kbeg = keys.data() + start;
    const uint64_t *kend = keys.data() + end;
    const uint64_t *it = std::lower_bound(kbeg, kend, start_key);

    size_t emitted = 0;
    const uint64_t *kptr = keys.data();
    const uint64_t *vptr = values.data();

    // 3) 从当前 bucket 开始扫描
    for (; it != kend && emitted < max_count; ++it) {
      size_t idx = static_cast<size_t>(it - keys.data());
      f(kptr[idx], vptr[idx]);
      ++emitted;
    }

    // 4) 后续 bucket 直接顺序扫（不再做比较）
    for (uint16_t nb = b + 1; nb < bucket_count && emitted < max_count; ++nb) {
      size_t off = bucket_offsets[nb];
      size_t e = std::min(static_cast<size_t>(count), off + kBucketSize);
      for (size_t i = off; i < e && emitted < max_count; ++i) {
        f(kptr[i], vptr[i]);
        ++emitted;
      }
    }

    return emitted;
  }
};

// =========================
// DataBlock Allocator (Memory Pool)
// =========================

class DataBlockAllocator {
public:
  DataBlockAllocator() {}

  ~DataBlockAllocator() {}

  // 分配一个 DataBlock（从 thread-local freelist 获取或新建）
  DataBlock *allocate() {
    // 先尝试从 thread-local freelist 获取
    thread_local std::vector<DataBlock *> *tls_freelist = nullptr;
    if (!tls_freelist) {
      tls_freelist = new std::vector<DataBlock *>();
      tls_freelist->reserve(64); // 预分配容量
    }

    if (!tls_freelist->empty()) {
      DataBlock *blk = tls_freelist->back();
      tls_freelist->pop_back();
      blk->clear();
      return blk;
    }

    // 如果 thread-local freelist 为空，分配新的 block
    // 使用 new 分配，因为 DataBlock 包含 std::array，需要正确构造
    DataBlock *blk = new DataBlock();
    return blk;
  }

  // 释放一个 DataBlock（归还到 thread-local freelist）
  void deallocate(DataBlock *blk) {
    if (!blk) {
      return;
    }
    // 归还到 thread-local freelist
    thread_local std::vector<DataBlock *> *tls_freelist = nullptr;
    if (!tls_freelist) {
      tls_freelist = new std::vector<DataBlock *>();
      tls_freelist->reserve(64);
    }
    // 限制 freelist 大小，避免内存泄漏
    if (tls_freelist->size() < 128) {
      tls_freelist->push_back(blk);
    } else {
      // 如果 freelist 太大，直接删除（避免内存泄漏）
      delete blk;
    }
  }
};

// 全局 allocator 实例（简化设计，避免传递 allocator 指针）
inline DataBlockAllocator &get_block_allocator() {
  static DataBlockAllocator allocator;
  return allocator;
}

// 自定义 deleter 用于 unique_ptr，归还到 allocator
struct DataBlockDeleter {
  void operator()(DataBlock *blk) const {
    if (blk) {
      get_block_allocator().deallocate(blk);
    }
  }
};

// 使用自定义 deleter 的 unique_ptr 类型
using DataBlockPtr = std::unique_ptr<DataBlock, DataBlockDeleter>;

// Max number of records that can fit into one slot of size SLOT_SIZE_BYTES.
// This is the compile-time capacity used by writers (hwm limit).
static constexpr uint32_t SLOT_CAPACITY = SLOT_SIZE_BYTES / sizeof(Record);

static_assert(SLOT_SIZE_BYTES % alignof(Record) == 0,
              "SLOT_SIZE_BYTES must be a multiple of Record alignment");

// =========================
// Slot (SWMR)
// =========================

// Slot state: 0 = WRITING, 1 = SEALED, 2 = CONSUMED
enum : uint8_t {
  SLOT_STATE_WRITING = 0,
  SLOT_STATE_SEALED = 1,
  SLOT_STATE_CONSUMED = 2,
};

struct Slot {
  // Pointer to contiguous records buffer (size: SLOT_SIZE_BYTES /
  // sizeof(Record))
  Record *recs;
  // High-water mark: number of valid records in this slot.
  // Only the owning writer thread increments this; readers/mergers read it
  // after observing state == SLOT_STATE_SEALED. We still make it atomic so that
  // flush/merge/统计代码在并发场景下没有数据竞争。
  std::atomic<uint16_t> hwm;
  // Slot state: WRITING / SEALED / CONSUMED. Accessed concurrently by writer,
  // merge thread, and flush logic, so it must be atomic.
  std::atomic<uint8_t> state;

  // Slot-level key range metadata (for RDS-side pruning in range_query).
  // Writer thread maintains these while state == WRITING; readers only access
  // them after observing state == SEALED.
  std::atomic<uint64_t> min_key;
  std::atomic<uint64_t> max_key;

  Slot()
      : recs(nullptr), hwm(0), state(SLOT_STATE_WRITING),
        min_key(std::numeric_limits<uint64_t>::max()), max_key(0) {}

  // ============ Semantic Helpers (encapsulate atomic + memory_order)
  // ============

  // 写线程在 fast path 调用：获取 append 位置。
  // 严格一致性模式下使用 acq_rel，性能模式下使用 relaxed，
  // 真正跨线程的可见性由 seal 前后的 fence + state.store(release) 来保证。
  inline uint16_t append_pos() {
#if TSDB_STRICT_CONSISTENCY
    return hwm.fetch_add(1, std::memory_order_acq_rel);
#else
    return hwm.fetch_add(1, std::memory_order_relaxed);
#endif
  }

  // merge / flush / reader 侧读取当前大小，使用 acquire 同步
  inline uint16_t size_acquire() const {
    return hwm.load(std::memory_order_acquire);
  }

  // 新分配 slot 时调用，初始化为 WRITING 状态，使用 release 保证可见性
  inline void reset_for_write() {
    hwm.store(0, std::memory_order_release);
    state.store(SLOT_STATE_WRITING, std::memory_order_release);
    min_key.store(std::numeric_limits<uint64_t>::max(),
                  std::memory_order_relaxed);
    max_key.store(0, std::memory_order_relaxed);
  }

  // 写线程准备切走这个 slot 时调用：直接 seal 为 SEALED 状态
  // （调用者已经确保 hwm > 0，所以这里不再额外检查）
  inline void seal() {
    state.store(SLOT_STATE_SEALED, std::memory_order_release);
  }

  // 写线程想检查一个 slot 是否值得 seal（不为空）
  inline bool has_data_acquire() const { return size_acquire() > 0; }

  // reader 侧检查 slot 是否已被 seal
  inline bool is_sealed() const {
    return state.load(std::memory_order_acquire) == SLOT_STATE_SEALED;
  }

  // reader 侧检查 slot 是否已被消费
  inline bool is_consumed() const {
    return state.load(std::memory_order_acquire) == SLOT_STATE_CONSUMED;
  }

  // merge 完成后调用：标记为已消费，清空 hwm
  inline void mark_consumed() {
    hwm.store(0, std::memory_order_release);
    state.store(SLOT_STATE_CONSUMED, std::memory_order_release);
  }
};

// =========================
// Buffer (Double-Buffer)
// =========================

// Buffer state: 0 = WRITING, 1 = SEALED
enum : uint8_t {
  BUFFER_STATE_WRITING = 0,
  BUFFER_STATE_SEALED = 1,
};

struct Buffer {
  Slot *slots;            // array of slots
  uint32_t slot_capacity; // total slot count
  std::atomic<uint32_t> alloc_idx;
  std::atomic<uint8_t> state; // WRITING / SEALED

  Buffer() : slots(nullptr), slot_capacity(0), alloc_idx(0), state(0) {}
};

// =========================
// Thread-local Write Context
// =========================

struct ThreadLocalCtx {
  Slot *current_slot;
  uint32_t current_buf_idx;

  ThreadLocalCtx() : current_slot(nullptr), current_buf_idx(0) {}
};

inline thread_local ThreadLocalCtx tls_ctx;

// =========================
// Buffer Manager
// =========================

class BufferManager {
public:
  BufferManager(uint32_t slots_per_buffer);

  ~BufferManager();

  BufferManager(const BufferManager &) = delete;
  BufferManager &operator=(const BufferManager &) = delete;

  // allocate one slot from current write buffer (low frequency)
  Slot *allocate_slot();

  // flip W buffer (every time slice)
  void flip_buffers();

  // Seal all non-empty WRITING slots and mark buffers SEALED (for
  // flush/shutdown)
  void seal_all_slots_for_flush();

  // get active write buffer idx (0/1)
  inline uint32_t current_w_idx() const {
    return w_idx.load(std::memory_order_acquire);
  }

  Buffer buffers[2];

  // count of allocation failures (buffer full)
  std::atomic<uint64_t> alloc_failures;

private:
  std::atomic<uint32_t> w_idx; // 0 or 1
};

// =========================
// Inline Implementations
// =========================

inline BufferManager::BufferManager(uint32_t slots_per_buffer)
    : w_idx(0), alloc_failures(0) {
  for (int i = 0; i < 2; ++i) {
    Buffer &buf = buffers[i];
    buf.slot_capacity = slots_per_buffer;
    buf.alloc_idx.store(0, std::memory_order_relaxed);
    buf.state.store(BUFFER_STATE_WRITING, std::memory_order_relaxed);

    buf.slots = new Slot[slots_per_buffer];
    for (uint32_t j = 0; j < slots_per_buffer; ++j) {
      Slot &s = buf.slots[j];
      s.hwm.store(0, std::memory_order_relaxed);
      s.state.store(SLOT_STATE_WRITING, std::memory_order_relaxed);

      void *mem = std::aligned_alloc(64, SLOT_SIZE_BYTES);
      if (!mem) {
        throw std::bad_alloc();
      }
      s.recs = static_cast<Record *>(mem);
    }
  }
}

inline Slot *BufferManager::allocate_slot() {
  // Capture current write buffer index once; threads may continue using
  // slots from this buffer even after a flip (SWMR per-slot).
  uint32_t w = current_w_idx();
  Buffer &buf = buffers[w];

  uint32_t idx = buf.alloc_idx.fetch_add(1, std::memory_order_relaxed);
  if (idx >= buf.slot_capacity) {
    // Buffer is full. Track the failure; caller decides how to react.
    alloc_failures.fetch_add(1, std::memory_order_relaxed);
    return nullptr;
  }

  Slot &s = buf.slots[idx];
  // Initialize slot for writing (encapsulated in reset_for_write with release
  // semantics)
  s.reset_for_write();
  return &s;
}

inline BufferManager::~BufferManager() {
  for (int i = 0; i < 2; ++i) {
    Buffer &buf = buffers[i];
    if (!buf.slots) {
      continue;
    }
    for (uint32_t j = 0; j < buf.slot_capacity; ++j) {
      Slot &s = buf.slots[j];
      if (s.recs) {
        std::free(s.recs);
        s.recs = nullptr;
      }
    }
    delete[] buf.slots;
    buf.slots = nullptr;
  }
}

inline void BufferManager::flip_buffers() {
  uint32_t old = w_idx.load(std::memory_order_relaxed);
  uint32_t next = old ^ 1u;

  // P0 Correctness Fix: fence (controlled by TSDB_STRICT_CONSISTENCY)
  TSDB_FENCE_BEFORE_SEAL();

  // Flip active write buffer index (single atomic store on hot path).
  w_idx.store(next, std::memory_order_release);

  // Mark old buffer sealed so background workers can start processing it.
  // Use release semantics to ensure visibility of slot state changes.
  buffers[old].state.store(BUFFER_STATE_SEALED, std::memory_order_release);

  // Reset next buffer allocator & mark as writing.
  buffers[next].alloc_idx.store(0, std::memory_order_relaxed);
  buffers[next].state.store(BUFFER_STATE_WRITING, std::memory_order_relaxed);
}

inline void BufferManager::seal_all_slots_for_flush() {
  for (int bi = 0; bi < 2; ++bi) {
    Buffer &buf = buffers[bi];

    uint32_t upper = buf.alloc_idx.load(std::memory_order_acquire);
    if (upper > buf.slot_capacity) {
      upper = buf.slot_capacity;
    }

    for (uint32_t si = 0; si < upper; ++si) {
      Slot &s = buf.slots[si];
      uint8_t st = s.state.load(std::memory_order_acquire);
      uint16_t h = s.size_acquire(); // Use size_acquire() helper
      // 收尾时：只要还有数据且未被消费的 slot，都应当标记为已封口
      if (h > 0 && st != SLOT_STATE_CONSUMED) {
        // P0 Correctness Fix: fence (controlled by TSDB_STRICT_CONSISTENCY)
        TSDB_FENCE_BEFORE_SEAL();
        s.state.store(SLOT_STATE_SEALED, std::memory_order_release);
      }
    }
    // 将 buffer 标记为 SEALED，允许 MergeWorker 消费
    // Use release semantics to ensure merge worker sees all slot state changes.
    buf.state.store(BUFFER_STATE_SEALED, std::memory_order_release);
  }
}

class SBTreeLeaf {
public:
  // min_key_ = UINT64_MAX, max_key_ = 0 indicates empty leaf (no data yet).
  // After first merge, min_key_ and max_key_ will be set to valid values.
  SBTreeLeaf();

  void merge_runs(const std::vector<Record> &newrun);

  uint64_t min_key() const;
  uint64_t max_key() const;

  void snapshot_all(std::vector<Record> &out) const;
  void scan_range(uint64_t key_lo, uint64_t key_hi,
                  std::vector<Record> &out) const;
  // 无锁版本，调用方需确保已持有 shared_lock
  void scan_range_nolock(uint64_t key_lo, uint64_t key_hi,
                         std::vector<Record> &out) const;
  // 带锁版本，用于 Reader 只加一次锁
  void scan_range_with_lock(uint64_t key_lo, uint64_t key_hi,
                            std::vector<Record> &out) const;
  // 从 start_key 开始，最多返回 max_count 条记录，返回实际条数
  size_t scan_from(uint64_t start_key, size_t max_count,
                   std::vector<Record> &out) const;
  // 无锁版本，调用方需确保已持有 shared_lock
  size_t scan_from_nolock(uint64_t start_key, size_t max_count,
                          std::vector<Record> &out) const;
  // 带锁版本，用于 Reader 只加一次锁
  size_t scan_from_with_lock(uint64_t start_key, size_t max_count,
                             std::vector<Record> &out) const;
  // Visitor 版本：不构造 Record，只调用 visitor 函数
  template <class Func>
  size_t scan_from_visit_nolock(uint64_t start_key, size_t max_count,
                                Func &&f) const;
  template <class Func>
  size_t scan_from_visit_with_lock(uint64_t start_key, size_t max_count,
                                   Func &&f) const;
  // 精确查找一个 key，找到返回 true，并把结果写到 out
  bool lookup(uint64_t key, Record &out) const;
  size_t size() const;

  // DataBlock 访问接口
  size_t block_count() const;
  DataBlock *block_at(size_t idx);
  const DataBlock *block_at(size_t idx) const;

private:
  void insert_block_locked(DataBlockPtr block);
  void update_min_max_locked();

  // 二分查找：找到第一个 max_key >= key 的 block 的下标
  // blocks_ 已经按 min_key 有序，但查找时用 max_key 更精确
  size_t lower_bound_block(uint64_t key) const {
    auto it = std::lower_bound(blocks_.begin(), blocks_.end(), key,
                               [](const DataBlockPtr &blk, uint64_t k) {
                                 const DataBlock *b = blk.get();
                                 return b && b->count > 0 && b->max_key < k;
                               });
    return static_cast<size_t>(it - blocks_.begin());
  }

  mutable std::shared_mutex mutex_;
  std::vector<DataBlockPtr> blocks_;
  size_t total_records_;
  uint64_t min_key_; // 最小 key（用于 range 查询）
  uint64_t max_key_; // 最大 key（用于 range 查询）
};

// 全局变量：已合并到 tree 的最大 key（用于 RDS 剪枝优化）
inline std::atomic<uint64_t> g_tree_max_merged_key{0};

class SBTree {
public:
  static constexpr size_t kLeafCount = 16;
  static constexpr int kLeafBits = 4; // log2(kLeafCount)

  SBTree();

  // route by key to leaf
  SBTreeLeaf *locate_leaf(uint64_t key);

  // 新增：根据 key 计算 leaf 下标
  size_t leaf_index_for_key(uint64_t key) const noexcept;

  // 新增：根据 key 获取对应的 leaf（mutable）
  SBTreeLeaf *leaf_for_key(uint64_t key) noexcept;

  // 新增：根据 key 获取对应的 leaf（const）
  const SBTreeLeaf *leaf_for_key(uint64_t key) const noexcept;

  // merge run from RDS into a leaf
  void merge_run_into_leaf(SBTreeLeaf *leaf, const std::vector<Record> &run);

  // testing helper: access first leaf (for backward compatibility)
  SBTreeLeaf *root_leaf() { return &leaves_[0]; }

  // testing helper: access all leaves (const)
  const SBTreeLeaf *leaf_at(size_t idx) const {
    return idx < kLeafCount ? &leaves_[idx] : nullptr;
  }

  // number of leaves
  size_t leaf_count() const { return kLeafCount; }

  // mutable leaf accessor by index
  SBTreeLeaf *leaf_at_mut(size_t idx) {
    return idx < kLeafCount ? &leaves_[idx] : nullptr;
  }

  // get leaf index from pointer
  size_t leaf_index(const SBTreeLeaf *leaf) const {
    return static_cast<size_t>(leaf - &leaves_[0]);
  }

private:
  SBTreeLeaf leaves_[kLeafCount];
};

// =========================
// Merge Worker (background)
// =========================

class MergeWorker {
public:
  explicit MergeWorker(BufferManager *mgr, SBTree *tree);

  // Called by background thread
  void run_once(); // sort + route + merge

  // slot sorting
  void sort_slot(Slot *s);

  // leaf routing (build runs for tree merge)
  void route_slot(Slot *s);

private:
  BufferManager *bm;
  SBTree *tree_;
  std::array<std::vector<Record>, SBTree::kLeafCount> leaf_batches_;
};

// =========================
// MergeWorker Inline Implementations
// =========================

// =========================
// Global Insert API
// =========================

class Engine {
public:
  Engine(BufferManager *buf_mgr, SBTree *tree);

  // API used by user threads
  void insert(uint64_t key, uint64_t value);

  // Flush thread-local slot so that tail WRITING data becomes visible.
  void flush_thread_local();

private:
  BufferManager *bm;
  SBTree *tree;
};

// =========================
// Reader (Tree + RDS merge)
// =========================

class Reader {
public:
  Reader(BufferManager *bm, SBTree *tree, bool enable_rds = true)
      : bm_(bm), tree_(tree), enable_rds_(enable_rds) {}

  // 纯 tree 模式（离线查询）
  explicit Reader(SBTree *tree)
      : bm_(nullptr), tree_(tree), enable_rds_(false) {}

  // Full scan for testing freshness: merge Tree data from ALL leaves with
  // current SEALED RDS.
  std::vector<Record> scan_all() const;

  // Range query: return records with key in [key_lo, key_hi].
  // Queries both Tree (using leaf min/max for pruning) and RDS.
  std::vector<Record> range_query(uint64_t key_lo, uint64_t key_hi) const;

  // Range query into existing buffer: append results to out (clears out first).
  // This version is more efficient for repeated queries as it reuses the
  // buffer.
  void range_query_into(uint64_t key_lo, uint64_t key_hi,
                        std::vector<Record> &out) const;

  // 新接口：从 start_key 开始，最多扫 count 条（论文风格）
  size_t scan_n(uint64_t start_key, size_t count,
                std::vector<Record> &out) const;

  // Visitor 版本：不构造 Record，只调用 visitor 函数
  template <class Func>
  size_t scan_n_visit(uint64_t start_key, size_t count, Func &&f) const;

  // 专用 point lookup：只查一个 key，找到返回 true，并把结果写到 out
  bool lookup(uint64_t key, Record &out) const;

private:
  // Implementation of scan_all (moved to inline function below)
  std::vector<Record> scan_all_impl() const {
    std::vector<Record> result;
    if (!bm_ || !tree_) {
      return result;
    }

    // 1) 从 Tree 的所有 leaf 聚合数据
    std::vector<Record> tree_data;
    // 注意：SBTree 的叶子按照 key 高位做区间分片，且每个 leaf 的 data()
    // 内部有序，且不同 leaf 的 key 区间不重叠。因此按 leaf_idx 从小到大
    // 依次 append 即可保证全局有序，无需再 sort。
    for (size_t li = 0; li < tree_->leaf_count(); ++li) {
      const SBTreeLeaf *leaf = tree_->leaf_at(li);
      if (!leaf) {
        continue;
      }
      leaf->snapshot_all(tree_data);
    }

    // 2) 从 RDS（sealed buffer 中的 sealed slot）聚合数据
    std::vector<Record> rds;
    for (int bi = 0; bi < 2; ++bi) {
      const Buffer &buf = bm_->buffers[bi];
      uint8_t bst = buf.state.load(std::memory_order_acquire);
      if (bst != BUFFER_STATE_SEALED) {
        continue;
      }

      // 只遍历已分配的 slot 范围
      uint32_t upper = buf.alloc_idx.load(std::memory_order_acquire);
      if (upper > buf.slot_capacity) {
        upper = buf.slot_capacity; // 防御性，理论上不会 > slot_capacity
      }

      for (uint32_t i = 0; i < upper; ++i) {
        const Slot &s = buf.slots[i];
        uint8_t st = s.state.load(std::memory_order_acquire);
        if (st != SLOT_STATE_SEALED) {
          continue;
        }
        uint16_t n = s.hwm.load(std::memory_order_acquire);
        if (n == 0) {
          continue;
        }
        rds.insert(rds.end(), s.recs, s.recs + n);
      }
    }
    if (!rds.empty()) {
      std::sort(rds.begin(), rds.end(),
                [](const Record &a, const Record &b) { return a.key < b.key; });
    }

    // 3) 两路合并 Tree 和 RDS（都已排序）
    result.reserve(tree_data.size() + rds.size());
    size_t i = 0, j = 0;
    while (i < tree_data.size() && j < rds.size()) {
      if (tree_data[i].key <= rds[j].key) {
        result.push_back(tree_data[i++]);
      } else {
        result.push_back(rds[j++]);
      }
    }
    while (i < tree_data.size()) {
      result.push_back(tree_data[i++]);
    }
    while (j < rds.size()) {
      result.push_back(rds[j++]);
    }

    return result;
  }

private:
  BufferManager *bm_;
  SBTree *tree_;
  bool enable_rds_;
};

// =========================
// Reader Inline Implementations
// =========================

inline std::vector<Record> Reader::scan_all() const { return scan_all_impl(); }

inline std::vector<Record> Reader::range_query(uint64_t key_lo,
                                               uint64_t key_hi) const {
  // ===== Tree-only fast path =====
  if (!enable_rds_) {
    std::vector<Record> out;
    range_query_into(key_lo, key_hi, out);
    return out;
  }

  // ===== 保留原来的 Tree+RDS 实现 =====
  std::vector<Record> tree_part;
  std::vector<Record> rds_part;

  if (!tree_) {
    return {};
  }

  // === Tree part: 只访问相关 leaf ===
  if (key_lo > key_hi) {
    return {};
  }

  if (tree_) {
    size_t idx_lo = tree_->leaf_index_for_key(key_lo);
    size_t idx_hi = tree_->leaf_index_for_key(key_hi);

    if (idx_lo == idx_hi) {
      // 大多数情况：单 series 的 time range，只访问一个 leaf
      const SBTreeLeaf *leaf = tree_->leaf_at(idx_lo);
      if (leaf) {
        uint64_t leaf_min = leaf->min_key();
        uint64_t leaf_max = leaf->max_key();
        if (!(leaf_min > key_hi || leaf_max < key_lo)) {
          leaf->scan_range(key_lo, key_hi, tree_part);
        }
      }
    } else {
      // 跨多个 leaf 的范围（理论上可能存在，比如跨 series 的 range）
      for (size_t li = idx_lo; li <= idx_hi && li < tree_->leaf_count(); ++li) {
        const SBTreeLeaf *leaf = tree_->leaf_at(li);
        if (!leaf)
          continue;
        uint64_t leaf_min = leaf->min_key();
        uint64_t leaf_max = leaf->max_key();
        if (leaf_min > key_hi || leaf_max < key_lo)
          continue;
        leaf->scan_range(key_lo, key_hi, tree_part);
      }
    }
  }

  // === RDS part: linear scan with filter ===
  if (enable_rds_ && bm_ != nullptr) {
    rds_part.reserve(128); // 预估容量，避免频繁扩容

    // 读取已合并到 tree 的最大 key（用于剪枝）
    uint64_t merged_max = g_tree_max_merged_key.load(std::memory_order_acquire);

    for (int bi = 0; bi < 2; ++bi) {
      const Buffer &buf = bm_->buffers[bi];
      uint8_t bst = buf.state.load(std::memory_order_acquire);
      if (bst != BUFFER_STATE_SEALED)
        continue;

      uint32_t upper = buf.alloc_idx.load(std::memory_order_acquire);
      if (upper > buf.slot_capacity) {
        upper = buf.slot_capacity;
      }

      for (uint32_t si = 0; si < upper; ++si) {
        const Slot &s = buf.slots[si];
        uint8_t st = s.state.load(std::memory_order_acquire);
        if (st != SLOT_STATE_SEALED)
          continue;

        uint16_t n = s.hwm.load(std::memory_order_acquire);
        if (n == 0)
          continue;

        // Slot-level key range 剪枝：如果整个 slot 区间与 [key_lo, key_hi]
        // 无交集，则可以直接跳过。
        uint64_t slot_min =
            s.min_key.load(std::memory_order_acquire); // 已在 WRITING 阶段维护
        uint64_t slot_max =
            s.max_key.load(std::memory_order_acquire); // 已在 WRITING 阶段维护

        // 1) 如果整个 slot 的数据都 <= merged_max，说明这些 key 已经在 tree
        // 里，可跳过
        if (slot_max <= merged_max) {
          continue;
        }

        // 2) 原来的区间剪枝
        if (slot_max < key_lo || slot_min > key_hi) {
          continue;
        }

        const Record *base = s.recs;
        for (uint16_t i = 0; i < n; ++i) {
          const auto &r = base[i];
          if (r.key >= key_lo && r.key <= key_hi) {
            rds_part.push_back(r);
          }
        }
      }
    }

    // Sort RDS part only if it has more than 1 element
    if (rds_part.size() > 1) {
      std::sort(rds_part.begin(), rds_part.end(),
                [](const Record &a, const Record &b) { return a.key < b.key; });
    }
  }

  // === Merge tree_part and rds_part (both sorted) ===
  std::vector<Record> out;
  out.reserve(tree_part.size() + rds_part.size());

  size_t i = 0, j = 0;
  while (i < tree_part.size() && j < rds_part.size()) {
    if (tree_part[i].key <= rds_part[j].key) {
      out.push_back(tree_part[i++]);
    } else {
      out.push_back(rds_part[j++]);
    }
  }
  while (i < tree_part.size()) {
    out.push_back(tree_part[i++]);
  }
  while (j < rds_part.size()) {
    out.push_back(rds_part[j++]);
  }

  return out;
}

inline void Reader::range_query_into(uint64_t key_lo, uint64_t key_hi,
                                     std::vector<Record> &out) const {
  // Clear output but keep capacity for reuse
  out.clear();
  // 对于 ts 场景，scan 一般不会拉非常多条，先给一个经验值
  out.reserve(128);

  // ===== Tree-only fast path =====
  if (!enable_rds_) {
    if (!tree_ || key_lo > key_hi) {
      return;
    }

    // 计算叶子区间
    size_t idx_lo = tree_->leaf_index_for_key(key_lo);
    size_t idx_hi = tree_->leaf_index_for_key(key_hi);

    if (idx_lo == idx_hi) {
      const SBTreeLeaf *leaf = tree_->leaf_at(idx_lo);
      if (!leaf)
        return;
      // 让 leaf 自己做 min/max 剪枝
      leaf->scan_range(key_lo, key_hi, out);
    } else {
      for (size_t li = idx_lo; li <= idx_hi && li < tree_->leaf_count(); ++li) {
        const SBTreeLeaf *leaf = tree_->leaf_at(li);
        if (!leaf)
          continue;
        leaf->scan_range(key_lo, key_hi, out);
      }
    }
    return;
  }

  // ===== 下面保留原来的 Tree+RDS 路径 =====
  std::vector<Record> tree_part;
  std::vector<Record> rds_part;

  if (!tree_) {
    return;
  }

  // === Tree part: 只访问相关 leaf ===
  if (key_lo > key_hi) {
    return;
  }

  if (tree_) {
    size_t idx_lo = tree_->leaf_index_for_key(key_lo);
    size_t idx_hi = tree_->leaf_index_for_key(key_hi);

    if (idx_lo == idx_hi) {
      // 大多数情况：单 series 的 time range，只访问一个 leaf
      const SBTreeLeaf *leaf = tree_->leaf_at(idx_lo);
      if (leaf) {
        uint64_t leaf_min = leaf->min_key();
        uint64_t leaf_max = leaf->max_key();
        if (!(leaf_min > key_hi || leaf_max < key_lo)) {
          leaf->scan_range(key_lo, key_hi, tree_part);
        }
      }
    } else {
      // 跨多个 leaf 的范围（理论上可能存在，比如跨 series 的 range）
      for (size_t li = idx_lo; li <= idx_hi && li < tree_->leaf_count(); ++li) {
        const SBTreeLeaf *leaf = tree_->leaf_at(li);
        if (!leaf)
          continue;
        uint64_t leaf_min = leaf->min_key();
        uint64_t leaf_max = leaf->max_key();
        if (leaf_min > key_hi || leaf_max < key_lo)
          continue;
        leaf->scan_range(key_lo, key_hi, tree_part);
      }
    }
  }

  // === RDS part: linear scan with filter ===
  if (enable_rds_ && bm_ != nullptr) {
    rds_part.reserve(128); // 预估容量，避免频繁扩容

    // 读取已合并到 tree 的最大 key（用于剪枝）
    uint64_t merged_max = g_tree_max_merged_key.load(std::memory_order_acquire);

    for (int bi = 0; bi < 2; ++bi) {
      const Buffer &buf = bm_->buffers[bi];
      uint8_t bst = buf.state.load(std::memory_order_acquire);
      if (bst != BUFFER_STATE_SEALED)
        continue;

      uint32_t upper = buf.alloc_idx.load(std::memory_order_acquire);
      if (upper > buf.slot_capacity) {
        upper = buf.slot_capacity;
      }

      for (uint32_t si = 0; si < upper; ++si) {
        const Slot &s = buf.slots[si];
        uint8_t st = s.state.load(std::memory_order_acquire);
        if (st != SLOT_STATE_SEALED)
          continue;

        uint16_t n = s.hwm.load(std::memory_order_acquire);
        if (n == 0)
          continue;

        // Slot-level key range 剪枝
        uint64_t slot_min = s.min_key.load(std::memory_order_acquire);
        uint64_t slot_max = s.max_key.load(std::memory_order_acquire);

        // 1) 如果整个 slot 的数据都 <= merged_max，说明这些 key 已经在 tree
        // 里，可跳过
        if (slot_max <= merged_max) {
          continue;
        }

        // 2) 原来的区间剪枝
        if (slot_max < key_lo || slot_min > key_hi) {
          continue;
        }

        const Record *base = s.recs;
        for (uint16_t i = 0; i < n; ++i) {
          const auto &r = base[i];
          if (r.key >= key_lo && r.key <= key_hi) {
            rds_part.push_back(r);
          }
        }
      }
    }

    // Sort RDS part only if it has more than 1 element
    if (rds_part.size() > 1) {
      std::sort(rds_part.begin(), rds_part.end(),
                [](const Record &a, const Record &b) { return a.key < b.key; });
    }
  }

  // === Merge tree_part and rds_part (both sorted) ===
  out.reserve(tree_part.size() + rds_part.size());

  size_t i = 0, j = 0;
  while (i < tree_part.size() && j < rds_part.size()) {
    if (tree_part[i].key <= rds_part[j].key) {
      out.push_back(tree_part[i++]);
    } else {
      out.push_back(rds_part[j++]);
    }
  }
  while (i < tree_part.size()) {
    out.push_back(tree_part[i++]);
  }
  while (j < rds_part.size()) {
    out.push_back(rds_part[j++]);
  }
}

template <class Func>
inline size_t Reader::scan_n_visit(uint64_t start_key, size_t count,
                                   Func &&f) const {
  // 只谈 tree-only 的 fast path，因为 Scan-only 就是这种模式
  if (enable_rds_) {
    // 在线模式暂不支持 visitor，回退到普通版本
    std::vector<Record> out;
    return scan_n(start_key, count, out);
  }

  if (!tree_ || count == 0)
    return 0;

  size_t leaf_idx = tree_->leaf_index_for_key(start_key);
  size_t remaining = count;
  size_t got_total = 0;

  for (size_t li = leaf_idx; li < SBTree::kLeafCount && remaining > 0; ++li) {
    const SBTreeLeaf *leaf = tree_->leaf_at(li);
    if (!leaf || leaf->size() == 0)
      continue;

    uint64_t leaf_start = (li == leaf_idx) ? start_key : leaf->min_key();
    size_t got = leaf->scan_from_visit_nolock(leaf_start, remaining,
                                              std::forward<Func>(f));
    got_total += got;
    if (got_total >= count)
      break;
    remaining = count - got_total;
  }
  return got_total;
}

inline size_t Reader::scan_n(uint64_t start_key, size_t count,
                             std::vector<Record> &out) const {
  out.clear();
  if (!tree_ || count == 0)
    return 0;

  // 兼容旧接口：使用 visitor 模式
  out.reserve(std::max(out.capacity(), count));
  return scan_n_visit(start_key, count, [&](uint64_t k, uint64_t v) {
    out.emplace_back(Record{k, v, 0});
  });
}

inline bool Reader::lookup(uint64_t key, Record &out) const {
  bool found = false;
  Record best{};

  // 1. 先查 tree 部分：直接定位到对应 leaf
  if (tree_) {
    const SBTreeLeaf *leaf = tree_->leaf_for_key(key);
    if (leaf) {
      uint64_t leaf_min = leaf->min_key();
      uint64_t leaf_max = leaf->max_key();
      if (key >= leaf_min && key <= leaf_max) {
        Record r;
        if (leaf->lookup(key, r)) {
          best = r;
          found = true;
        }
      }
    }
  }

  // 如果是离线 tree-only Reader，直接返回 tree 的结果
  if (!enable_rds_) {
    if (found) {
      out = best;
    }
    return found;
  }

  // 2. 再查 buffer 部分：两个 buffer 中 SEALED 的 slot
  // buffer 里的记录更"新"，所以如果找到，应该优先使用 buffer 的结果
  if (enable_rds_ && bm_ != nullptr) {
    for (int bi = 0; bi < 2; ++bi) {
      const Buffer &buf = bm_->buffers[bi];
      uint8_t bst = buf.state.load(std::memory_order_acquire);
      if (bst != BUFFER_STATE_SEALED) {
        continue;
      }

      uint32_t upper = buf.alloc_idx.load(std::memory_order_acquire);
      if (upper > buf.slot_capacity) {
        upper = buf.slot_capacity;
      }

      for (uint32_t si = 0; si < upper; ++si) {
        const Slot &s = buf.slots[si];
        uint8_t st = s.state.load(std::memory_order_acquire);
        if (st != SLOT_STATE_SEALED) {
          continue;
        }

        // 用 slot 的 key range 快速过滤
        uint64_t slot_min = s.min_key.load(std::memory_order_acquire);
        uint64_t slot_max = s.max_key.load(std::memory_order_acquire);
        if (key < slot_min || key > slot_max) {
          continue;
        }

        uint16_t sz = s.hwm.load(std::memory_order_acquire);
        if (sz == 0) {
          continue;
        }

        const Record *recs = s.recs;

        // slot 内一般就几十 / 几百条，线性扫即可
        for (uint16_t i = 0; i < sz; ++i) {
          if (recs[i].key == key) {
            best = recs[i];
            found = true;
            // 如果以后引入 epoch / version，可以在这里比较"更新"的记录
            // 目前 buffer 的数据更新，所以直接覆盖 tree 的结果
          }
        }
      }
    }
  }

  if (found) {
    out = best;
    return true;
  }
  return false;
}

// =========================
// Engine Inline Implementations
// =========================

inline SBTreeLeaf::SBTreeLeaf()
    : total_records_(0), min_key_(std::numeric_limits<uint64_t>::max()),
      max_key_(0) {}

inline size_t SBTreeLeaf::block_count() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return blocks_.size();
}

inline DataBlock *SBTreeLeaf::block_at(size_t idx) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (idx >= blocks_.size()) {
    return nullptr;
  }
  return blocks_[idx].get();
}

inline const DataBlock *SBTreeLeaf::block_at(size_t idx) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (idx >= blocks_.size()) {
    return nullptr;
  }
  return blocks_[idx].get();
}

inline void SBTreeLeaf::insert_block_locked(DataBlockPtr block) {
  if (!block || block->count == 0) {
    return;
  }

  uint32_t block_count = block->count;

  // 找到插入位置（保持 blocks 按 min_key 有序）
  auto pos = blocks_.begin();
  while (pos != blocks_.end() && (*pos)->min_key < block->min_key) {
    ++pos;
  }
  blocks_.insert(pos, std::move(block));

  // 更新 min/max 和 total_records
  total_records_ += block_count;
  update_min_max_locked();
}

inline void SBTreeLeaf::update_min_max_locked() {
  if (blocks_.empty()) {
    min_key_ = std::numeric_limits<uint64_t>::max();
    max_key_ = 0;
    return;
  }
  uint64_t new_min = std::numeric_limits<uint64_t>::max();
  uint64_t new_max = 0;
  for (const auto &block : blocks_) {
    if (!block || block->count == 0) {
      continue;
    }
    new_min = std::min(new_min, block->min_key);
    new_max = std::max(new_max, block->max_key);
  }
  if (new_min == std::numeric_limits<uint64_t>::max()) {
    min_key_ = std::numeric_limits<uint64_t>::max();
    max_key_ = 0;
  } else {
    min_key_ = new_min;
    max_key_ = new_max;
  }
}

inline void SBTreeLeaf::merge_runs(const std::vector<Record> &newrun) {
  if (newrun.empty()) {
    return;
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);

  // ----------------------------
  // 1) 快路径：append-only 情况
  //    - 当前 leaf 为空，或者新 run 的最小 key >= 当前 leaf 的 max_key_
  //    - 对应正常 TS 工作负载（单调递增），直接在右侧追加即可
  // ----------------------------
  const uint64_t run_min = newrun.front().key;
  const uint64_t run_max = newrun.back().key;

  bool can_append_right = (total_records_ == 0) || (run_min >= max_key_);

  if (can_append_right) {
    size_t pos = 0;
    while (pos < newrun.size()) {
      DataBlock *raw_blk = get_block_allocator().allocate();
      pos = raw_blk->fill_from_run(newrun, pos);
      if (raw_blk->count == 0) {
        get_block_allocator().deallocate(raw_blk);
        break;
      }
      total_records_ += raw_blk->count;
      blocks_.push_back(DataBlockPtr(raw_blk));
    }

    // 更新 leaf 的 min/max
    if (total_records_ > 0) {
      if (min_key_ == std::numeric_limits<uint64_t>::max()) {
        // 第一次 merge
        min_key_ = blocks_.front()->min_key;
      }
      max_key_ = std::max(max_key_, run_max);
    }
    return;
  }

  // ----------------------------
  // 2) 慢路径：存在乱序 / delayed data
  //    - 为保证 snapshot_all / scan_range 的全局有序性，
  //      我们重新构建整个 leaf：
  //      1) 导出所有已有记录 + newrun
  //      2) 一次 sort
  //      3) 重新切成 DataBlock
  // ----------------------------

  // 2.1 把已有 blocks_ 中的数据 dump 出来
  std::vector<Record> all;
  all.reserve(total_records_ + newrun.size());

  for (const auto &blk_ptr : blocks_) {
    const DataBlock *blk = blk_ptr.get();
    if (!blk || blk->count == 0) {
      continue;
    }
    for (uint32_t i = 0; i < blk->count; ++i) {
      all.push_back(Record{blk->keys[i], blk->values[i], 0});
    }
  }

  // 2.2 把新 run 追加进去
  all.insert(all.end(), newrun.begin(), newrun.end());

  // 2.3 全量排序（按 key）
  std::sort(all.begin(), all.end(),
            [](const Record &a, const Record &b) { return a.key < b.key; });

  // 2.4 清空旧 blocks，重新切块
  blocks_.clear();
  total_records_ = 0;
  min_key_ = std::numeric_limits<uint64_t>::max();
  max_key_ = 0;

  size_t pos = 0;
  while (pos < all.size()) {
    DataBlock *raw_blk = get_block_allocator().allocate();
    pos = raw_blk->fill_from_run(all, pos);
    if (raw_blk->count == 0) {
      get_block_allocator().deallocate(raw_blk);
      break;
    }
    total_records_ += raw_blk->count;
    blocks_.push_back(DataBlockPtr(raw_blk));
  }

  // 2.5 更新 leaf 级 min/max
  update_min_max_locked();
}

inline uint64_t SBTreeLeaf::min_key() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return min_key_;
}

inline uint64_t SBTreeLeaf::max_key() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return max_key_;
}

inline void SBTreeLeaf::snapshot_all(std::vector<Record> &out) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (total_records_ == 0) {
    return;
  }

  out.reserve(out.size() + total_records_);
  for (const auto &blk_ptr : blocks_) {
    const DataBlock *blk = blk_ptr.get();
    if (!blk || blk->count == 0) {
      continue;
    }
    for (uint32_t i = 0; i < blk->count; ++i) {
      out.push_back(Record{blk->keys[i], blk->values[i], 0});
    }
  }
}

inline void SBTreeLeaf::scan_range(uint64_t key_lo, uint64_t key_hi,
                                   std::vector<Record> &out) const {
  scan_range_with_lock(key_lo, key_hi, out);
}

inline void SBTreeLeaf::scan_range_with_lock(uint64_t key_lo, uint64_t key_hi,
                                             std::vector<Record> &out) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  scan_range_nolock(key_lo, key_hi, out);
}

inline void SBTreeLeaf::scan_range_nolock(uint64_t key_lo, uint64_t key_hi,
                                          std::vector<Record> &out) const {
  // 不加锁版，**只在调用方已经持有 shared_lock 时使用**
  // 快速剪枝：如果整个 leaf 的范围与查询范围无交集，直接返回
  if (blocks_.empty() || max_key_ < key_lo || min_key_ > key_hi) {
    return;
  }

  // 在 blocks_ 中找到第一个 max_key >= key_lo 的 block
  auto it = std::lower_bound(
      blocks_.begin(), blocks_.end(), key_lo,
      [](const DataBlockPtr &blk, uint64_t key) {
        const DataBlock *b = blk.get();
        return b && b->count > 0 &&
               b->max_key < key; // true => 这个 block 完全在 key_lo 左边
      });

  for (; it != blocks_.end(); ++it) {
    const DataBlock *blk = it->get();
    if (!blk || blk->count == 0) {
      continue;
    }
    // block 起始 key 已经超过 hi，后面都可以跳过
    if (blk->min_key > key_hi) {
      break; // 后面的 block 都在右边了
    }
    blk->scan_range(key_lo, key_hi, out);
  }
}

inline size_t SBTreeLeaf::scan_from(uint64_t start_key, size_t max_count,
                                    std::vector<Record> &out) const {
  return scan_from_with_lock(start_key, max_count, out);
}

inline size_t SBTreeLeaf::scan_from_with_lock(uint64_t start_key,
                                              size_t max_count,
                                              std::vector<Record> &out) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return scan_from_nolock(start_key, max_count, out);
}

inline size_t SBTreeLeaf::scan_from_nolock(uint64_t start_key, size_t max_count,
                                           std::vector<Record> &out) const {
  if (blocks_.empty() || max_key_ < start_key) {
    return 0;
  }

  // blocks_ 已经按 min_key 排序，先二分定位 block 起点
  auto it = std::lower_bound(blocks_.begin(), blocks_.end(), start_key,
                             [](const DataBlockPtr &b, uint64_t k) {
                               const DataBlock *blk = b.get();
                               return blk && blk->count > 0 && blk->max_key < k;
                             });

  if (it == blocks_.end())
    return 0;

  size_t remaining = max_count;
  size_t got_total = 0;
  uint64_t cur_key = start_key;

  for (; it != blocks_.end() && remaining > 0; ++it) {
    const auto &blk = *it;
    if (!blk || blk->count == 0)
      continue;

    if (blk->max_key < cur_key)
      continue;

    size_t got = blk->scan_from(cur_key, remaining, out);
    if (got == 0)
      continue;

    got_total += got;
    if (got_total >= max_count)
      break;
    remaining = max_count - got_total;

    // 对后续 block，从 block 起点开始
    cur_key = 0;
  }
  return got_total;
}

template <class Func>
inline size_t SBTreeLeaf::scan_from_visit_with_lock(uint64_t start_key,
                                                    size_t max_count,
                                                    Func &&f) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return scan_from_visit_nolock(start_key, max_count, std::forward<Func>(f));
}

template <class Func>
inline size_t SBTreeLeaf::scan_from_visit_nolock(uint64_t start_key,
                                                 size_t max_count,
                                                 Func &&f) const {
  if (blocks_.empty() || max_key_ < start_key) {
    return 0;
  }

  // blocks_ 已经按 min_key 排序，先二分定位 block 起点
  auto it = std::lower_bound(blocks_.begin(), blocks_.end(), start_key,
                             [](const DataBlockPtr &b, uint64_t k) {
                               const DataBlock *blk = b.get();
                               return blk && blk->count > 0 && blk->max_key < k;
                             });

  if (it == blocks_.end())
    return 0;

  size_t remaining = max_count;
  size_t got_total = 0;
  uint64_t cur_key = start_key;

  for (; it != blocks_.end() && remaining > 0; ++it) {
    const auto &blk = *it;
    if (!blk || blk->count == 0)
      continue;

    if (blk->max_key < cur_key)
      continue;

    uint64_t block_start = (got_total == 0) ? cur_key : blk->min_key;
    size_t got =
        blk->scan_from_visit(block_start, remaining, std::forward<Func>(f));
    if (got == 0)
      continue;

    got_total += got;
    if (got_total >= max_count)
      break;
    remaining = max_count - got_total;

    // 对后续 block，从 block 起点开始
    cur_key = 0;
  }
  return got_total;
}

inline bool SBTreeLeaf::lookup(uint64_t key, Record &out) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 快速剪枝：如果 key 不在 leaf 的范围内，直接返回
  if (blocks_.empty() || key < min_key_ || key > max_key_) {
    return false;
  }

  // 使用二分查找定位起始 block
  size_t idx = lower_bound_block(key);
  const size_t n_blocks = blocks_.size();

  for (size_t bi = idx; bi < n_blocks; ++bi) {
    const DataBlock *blk = blocks_[bi].get();
    if (!blk || blk->count == 0) {
      continue;
    }
    // key 小于当前 block 的 min_key，后面的 min_key 只会更大，可以退出
    if (key < blk->min_key) {
      break;
    }
    // key 大于当前 block 的 max_key，继续看下一个 block
    if (key > blk->max_key) {
      continue;
    }
    // key 在当前 block 的范围内，尝试查找
    if (blk->lookup(key, out)) {
      return true;
    }
  }
  return false;
}

inline size_t SBTreeLeaf::size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return total_records_;
}

inline SBTree::SBTree() = default;

inline size_t SBTree::leaf_index_for_key(uint64_t key) const noexcept {
  // 与 locate_leaf 里的逻辑保持一致
  // key 的高 16 bit 是 series_id，用 series_id 的低 kLeafBits 位来选择 leaf
  if (kLeafCount == 0) {
    return 0;
  }
  static_assert((kLeafCount & (kLeafCount - 1)) == 0,
                "kLeafCount must be power of two");
  // 提取 series_id（高 16 bit），然后用低 kLeafBits 位来选择 leaf
  uint64_t series_id = key >> 48;
  size_t idx = static_cast<size_t>(series_id) & (kLeafCount - 1);
  if (idx >= kLeafCount) {
    idx = kLeafCount - 1;
  }
  return idx;
}

inline SBTreeLeaf *SBTree::leaf_for_key(uint64_t key) noexcept {
  return &leaves_[leaf_index_for_key(key)];
}

inline const SBTreeLeaf *SBTree::leaf_for_key(uint64_t key) const noexcept {
  return &leaves_[leaf_index_for_key(key)];
}

inline SBTreeLeaf *SBTree::locate_leaf(uint64_t key) {
  // 改为使用 leaf_for_key 实现
  return leaf_for_key(key);
}

inline void SBTree::merge_run_into_leaf(SBTreeLeaf *leaf,
                                        const std::vector<Record> &run) {
  if (!leaf) {
    return;
  }
  leaf->merge_runs(run);
}

inline MergeWorker::MergeWorker(BufferManager *mgr, SBTree *tree)
    : bm(mgr), tree_(tree) {}

inline void MergeWorker::sort_slot(Slot *s) {
  if (!s) {
    return;
  }
  const uint16_t n = s->hwm.load(std::memory_order_acquire);
  if (n <= 1) {
    return;
  }

  std::sort(s->recs, s->recs + n,
            [](const Record &a, const Record &b) { return a.key < b.key; });
}

// Legacy per-slot routing kept for reference; no longer used in run_once.
inline void MergeWorker::route_slot(Slot *s) {
  if (!s || !tree_) {
    return;
  }
  const uint16_t n = s->hwm.load(std::memory_order_acquire);
  if (n == 0) {
    return;
  }
  uint16_t start = 0;
  while (start < n) {
    const uint64_t key = s->recs[start].key;
    SBTreeLeaf *leaf = tree_->locate_leaf(key);
    if (!leaf) {
      break;
    }
    uint16_t end = start + 1;
    while (end < n && tree_->locate_leaf(s->recs[end].key) == leaf) {
      ++end;
    }
    std::vector<Record> run(s->recs + start, s->recs + end);
    tree_->merge_run_into_leaf(leaf, run);
    start = end;
  }
}

// Helper: route slot records into per-leaf batches.
// Simplified version (doesn't assume slot is sorted).
// IMPORTANT: Must be called after the caller has ensured state==SEALED
// with acquire semantics and posted a seq_cst fence.
template <size_t N>
inline void route_slot_to_batches(SBTree *tree, Slot *s,
                                  std::array<std::vector<Record>, N> &batches) {
  if (!tree || !s) {
    return;
  }
  const uint16_t n = s->hwm.load(std::memory_order_acquire);
  if (n == 0) {
    return;
  }

  // Simple: push each record to its leaf's batch individually.
  // Sorting happens later at leaf level, so no need to assume slot is sorted.
  for (uint16_t i = 0; i < n; ++i) {
    const Record &r = s->recs[i];
    SBTreeLeaf *leaf = tree->locate_leaf(r.key);
    if (!leaf) {
      continue; // Shouldn't happen, but skip if it does
    }
    size_t leaf_idx = tree->leaf_index(leaf);
    batches[leaf_idx].push_back(r);
  }
}

inline void MergeWorker::run_once() {
  if (!bm || !tree_) {
    return;
  }

  const size_t kLeafCount = leaf_batches_.size();
  for (auto &batch : leaf_batches_) {
    batch.clear();
  }

  // 1) 扫描所有 SEALED buffer，把 slot 中的数据按 leaf 聚合到 leaf_batches
  for (int bi = 0; bi < 2; ++bi) {
    Buffer &buf = bm->buffers[bi];
    uint8_t bst = buf.state.load(std::memory_order_acquire);
    if (bst != BUFFER_STATE_SEALED) {
      continue;
    }

    for (uint32_t i = 0; i < buf.slot_capacity; ++i) {
      Slot *s = &buf.slots[i];
      if (!s) {
        continue;
      }
      // 先读 state，确保看到 seal 之后再读 hwm
      uint8_t st = s->state.load(std::memory_order_acquire);
      if (st != SLOT_STATE_SEALED) {
        continue;
      }

      // P0 Correctness Fix: fence (controlled by TSDB_STRICT_CONSISTENCY)
      TSDB_FENCE_BEFORE_SEAL();

      uint16_t n = s->hwm.load(std::memory_order_acquire);
      if (n == 0) {
        continue;
      }

      // Optimization: skip per-slot sorting, let leaf-level sort handle it
      route_slot_to_batches(tree_, s, leaf_batches_);
      // 这个 slot 的内容已经完全挪到 leaf_batches 里了
      s->hwm.store(0, std::memory_order_release);
      s->state.store(SLOT_STATE_CONSUMED, std::memory_order_release);
    }
  }

  // 2) 对每个 leaf 的 batch sort 一次，然后 merge_runs 一次
  std::vector<size_t> leaf_indices_to_merge;
  leaf_indices_to_merge.reserve(kLeafCount);
  for (size_t leaf_idx = 0; leaf_idx < kLeafCount; ++leaf_idx) {
    if (!leaf_batches_[leaf_idx].empty()) {
      leaf_indices_to_merge.push_back(leaf_idx);
    }
  }

  if (leaf_indices_to_merge.empty()) {
    return;
  }

  auto merge_task = [&](size_t leaf_idx) {
    auto &batch = leaf_batches_[leaf_idx];
    if (batch.empty()) {
      return;
    }
    std::sort(batch.begin(), batch.end(),
              [](const Record &a, const Record &b) { return a.key < b.key; });
    SBTreeLeaf *leaf = tree_->leaf_at_mut(leaf_idx);
    tree_->merge_run_into_leaf(leaf, batch);

    // 更新全局最大已合并 key（batch 已排序，back() 就是最大 key）
    uint64_t run_max = batch.back().key;
    uint64_t prev = g_tree_max_merged_key.load(std::memory_order_relaxed);
    while (run_max > prev && !g_tree_max_merged_key.compare_exchange_weak(
                                 prev, run_max, std::memory_order_release,
                                 std::memory_order_relaxed)) {
      // CAS 失败会把新的 prev 带出来，再比一轮
    }
  };

  if (leaf_indices_to_merge.size() == 1) {
    merge_task(leaf_indices_to_merge.front());
    return;
  }

  const size_t hw_threads =
      std::max<size_t>(1, std::thread::hardware_concurrency());
  const size_t worker_count =
      std::min(hw_threads, leaf_indices_to_merge.size());
  std::atomic<size_t> next{0};

  auto worker_loop = [&]() {
    while (true) {
      size_t idx = next.fetch_add(1);
      if (idx >= leaf_indices_to_merge.size()) {
        break;
      }
      merge_task(leaf_indices_to_merge[idx]);
    }
  };

  std::vector<std::thread> merge_workers;
  merge_workers.reserve(worker_count);
  for (size_t i = 0; i < worker_count; ++i) {
    merge_workers.emplace_back(worker_loop);
  }
  for (auto &worker : merge_workers) {
    worker.join();
  }
}

inline Engine::Engine(BufferManager *buf_mgr, SBTree *tree)
    : bm(buf_mgr), tree(tree) {}

inline void Engine::insert(uint64_t key, uint64_t value) {
  Slot *s = tls_ctx.current_slot;

  // Read current write buffer index once per insert (already uses acquire
  // semantics).
  uint32_t w = bm->current_w_idx();

  // Slow path: no current slot or slot is full or buffer flipped.
  // Use size_acquire() to see the latest hwm value written by fast path
  // (fetch_add acq_rel)
  if (s == nullptr || s->size_acquire() >= SLOT_CAPACITY ||
      tls_ctx.current_buf_idx != w) {
    // Seal previous slot if switching away due to full or buffer flip.
    // Only seal if it has data (to avoid SEALED empty slots)
    if (s != nullptr && s->size_acquire() > 0) {
      // P0 Correctness Fix: fence (controlled by TSDB_STRICT_CONSISTENCY)
      TSDB_FENCE_BEFORE_SEAL();
      s->state.store(SLOT_STATE_SEALED, std::memory_order_release);
    }
    s = bm->allocate_slot();
    if (!s) {
      // Allocation failed (buffer full). In this minimal implementation we
      // simply drop the write; production code should implement backpressure
      // or error handling.
      return;
    }
    tls_ctx.current_slot = s;
    tls_ctx.current_buf_idx = w;
  }

  // Fast path: append-only within SWMR slot.
  // 使用 append_pos() 获取位置（encapsulated fetch_add with acq_rel semantics）
  uint16_t pos = s->append_pos();
  // 写入数据（对 merge/flush 可见，因为前面的 append_pos 建立全序）
  s->recs[pos] = Record{key, value, 0};

  // 更新 slot 级别的 key 范围元数据（用于 RDS 侧剪枝）。
  // 单写者（SWMR）模式下，这里只需使用 relaxed 原子操作即可。
  uint64_t cur_min = s->min_key.load(std::memory_order_relaxed);
  if (key < cur_min) {
    s->min_key.store(key, std::memory_order_relaxed);
  }
  uint64_t cur_max = s->max_key.load(std::memory_order_relaxed);
  if (key > cur_max) {
    s->max_key.store(key, std::memory_order_relaxed);
  }
}

inline void Engine::flush_thread_local() {
  Slot *s = tls_ctx.current_slot;
  if (!s) {
    return;
  }
  if (!s->has_data_acquire()) {
    tls_ctx.current_slot = nullptr;
    return;
  }
  TSDB_FENCE_BEFORE_SEAL();
  s->seal();
  tls_ctx.current_slot = nullptr;
}

// =========================
// Background Flipper Thread
// =========================

class Flipper {
public:
  // 构造函数：接收 buffer manager、tree、以及 flip 间隔（毫秒）
  Flipper(BufferManager *bm, SBTree *tree, uint32_t flip_interval_ms = 20)
      : bm_(bm), tree_(tree), flip_interval_ms_(flip_interval_ms),
        running_(false), worker_thread_(nullptr) {}

  ~Flipper() {
    if (running_) {
      stop();
    }
  }

  // 启动后台 flipper 线程
  void start() {
    if (running_) {
      return; // 已经在运行
    }
    running_.store(true, std::memory_order_release);
    worker_thread_ = std::make_unique<std::thread>([this]() { this->run(); });
  }

  // 停止后台 flipper 线程
  void stop() {
    if (!running_) {
      return;
    }
    running_.store(false, std::memory_order_release);
    if (worker_thread_ && worker_thread_->joinable()) {
      worker_thread_->join();
    }
    worker_thread_.reset();
  }

  // 等待线程结束（可选，主要用于测试/shutdown）
  void join() {
    if (worker_thread_ && worker_thread_->joinable()) {
      worker_thread_->join();
    }
  }

private:
  BufferManager *bm_;
  SBTree *tree_;
  uint32_t flip_interval_ms_;
  std::atomic<bool> running_;
  std::unique_ptr<std::thread> worker_thread_;

  // 后台线程主循环
  void run() {
    while (running_.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(flip_interval_ms_));

      if (!running_.load(std::memory_order_acquire)) {
        break;
      }

      // 执行一次 flip + merge 周期
      if (bm_ && tree_) {
        bm_->flip_buffers();
        MergeWorker mw(bm_, tree_);
        mw.run_once();
      }
    }
  }
};

// Flush all WRITING slots into SEALED state and run one merge pass.
// 典型用途：系统 shutdown 之前调用一次，保证所有尾部数据都 merge 到 Tree。
inline void flush_all_and_merge_once(BufferManager *bm, SBTree *tree) {
  if (!bm || !tree) {
    return;
  }
  bm->seal_all_slots_for_flush();
  MergeWorker mw(bm, tree);
  mw.run_once();
}
