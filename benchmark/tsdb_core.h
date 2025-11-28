#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib> // aligned_alloc, free
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
  static constexpr size_t kMaxEntries = 256; // 256 * 8 bytes keys + 256 * 8
                                             // bytes values = 4KB
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

  DataBlock() : count(0), min_key(0), max_key(0), bucket_count(0) {}

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
    size_t start_idx = 0;
    if (bucket_count > 0) {
      auto it =
          std::upper_bound(bucket_min_keys.begin(),
                           bucket_min_keys.begin() + bucket_count, key_lo);
      if (it == bucket_min_keys.begin()) {
        start_idx = 0;
      } else {
        start_idx = bucket_offsets[(it - bucket_min_keys.begin()) - 1];
      }
    }
    // advance start to first >= key_lo
    while (start_idx < count && keys[start_idx] < key_lo) {
      ++start_idx;
    }
    for (size_t i = start_idx; i < count && keys[i] <= key_hi; ++i) {
      out.push_back(Record{keys[i], values[i], 0});
    }
  }
};

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
    for (uint32_t si = 0; si < buf.slot_capacity; ++si) {
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
  // 精确查找一个 key，找到返回 true，并把结果写到 out
  bool lookup(uint64_t key, Record &out) const;
  size_t size() const;

  // DataBlock 访问接口
  size_t block_count() const;
  DataBlock *block_at(size_t idx);
  const DataBlock *block_at(size_t idx) const;

private:
  void insert_block_locked(std::unique_ptr<DataBlock> block);
  void update_min_max_locked();

  mutable std::shared_mutex mutex_;
  std::vector<std::unique_ptr<DataBlock>> blocks_;
  size_t total_records_;
  uint64_t min_key_; // 最小 key（用于 range 查询）
  uint64_t max_key_; // 最大 key（用于 range 查询）
};

class SBTree {
public:
  static constexpr size_t kLeafCount = 16;
  static constexpr int kLeafBits = 4; // log2(kLeafCount)

  SBTree();

  // route by key to leaf
  SBTreeLeaf *locate_leaf(uint64_t key);

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
  Reader(BufferManager *bm, SBTree *tree) : bm_(bm), tree_(tree) {}

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
      for (uint32_t i = 0; i < buf.slot_capacity; ++i) {
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
};

// =========================
// Reader Inline Implementations
// =========================

inline std::vector<Record> Reader::scan_all() const { return scan_all_impl(); }

inline std::vector<Record> Reader::range_query(uint64_t key_lo,
                                               uint64_t key_hi) const {
  std::vector<Record> tree_part;
  std::vector<Record> rds_part;

  if (!tree_) {
    return {};
  }

  // === Tree part: use leaf min/max for pruning, binary search within range ===
  const size_t leaf_cnt = tree_->leaf_count();
  for (size_t li = 0; li < leaf_cnt; ++li) {
    const SBTreeLeaf *leaf = tree_->leaf_at(li);
    if (!leaf)
      continue;

    // Quick prune: no overlap with [key_lo, key_hi]
    // Skip if leaf is completely below key_lo or completely above key_hi
    // (Note: empty leaves have min_key=UINT64_MAX, max_key=0, which doesn't
    // enter this condition)
    if (leaf->max_key() < key_lo || leaf->min_key() > key_hi) {
      continue;
    }

    leaf->scan_range(key_lo, key_hi, tree_part);
  }

  // === RDS part: linear scan with filter ===
  if (bm_) {
    for (int bi = 0; bi < 2; ++bi) {
      const Buffer &buf = bm_->buffers[bi];
      uint8_t bst = buf.state.load(std::memory_order_acquire);
      if (bst != BUFFER_STATE_SEALED)
        continue;

      for (uint32_t si = 0; si < buf.slot_capacity; ++si) {
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
  }

  // Sort RDS part to ensure it's ordered
  std::sort(rds_part.begin(), rds_part.end(),
            [](const Record &a, const Record &b) { return a.key < b.key; });

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

  std::vector<Record> tree_part;
  std::vector<Record> rds_part;

  if (!tree_) {
    return;
  }

  // === Tree part: use leaf min/max for pruning, binary search within range ===
  const size_t leaf_cnt = tree_->leaf_count();
  for (size_t li = 0; li < leaf_cnt; ++li) {
    const SBTreeLeaf *leaf = tree_->leaf_at(li);
    if (!leaf)
      continue;

    // Quick prune: no overlap with [key_lo, key_hi]
    if (leaf->max_key() < key_lo || leaf->min_key() > key_hi) {
      continue;
    }

    leaf->scan_range(key_lo, key_hi, tree_part);
  }

  // === RDS part: linear scan with filter ===
  if (bm_) {
    for (int bi = 0; bi < 2; ++bi) {
      const Buffer &buf = bm_->buffers[bi];
      uint8_t bst = buf.state.load(std::memory_order_acquire);
      if (bst != BUFFER_STATE_SEALED)
        continue;

      for (uint32_t si = 0; si < buf.slot_capacity; ++si) {
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
  }

  // Sort RDS part to ensure it's ordered
  std::sort(rds_part.begin(), rds_part.end(),
            [](const Record &a, const Record &b) { return a.key < b.key; });

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

inline bool Reader::lookup(uint64_t key, Record &out) const {
  bool found = false;
  Record best{};

  // 1. 先查 tree 部分：只定位一个 leaf
  if (tree_) {
    SBTreeLeaf *leaf = tree_->locate_leaf(key);
    if (leaf) {
      Record r;
      if (leaf->lookup(key, r)) {
        best = r;
        found = true;
      }
    }
  }

  // 2. 再查 buffer 部分：两个 buffer 中 SEALED 的 slot
  // buffer 里的记录更"新"，所以如果找到，应该优先使用 buffer 的结果
  if (bm_) {
    for (int bi = 0; bi < 2; ++bi) {
      const Buffer &buf = bm_->buffers[bi];
      uint8_t bst = buf.state.load(std::memory_order_acquire);
      if (bst != BUFFER_STATE_SEALED) {
        continue;
      }

      const uint32_t slots = buf.slot_capacity;
      for (uint32_t si = 0; si < slots; ++si) {
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
  }
  return found;
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

inline void SBTreeLeaf::insert_block_locked(std::unique_ptr<DataBlock> block) {
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

  // newrun 已经是针对该 leaf 的有序 run，直接切成若干 DataBlock
  size_t pos = 0;
  while (pos < newrun.size()) {
    std::unique_ptr<DataBlock> blk = std::make_unique<DataBlock>();
    pos = blk->fill_from_run(newrun, pos);
    insert_block_locked(std::move(blk));
  }
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
  std::shared_lock<std::shared_mutex> lock(mutex_);
  // 快速剪枝：如果整个 leaf 的范围与查询范围无交集，直接返回
  if (total_records_ == 0 || max_key_ < key_lo || min_key_ > key_hi) {
    return;
  }

  for (const auto &blk_ptr : blocks_) {
    const DataBlock *blk = blk_ptr.get();
    if (!blk || blk->count == 0) {
      continue;
    }
    if (blk->max_key < key_lo || blk->min_key > key_hi) {
      continue;
    }
    blk->scan_range(key_lo, key_hi, out);
  }
}

inline bool SBTreeLeaf::lookup(uint64_t key, Record &out) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  // 快速剪枝：如果 key 不在 leaf 的范围内，直接返回
  if (blocks_.empty() || key < min_key_ || key > max_key_) {
    return false;
  }

  // 遍历所有 DataBlock（一般就 1~几块）
  for (const auto &blk_ptr : blocks_) {
    const DataBlock *blk = blk_ptr.get();
    if (!blk || blk->count == 0) {
      continue;
    }

    // 利用 block 的 key 范围做快速剪枝
    if (key < blk->min_key || key > blk->max_key) {
      continue;
    }

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

inline SBTreeLeaf *SBTree::locate_leaf(uint64_t key) {
  // Simple range partitioning by high bits: split 64-bit key space into
  // kLeafCount contiguous ranges.
  if (kLeafCount == 0) {
    return nullptr;
  }
  // Use the top log2(kLeafCount) bits to choose a leaf.
  static_assert((kLeafCount & (kLeafCount - 1)) == 0,
                "kLeafCount must be power of two");
  size_t idx = static_cast<size_t>(key >> (64 - kLeafBits));
  if (idx >= kLeafCount) {
    idx = kLeafCount - 1;
  }
  return &leaves_[idx];
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
    std::sort(batch.begin(), batch.end(),
              [](const Record &a, const Record &b) { return a.key < b.key; });
    SBTreeLeaf *leaf = tree_->leaf_at_mut(leaf_idx);
    tree_->merge_run_into_leaf(leaf, batch);
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
