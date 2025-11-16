#pragma once
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib> // aligned_alloc, free
#include <functional>
#include <thread>
#include <vector>
#include <array>

// =========================
// Configurable Parameters
// =========================

static constexpr uint32_t SLOT_SIZE_BYTES = 16 * 1024; // 16KB
static constexpr uint32_t RECORD_SIZE = 24;            // typical
static constexpr uint32_t MAX_SLOTS = 1 << 20;         // configurable

// =========================
// Record
// =========================

struct Record {
  uint64_t key;   // timestamp or logical key
  uint64_t value; // payload
  uint32_t epoch; // optional
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
  Record *recs;  // pointer to continuous memory (16KB)
  uint16_t hwm;  // number of valid records (SWMR)
  uint8_t state; // 0=WRITING, 1=SEALED

  Slot() : recs(nullptr), hwm(0), state(0) {}
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
      s.hwm = 0;
      s.state = SLOT_STATE_WRITING;

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
  s.hwm = 0;
  s.state = SLOT_STATE_WRITING;
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

  // Flip active write buffer index (single atomic store on hot path).
  w_idx.store(next, std::memory_order_release);

  // Mark old buffer sealed so background workers can start processing it.
  buffers[old].state.store(BUFFER_STATE_SEALED, std::memory_order_relaxed);

  // Reset next buffer allocator & mark as writing.
  buffers[next].alloc_idx.store(0, std::memory_order_relaxed);
  buffers[next].state.store(BUFFER_STATE_WRITING, std::memory_order_relaxed);
}

// =========================
// Merge Worker (background)
// =========================

class MergeWorker {
public:
  explicit MergeWorker(BufferManager *mgr, class SBTree *tree);

  // Called by background thread
  void run_once(); // sort + route + merge

  // slot sorting
  void sort_slot(Slot *s);

  // leaf routing (build runs for tree merge)
  void route_slot(Slot *s);

private:
  BufferManager *bm;
  class SBTree *tree_;
};

// =========================
// MergeWorker Inline Implementations
// =========================

// =========================
// SB-Tree Interfaces (Final Storage Layer)
// =========================

class SBTreeLeaf {
public:
  SBTreeLeaf() = default;
  void merge_runs(const std::vector<Record> &newrun);

  // testing helper
  const std::vector<Record> &data() const { return data_; }

private:
  std::vector<Record> data_;
};

class SBTree {
public:
  static constexpr size_t kLeafCount = 4;

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
// Global Insert API
// =========================

class Engine {
public:
  Engine(BufferManager *buf_mgr, SBTree *tree);

  // API used by user threads
  void insert(uint64_t key, uint64_t value);

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

  // Full scan for testing freshness: merge Tree data with current SEALED RDS.
  std::vector<Record> scan_all() const {
    std::vector<Record> result;
    if (!bm_ || !tree_) {
      return result;
    }

    const std::vector<Record> &tree_data = tree_->root_leaf()->data();

    std::vector<Record> rds;
    // Collect all sealed slots from sealed buffers
    for (int bi = 0; bi < 2; ++bi) {
      const Buffer &buf = bm_->buffers[bi];
      uint8_t st = buf.state.load(std::memory_order_acquire);
      if (st != BUFFER_STATE_SEALED) {
        continue;
      }
      for (uint32_t i = 0; i < buf.slot_capacity; ++i) {
        const Slot &s = buf.slots[i];
        if (s.hwm == 0) {
          continue;
        }
        if (s.state != SLOT_STATE_SEALED) {
          continue;
        }
        rds.insert(rds.end(), s.recs, s.recs + s.hwm);
      }
    }

    if (!rds.empty()) {
      std::sort(rds.begin(), rds.end(),
                [](const Record &a, const Record &b) { return a.key < b.key; });
    }

    // Two-way merge (both inputs sorted)
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
// Engine Inline Implementations
// =========================

inline SBTree::SBTree() = default;

inline SBTreeLeaf *SBTree::locate_leaf(uint64_t key) {
  // Simple range partitioning by high bits: split 64-bit key space into
  // kLeafCount contiguous ranges.
  if (kLeafCount == 0) {
    return nullptr;
  }
  // Use the top log2(kLeafCount) bits to choose a leaf.
  constexpr int bits = 2; // since kLeafCount == 4
  size_t idx = static_cast<size_t>(key >> (64 - bits));
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

inline void SBTreeLeaf::merge_runs(const std::vector<Record> &newrun) {
  if (newrun.empty()) {
    return;
  }

  // Assume newrun is sorted by key (it should come from sorted slots).
  // Merge existing sorted data_ with newrun into a new vector.
  std::vector<Record> merged;
  merged.reserve(data_.size() + newrun.size());

  size_t i = 0;
  size_t j = 0;

  while (i < data_.size() && j < newrun.size()) {
    if (data_[i].key <= newrun[j].key) {
      merged.push_back(data_[i++]);
    } else {
      merged.push_back(newrun[j++]);
    }
  }
  while (i < data_.size()) {
    merged.push_back(data_[i++]);
  }
  while (j < newrun.size()) {
    merged.push_back(newrun[j++]);
  }

  data_.swap(merged);
}

inline MergeWorker::MergeWorker(BufferManager *mgr, SBTree *tree)
    : bm(mgr), tree_(tree) {}

inline void MergeWorker::sort_slot(Slot *s) {
  if (!s) {
    return;
  }
  const uint16_t n = s->hwm;
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
  const uint16_t n = s->hwm;
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

// New helper: route one sorted slot into per-leaf batches without merging.
template <size_t N>
inline void route_slot_to_batches(SBTree *tree, Slot *s,
                                  std::array<std::vector<Record>, N> &batches) {
  if (!tree || !s) {
    return;
  }
  const uint16_t n = s->hwm;
  if (n == 0) {
    return;
  }
  uint16_t start = 0;
  while (start < n) {
    const uint64_t key = s->recs[start].key;
    SBTreeLeaf *leaf = tree->locate_leaf(key);
    if (!leaf) {
      break;
    }
    size_t leaf_idx = tree->leaf_index(leaf);
    uint16_t end = start + 1;
    while (end < n &&
           tree->leaf_index(tree->locate_leaf(s->recs[end].key)) == leaf_idx) {
      ++end;
    }
    auto &vec = batches[leaf_idx];
    vec.insert(vec.end(), s->recs + start, s->recs + end);
    start = end;
  }
}

inline void MergeWorker::run_once() {
  if (!bm || !tree_) {
    return;
  }

  constexpr size_t kLeafCount = SBTree::kLeafCount;
  std::array<std::vector<Record>, kLeafCount> leaf_batches;

  // 1) 扫描所有 SEALED buffer，把 slot 中的数据按 leaf 聚合到 leaf_batches
  for (int bi = 0; bi < 2; ++bi) {
    Buffer &buf = bm->buffers[bi];
    uint8_t st = buf.state.load(std::memory_order_acquire);
    if (st != BUFFER_STATE_SEALED) {
      continue;
    }
    for (uint32_t i = 0; i < buf.slot_capacity; ++i) {
      Slot *s = &buf.slots[i];
      if (!s || s->hwm == 0) {
        continue;
      }
      if (s->state != SLOT_STATE_SEALED) {
        continue;
      }
      sort_slot(s);
      route_slot_to_batches(tree_, s, leaf_batches);
      s->hwm = 0;
      s->state = SLOT_STATE_CONSUMED;
    }
  }

  // 2) 对每个 leaf 的 batch sort 一次，然后 merge_runs 一次
  for (size_t leaf_idx = 0; leaf_idx < kLeafCount; ++leaf_idx) {
    auto &batch = leaf_batches[leaf_idx];
    if (batch.empty()) {
      continue;
    }
    std::sort(batch.begin(), batch.end(),
              [](const Record &a, const Record &b) { return a.key < b.key; });
    SBTreeLeaf *leaf = tree_->leaf_at_mut(leaf_idx);
    tree_->merge_run_into_leaf(leaf, batch);
  }
}

inline Engine::Engine(BufferManager *buf_mgr, SBTree *tree)
    : bm(buf_mgr), tree(tree) {}

inline void Engine::insert(uint64_t key, uint64_t value) {
  Slot *s = tls_ctx.current_slot;

  // Read current write buffer index once per insert.
  uint32_t w = bm->current_w_idx();

  // Slow path: no current slot or slot is full.
  if (s == nullptr || s->hwm >= SLOT_CAPACITY || tls_ctx.current_buf_idx != w) {
    // Seal previous slot if switching away due to full or buffer flip.
    if (s != nullptr && (s->hwm > 0)) {
      s->state = SLOT_STATE_SEALED;
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

  // Fast path: append-only within SWMR slot, hwm is non-atomic.
  uint16_t pos = s->hwm++;
  // Epoch is currently unused; set to 0.
  s->recs[pos] = Record{key, value, 0};
}
