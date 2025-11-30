// 统一测试脚本：支持插入、查询、范围查询、混合测试
// 适配 tsdb_core.h，保持与现有脚本风格一致
#include "tsdb_core.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <thread>
#include <vector>

// =========================
// 基本类型定义（与现有脚本一致）
// =========================
using Timestamp = uint64_t;
using Value = uint64_t;
using SeriesId = uint32_t;

// 一条"输入数据"：某个传感器在某个时间点的一条记录
struct InputPoint {
  Timestamp ts;
  Value value;
  SeriesId series;
};

// =========================
// 全局配置
// =========================

// 传感器数量（可配置，默认与 sf_tsd_index_benchmark 一致）
static constexpr uint32_t kNumSeries = 1024;

// 每个传感器生成多少条记录
static constexpr uint64_t kRecordsPerSensor = 10000;

// 总记录数
static constexpr uint64_t kTotalRecords = kRecordsPerSensor * kNumSeries;

// 每条记录的时间间隔（ms）
static constexpr uint64_t kTimeStepMs = 50;

// 时间戳起点（毫秒）
static constexpr Timestamp kStartTimestamp =
    1756684800000ULL; // 2025-09-01 00:00:00.000

// 每个传感器一条序列
static std::vector<InputPoint> g_series[kNumSeries];

// 预生成的 Scan 查询（Step 1 优化：分离 RNG 开销）
struct ScanQuery {
  uint64_t start_key;
  size_t count;
};
static std::vector<ScanQuery> g_scan_queries;

// 多线程写入时用的原子分配器：按 sensor 维度分配 work
static std::atomic<uint32_t> g_insert_series_id{0};

// 查询/扫描任务分配
static std::atomic<uint32_t> g_get_sensor_id{0};
static std::atomic<uint32_t> g_scan_sensor_id{0};

// =========================
// Workload 类型
// =========================

enum class WorkloadType { InsertOnly, LookupOnly, ScanOnly, Mixed, All };

WorkloadType ParseWorkload(const char *arg) {
  if (!arg)
    return WorkloadType::InsertOnly;
  std::string s(arg);
  for (auto &c : s)
    c = static_cast<char>(std::tolower(c));
  if (s == "ins" || s == "insert")
    return WorkloadType::InsertOnly;
  if (s == "get" || s == "lookup")
    return WorkloadType::LookupOnly;
  if (s == "scan")
    return WorkloadType::ScanOnly;
  if (s == "mix" || s == "mixed")
    return WorkloadType::Mixed;
  if (s == "all")
    return WorkloadType::All;
  return WorkloadType::InsertOnly;
}

// =========================
// 简单计时器（与现有脚本一致）
// =========================

class Timer {
public:
  void Start() { start_ = std::chrono::high_resolution_clock::now(); }

  double EndUs() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start_)
        .count();
  }

  double EndMs() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start_)
        .count();
  }

private:
  std::chrono::high_resolution_clock::time_point start_;
};

// =========================
// 数据生成：模拟真实时序模式（与现有脚本一致）
// =========================

// 将 sensor_id + timestamp 编码到一个 64bit key 里：
//   [63..48] : series_id
//   [47.. 0] : timestamp (低 48 位)
inline uint64_t MakeKey(SeriesId sid, Timestamp ts) {
  constexpr uint64_t kLowMask = (1ULL << 48) - 1;
  return (static_cast<uint64_t>(sid) << 48) | (ts & kLowMask);
}

// 生成总共 num_records 条数据，按传感器拆成多条等长时间序列
void GenerateTimeSeriesData(uint64_t num_records) {
  if (num_records == 0)
    return;
  const uint64_t per_series_cap =
      (num_records + kNumSeries - 1) / kNumSeries; // 上界预估

  std::cout << "[Generator] target records = " << num_records << "\n";
  std::cout << "[Generator] series = " << kNumSeries << ", per_series_cap ≈ "
            << per_series_cap << "\n";

  uint64_t generated = 0;
  for (SeriesId sid = 0; sid < kNumSeries && generated < num_records; ++sid) {
    auto &vec = g_series[sid];
    vec.reserve(per_series_cap);
    for (uint64_t j = 0; j < per_series_cap && generated < num_records;
         ++j, ++generated) {
      Timestamp ts = kStartTimestamp + j * kTimeStepMs;
      Value val = ts; // 为了方便验证，这里直接 value = ts
      vec.push_back(InputPoint{ts, val, sid});
    }
  }
  std::cout << "[Generator] actually generated = " << generated << " points.\n";
}

// =========================
// 辅助函数（与现有脚本一致）
// =========================

uint64_t CountBufferedRecords(const BufferManager &bm) {
  uint64_t total = 0;
  for (int bi = 0; bi < 2; ++bi) {
    const Buffer &buf = bm.buffers[bi];
    for (uint32_t i = 0; i < buf.slot_capacity; ++i) {
      const Slot &s = buf.slots[i];
      total += s.hwm.load(std::memory_order_acquire);
    }
  }
  return total;
}

uint64_t CountTreeRecords(const SBTree &tree) {
  uint64_t total = 0;
  for (size_t i = 0; i < SBTree::kLeafCount; ++i) {
    const SBTreeLeaf *leaf = tree.leaf_at(i);
    if (!leaf) {
      continue;
    }
    total += leaf->size();
  }
  return total;
}

size_t GetMemoryUsage(const BufferManager &bm, const SBTree &tree) {
  size_t buffer_mem = 0;
  for (int bi = 0; bi < 2; ++bi) {
    const Buffer &buf = bm.buffers[bi];
    buffer_mem += buf.slot_capacity * SLOT_SIZE_BYTES;
  }
  size_t tree_mem = CountTreeRecords(tree) * sizeof(Record);
  return buffer_mem + tree_mem;
}

struct SlotStats {
  uint64_t used_slots = 0;
  uint64_t filled_records = 0;
};

SlotStats ComputeSlotStats(const BufferManager &bm) {
  SlotStats stats;
  for (int bi = 0; bi < 2; ++bi) {
    const Buffer &buf = bm.buffers[bi];
    for (uint32_t i = 0; i < buf.slot_capacity; ++i) {
      const Slot &s = buf.slots[i];
      uint16_t h = s.hwm.load(std::memory_order_acquire);
      if (h > 0) {
        ++stats.used_slots;
        stats.filled_records += h;
      }
    }
  }
  return stats;
}

// =========================
// 线程函数
// =========================

// Insert 线程：与现有脚本风格一致
void InsertThreadFunc(Engine *eng, double &elapsed_us,
                      uint64_t &inserted_count) {
  Timer timer;
  timer.Start();
  uint64_t local_count = 0;

  while (true) {
    uint32_t sid = g_insert_series_id.fetch_add(1, std::memory_order_relaxed);
    if (sid >= kNumSeries) {
      break;
    }
    const auto &vec = g_series[sid];
    for (const auto &p : vec) {
      uint64_t key = MakeKey(p.series, p.ts);
      eng->insert(key, p.value);
      ++local_count;
    }
  }

  eng->flush_thread_local();
  elapsed_us = timer.EndUs();
  inserted_count = local_count;
}

// Get 线程：点查询
void GetThreadFunc(Reader *reader, double &elapsed_us, size_t &probe_count,
                   size_t &found_count) {
  Timer timer;
  timer.Start();
  probe_count = 0;
  found_count = 0;

  while (true) {
    uint32_t sensor_id =
        g_get_sensor_id.fetch_add(1, std::memory_order_relaxed);
    if (sensor_id >= kNumSeries) {
      break;
    }
    const auto &tuples = g_series[sensor_id];
    if (tuples.empty())
      continue;

    // 每 10 个取 1 个，大约 10% 抽样
    for (size_t m = 0; m < tuples.size(); m += 10) {
      ++probe_count;
      // 使用专用的 lookup 方法做点查询
      uint64_t key = MakeKey(tuples[m].series, tuples[m].ts);
      Record r;
      if (reader->lookup(key, r)) {
        ++found_count;
      }
    }
  }

  elapsed_us = timer.EndUs();
}

// Scan 线程：范围查询
// Step 1: 预生成 Scan 查询，分离 RNG 开销
// 修复：确保 query 能命中数据，start_key 必须在数据范围内
void BuildScanQueries() {
  g_scan_queries.clear();
  g_scan_queries.reserve(kNumSeries * 5); // 每个 sensor 5 条

  std::mt19937_64 rng(123456);
  // 确保 start_idx 在有效范围内，且 count 不会超出序列长度
  std::uniform_int_distribution<size_t> dist_idx(0, kRecordsPerSensor - 1);
  std::uniform_int_distribution<size_t> dist_len(10, 100);

  for (SeriesId sid = 0; sid < kNumSeries; ++sid) {
    const auto &tuples = g_series[sid];
    if (tuples.empty())
      continue;

    for (int k = 0; k < 5; ++k) {
      size_t idx = dist_idx(rng);
      if (idx >= tuples.size()) {
        idx = tuples.size() - 1;
      }
      // 确保 start_key 在数据范围内
      uint64_t start_key = MakeKey(tuples[idx].series, tuples[idx].ts);
      size_t len = dist_len(rng);
      // 确保 count 不会超出序列剩余长度
      if (idx + len > tuples.size()) {
        len = tuples.size() - idx;
      }
      if (len == 0) {
        len = 1; // 至少扫描 1 条
      }
      g_scan_queries.push_back(ScanQuery{start_key, len});
    }
  }

  std::cout << "[ScanQuery] Generated " << g_scan_queries.size()
            << " scan queries (each should return 10-100 records)\n";
}

// 修改后的 ScanThreadFunc：只消费预生成的查询，不做 RNG
void ScanThreadFunc(Reader *reader, double &elapsed_us, size_t &scan_op_count,
                    size_t &total_returned, size_t begin, size_t end) {
  Timer timer;
  timer.Start();
  scan_op_count = 0;
  total_returned = 0;

  uint64_t local_sum = 0; // 非零工作，防止被优化掉

  for (size_t i = begin; i < end; ++i) {
    const auto &q = g_scan_queries[i];
    size_t got = reader->scan_n_visit(
        q.start_key, q.count, [&](uint64_t k, uint64_t v) { local_sum += v; });
    ++scan_op_count;
    total_returned += got;
  }

  elapsed_us = timer.EndUs();

  // 防止编译器优化掉 local_sum
  if (local_sum == 0xDEADBEEFULL) {
    fprintf(stderr, "impossible: %llu\n", (unsigned long long)local_sum);
  }
}

// Mixed 线程：50% insert, 30% long scan, 10% short scan, 10% lookup
void MixedThreadFunc(Engine *eng, Reader *reader, double &elapsed_us,
                     uint64_t &insert_ops, uint64_t &lookup_ops,
                     uint64_t &scan_ops, uint64_t ops_per_thread) {
  Timer timer;
  timer.Start();
  insert_ops = 0;
  lookup_ops = 0;
  scan_ops = 0;

  // 本地 buffer 复用，减少分配开销
  std::vector<Record> local_buf;
  local_buf.reserve(256); // 初始估计容量

  std::mt19937_64 rng(987654 +
                      std::hash<std::thread::id>{}(std::this_thread::get_id()));
  std::uniform_real_distribution<double> prob(0.0, 1.0);
  std::uniform_int_distribution<uint32_t> sensor_dist(0, kNumSeries - 1);
  std::uniform_int_distribution<size_t> idx_dist(0, kRecordsPerSensor - 1);
  std::uniform_int_distribution<size_t> long_len_dist(10, 100);
  std::uniform_int_distribution<size_t> short_len_dist(5, 10);

  // 继续插入的起始位置（从 kTotalRecords 之后开始）
  uint64_t insert_pos = kTotalRecords;

  for (uint64_t i = 0; i < ops_per_thread; ++i) {
    double u = prob(rng);

    if (u < 0.5) {
      // 50% Insert：继续往右写新数据
      uint32_t sensor_id = static_cast<uint32_t>(insert_pos % kNumSeries);
      uint64_t j = insert_pos / kNumSeries;
      uint64_t raw_ts = kStartTimestamp + j * kTimeStepMs;
      uint64_t key = MakeKey(sensor_id, raw_ts);
      eng->insert(key, raw_ts);
      ++insert_ops;
      ++insert_pos;
    } else if (u < 0.8) {
      // 30% long scan
      uint32_t sensor_id = sensor_dist(rng);
      const auto &tuples = g_series[sensor_id];
      if (!tuples.empty()) {
        size_t start_idx = idx_dist(rng);
        uint64_t start_key =
            MakeKey(tuples[start_idx].series, tuples[start_idx].ts);
        size_t len = long_len_dist(rng);
        uint64_t end_key = start_key + len * kTimeStepMs * kNumSeries;
        reader->range_query_into(start_key, end_key, local_buf);
      }
      ++scan_ops;
    } else if (u < 0.9) {
      // 10% short scan
      uint32_t sensor_id = sensor_dist(rng);
      const auto &tuples = g_series[sensor_id];
      if (!tuples.empty()) {
        size_t start_idx = idx_dist(rng);
        uint64_t start_key =
            MakeKey(tuples[start_idx].series, tuples[start_idx].ts);
        size_t len = short_len_dist(rng);
        uint64_t end_key = start_key + len * kTimeStepMs * kNumSeries;
        reader->range_query_into(start_key, end_key, local_buf);
      }
      ++scan_ops;
    } else {
      // 10% lookup
      uint32_t sensor_id = sensor_dist(rng);
      const auto &tuples = g_series[sensor_id];
      if (!tuples.empty()) {
        size_t idx = idx_dist(rng);
        uint64_t key = MakeKey(tuples[idx].series, tuples[idx].ts);
        Record r;
        (void)reader->lookup(key, r); // 使用专用的 lookup 方法
      }
      ++lookup_ops;
    }
  }

  eng->flush_thread_local();
  elapsed_us = timer.EndUs();
}

// =========================
// Workload 函数
// =========================

// Insert-only workload
void RunInsertOnly(uint32_t num_threads, int flip_interval_ms) {
  std::cout << "\n=== INSERT-ONLY workload ===" << std::endl;

  // 计算 slots_per_buffer
  uint32_t slots_per_buffer =
      static_cast<uint32_t>((kTotalRecords + SLOT_CAPACITY - 1) /
                            static_cast<uint64_t>(SLOT_CAPACITY)) +
      num_threads * 2;
  if (slots_per_buffer < 1024) {
    slots_per_buffer = 1024;
  }

  std::cout << "  series          : " << kNumSeries << "\n";
  std::cout << "  total records   : " << kTotalRecords << "\n";
  std::cout << "  slots/buffer    : " << slots_per_buffer << "\n";
  std::cout << "  threads         : " << num_threads << "\n";
  std::cout << "  flip interval   : " << flip_interval_ms << " ms\n";

  // 生成数据
  GenerateTimeSeriesData(kTotalRecords);

  // 初始化组件
  BufferManager bm(slots_per_buffer);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);
  Reader reader(&bm, &tree);

  // 启动后台 Flipper
  std::atomic<bool> keep_flipping{true};
  uint64_t merge_iterations = 0;
  uint64_t merge_time_us = 0;

  std::thread flipper([&]() {
    using clock = std::chrono::high_resolution_clock;
    auto next = clock::now();
    while (keep_flipping.load(std::memory_order_relaxed)) {
      next += std::chrono::milliseconds(flip_interval_ms);
      std::this_thread::sleep_until(next);

      auto m0 = clock::now();
      bm.flip_buffers();
      worker.run_once();
      auto m1 = clock::now();

      merge_time_us +=
          std::chrono::duration_cast<std::chrono::microseconds>(m1 - m0)
              .count();
      ++merge_iterations;
    }
  });

  // 插入线程
  g_insert_series_id.store(0, std::memory_order_relaxed);
  std::vector<std::thread> threads;
  std::vector<double> thread_times(num_threads, 0.0);
  std::vector<uint64_t> thread_inserts(num_threads, 0);
  threads.reserve(num_threads);

  Timer total_timer;
  total_timer.Start();
  for (uint32_t t = 0; t < num_threads; ++t) {
    threads.emplace_back(InsertThreadFunc, &eng, std::ref(thread_times[t]),
                         std::ref(thread_inserts[t]));
  }
  for (auto &th : threads) {
    th.join();
  }

  // 停止 flipper
  keep_flipping.store(false, std::memory_order_relaxed);
  if (flipper.joinable()) {
    flipper.join();
  }

  double total_time_us = total_timer.EndUs();
  double total_time_s = total_time_us / 1e6;

  uint64_t total_inserted = 0;
  double max_thread_ms = 0.0;
  double min_thread_ms = std::numeric_limits<double>::max();
  for (uint32_t t = 0; t < num_threads; ++t) {
    total_inserted += thread_inserts[t];
    double ms = thread_times[t] / 1000.0;
    max_thread_ms = std::max(max_thread_ms, ms);
    min_thread_ms = std::min(min_thread_ms, ms);
  }

  std::vector<double> sorted_times = thread_times;
  std::sort(sorted_times.begin(), sorted_times.end());
  double median_thread_us = sorted_times[sorted_times.size() / 2];
  double insert_wall_sec =
      (median_thread_us > 0.0) ? (median_thread_us / 1e6) : 0.0;

  // 修正：使用总 wall time 计算真实吞吐（包含 merge 时间）
  double real_throughput_mops =
      (total_time_s > 0.0)
          ? (static_cast<double>(total_inserted) / total_time_s / 1e6)
          : 0.0;

  // 保留 writer-only 吞吐作为参考（纯写入阶段峰值）
  double writer_only_throughput_mops =
      (insert_wall_sec > 0.0)
          ? (static_cast<double>(total_inserted) / insert_wall_sec / 1e6)
          : 0.0;

  uint64_t buffered_records = CountBufferedRecords(bm);
  uint64_t tree_records = CountTreeRecords(tree);
  uint64_t alloc_failures = bm.alloc_failures.load(std::memory_order_relaxed);

  // 检查数据完整性：alloc_failures 应该为 0
  if (alloc_failures > 0) {
    std::cerr << "  [WARNING] Alloc failures = " << alloc_failures
              << " - data may be lost!\n";
  }

  SlotStats slot_stats = ComputeSlotStats(bm);
  const double slot_capacity = static_cast<double>(SLOT_CAPACITY);
  const double buffer_capacity =
      static_cast<double>(2ull * slots_per_buffer * SLOT_CAPACITY);
  double buffer_waste_ratio =
      buffer_capacity > 0.0
          ? 1.0 - static_cast<double>(tree_records + buffered_records) /
                      buffer_capacity
          : 0.0;
  double slot_fill_ratio =
      slot_stats.used_slots > 0
          ? static_cast<double>(slot_stats.filled_records) /
                (static_cast<double>(slot_stats.used_slots) * slot_capacity)
          : 0.0;
  double merge_ratio =
      (total_time_us > 0.0) ? (merge_time_us / total_time_us) : 0.0;

  uint64_t tree_records_post = 0;

  std::cout << "Insert Finished!\n";
  std::cout << "  Total inserted records : " << total_inserted << "\n";
  std::cout << "  Total time             : " << total_time_us / 1000.0
            << " ms\n";
  std::cout << "  Real Throughput        : " << real_throughput_mops
            << " Mops/sec (wall time, includes merge)\n";
  std::cout << "  Writer-only Throughput : " << writer_only_throughput_mops
            << " Mops/sec (median thread time, write peak)\n";
  std::cout << "  Per-thread time range  : [" << min_thread_ms << " ms, "
            << max_thread_ms << " ms]\n";
  std::cout << "  Memory Usage           : "
            << GetMemoryUsage(bm, tree) / 1024.0 / 1024.0 << " MB\n";
  std::cout << "  Buffered records       : " << buffered_records << "\n";
  std::cout << "  Tree records           : " << tree_records << "\n";
  std::cout << "  Alloc failures         : " << alloc_failures << "\n";
  std::cout << "  Buffer Waste Ratio     : " << buffer_waste_ratio * 100.0
            << " %\n";
  std::cout << "  Slot Fill Ratio        : " << slot_fill_ratio * 100.0
            << " %\n";

  double merge_ms = merge_time_us / 1000.0;
  std::cout << "  Merge iterations       : " << merge_iterations << "\n";
  std::cout << "  Merge total time       : " << merge_ms << " ms\n";
  std::cout << "  Merge CPU Ratio        : " << merge_ratio * 100.0 << " %\n";

  flush_all_and_merge_once(&bm, &tree);
  tree_records_post = CountTreeRecords(tree);
  std::cout << "  Tree records (post-flush) : " << tree_records_post << "\n";
}

// Lookup-only workload
void RunLookupOnly(uint32_t num_threads) {
  std::cout << "\n=== LOOKUP-ONLY workload ===" << std::endl;

  // 先建索引
  uint32_t slots_per_buffer =
      static_cast<uint32_t>((kTotalRecords + SLOT_CAPACITY - 1) /
                            static_cast<uint64_t>(SLOT_CAPACITY)) +
      num_threads * 2;
  if (slots_per_buffer < 1024) {
    slots_per_buffer = 1024;
  }

  GenerateTimeSeriesData(kTotalRecords);

  BufferManager bm(slots_per_buffer);
  SBTree tree;
  Engine eng(&bm, &tree);

  // 构建索引
  std::cout << "[BuildIndex] Building index..." << std::endl;
  g_insert_series_id.store(0, std::memory_order_relaxed);
  std::vector<std::thread> build_threads;
  for (uint32_t t = 0; t < num_threads; ++t) {
    double dummy_time;
    uint64_t dummy_count;
    build_threads.emplace_back(InsertThreadFunc, &eng, std::ref(dummy_time),
                               std::ref(dummy_count));
  }
  for (auto &th : build_threads) {
    th.join();
  }
  flush_all_and_merge_once(&bm, &tree);
  uint64_t tree_records_after_build = CountTreeRecords(tree);
  std::cout << "[BuildIndex] Done. Tree records: " << tree_records_after_build
            << "\n"
            << std::endl;

  // Sanity check: 验证数据确实在 tree 里
  if (tree_records_after_build == 0) {
    std::cerr << "[ERROR] Tree is empty after build! Lookup test will fail.\n";
  } else {
    // 随机测试几个 key 确保能查到
    Reader test_reader(&tree);
    size_t sanity_checks = 0;
    size_t sanity_hits = 0;
    for (SeriesId sid = 0; sid < std::min(kNumSeries, 10U); ++sid) {
      const auto &tuples = g_series[sid];
      if (!tuples.empty()) {
        uint64_t test_key = MakeKey(tuples[0].series, tuples[0].ts);
        Record r;
        if (test_reader.lookup(test_key, r)) {
          ++sanity_hits;
        }
        ++sanity_checks;
      }
    }
    std::cout << "[SanityCheck] Tested " << sanity_checks << " keys, found "
              << sanity_hits << " (expected: " << sanity_checks << ")\n"
              << std::endl;
    if (sanity_hits == 0 && sanity_checks > 0) {
      std::cerr << "[WARNING] Sanity check failed - no keys found in tree!\n";
    }
  }

  // 使用 tree-only Reader（离线查询模式）
  Reader reader(&tree);

  // 查询测试
  g_get_sensor_id.store(0, std::memory_order_relaxed);
  std::vector<std::thread> threads;
  std::vector<double> thread_times(num_threads, 0.0);
  std::vector<size_t> thread_probes(num_threads, 0);
  std::vector<size_t> thread_found(num_threads, 0);

  Timer total_timer;
  total_timer.Start();
  for (uint32_t t = 0; t < num_threads; ++t) {
    threads.emplace_back(GetThreadFunc, &reader, std::ref(thread_times[t]),
                         std::ref(thread_probes[t]), std::ref(thread_found[t]));
  }
  for (auto &th : threads) {
    th.join();
  }

  double total_time_us = total_timer.EndUs();
  double total_time_s = total_time_us / 1e6;

  size_t total_probes = 0;
  size_t total_found = 0;
  for (uint32_t t = 0; t < num_threads; ++t) {
    total_probes += thread_probes[t];
    total_found += thread_found[t];
  }

  double lookup_throughput_mops =
      (total_time_s > 0.0)
          ? (static_cast<double>(total_probes) / total_time_s / 1e6)
          : 0.0;

  std::cout << "Lookup Finished!\n";
  std::cout << "  Total lookups issued   : " << total_probes << "\n";
  std::cout << "  Total hits             : " << total_found << "\n";
  std::cout << "  Hit rate               : "
            << (total_probes ? (100.0 * total_found / total_probes) : 0.0)
            << " %\n";
  std::cout << "  Total time             : " << total_time_us / 1000.0
            << " ms\n";
  std::cout << "  Throughput             : " << lookup_throughput_mops
            << " Mops/sec\n";
}

// Scan-only workload
void RunScanOnly(uint32_t num_threads) {
  std::cout << "\n=== SCAN-ONLY workload ===" << std::endl;

  // 先建索引
  uint32_t slots_per_buffer =
      static_cast<uint32_t>((kTotalRecords + SLOT_CAPACITY - 1) /
                            static_cast<uint64_t>(SLOT_CAPACITY)) +
      num_threads * 2;
  if (slots_per_buffer < 1024) {
    slots_per_buffer = 1024;
  }

  GenerateTimeSeriesData(kTotalRecords);

  BufferManager bm(slots_per_buffer);
  SBTree tree;
  Engine eng(&bm, &tree);

  // 构建索引
  std::cout << "[BuildIndex] Building index..." << std::endl;
  g_insert_series_id.store(0, std::memory_order_relaxed);
  std::vector<std::thread> build_threads;
  for (uint32_t t = 0; t < num_threads; ++t) {
    double dummy_time;
    uint64_t dummy_count;
    build_threads.emplace_back(InsertThreadFunc, &eng, std::ref(dummy_time),
                               std::ref(dummy_count));
  }
  for (auto &th : build_threads) {
    th.join();
  }
  flush_all_and_merge_once(&bm, &tree);
  uint64_t tree_records_after_build = CountTreeRecords(tree);
  std::cout << "[BuildIndex] Done. Tree records: " << tree_records_after_build
            << "\n"
            << std::endl;

  // 使用 tree-only Reader（离线查询模式）
  Reader reader(&tree);

  // Step 1: 预生成查询，分离 RNG 开销
  std::cout << "[Scan] Building scan queries..." << std::endl;
  BuildScanQueries();

  // Sanity check: 测试几个 query 确保能返回数据
  if (!g_scan_queries.empty() && tree_records_after_build > 0) {
    size_t test_queries = std::min(g_scan_queries.size(), size_t(10));
    size_t test_returned = 0;
    for (size_t i = 0; i < test_queries; ++i) {
      const auto &q = g_scan_queries[i];
      size_t got = reader.scan_n_visit(q.start_key, q.count,
                                       [](uint64_t k, uint64_t v) {});
      test_returned += got;
    }
    std::cout << "[SanityCheck] Tested " << test_queries
              << " queries, returned " << test_returned
              << " records (expected: > 0)\n"
              << std::endl;
    if (test_returned == 0) {
      std::cerr << "[WARNING] Sanity check failed - scan queries returned no "
                   "records!\n";
    }
  }

  std::cout << "[Scan] Generated " << g_scan_queries.size()
            << " scan queries.\n"
            << std::endl;

  // 扫描测试
  std::vector<std::thread> threads;
  std::vector<double> thread_times(num_threads, 0.0);
  std::vector<size_t> thread_scan_ops(num_threads, 0);
  std::vector<size_t> thread_scan_results(num_threads, 0);

  size_t total_queries = g_scan_queries.size();
  size_t per_thread = (total_queries + num_threads - 1) / num_threads;

  Timer total_timer;
  total_timer.Start();
  for (uint32_t t = 0; t < num_threads; ++t) {
    size_t begin = t * per_thread;
    size_t end = std::min(begin + per_thread, total_queries);
    if (begin >= end)
      break;
    threads.emplace_back(ScanThreadFunc, &reader, std::ref(thread_times[t]),
                         std::ref(thread_scan_ops[t]),
                         std::ref(thread_scan_results[t]), begin, end);
  }
  for (auto &th : threads) {
    th.join();
  }

  double total_time_us = total_timer.EndUs();
  double total_time_s = total_time_us / 1e6;

  size_t total_scan_ops = 0;
  size_t total_scan_results = 0;
  for (uint32_t t = 0; t < num_threads; ++t) {
    total_scan_ops += thread_scan_ops[t];
    total_scan_results += thread_scan_results[t];
  }

  double scan_throughput_kops =
      (total_time_s > 0.0)
          ? (static_cast<double>(total_scan_ops) / total_time_s / 1e3)
          : 0.0;

  std::cout << "Scan Finished!\n";
  std::cout << "  Total scan ops         : " << total_scan_ops << "\n";
  std::cout << "  Total returned records : " << total_scan_results << "\n";
  std::cout << "  Avg records per scan   : "
            << (total_scan_ops
                    ? (double)total_scan_results / (double)total_scan_ops
                    : 0.0)
            << "\n";
  std::cout << "  Total time             : " << total_time_us / 1000.0
            << " ms\n";
  std::cout << "  Throughput             : " << scan_throughput_kops
            << " Kops/sec (scan operations)\n";
}

// Mixed workload
void RunMixed(uint32_t num_threads, int flip_interval_ms) {
  std::cout << "\n=== MIXED workload ===" << std::endl;

  // 先建一个初始 index
  uint32_t slots_per_buffer =
      static_cast<uint32_t>((kTotalRecords + SLOT_CAPACITY - 1) /
                            static_cast<uint64_t>(SLOT_CAPACITY)) +
      num_threads * 2;
  if (slots_per_buffer < 1024) {
    slots_per_buffer = 1024;
  }

  GenerateTimeSeriesData(kTotalRecords);

  BufferManager bm(slots_per_buffer);
  SBTree tree;
  Engine eng(&bm, &tree);
  Reader reader(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  std::cout << "[BuildIndex] Building initial index..." << std::endl;
  g_insert_series_id.store(0, std::memory_order_relaxed);
  std::vector<std::thread> build_threads;
  for (uint32_t t = 0; t < num_threads; ++t) {
    double dummy_time;
    uint64_t dummy_count;
    build_threads.emplace_back(InsertThreadFunc, &eng, std::ref(dummy_time),
                               std::ref(dummy_count));
  }
  for (auto &th : build_threads) {
    th.join();
  }
  flush_all_and_merge_once(&bm, &tree);
  std::cout << "[BuildIndex] Done.\n" << std::endl;

  // 启动后台 Flipper
  std::atomic<bool> keep_flipping{true};
  std::atomic<uint64_t> merge_iterations{0};
  std::atomic<uint64_t> merge_time_us{0};
  std::thread flipper([&]() {
    using clock = std::chrono::high_resolution_clock;
    auto next = clock::now();
    while (keep_flipping.load(std::memory_order_relaxed)) {
      next += std::chrono::milliseconds(flip_interval_ms);
      std::this_thread::sleep_until(next);
      auto m0 = clock::now();
      bm.flip_buffers();
      worker.run_once();
      auto m1 = clock::now();
      merge_time_us.fetch_add(
          std::chrono::duration_cast<std::chrono::microseconds>(m1 - m0)
              .count(),
          std::memory_order_relaxed);
      merge_iterations.fetch_add(1, std::memory_order_relaxed);
    }
  });

  // 混合测试
  const uint64_t total_ops = kTotalRecords;
  const uint64_t ops_per_thread = (total_ops + num_threads - 1) / num_threads;

  std::vector<std::thread> threads;
  std::vector<double> thread_times(num_threads, 0.0);
  std::vector<uint64_t> thread_insert_ops(num_threads, 0);
  std::vector<uint64_t> thread_lookup_ops(num_threads, 0);
  std::vector<uint64_t> thread_scan_ops(num_threads, 0);

  Timer total_timer;
  total_timer.Start();
  for (uint32_t t = 0; t < num_threads; ++t) {
    threads.emplace_back(
        MixedThreadFunc, &eng, &reader, std::ref(thread_times[t]),
        std::ref(thread_insert_ops[t]), std::ref(thread_lookup_ops[t]),
        std::ref(thread_scan_ops[t]), ops_per_thread);
  }
  for (auto &th : threads) {
    th.join();
  }

  // 停止 flipper
  keep_flipping.store(false, std::memory_order_relaxed);
  if (flipper.joinable()) {
    flipper.join();
  }

  double total_time_us = total_timer.EndUs();
  double total_time_s = total_time_us / 1e6;

  uint64_t total_insert_ops = 0;
  uint64_t total_lookup_ops = 0;
  uint64_t total_scan_ops = 0;
  for (uint32_t t = 0; t < num_threads; ++t) {
    total_insert_ops += thread_insert_ops[t];
    total_lookup_ops += thread_lookup_ops[t];
    total_scan_ops += thread_scan_ops[t];
  }

  uint64_t total_ops_done =
      total_insert_ops + total_lookup_ops + total_scan_ops;
  double throughput_mops =
      (total_time_s > 0.0)
          ? (static_cast<double>(total_ops_done) / total_time_s / 1e6)
          : 0.0;
  uint64_t buffered_records = CountBufferedRecords(bm);
  uint64_t tree_records = CountTreeRecords(tree);
  SlotStats slot_stats = ComputeSlotStats(bm);
  const double slot_capacity = static_cast<double>(SLOT_CAPACITY);
  const double buffer_capacity =
      static_cast<double>(2ull * slots_per_buffer * SLOT_CAPACITY);
  double buffer_waste_ratio =
      buffer_capacity > 0.0
          ? 1.0 - static_cast<double>(tree_records + buffered_records) /
                      buffer_capacity
          : 0.0;
  double slot_fill_ratio =
      slot_stats.used_slots > 0
          ? static_cast<double>(slot_stats.filled_records) /
                (static_cast<double>(slot_stats.used_slots) * slot_capacity)
          : 0.0;
  double merge_ratio = (total_time_us > 0.0)
                           ? (static_cast<double>(merge_time_us.load(
                                  std::memory_order_relaxed)) /
                              total_time_us)
                           : 0.0;

  std::cout << "Mixed Finished!\n";
  std::cout << "  Total ops              : " << total_ops_done << "\n";
  std::cout << "    Insert ops           : " << total_insert_ops << "\n";
  std::cout << "    Lookup ops           : " << total_lookup_ops << "\n";
  std::cout << "    Scan ops             : " << total_scan_ops << "\n";
  std::cout << "  Total time             : " << total_time_us / 1000.0
            << " ms\n";
  std::cout << "  Throughput             : " << throughput_mops
            << " Mops/sec\n";
  std::cout << "  Buffered records       : " << buffered_records << "\n";
  std::cout << "  Tree records           : " << tree_records << "\n";
  std::cout << "  Buffer Waste Ratio     : " << buffer_waste_ratio * 100.0
            << " %\n";
  std::cout << "  Slot Fill Ratio        : " << slot_fill_ratio * 100.0
            << " %\n";
  std::cout << "  Merge CPU Ratio        : " << merge_ratio * 100.0 << " %\n";

  flush_all_and_merge_once(&bm, &tree);
  uint64_t tree_records_post = CountTreeRecords(tree);
  std::cout << "  Tree records (post-flush) : " << tree_records_post << "\n";
}

// =========================
// 主函数
// =========================

int main(int argc, char *argv[]) {
  uint32_t num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0)
    num_threads = 4;
  if (argc >= 2) {
    uint32_t t = static_cast<uint32_t>(std::atoi(argv[1]));
    if (t > 0 && t <= 128) {
      num_threads = t;
    }
  }

  int flip_interval_ms = 20;
  if (argc >= 3) {
    int v = std::atoi(argv[2]);
    if (v > 0 && v <= 1000) {
      flip_interval_ms = v;
    }
  }

  WorkloadType wl = WorkloadType::InsertOnly;
  if (argc >= 4) {
    wl = ParseWorkload(argv[3]);
  }

  std::cout << "===== TSDB Unified Benchmark =====\n";
  std::cout << "Total records      : " << kTotalRecords << "\n";
  std::cout << "Records per sensor : " << kRecordsPerSensor << "\n";
  std::cout << "Sensors            : " << kNumSeries << "\n";
  std::cout << "Threads            : " << num_threads << "\n";
  std::cout << "Flip interval      : " << flip_interval_ms << " ms\n";

  switch (wl) {
  case WorkloadType::InsertOnly:
    RunInsertOnly(num_threads, flip_interval_ms);
    break;
  case WorkloadType::LookupOnly:
    RunLookupOnly(num_threads);
    break;
  case WorkloadType::ScanOnly:
    RunScanOnly(num_threads);
    break;
  case WorkloadType::Mixed:
    RunMixed(num_threads, flip_interval_ms);
    break;
  case WorkloadType::All:
    RunInsertOnly(num_threads, flip_interval_ms);
    RunLookupOnly(num_threads);
    RunScanOnly(num_threads);
    RunMixed(num_threads, flip_interval_ms);
    break;
  }

  std::cout << "\n===== TSDB Unified Benchmark Done =====\n";
  return 0;
}
