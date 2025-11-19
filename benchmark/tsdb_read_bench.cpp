#include "tsdb_core.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

// =========================
// 基本类型定义
// =========================
using Timestamp = uint64_t;
using Value = uint64_t;
using SeriesId = uint32_t;

// 一条“输入数据”：某个传感器在某个时间点的一条记录
struct InputPoint {
  Timestamp ts;
  Value value;
  SeriesId series;
};

// =========================
// 全局配置：模拟真实时序写入（与 tsdb_benchmark 保持一致）
// =========================

// 传感器数量（tag 数量）
static constexpr uint32_t kNumSeries = 256;

// 总记录数（所有传感器合计）
static constexpr uint64_t kTotalRecords = 2'000'000ULL;

// 每条记录的时间间隔（ms）
static constexpr uint64_t kTimeStepMs = 50;

// 每个传感器一条序列
static std::vector<InputPoint> g_series[kNumSeries];

// =========================
// 简单计时器
// =========================

class Timer {
public:
  void Start() { start_ = std::chrono::high_resolution_clock::now(); }

  double EndUs() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start_)
        .count();
  }

private:
  std::chrono::high_resolution_clock::time_point start_;
};

// =========================
// 数据生成：模拟真实时序模式
// =========================

// 将 sensor_id + timestamp 编码到一个 64bit key 里：
//   [63..48] : series_id
//   [47.. 0] : timestamp (低 48 位)
inline uint64_t MakeKey(SeriesId sid, Timestamp ts) {
  constexpr uint64_t kLowMask = (1ULL << 48) - 1;
  return (static_cast<uint64_t>(sid) << 48) | (ts & kLowMask);
}

// 生成总共 num_records 条数据，按传感器拆成多条等长时间序列
static void GenerateTimeSeriesData(uint64_t num_records) {
  if (num_records == 0)
    return;
  const Timestamp start_ts = 1756684800000ULL; // 2025-09-01 00:00:00.000
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
      Timestamp ts = start_ts + j * kTimeStepMs;
      Value val = ts; // 为了方便验证，这里直接 value = ts
      vec.push_back(InputPoint{ts, val, sid});
    }
  }
  std::cout << "[Generator] actually generated = " << generated << " points.\n";
}

// =========================
// 插入线程函数：只负责构建数据集
// =========================

static std::atomic<uint32_t> g_insert_series_id{0};

static void InsertThreadFunc(Engine *eng, double &elapsed_us,
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

  elapsed_us = timer.EndUs();
  inserted_count = local_count;
}

// =========================
// 主函数：构建数据集 + 测试读路径
// =========================
//
// 用法：
//   ./tsdb_read_bench [scan|range] [threads_for_build]
//
// - 第一个参数：读测试类型
//     scan  : 只测试 Reader::scan_all()
//     range : 只测试 Reader::range_query()
//     both  : 两者都测（默认）
// - 第二个参数：用于构建数据集的写线程数（可选，默认 = 硬件并发或 4）
//
int main(int argc, char *argv[]) {
  std::string mode = "both";
  if (argc >= 2) {
    mode = argv[1];
  }

  uint32_t num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0)
    num_threads = 4;
  if (argc >= 3) {
    uint32_t t = static_cast<uint32_t>(std::atoi(argv[2]));
    if (t > 0 && t <= 128) {
      num_threads = t;
    }
  }

  // 与 tsdb_benchmark 相同的 slots_per_buffer 估算方式
  uint32_t slots_per_buffer =
      static_cast<uint32_t>((kTotalRecords + SLOT_CAPACITY - 1) /
                            static_cast<uint64_t>(SLOT_CAPACITY)) +
      num_threads * 2;
  if (slots_per_buffer < 1024) {
    slots_per_buffer = 1024;
  }

  std::cout << "===== TSDB Read Benchmark (tsdb_core.h) =====\n";
  std::cout << "  mode            : " << mode << "\n";
  std::cout << "  series          : " << kNumSeries << "\n";
  std::cout << "  total records   : " << kTotalRecords << "\n";
  std::cout << "  time step (ms)  : " << kTimeStepMs << "\n";
  std::cout << "  slots/buffer    : " << slots_per_buffer
            << "  (capacity per buffer ≈ "
            << (static_cast<uint64_t>(slots_per_buffer) * SLOT_CAPACITY)
            << " records)\n";
  std::cout << "  build threads   : " << num_threads << "\n";

  // 1) 生成时序数据
  GenerateTimeSeriesData(kTotalRecords);

  // 2) 初始化 tsdb 组件
  BufferManager bm(slots_per_buffer);
  SBTree tree;
  Engine eng(&bm, &tree);
  std::cout << "[Init] BufferManager + Engine constructed.\n";

  // 3) 多线程写入，只用于构建数据集
  g_insert_series_id.store(0, std::memory_order_relaxed);
  std::vector<std::thread> threads;
  std::vector<double> thread_times(num_threads, 0.0);
  std::vector<uint64_t> thread_counts(num_threads, 0);
  threads.reserve(num_threads);

  for (uint32_t t = 0; t < num_threads; ++t) {
    threads.emplace_back(InsertThreadFunc, &eng, std::ref(thread_times[t]),
                         std::ref(thread_counts[t]));
  }
  for (auto &th : threads) {
    th.join();
  }

  uint64_t total_insert_calls = 0;
  for (uint32_t t = 0; t < num_threads; ++t) {
    total_insert_calls += thread_counts[t];
  }

  std::cout << "[Build] total insert calls = " << total_insert_calls << "\n";

  // 4) 强制将所有数据 merge 到 Tree（读测试时不再有后台线程）
  flush_all_and_merge_once(&bm, &tree);

  Reader reader(&bm, &tree);

  // 统计 Tree 中记录数
  uint64_t tree_records = 0;
  for (size_t i = 0; i < SBTree::kLeafCount; ++i) {
    const SBTreeLeaf *leaf = tree.leaf_at(i);
    if (!leaf) {
      continue;
    }
    tree_records += leaf->data().size();
  }
  std::cout << "[Build] tree records          : " << tree_records << "\n";

  // ========= 测试 Reader::scan_all() =========
  if (mode == "both" || mode == "scan") {
    const int kScanIters = 5;
    std::cout << "\n[Read] scan_all() x " << kScanIters << " ..." << std::endl;
    Timer timer;
    timer.Start();
    uint64_t total_scanned = 0;
    for (int i = 0; i < kScanIters; ++i) {
      auto result = reader.scan_all();
      if (i == 0) {
        // 简单校验：第一次 scan 的条数应该等于 tree_records
        std::cout << "  scan_all() first result size : " << result.size()
                  << std::endl;
      }
      total_scanned += result.size();
    }
    double us = timer.EndUs();
    double sec = us / 1e6;
    double mrec_per_sec =
        (sec > 0.0) ? (static_cast<double>(total_scanned) / sec / 1e6) : 0.0;
    std::cout << "[Read] scan_all throughput    : " << mrec_per_sec
              << " M recs/sec (total scanned=" << total_scanned
              << ", time=" << us / 1000.0 << " ms)\n";
  }

  // ========= 测试 Reader::range_query() =========
  if (mode == "both" || mode == "range") {
    // 设计一个中等大小的时间窗口，比如 10,000 条记录左右。
    // 对于每个 series，时间单调递增，整体 key 分布也较平滑。
    const uint64_t range_len = 10'000;
    const int kRangeIters = 200;

    // 选择一段中间区间，避免边界效应
    uint64_t key_lo =
        MakeKey(/*sid=*/kNumSeries / 2, /*ts=*/1756684800000ULL + 500 * kTimeStepMs);
    uint64_t key_hi = key_lo + range_len;

    std::cout << "\n[Read] range_query() x " << kRangeIters << " ..."
              << std::endl;
    std::cout << "  query range key_lo=" << key_lo << ", key_hi=" << key_hi
              << std::endl;

    Timer timer;
    timer.Start();
    uint64_t total_found = 0;
    for (int i = 0; i < kRangeIters; ++i) {
      auto result = reader.range_query(key_lo, key_hi);
      if (i == 0) {
        std::cout << "  first range_query result size : " << result.size()
                  << std::endl;
      }
      total_found += result.size();
    }
    double us = timer.EndUs();
    double sec = us / 1e6;
    double qps =
        (sec > 0.0) ? (static_cast<double>(kRangeIters) / sec) : 0.0;
    std::cout << "[Read] range_query QPS        : " << qps
              << " queries/sec (total found=" << total_found
              << ", time=" << us / 1000.0 << " ms)\n";
  }

  std::cout << "\n===== TSDB Read Benchmark Done =====\n";
  return 0;
}


