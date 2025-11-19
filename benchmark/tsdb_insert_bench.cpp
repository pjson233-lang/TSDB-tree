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
// 全局配置：模拟真实时序写入
// =========================

// 传感器数量（tag 数量）
static constexpr uint32_t kNumSeries = 256;

// 总记录数（所有传感器合计）
static constexpr uint64_t kTotalRecords = 2'000'000ULL;

// 每条记录的时间间隔（ms）
static constexpr uint64_t kTimeStepMs = 50;

// 每个传感器一条序列
static std::vector<InputPoint> g_series[kNumSeries];

// 多线程写入时用的原子分配器：按 sensor 维度分配 work
static std::atomic<uint32_t> g_insert_series_id{0};

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
// 插入线程函数（纯插入性能）
// =========================

// 每个线程：从 g_insert_series_id 中领取一个 series_id，
// 然后把该 series 的全部 InputPoint 插入到全局 Engine 中。
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
// 主函数：仅测纯 Engine::insert 吞吐
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

  // 动态估算 slots_per_buffer：与 tsdb_benchmark 保持一致
  uint32_t slots_per_buffer =
      static_cast<uint32_t>((kTotalRecords + SLOT_CAPACITY - 1) /
                            static_cast<uint64_t>(SLOT_CAPACITY)) +
      num_threads * 2;
  if (slots_per_buffer < 1024) {
    slots_per_buffer = 1024;
  }

  std::cout << "===== TSDB Insert-Only Benchmark =====\n";
  std::cout << "  series          : " << kNumSeries << "\n";
  std::cout << "  total records   : " << kTotalRecords << "\n";
  std::cout << "  time step (ms)  : " << kTimeStepMs << "\n";
  std::cout << "  slots/buffer    : " << slots_per_buffer
            << "  (capacity per buffer ≈ "
            << (static_cast<uint64_t>(slots_per_buffer) * SLOT_CAPACITY)
            << " records)\n";
  std::cout << "  threads         : " << num_threads << "\n";

  // 1) 生成时序数据
  GenerateTimeSeriesData(kTotalRecords);

  // 2) 初始化 tsdb 组件（没有 flipper / merge 线程）
  BufferManager bm(slots_per_buffer);
  SBTree tree;
  Engine eng(&bm, &tree);
  std::cout << "[Init] BufferManager + Engine constructed.\n";

  // 3) 多线程写入，仅测 Engine::insert 时间
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

  // 4) 汇总结果（纯插入吞吐：使用线程时间中位数）
  uint64_t total_insert_calls = 0;
  for (uint32_t t = 0; t < num_threads; ++t) {
    total_insert_calls += thread_counts[t];
  }

  std::vector<double> sorted_times = thread_times;
  std::sort(sorted_times.begin(), sorted_times.end());
  double median_thread_us = sorted_times[sorted_times.size() / 2];
  double insert_wall_sec =
      (median_thread_us > 0.0) ? (median_thread_us / 1.0e6) : 0.0;
  double mops_insert =
      (insert_wall_sec > 0.0)
          ? (static_cast<double>(total_insert_calls) / insert_wall_sec / 1.0e6)
          : 0.0;

  std::cout << "\n===== Insert-Only Benchmark Result =====\n";
  std::cout << "total insert calls (Engine::insert) : " << total_insert_calls
            << "\n";
  std::cout << "insert throughput                   : " << mops_insert
            << " Mops/sec (pure Engine::insert)\n";
  std::cout << "thread median time (us)             : " << median_thread_us
            << "\n";
  std::cout << "=============================================\n";

  return 0;
}


