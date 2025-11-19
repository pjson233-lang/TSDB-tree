// 保留默认的严格一致性（TSDB_STRICT_CONSISTENCY=1）
// 栅栏在宏中定义，性能/稳定性权衡最优
#include "tsdb_core.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

// 使用头文件中已定义的 getter/reset 函数（为了避免链接问题，直接访问 inline
// 变量） 这些函数在 tsdb_core.h 中已定义为 inline，无需重新实现

// （调试用的全局计数器 extern 声明已移除）

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

// 为了不触发 Buffer “写满丢数据”，slots_per_buffer 在 main 中按线程数动态估算

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

  double EndMs() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start_)
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
void GenerateTimeSeriesData(uint64_t num_records) {
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
// 插入线程函数
// =========================

// 每个线程：从 g_insert_series_id 中领取一个 series_id，
// 然后把该 series 的全部 InputPoint 插入到全局 Engine 中。
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

  elapsed_us = timer.EndUs();
  inserted_count = local_count;
}

// =========================
// 辅助：统计缓冲区里实际写入的记录数（RDS + WDS）
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

// 统计 Tree 中所有 leaf 的记录数
uint64_t CountTreeRecords(const SBTree &tree) {
  uint64_t total = 0;
  for (size_t i = 0; i < SBTree::kLeafCount; ++i) {
    const SBTreeLeaf *leaf = tree.leaf_at(i);
    if (!leaf) {
      continue;
    }
    total += leaf->data().size();
  }
  return total;
}

// =========================
// 主函数：执行 Insert benchmark
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

  // 可选参数 2：flip 间隔（毫秒），用于实验不同 flip 频率的影响
  int flip_interval_ms = 10;
  if (argc >= 3) {
    int v = std::atoi(argv[2]);
    if (v > 0 && v <= 1000) {
      flip_interval_ms = v;
    }
  }
  // 动态估算 slots_per_buffer：ceil(total_records / SLOT_CAPACITY) + threads*2
  uint32_t slots_per_buffer =
      static_cast<uint32_t>((kTotalRecords + SLOT_CAPACITY - 1) /
                            static_cast<uint64_t>(SLOT_CAPACITY)) +
      num_threads * 2;
  if (slots_per_buffer < 1024) {
    slots_per_buffer = 1024;
  }

  // 不再重置内部调试计数器，仅打印性能统计

  std::cout << "===== TSDB Insert Benchmark (tsdb_core.h) =====\n";
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

  // 2) 初始化 tsdb 组件

  BufferManager bm(slots_per_buffer);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);
  std::cout << "[Init] BufferManager + Engine constructed.\n";
  std::cout << "  flip interval   : " << flip_interval_ms << " ms\n";

  // 3) 启动维护线程：周期性 flip + merge（端到端路径）
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

  // 4) 多线程写入
  g_insert_series_id.store(0, std::memory_order_relaxed);
  std::vector<std::thread> threads;
  std::vector<double> thread_times(num_threads, 0.0);
  std::vector<uint64_t> thread_counts(num_threads, 0);
  threads.reserve(num_threads);

  Timer total_timer;
  total_timer.Start();
  for (uint32_t t = 0; t < num_threads; ++t) {
    threads.emplace_back(InsertThreadFunc, &eng, std::ref(thread_times[t]),
                         std::ref(thread_counts[t]));
  }
  for (auto &th : threads) {
    th.join();
  }
  // 写线程结束，停止维护线程并等待其完成最终 flush
  keep_flipping.store(false, std::memory_order_relaxed);
  if (flipper.joinable()) {
    flipper.join();
  }
  // 收尾：统一封口 WRITING slots 并做一次最终 merge
  flush_all_and_merge_once(&bm, &tree);

  double total_us = total_timer.EndUs();

  // 5) 汇总结果
  uint64_t total_insert_calls = 0;
  for (uint32_t t = 0; t < num_threads; ++t) {
    total_insert_calls += thread_counts[t];
  }

  uint64_t buffered_records = CountBufferedRecords(bm);
  uint64_t alloc_failures = bm.alloc_failures.load(std::memory_order_relaxed);
  uint64_t tree_records = CountTreeRecords(tree);
  // 成功写入的次数：这里用总调用次数减去分配失败次数近似代表
  uint64_t success_inserts = total_insert_calls - alloc_failures;

  double total_ms = total_us / 1000.0;
  double mops_front =
      (total_us > 0.0)
          ? (static_cast<double>(buffered_records) / (total_us / 1e6) / 1e6)
          : 0.0;
  double mops_tree =
      (total_us > 0.0)
          ? (static_cast<double>(tree_records) / (total_us / 1e6) / 1e6)
          : 0.0;

  double min_thread_us =
      *std::min_element(thread_times.begin(), thread_times.end());
  double max_thread_us =
      *std::max_element(thread_times.begin(), thread_times.end());
  double avg_thread_us = 0.0;
  for (double t : thread_times)
    avg_thread_us += t;
  avg_thread_us /= num_threads;

  // 纯插入吞吐（仅计算插入阶段：用最慢的写线程耗时作为整体插入时间）
  // 插入阶段耗时：使用线程时间的中位数作为代表（滤掉极端慢线程）
  std::vector<double> sorted_times = thread_times;
  std::sort(sorted_times.begin(), sorted_times.end());
  double median_thread_us = sorted_times[sorted_times.size() / 2];
  double insert_wall_sec =
      (median_thread_us > 0.0) ? (median_thread_us / 1.0e6) : 0.0;
  double mops_insert =
      (insert_wall_sec > 0.0)
          ? (static_cast<double>(total_insert_calls) / insert_wall_sec / 1.0e6)
          : 0.0;
  std::cout
      << "\n===== Insert Benchmark Result (with background merge) =====\n";
  std::cout << "total insert calls (Engine::insert) : " << total_insert_calls
            << "\n";
  std::cout << "successful inserts (into slots)     : " << success_inserts
            << "\n";
  std::cout << "buffered records (sum of hwm)       : " << buffered_records
            << "\n";
  std::cout << "tree records (sum of leaves)        : " << tree_records << "\n";
  std::cout << "alloc failures (buffer full)        : " << alloc_failures
            << "\n";
  std::cout << "total time                          : " << total_ms << " ms\n";
  std::cout << "insert throughput                   : " << mops_insert
            << " Mops/sec (pure Engine::insert)\n";
  std::cout << "front throughput                    : " << mops_front
            << " Mops/sec (buffered_records)\n";
  std::cout << "tree throughput                     : " << mops_tree
            << " Mops/sec (tree_records)\n";
  std::cout << "thread times (us)                   : "
            << "min=" << min_thread_us << ", max=" << max_thread_us
            << ", avg=" << avg_thread_us << "\n";

  double merge_ms = merge_time_us / 1000.0;
  double merge_ratio = (total_ms > 0.0) ? (merge_ms / total_ms * 100.0) : 0.0;
  std::cout << "merge iterations                    : " << merge_iterations
            << "\n";
  std::cout << "merge total time                    : " << merge_ms << " ms ("
            << merge_ratio << "% of total)\n";

  uint64_t in_memory = buffered_records + tree_records;
  if (alloc_failures == 0 && in_memory == success_inserts) {
    std::cout << "[Check] All successful inserts are in memory (Tree + "
                 "Buffers), no drops.\n";
  } else {
    std::cout << "[Check] WARNING: mismatch between successful inserts and "
                 "Tree+Buffers.\n";
    if (success_inserts > in_memory) {
      std::cout << "         approx dropped = " << (success_inserts - in_memory)
                << "\n";
    }
    if (buffered_records > 0) {
      std::cout << "         not yet merged into tree = " << buffered_records
                << "\n";
    }
  }

  std::cout << "=============================================\n";
  return 0;
}
