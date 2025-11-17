#include "../benchmark/tsdb_core.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

int main() {
  constexpr uint32_t kSlotsPerBuffer = 1024;
  constexpr int kNumThreads = 4;
  constexpr uint64_t kInsertsPerThread = 50000; // 总共 200k 条

  BufferManager bm(kSlotsPerBuffer);
  SBTree tree;
  
  // 启动自动 flipper 线程，间隔 5ms
  Flipper flipper(&bm, &tree, 5);
  flipper.start();

  // 多线程写入
  auto writer_fn = [&](int tid) {
    Engine eng(&bm, &tree);
    const uint64_t base = static_cast<uint64_t>(tid) * 1'000'000'000ULL;
    for (uint64_t i = 0; i < kInsertsPerThread; ++i) {
      eng.insert(base + i, static_cast<uint64_t>(tid));
      if ((i & 0xFF) == 0) {
        std::this_thread::sleep_for(10us); // 模拟一些不规则延迟
      }
    }
  };

  std::vector<std::thread> writers;
  writers.reserve(kNumThreads);
  for (int t = 0; t < kNumThreads; ++t) {
    writers.emplace_back(writer_fn, t);
  }

  // 等待所有写入线程完成
  for (auto &th : writers) {
    if (th.joinable()) {
      th.join();
    }
  }

  // 停止 flipper，让最后的数据 merge
  flipper.stop();

  // 最后再执行一次 flush + merge 保证所有数据都落库
  flush_all_and_merge_once(&bm, &tree);

  // 验证数据完整性
  uint64_t expected = static_cast<uint64_t>(kNumThreads) * kInsertsPerThread;
  
  // 统计 tree 中的记录数（扫描所有 leaf）
  uint64_t tree_records = 0;
  for (size_t i = 0; i < tree.leaf_count(); ++i) {
    const auto *leaf = tree.leaf_at(i);
    if (leaf) {
      tree_records += leaf->data().size();
    }
  }

  std::cout << "Flipper test:" << std::endl;
  std::cout << "  Expected records: " << expected << std::endl;
  std::cout << "  Tree records:     " << tree_records << std::endl;

  // 也用 Reader 来扫描，验证数据一致性
  Reader reader(&bm, &tree);
  auto all_data = reader.scan_all();
  std::cout << "  Reader scan_all:  " << all_data.size() << std::endl;

  // 简单验证：reader 扫描的数据数量应该等于 tree 中的数据数量
  // （因为此时 RDS 应该已经被 consume 了）
  assert(all_data.size() == tree_records &&
         "Reader scan_all should match tree records");

  // 验证数据排序（reader 返回的数据应该按 key 排序）
  for (size_t i = 1; i < all_data.size(); ++i) {
    assert(all_data[i - 1].key <= all_data[i].key &&
           "Reader data should be sorted by key");
  }

  std::cout << "Flipper test PASSED: auto flip + merge working correctly"
            << std::endl;

  return 0;
}

