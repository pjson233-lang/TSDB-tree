#include "../benchmark/tsdb_core.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

// 简化版性能测试：专注测试 merge 性能，不涉及复杂的多线程写入

struct TestConfig {
  uint64_t num_records;
  uint32_t num_threads;
  uint32_t slots_per_buffer;
};

class SimpleInsertTest {
public:
  SimpleInsertTest(const TestConfig &cfg) : cfg_(cfg) {
    bm_ = std::make_unique<BufferManager>(cfg.slots_per_buffer);
    tree_ = std::make_unique<SBTree>();
    eng_ = std::make_unique<Engine>(bm_.get(), tree_.get());
  }

  // 简单的多线程插入，然后 flush + merge
  void run() {
    using clock = std::chrono::high_resolution_clock;
    auto t0 = clock::now();

    // 1. 多线程插入
    std::atomic<uint64_t> insert_idx{0};
    std::vector<std::thread> insert_threads;

    for (uint32_t t = 0; t < cfg_.num_threads; ++t) {
      insert_threads.emplace_back([this, &insert_idx]() {
        while (true) {
          uint64_t idx = insert_idx.fetch_add(1, std::memory_order_relaxed);
          if (idx >= cfg_.num_records) break;
          // key = idx + random lower bits for distribution
          uint64_t key = idx;
          uint64_t value = idx + 12345;
          eng_->insert(key, value);
        }
      });
    }

    for (auto &th : insert_threads) {
      th.join();
    }

    auto t1 = clock::now();
    double insert_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    // 2. Flush + Merge
    auto t2 = clock::now();
    flush_all_and_merge_once(bm_.get(), tree_.get());
    auto t3 = clock::now();
    double merge_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count();

    // 3. 验证
    uint64_t tree_records = 0;
    for (size_t i = 0; i < SBTree::kLeafCount; ++i) {
      const SBTreeLeaf *leaf = tree_->leaf_at(i);
      if (leaf) {
        tree_records += leaf->data().size();
      }
    }

    // 输出结果
    std::cout << "=== Performance Test Result ===\n";
    std::cout << "Config:\n";
    std::cout << "  records       : " << cfg_.num_records << "\n";
    std::cout << "  threads       : " << cfg_.num_threads << "\n";
    std::cout << "  slots/buffer  : " << cfg_.slots_per_buffer << "\n";
    std::cout << "\nResults:\n";
    std::cout << "  insert phase  : " << insert_ms << " ms\n";
    std::cout << "  flush+merge   : " << merge_ms << " ms\n";
    std::cout << "  total         : " << (insert_ms + merge_ms) << " ms\n";
    std::cout << "  tree records  : " << tree_records << "\n";
    std::cout << "  insert Mops/s : "
              << (cfg_.num_records / 1e6) / (insert_ms / 1000.0) << "\n";
    std::cout << "  merge % time  : "
              << (merge_ms / (insert_ms + merge_ms) * 100.0) << "%\n";
    std::cout << "==============================\n";

    if (tree_records == cfg_.num_records) {
      std::cout << "[Check] ✓ All records in tree.\n";
    } else {
      std::cout << "[Check] ✗ Missing records! Expected " << cfg_.num_records
                << ", got " << tree_records << "\n";
    }
  }

private:
  TestConfig cfg_;
  std::unique_ptr<BufferManager> bm_;
  std::unique_ptr<SBTree> tree_;
  std::unique_ptr<Engine> eng_;
};

int main(int argc, char *argv[]) {
  TestConfig cfg;
  cfg.num_records = 1'000'000; // 100万条记录
  cfg.num_threads = 16;
  cfg.slots_per_buffer = 2048;

  if (argc >= 2) {
    cfg.num_records = std::atoll(argv[1]);
  }
  if (argc >= 3) {
    cfg.num_threads = std::atoi(argv[2]);
  }

  SimpleInsertTest test(cfg);
  test.run();

  return 0;
}

