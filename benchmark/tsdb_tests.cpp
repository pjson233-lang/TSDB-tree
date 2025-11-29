#include "tsdb_core.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

namespace {

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

void TestSingleThreadFlushAll() {
  std::cout << "[TestSingleThreadFlushAll] start\n";
  BufferManager bm(/*slots_per_buffer=*/8);
  SBTree tree;
  Engine eng(&bm, &tree);

  constexpr int kN = 1000;
  for (int i = 0; i < kN; ++i) {
    eng.insert(i, i);
  }
  eng.flush_thread_local();

  flush_all_and_merge_once(&bm, &tree);

  uint64_t tree_records = CountTreeRecords(tree);
  uint64_t buffered_records = CountBufferedRecords(bm);
  assert(tree_records == static_cast<uint64_t>(kN));
  assert(buffered_records == 0);
  std::cout << "[TestSingleThreadFlushAll] passed\n";
}

void TestMultiThreadWithFlipper() {
  std::cout << "[TestMultiThreadWithFlipper] start\n";
  constexpr uint32_t kThreads = 4;
  constexpr uint32_t kSlotsPerBuffer = 1024;
  constexpr int kN = 5000;

  BufferManager bm(kSlotsPerBuffer);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  std::atomic<bool> keep_flipping{true};
  std::thread flipper([&]() {
    using clock = std::chrono::high_resolution_clock;
    auto next = clock::now();
    while (keep_flipping.load(std::memory_order_relaxed)) {
      next += std::chrono::milliseconds(5);
      std::this_thread::sleep_until(next);
      bm.flip_buffers();
      worker.run_once();
    }
  });

  auto writer = [&](int base) {
    for (int i = 0; i < kN; ++i) {
      eng.insert(base + i, base + i);
    }
    eng.flush_thread_local();
  };

  std::vector<std::thread> threads;
  for (uint32_t t = 0; t < kThreads; ++t) {
    threads.emplace_back(writer, static_cast<int>(t) * kN);
  }
  for (auto &th : threads) {
    th.join();
  }

  keep_flipping.store(false);
  if (flipper.joinable()) {
    flipper.join();
  }

  flush_all_and_merge_once(&bm, &tree);

  uint64_t tree_records = CountTreeRecords(tree);
  uint64_t buffered_records = CountBufferedRecords(bm);

  assert(tree_records == static_cast<uint64_t>(kN) * kThreads);
  assert(buffered_records == 0);
  std::cout << "[TestMultiThreadWithFlipper] passed\n";
}

void TestTailWritingSlotVisibility() {
  std::cout << "[TestTailWritingSlotVisibility] start\n";
  BufferManager bm(/*slots_per_buffer=*/4);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  std::atomic<bool> keep_flipping{true};
  std::thread flipper([&]() {
    using clock = std::chrono::high_resolution_clock;
    auto next = clock::now();
    while (keep_flipping.load(std::memory_order_relaxed)) {
      next += std::chrono::milliseconds(5);
      std::this_thread::sleep_until(next);
      bm.flip_buffers();
      worker.run_once();
    }
  });

  constexpr int kN = 500;
  for (int i = 0; i < kN; ++i) {
    eng.insert(i, i);
  }
  eng.flush_thread_local();

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  keep_flipping.store(false);
  if (flipper.joinable()) {
    flipper.join();
  }

  Reader reader(&bm, &tree);
  auto all = reader.scan_all();
  assert(all.size() == static_cast<size_t>(kN));
  std::cout << "[TestTailWritingSlotVisibility] passed\n";
}

void TestSnapshotAllCorrectness() {
  std::cout << "[TestSnapshotAllCorrectness] start\n";
  SBTreeLeaf leaf;

  // 创建多个乱序的 runs，模拟真实的 segments_ 场景
  // Run 1: [10, 20, 30]
  std::vector<Record> run1 = {{10, 10, 0}, {20, 20, 0}, {30, 30, 0}};
  leaf.merge_runs(run1);

  // Run 2: [5, 15, 25] - 与 run1 有重叠，乱序
  std::vector<Record> run2 = {{5, 5, 0}, {15, 15, 0}, {25, 25, 0}};
  leaf.merge_runs(run2);

  // Run 3: [35, 40] - 在 run1 之后
  std::vector<Record> run3 = {{35, 35, 0}, {40, 40, 0}};
  leaf.merge_runs(run3);

  // Run 4: [1, 2, 3] - 在 run2 之前
  std::vector<Record> run4 = {{1, 1, 0}, {2, 2, 0}, {3, 3, 0}};
  leaf.merge_runs(run4);

  // 调用 snapshot_all
  std::vector<Record> result;
  leaf.snapshot_all(result);

  // 验证结果是有序的
  assert(result.size() == 11); // 总共 11 条记录
  for (size_t i = 1; i < result.size(); ++i) {
    assert(result[i - 1].key <= result[i].key);
  }

  // 验证结果包含所有记录（通过 key 集合验证）
  std::vector<uint64_t> keys;
  for (const auto &r : result) {
    keys.push_back(r.key);
  }
  std::sort(keys.begin(), keys.end());
  std::vector<uint64_t> expected_keys = {1,  2,  3,  5,  10, 15,
                                         20, 25, 30, 35, 40};
  assert(keys == expected_keys);

  // 验证多次调用 snapshot_all 结果一致
  std::vector<Record> result2;
  leaf.snapshot_all(result2);
  assert(result2.size() == result.size());
  for (size_t i = 0; i < result.size(); ++i) {
    assert(result[i].key == result2[i].key);
    assert(result[i].value == result2[i].value);
  }

  std::cout << "[TestSnapshotAllCorrectness] passed\n";
}

void TestScanRangeCorrectness() {
  std::cout << "[TestScanRangeCorrectness] start\n";
  SBTreeLeaf leaf;

  // 创建多个乱序的 runs
  // Run 1: [10, 20, 30]
  std::vector<Record> run1 = {{10, 10, 0}, {20, 20, 0}, {30, 30, 0}};
  leaf.merge_runs(run1);

  // Run 2: [5, 15, 25] - 与 run1 有重叠，乱序
  std::vector<Record> run2 = {{5, 5, 0}, {15, 15, 0}, {25, 25, 0}};
  leaf.merge_runs(run2);

  // Run 3: [35, 40] - 在 run1 之后
  std::vector<Record> run3 = {{35, 35, 0}, {40, 40, 0}};
  leaf.merge_runs(run3);

  // Run 4: [1, 2, 3] - 在 run2 之前
  std::vector<Record> run4 = {{1, 1, 0}, {2, 2, 0}, {3, 3, 0}};
  leaf.merge_runs(run4);

  // 测试 1: 范围查询 [12, 28] - 应该返回 [15, 20, 25]
  std::vector<Record> result1;
  leaf.scan_range(12, 28, result1);
  assert(result1.size() == 3);
  assert(result1[0].key == 15);
  assert(result1[1].key == 20);
  assert(result1[2].key == 25);
  // 验证有序
  for (size_t i = 1; i < result1.size(); ++i) {
    assert(result1[i - 1].key <= result1[i].key);
  }

  // 测试 2: 范围查询 [1, 5] - 应该返回 [1, 2, 3, 5]
  std::vector<Record> result2;
  leaf.scan_range(1, 5, result2);
  assert(result2.size() == 4);
  std::vector<uint64_t> keys2;
  for (const auto &r : result2) {
    keys2.push_back(r.key);
  }
  std::sort(keys2.begin(), keys2.end());
  std::vector<uint64_t> expected2 = {1, 2, 3, 5};
  assert(keys2 == expected2);

  // 测试 3: 范围查询 [30, 40] - 应该返回 [30, 35, 40]
  std::vector<Record> result3;
  leaf.scan_range(30, 40, result3);
  assert(result3.size() == 3);
  assert(result3[0].key == 30);
  assert(result3[1].key == 35);
  assert(result3[2].key == 40);

  // 测试 4: 范围查询 [100, 200] - 应该返回空
  std::vector<Record> result4;
  leaf.scan_range(100, 200, result4);
  assert(result4.empty());

  // 测试 5: 范围查询 [0, 0] - 应该返回空（没有 key=0 的记录）
  std::vector<Record> result5;
  leaf.scan_range(0, 0, result5);
  assert(result5.empty());

  // 测试 6: 范围查询 [1, 1] - 应该返回 [1]
  std::vector<Record> result6;
  leaf.scan_range(1, 1, result6);
  assert(result6.size() == 1);
  assert(result6[0].key == 1);

  std::cout << "[TestScanRangeCorrectness] passed\n";
}

void TestRangeQueryInto() {
  std::cout << "[TestRangeQueryInto] start\n";
  BufferManager bm(/*slots_per_buffer=*/8);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  // 插入一些数据
  constexpr int kN = 1000;
  for (int i = 0; i < kN; ++i) {
    eng.insert(i, i);
  }
  eng.flush_thread_local();

  // 执行一次 merge
  bm.flip_buffers();
  worker.run_once();
  flush_all_and_merge_once(&bm, &tree);

  Reader reader(&bm, &tree);

  // 测试 1: 使用 range_query
  auto result1 = reader.range_query(100, 200);

  // 测试 2: 使用 range_query_into
  std::vector<Record> result2;
  reader.range_query_into(100, 200, result2);

  // 验证结果一致
  assert(result1.size() == result2.size());
  for (size_t i = 0; i < result1.size(); ++i) {
    assert(result1[i].key == result2[i].key);
    assert(result1[i].value == result2[i].value);
  }

  // 测试 3: 多次调用 range_query_into，验证 buffer 复用
  std::vector<Record> result3;
  result3.reserve(256); // 预分配容量
  reader.range_query_into(300, 400, result3);
  size_t cap1 = result3.capacity();

  result3.clear();
  reader.range_query_into(500, 600, result3);
  size_t cap2 = result3.capacity();

  // 验证 capacity 被保留（至少不减少）
  assert(cap2 >= cap1 || cap2 >= 256);

  // 验证结果正确
  assert(result3.size() == 101); // 500 到 600 共 101 条记录
  assert(result3[0].key == 500);
  assert(result3[100].key == 600);

  std::cout << "[TestRangeQueryInto] passed\n";
}

void TestLookupCorrectness() {
  std::cout << "[TestLookupCorrectness] start\n";
  BufferManager bm(/*slots_per_buffer=*/8);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  // 插入一些数据
  constexpr int kN = 1000;
  for (int i = 0; i < kN; ++i) {
    eng.insert(i, i);
  }
  eng.flush_thread_local();

  // 执行一次 merge
  bm.flip_buffers();
  worker.run_once();
  flush_all_and_merge_once(&bm, &tree);

  Reader reader(&bm, &tree);

  // 测试：验证 lookup 和 range_query 的结果一致性
  for (int i = 0; i < kN; ++i) {
    uint64_t key = static_cast<uint64_t>(i);

    // 旧路径：range_query
    auto via_range = reader.range_query(key, key);

    // 新路径：lookup
    Record via_lookup{};
    bool found = reader.lookup(key, via_lookup);

    // 验证结果一致
    if (!via_range.empty()) {
      assert(found);
      assert(via_range[0].key == via_lookup.key);
      assert(via_range[0].value == via_lookup.value);
    } else {
      assert(!found);
    }
  }

  // 测试不存在的 key
  Record not_found{};
  assert(!reader.lookup(99999, not_found));
  auto empty_range = reader.range_query(99999, 99999);
  assert(empty_range.empty());

  std::cout << "[TestLookupCorrectness] passed\n";
}

// Stage 2 Step 2.1: 测试 build_blocks_from_slot 处理乱序数据
void TestBuildBlocksFromSlotOutOfOrder() {
  std::cout << "[TestBuildBlocksFromSlotOutOfOrder] start\n";
  BufferManager bm(/*slots_per_buffer=*/8);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  // 插入乱序数据：先插入大 key，再插入小 key
  constexpr int kN = 500;
  for (int i = kN - 1; i >= 0; --i) {
    eng.insert(i, i);
  }
  eng.flush_thread_local();

  // 执行 merge
  bm.flip_buffers();
  worker.run_once();
  flush_all_and_merge_once(&bm, &tree);

  // 验证所有数据都在 tree 中且有序
  Reader reader(&bm, &tree);
  auto all = reader.scan_all();
  assert(all.size() == static_cast<size_t>(kN));

  // 验证有序
  for (size_t i = 1; i < all.size(); ++i) {
    assert(all[i - 1].key <= all[i].key);
  }

  // 验证完整性：每个 key 都存在且 value 正确
  std::vector<bool> found(kN, false);
  for (const auto &r : all) {
    assert(r.key < static_cast<uint64_t>(kN));
    assert(r.value == r.key);
    found[r.key] = true;
  }
  for (int i = 0; i < kN; ++i) {
    assert(found[i]);
  }

  std::cout << "[TestBuildBlocksFromSlotOutOfOrder] passed\n";
}

// Stage 2 Step 2.1: 测试多个 slot 的数据正确合并
void TestMultipleSlotsMerge() {
  std::cout << "[TestMultipleSlotsMerge] start\n";
  BufferManager bm(/*slots_per_buffer=*/16);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  // 插入数据，确保会使用多个 slot
  constexpr int kN = 2000;
  for (int i = 0; i < kN; ++i) {
    eng.insert(i, i);
  }
  eng.flush_thread_local();

  // 执行 merge
  bm.flip_buffers();
  worker.run_once();
  flush_all_and_merge_once(&bm, &tree);

  // 验证数据完整性
  uint64_t tree_records = CountTreeRecords(tree);
  assert(tree_records == static_cast<uint64_t>(kN));

  // 验证所有数据可查询
  Reader reader(&bm, &tree);
  for (int i = 0; i < kN; i += 100) {
    Record r;
    assert(reader.lookup(i, r));
    assert(r.key == static_cast<uint64_t>(i));
    assert(r.value == static_cast<uint64_t>(i));
  }

  std::cout << "[TestMultipleSlotsMerge] passed\n";
}

// Stage 2 Step 2.1: 测试跨 leaf 的数据路由
void TestCrossLeafRouting() {
  std::cout << "[TestCrossLeafRouting] start\n";
  BufferManager bm(/*slots_per_buffer=*/8);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  // 插入跨多个 leaf 的数据（利用 key 的高位做 leaf 分片）
  constexpr int kN = 1000;
  // 插入不同 series_id 的数据，确保跨 leaf
  for (int series = 0; series < 10; ++series) {
    for (int ts = 0; ts < 100; ++ts) {
      uint64_t key =
          (static_cast<uint64_t>(series) << 48) | static_cast<uint64_t>(ts);
      eng.insert(key, key);
    }
  }
  eng.flush_thread_local();

  // 执行 merge
  bm.flip_buffers();
  worker.run_once();
  flush_all_and_merge_once(&bm, &tree);

  // 验证数据完整性
  uint64_t tree_records = CountTreeRecords(tree);
  assert(tree_records == static_cast<uint64_t>(kN));

  // 验证跨 leaf 查询
  Reader reader(&bm, &tree);
  for (int series = 0; series < 10; ++series) {
    uint64_t key = (static_cast<uint64_t>(series) << 48) | 50ULL;
    Record r;
    assert(reader.lookup(key, r));
    assert(r.key == key);
    assert(r.value == key);
  }

  std::cout << "[TestCrossLeafRouting] passed\n";
}

// Stage 2 Step 2.1: 测试全局最大已合并 key 更新
void TestGlobalMaxMergedKey() {
  std::cout << "[TestGlobalMaxMergedKey] start\n";
  BufferManager bm(/*slots_per_buffer=*/8);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  // 重置全局 key
  g_tree_max_merged_key.store(0, std::memory_order_relaxed);

  // 插入数据
  constexpr int kN = 500;
  for (int i = 0; i < kN; ++i) {
    eng.insert(i, i);
  }
  eng.flush_thread_local();

  // 执行 merge
  bm.flip_buffers();
  worker.run_once();
  flush_all_and_merge_once(&bm, &tree);

  // 验证全局最大已合并 key 已更新
  uint64_t max_key = g_tree_max_merged_key.load(std::memory_order_acquire);
  assert(max_key >= static_cast<uint64_t>(kN - 1));

  // 再次插入更大 key 的数据
  for (int i = kN; i < kN * 2; ++i) {
    eng.insert(i, i);
  }
  eng.flush_thread_local();

  // 再次 merge
  bm.flip_buffers();
  worker.run_once();
  flush_all_and_merge_once(&bm, &tree);

  // 验证全局最大已合并 key 已更新
  uint64_t max_key2 = g_tree_max_merged_key.load(std::memory_order_acquire);
  assert(max_key2 >= static_cast<uint64_t>(kN * 2 - 1));
  assert(max_key2 > max_key);

  std::cout << "[TestGlobalMaxMergedKey] passed\n";
}

// Stage 2 Step 2.1: 测试大量数据场景
void TestLargeScaleInsert() {
  std::cout << "[TestLargeScaleInsert] start\n";
  BufferManager bm(/*slots_per_buffer=*/1024);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  // 插入大量数据
  constexpr int kN = 10000;
  for (int i = 0; i < kN; ++i) {
    eng.insert(i, i);
  }
  eng.flush_thread_local();

  // 执行 merge
  bm.flip_buffers();
  worker.run_once();
  flush_all_and_merge_once(&bm, &tree);

  // 验证数据完整性
  uint64_t tree_records = CountTreeRecords(tree);
  assert(tree_records == static_cast<uint64_t>(kN));

  // 验证随机查询
  Reader reader(&bm, &tree);
  std::vector<int> test_keys = {0, 100, 1000, 5000, 9999};
  for (int k : test_keys) {
    Record r;
    assert(reader.lookup(k, r));
    assert(r.key == static_cast<uint64_t>(k));
    assert(r.value == static_cast<uint64_t>(k));
  }

  // 验证范围查询
  std::vector<Record> range_result;
  reader.range_query_into(1000, 2000, range_result);
  assert(range_result.size() == 1001); // 1000 到 2000 共 1001 条
  assert(range_result[0].key == 1000);
  assert(range_result[1000].key == 2000);

  std::cout << "[TestLargeScaleInsert] passed\n";
}

// Stage 2 Step 2.1: 测试并发插入和 merge
void TestConcurrentInsertAndMerge() {
  std::cout << "[TestConcurrentInsertAndMerge] start\n";
  constexpr uint32_t kThreads = 4;
  constexpr uint32_t kSlotsPerBuffer = 512;
  constexpr int kRecordsPerThread = 2000;

  BufferManager bm(kSlotsPerBuffer);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);

  std::atomic<bool> keep_flipping{true};
  std::thread flipper([&]() {
    using clock = std::chrono::high_resolution_clock;
    auto next = clock::now();
    while (keep_flipping.load(std::memory_order_relaxed)) {
      next += std::chrono::milliseconds(10);
      std::this_thread::sleep_until(next);
      bm.flip_buffers();
      worker.run_once();
    }
  });

  // 多线程插入
  std::vector<std::thread> threads;
  std::atomic<int> total_inserted{0};
  for (uint32_t t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t]() {
      int base = static_cast<int>(t) * kRecordsPerThread;
      for (int i = 0; i < kRecordsPerThread; ++i) {
        eng.insert(base + i, base + i);
        total_inserted.fetch_add(1, std::memory_order_relaxed);
      }
      eng.flush_thread_local();
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  keep_flipping.store(false);
  if (flipper.joinable()) {
    flipper.join();
  }

  // 最终 merge
  flush_all_and_merge_once(&bm, &tree);

  // 验证数据完整性
  uint64_t tree_records = CountTreeRecords(tree);
  assert(tree_records == static_cast<uint64_t>(kThreads * kRecordsPerThread));

  // 验证所有数据可查询
  Reader reader(&bm, &tree);
  for (uint32_t t = 0; t < kThreads; ++t) {
    int base = static_cast<int>(t) * kRecordsPerThread;
    for (int i = 0; i < kRecordsPerThread; i += 100) {
      Record r;
      assert(reader.lookup(base + i, r));
      assert(r.key == static_cast<uint64_t>(base + i));
      assert(r.value == static_cast<uint64_t>(base + i));
    }
  }

  std::cout << "[TestConcurrentInsertAndMerge] passed\n";
}

} // namespace

int main() {
  TestSingleThreadFlushAll();
  TestMultiThreadWithFlipper();
  TestTailWritingSlotVisibility();
  TestSnapshotAllCorrectness();
  TestScanRangeCorrectness();
  TestRangeQueryInto();
  TestLookupCorrectness();

  // Stage 2 Step 2.1: 新增正确性测试
  TestBuildBlocksFromSlotOutOfOrder();
  TestMultipleSlotsMerge();
  TestCrossLeafRouting();
  TestGlobalMaxMergedKey();
  TestLargeScaleInsert();
  TestConcurrentInsertAndMerge();

  std::cout << "All tsdb_core tests passed.\n";
  return 0;
}
