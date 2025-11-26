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
    total += leaf->data().size();
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

} // namespace

int main() {
  TestSingleThreadFlushAll();
  TestMultiThreadWithFlipper();
  TestTailWritingSlotVisibility();
  std::cout << "All tsdb_core tests passed.\n";
  return 0;
}

