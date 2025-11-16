#include "tsdb_core.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

int main() {
  // Reasonable slots_per_buffer to avoid excessive memory (two buffers total
  // ~32MB).
  constexpr uint32_t kSlotsPerBuffer = 1024;
  constexpr int kNumThreads = 4;
  constexpr uint64_t kInsertsPerThread = 10000;

  BufferManager bm(kSlotsPerBuffer);
  SBTree tree;
  MergeWorker worker(&bm, &tree);

  auto writer_fn = [&](int tid) {
    Engine eng(&bm, &tree);
    const uint64_t base = static_cast<uint64_t>(tid) * 1'000'000'000ULL;
    for (uint64_t i = 0; i < kInsertsPerThread; ++i) {
      // Keys are unique across threads by adding a large per-thread base.
      eng.insert(base + i, static_cast<uint64_t>(tid));
      if ((i & 0x3FF) == 0) {
        std::this_thread::sleep_for(50us);
      }
    }
  };

  // Launch writers.
  std::vector<std::thread> writers;
  writers.reserve(kNumThreads);
  for (int t = 0; t < kNumThreads; ++t) {
    writers.emplace_back(writer_fn, t);
  }

  // Flip in a background thread controlled by an atomic flag.
  std::atomic<bool> keep_flipping{true};
  std::thread flipper([&]() {
    while (keep_flipping.load(std::memory_order_relaxed)) {
      bm.flip_buffers();
      // Run a merge cycle so the just-sealed buffer is consumed before
      // it is potentially reused by a later flip.
      worker.run_once();
      std::this_thread::sleep_for(5ms);
    }
  });

  for (auto &th : writers) {
    if (th.joinable()) {
      th.join();
    }
  }
  // Stop flipper and join it.
  keep_flipping.store(false, std::memory_order_relaxed);
  if (flipper.joinable()) {
    flipper.join();
  }

  // Final flip to seal the last active write buffer.
  bm.flip_buffers();

  // Ensure all non-empty slots are sealed so merge worker will consume them.
  for (int bi = 0; bi < 2; ++bi) {
    Buffer &buf = bm.buffers[bi];
    for (uint32_t i = 0; i < buf.slot_capacity; ++i) {
      Slot &s = buf.slots[i];
      if (s.hwm > 0) {
        s.state = SLOT_STATE_SEALED;
      }
    }
    // Ensure buffer is visible to merge worker.
    buf.state.store(BUFFER_STATE_SEALED, std::memory_order_release);
  }

  // After writers finished, run merge multiple times to consume all SEALED
  // buffers (including the final sealed buffer).
  for (int i = 0; i < 32; ++i) {
    worker.run_once();
    std::this_thread::sleep_for(1ms);
  }

  // Validate total records and ordering in the tree.
  const auto &data = tree.root_leaf()->data();
  const uint64_t expected =
      static_cast<uint64_t>(kNumThreads) * kInsertsPerThread;
  assert(data.size() == expected && "Mismatch in total merged records");

  // Data is globally sorted by key.
  for (size_t i = 1; i < data.size(); ++i) {
    assert(data[i - 1].key <= data[i].key);
  }

  std::cout << "concurrent_insert_flip_test passed. total=" << data.size()
            << std::endl;
  return 0;
}
