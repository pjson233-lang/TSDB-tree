#include "tsdb_core.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>

// Validate that Reader::scan_all can see data from both Tree (merged) and
// RDS (sealed buffers that haven't been merged yet).
int main() {
  BufferManager bm(/*slots_per_buffer=*/64);
  SBTree tree;
  Engine eng(&bm, &tree);
  MergeWorker worker(&bm, &tree);
  Reader reader(&bm, &tree);

  // Phase 1: write a first batch, flip, and merge into Tree.
  const uint64_t batch1 = 100;
  for (uint64_t i = 0; i < batch1; ++i) {
    eng.insert(i, i * 10);
  }

  bm.flip_buffers();

  // Explicitly seal non-empty slots in the sealed buffer so MergeWorker
  // will consume them into the Tree.
  for (int bi = 0; bi < 2; ++bi) {
    Buffer &buf = bm.buffers[bi];
    if (buf.state.load(std::memory_order_acquire) != BUFFER_STATE_SEALED) {
      continue;
    }
    for (uint32_t si = 0; si < buf.slot_capacity; ++si) {
      Slot &s = buf.slots[si];
      if (s.hwm.load(std::memory_order_acquire) > 0) {
        s.state.store(SLOT_STATE_SEALED, std::memory_order_release);
      }
    }
  }

  // Merge first batch into the Tree.
  worker.run_once();

  // Phase 2: write a second batch, flip, but do NOT merge.
  const uint64_t batch2 = 100;
  for (uint64_t i = 0; i < batch2; ++i) {
    eng.insert(1000 + i, (1000 + i) * 10);
  }

  bm.flip_buffers();

  // For the purpose of this test, explicitly seal all non-empty slots in
  // sealed buffers so Reader can treat them as RDS.
  for (int bi = 0; bi < 2; ++bi) {
    Buffer &buf = bm.buffers[bi];
    if (buf.state.load(std::memory_order_acquire) != BUFFER_STATE_SEALED) {
      continue;
    }
    for (uint32_t si = 0; si < buf.slot_capacity; ++si) {
      Slot &s = buf.slots[si];
      if (s.hwm.load(std::memory_order_acquire) > 0) {
        s.state.store(SLOT_STATE_SEALED, std::memory_order_release);
      }
    }
  }

  // Reader should now see both Tree data (batch1) and RDS data (batch2).
  std::vector<Record> all = reader.scan_all();

  const uint64_t expected = batch1 + batch2;
  assert(all.size() == expected && "Reader did not see all records");

  // Verify ordering by key.
  for (size_t i = 1; i < all.size(); ++i) {
    assert(all[i - 1].key <= all[i].key);
  }

  std::cout << "read_fresh_test passed. total=" << all.size() << std::endl;
  return 0;
}
