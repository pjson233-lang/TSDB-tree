#include "tsdb_core.h"

#include <cassert>
#include <cstdint>
#include <iostream>

// Pipeline test: slot (unsorted) -> sort -> route -> leaf.merge_runs
int main() {
  BufferManager bm(/*slots_per_buffer=*/1);
  SBTree tree;
  MergeWorker worker(&bm, &tree);

  // Use the first slot in current write buffer.
  uint32_t w = bm.current_w_idx();
  Buffer &buf = bm.buffers[w];
  assert(buf.slot_capacity >= 1);
  Slot &s = buf.slots[0];

  // Fill unsorted data into the slot.
  const uint16_t n = 5;
  uint64_t keys[n] = {5, 1, 3, 4, 2};
  for (uint16_t i = 0; i < n; ++i) {
    s.recs[i].key = keys[i];
    s.recs[i].value = keys[i] * 10;
    s.recs[i].epoch = 0;
  }
  s.hwm = n;

  // Seal the buffer so worker.run_once() will process it.
  buf.state.store(BUFFER_STATE_SEALED, std::memory_order_release);

  worker.run_once();

  // Verify that data landed in the root leaf sorted by key.
  SBTreeLeaf *leaf = tree.root_leaf();
  const auto &data = leaf->data();
  assert(data.size() == n);
  for (size_t i = 0; i < data.size(); ++i) {
    if (i > 0) {
      assert(data[i].key >= data[i - 1].key);
    }
  }

  // Expect sorted keys: 1,2,3,4,5
  assert(data[0].key == 1);
  assert(data[1].key == 2);
  assert(data[2].key == 3);
  assert(data[3].key == 4);
  assert(data[4].key == 5);

  std::cout << "merge_basic_test passed." << std::endl;
  return 0;
}


