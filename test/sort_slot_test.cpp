#include "tsdb_core.h"

#include <cassert>
#include <cstdint>
#include <iostream>

// Basic correctness test for MergeWorker::sort_slot.
// Constructs a single slot, fills it with unsorted keys, runs sort_slot,
// and verifies the records are sorted by key.
int main() {
  constexpr uint32_t kSlotsPerBuffer = 1;

  BufferManager bm(kSlotsPerBuffer);
  MergeWorker worker(&bm);

  // Use a slot from the current write buffer.
  uint32_t w = bm.current_w_idx();
  Buffer &buf = bm.buffers[w];
  assert(buf.slot_capacity >= 1);

  Slot &s = buf.slots[0];

  // Prepare unsorted records.
  const uint16_t n = 5;
  assert(n <= SLOT_CAPACITY);

  uint64_t keys[n] = {5, 1, 3, 4, 2};

  for (uint16_t i = 0; i < n; ++i) {
    s.recs[i].key = keys[i];
    s.recs[i].value = keys[i] * 10;
    s.recs[i].epoch = 0;
  }
  s.hwm = n;

  worker.sort_slot(&s);

  // Verify sorted by key ascending.
  uint64_t prev_key = 0;
  for (uint16_t i = 0; i < n; ++i) {
    const Record &r = s.recs[i];
    if (i > 0) {
      assert(r.key >= prev_key);
    }
    prev_key = r.key;
  }

  std::cout << "sort_slot_test passed with " << n << " records." << std::endl;
  return 0;
}


