#include "tsdb_core.h"

#include <cassert>
#include <cstdint>
#include <iostream>

// Basic correctness test for BufferManager + Engine::insert.
// Single-threaded:
//  - Inserts N records.
//  - Verifies that the total number of records stored in the active buffer
//    equals N and that the keys/values match the inserted pattern.
int main() {
  constexpr uint32_t kSlotsPerBuffer = 4;
  constexpr uint64_t kNumInserts = 1000;

  BufferManager bm(kSlotsPerBuffer);
  // Engine currently does not use SBTree on the write path, so we can pass
  // nullptr here.
  Engine eng(&bm, nullptr);

  for (uint64_t i = 0; i < kNumInserts; ++i) {
    eng.insert(i, i * 10);
  }

  // After all inserts, all data should reside in the current write buffer.
  uint32_t w = bm.current_w_idx();
  Buffer &buf = bm.buffers[w];

  uint64_t total_records = 0;
  uint64_t expected_key = 0;

  for (uint32_t si = 0; si < buf.slot_capacity; ++si) {
    Slot &s = buf.slots[si];
    // Each record should appear exactly once in insertion order within slots.
    for (uint16_t j = 0; j < s.hwm; ++j) {
      const Record &r = s.recs[j];
      assert(r.key == expected_key);
      assert(r.value == expected_key * 10);
      ++expected_key;
      ++total_records;
    }
  }

  assert(total_records == kNumInserts);
  std::cout << "basic_insert_test passed with " << total_records
            << " records." << std::endl;

  return 0;
}


