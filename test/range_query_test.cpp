#include "../benchmark/tsdb_core.h"

#include <cassert>
#include <iostream>
#include <vector>

int main() {
  constexpr uint32_t kSlotsPerBuffer = 1024;

  BufferManager bm(kSlotsPerBuffer);
  SBTree tree;

  std::cout << "=== Range Query Tests ===" << std::endl;

  // ============ Test 1: Tree only (after flush+merge) ============
  std::cout << "\nTest 1: Range query on Tree only (after flush+merge)"
            << std::endl;
  {
    Engine eng(&bm, &tree);

    // Insert keys 0..999
    for (uint64_t k = 0; k < 1000; ++k) {
      eng.insert(k, k * 10);
    }

    // Flush and merge to Tree
    flush_all_and_merge_once(&bm, &tree);

    // Query [100, 199]
    Reader reader(&bm, &tree);
    auto result = reader.range_query(100, 199);

    std::cout << "  Inserted: 0..999 (1000 records)" << std::endl;
    std::cout << "  Query [100, 199]: " << result.size() << " records"
              << std::endl;

    assert(result.size() == 100 && "Expected 100 records in range [100, 199]");
    assert(result.front().key == 100 && "First record should have key=100");
    assert(result.back().key == 199 && "Last record should have key=199");

    std::cout << "  ✓ Test 1 passed!" << std::endl;
  }

  // ============ Test 2: Empty tree (no results) ============
  std::cout << "\nTest 2: Range query on empty tree" << std::endl;
  {
    SBTree tree2;
    BufferManager bm2(kSlotsPerBuffer);

    Reader reader(&bm2, &tree2);
    auto result = reader.range_query(100, 900);

    std::cout << "  Empty tree query [100, 900]: " << result.size()
              << " records" << std::endl;

    assert(result.size() == 0 && "Empty tree should return no results");

    std::cout << "  ✓ Test 2 passed!" << std::endl;
  }

  std::cout << "\n=== All range query tests passed! ===" << std::endl;
  return 0;
}
