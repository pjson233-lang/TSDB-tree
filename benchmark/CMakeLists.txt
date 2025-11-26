cmake_minimum_required(VERSION 3.16)

project(tsdb_benchmark_proj LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)

add_executable(tsdb_unified_bench
  tsdb_unified_bench.cpp
)
target_link_libraries(tsdb_unified_bench PRIVATE pthread)

add_executable(tsdb_tests
  tsdb_tests.cpp
)
target_link_libraries(tsdb_tests PRIVATE pthread)
