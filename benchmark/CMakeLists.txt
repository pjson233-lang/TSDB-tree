cmake_minimum_required(VERSION 3.16)

project(tsdb_benchmark_proj LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)

add_executable(tsdb_benchmark
  tsdb_benchmark.cpp
)

target_link_libraries(tsdb_benchmark PRIVATE pthread)

add_executable(tsdb_insert_bench
  tsdb_insert_bench.cpp
)

target_link_libraries(tsdb_insert_bench PRIVATE pthread)


