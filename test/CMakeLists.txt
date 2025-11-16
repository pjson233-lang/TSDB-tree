cmake_minimum_required(VERSION 3.16)

project(tsdb_sbtree_tests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)

enable_testing()

add_executable(merge_basic_test
  merge_basic_test.cpp
)

add_test(NAME merge_basic_test COMMAND merge_basic_test)

add_executable(concurrent_insert_flip_test
  concurrent_insert_flip_test.cpp
)

add_test(NAME concurrent_insert_flip_test COMMAND concurrent_insert_flip_test)

