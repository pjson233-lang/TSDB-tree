cmake_minimum_required(VERSION 3.16)

project(tsdb_tests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Tests should see the project root so they can include "tsdb_core.h".
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)

enable_testing()

add_executable(basic_insert_test
  basic_insert_test.cpp
)

add_test(NAME basic_insert_test COMMAND basic_insert_test)

add_executable(sort_slot_test
  sort_slot_test.cpp
)

add_test(NAME sort_slot_test COMMAND sort_slot_test)



