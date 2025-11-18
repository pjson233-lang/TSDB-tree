# CMake generated Testfile for 
# Source directory: /home/www/sbtree1/test
# Build directory: /home/www/sbtree1/test/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(merge_basic_test "/home/www/sbtree1/test/build/merge_basic_test")
set_tests_properties(merge_basic_test PROPERTIES  _BACKTRACE_TRIPLES "/home/www/sbtree1/test/CMakeLists.txt;17;add_test;/home/www/sbtree1/test/CMakeLists.txt;0;")
add_test(concurrent_insert_flip_test "/home/www/sbtree1/test/build/concurrent_insert_flip_test")
set_tests_properties(concurrent_insert_flip_test PROPERTIES  _BACKTRACE_TRIPLES "/home/www/sbtree1/test/CMakeLists.txt;23;add_test;/home/www/sbtree1/test/CMakeLists.txt;0;")
add_test(read_fresh_test "/home/www/sbtree1/test/build/read_fresh_test")
set_tests_properties(read_fresh_test PROPERTIES  _BACKTRACE_TRIPLES "/home/www/sbtree1/test/CMakeLists.txt;29;add_test;/home/www/sbtree1/test/CMakeLists.txt;0;")
add_test(flipper_test "/home/www/sbtree1/test/build/flipper_test")
set_tests_properties(flipper_test PROPERTIES  _BACKTRACE_TRIPLES "/home/www/sbtree1/test/CMakeLists.txt;35;add_test;/home/www/sbtree1/test/CMakeLists.txt;0;")
add_test(range_query_test "/home/www/sbtree1/test/build/range_query_test")
set_tests_properties(range_query_test PROPERTIES  _BACKTRACE_TRIPLES "/home/www/sbtree1/test/CMakeLists.txt;41;add_test;/home/www/sbtree1/test/CMakeLists.txt;0;")
add_test(perf_test "/home/www/sbtree1/test/build/perf_test" "1000000" "16")
set_tests_properties(perf_test PROPERTIES  _BACKTRACE_TRIPLES "/home/www/sbtree1/test/CMakeLists.txt;47;add_test;/home/www/sbtree1/test/CMakeLists.txt;0;")
