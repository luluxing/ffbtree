cmake_minimum_required(VERSION 3.3)

# Set the project name
project(deterministicperformance)

# Specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Set compile options for different build types
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# Set the build type to Release by default
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Include directories
include_directories(include)
include_directories(src)

# Add the executable
add_executable(bm_1d benchmark/one_dim.cpp)
add_executable(simulate_btree benchmark/simulate_btree_insert.cpp)
add_executable(bm_lru benchmark/benchmark_lru.cpp)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)


# Enable testing
enable_testing()
add_executable(
  test_simbtree
  tests/test_simbtree.cpp
)
target_link_libraries(
  test_simbtree
  GTest::gtest_main
)

add_executable(
  test_btree_disk
  tests/test_btree_disk.cpp
)
target_link_libraries(
  test_btree_disk
  GTest::gtest_main
)

add_executable(test_rtree tests/test_rtree.cpp include/rtree/rtree.cpp)
target_link_libraries(test_rtree GTest::gtest_main)

add_executable(test_blinktree tests/test_blinktree.cpp)
target_link_libraries(test_blinktree GTest::gtest_main)

add_executable(test_blinktree_disk tests/test_blinktree_disk.cpp)
target_link_libraries(test_blinktree_disk GTest::gtest_main)

add_executable(test_cctree tests/test_cctree.cpp)
target_link_libraries(test_cctree GTest::gtest_main)

add_executable(test_lru_cache tests/test_lru_cache.cpp)
target_link_libraries(test_lru_cache GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(test_simbtree)
gtest_discover_tests(test_btree_disk)
gtest_discover_tests(test_rtree)
gtest_discover_tests(test_lru_cache)