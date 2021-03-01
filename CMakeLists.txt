cmake_minimum_required(VERSION 3.17)
project(mm_measure)

set(CMAKE_CXX_STANDARD 14)

add_executable(dram_read_latency dram_read_latency.cpp)
add_executable(dram_throughput dram_throughput.cpp)

target_link_libraries(dram_throughput pthread)
