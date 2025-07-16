cmake_minimum_required(VERSION 3.14)

project(ycsb CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")

# DEBUG mode
# 一些信息内容输出
# cmake -DDEBUG=ON
option(DEBUG "debug" OFF)
if(DEBUG)
    add_definitions(-DDEBUG)
endif()
message(STATUS "*** DEBUG: ${DEBUG}")

find_package(Threads REQUIRED)

# find TBB
find_package(TBB REQUIRED)
message(STATUS "Using TBB ${TBB_VERSION}")

# ycsb 根目录作为 include 起始路径
set(YCSB_INCLUDE_DIR
    ${CMAKE_SOURCE_DIR})

# 指定包含目录
include_directories(
    ${CMAKE_CURRENT_BINARY_DIR}
    ${YCSB_INCLUDE_DIR})
message(STATUS "INCLUDE_DIRS: ${CMAKE_CURRENT_BINARY_DIR} ${YCSB_INCLUDE_DIR}")

# 递归获取源文件
file(GLOB_RECURSE SOURCES
    ${CMAKE_SOURCE_DIR}/db/*.cc
    ${CMAKE_SOURCE_DIR}/lib/*.cc
    ${CMAKE_SOURCE_DIR}/core/*.cc
    ${CMAKE_SOURCE_DIR}/modules/*.cc)
list(REMOVE_ITEM SOURCES
    "${CMAKE_SOURCE_DIR}/modules/test_separator.cc")

add_executable(ycsb ${CMAKE_SOURCE_DIR}/ycsbc.cc ${SOURCES})

target_link_libraries(ycsb
        TBB::tbb
        -lpthread)