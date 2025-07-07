cmake_minimum_required(VERSION 3.14)

project(test_separator CXX C)

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

# 指定包含目录
include_directories(
    ${CMAKE_SOURCE_DIR})
message(STATUS "INCLUDE_DIRS: ${CMAKE_SOURCE_DIR}")

# 递归获取源文件
file(GLOB_RECURSE SOURCES
    ${CMAKE_SOURCE_DIR}/*.cc)

add_executable(test_separator ${SOURCES})

target_link_libraries(test_separator
        -lpthread)