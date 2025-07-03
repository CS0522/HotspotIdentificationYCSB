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

# 是否位于服务器端运行
# cmake -DCHECK_ALIVE=ON
option(CHECK_ALIVE "check alive" ON)
if (CHECK_ALIVE)
    add_definitions(-DCHECK_ALIVE)
endif()
message(STATUS "*** CHECK_ALIVE: ${CHECK_ALIVE}")

find_package(Threads REQUIRED)

# find TBB
find_package(TBB REQUIRED)
message(STATUS "Using TBB ${TBB_VERSION}")

# find Protobuf installation
option(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

# find gRPC installation
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

if(CMAKE_CROSSCOMPILING)
  find_program(_PROTOBUF_PROTOC protoc)
else()
  set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()
if(CMAKE_CROSSCOMPILING)
  find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
  set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

# Proto file
get_filename_component(pf_proto "${CMAKE_SOURCE_DIR}/../rpc/proto/db.proto" ABSOLUTE)
get_filename_component(pf_proto_path "${pf_proto}" PATH)

# Generated sources
set(pf_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/db.pb.cc")
set(pf_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/db.pb.h")
set(pf_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/db.grpc.pb.cc")
set(pf_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/db.grpc.pb.h")
add_custom_command(
      OUTPUT "${pf_proto_srcs}" "${pf_proto_hdrs}" "${pf_grpc_srcs}" "${pf_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${pf_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${pf_proto}"
      DEPENDS "${pf_proto}")


# ycsb 根目录作为 include 起始路径
set(YCSB_INCLUDE_DIR
    ${CMAKE_SOURCE_DIR})
set(NOFDB_INCLUDE_DIRS
    ${CMAKE_SOURCE_DIR}/../rpc/include
    ${CMAKE_SOURCE_DIR}/../monitor/include
    ${CMAKE_SOURCE_DIR}/../rocksdb)

# 指定包含目录
include_directories(
    ${CMAKE_CURRENT_BINARY_DIR}
    ${ROCKSDB_INCLUDE_DIR}
    ${GRPC_INCLUDE_DIRS}
    ${NOFDB_INCLUDE_DIRS}
    ${YCSB_INCLUDE_DIR})
message(STATUS "INCLUDE_DIRS: ${CMAKE_CURRENT_BINARY_DIR} ${ROCKSDB_INCLUDE_DIR} ${GRPC_INCLUDE_DIRS} ${NOFDB_INCLUDE_DIRS} ${YCSB_INCLUDE_DIR}")

# 递归获取源文件
file(GLOB_RECURSE SOURCES
    ${CMAKE_SOURCE_DIR}/db/*.cc
    ${CMAKE_SOURCE_DIR}/lib/*.cc
    ${CMAKE_SOURCE_DIR}/core/*.cc)

add_executable(ycsb ${CMAKE_SOURCE_DIR}/ycsbc.cc 
                    ${SOURCES}
                    ${CMAKE_SOURCE_DIR}/../rpc/src/nofdb_client.cc
                    ${CMAKE_SOURCE_DIR}/../rpc/src/nofdb_connection.cc
                    ${CMAKE_SOURCE_DIR}/../monitor/src/nofdb_monitor.cc
                    ${pf_proto_srcs} ${pf_grpc_srcs})

target_link_libraries(ycsb
        TBB::tbb
        protobuf::libprotobuf
        gRPC::grpc++_reflection
        gRPC::grpc++
        -lpthread -ldl -lrt -lsnappy -lgflags -lz -lbz2 -llz4 -lzstd)