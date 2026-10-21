cmake_minimum_required(VERSION 3.16)
project(distributed_task_scheduler CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(-Wall -Wextra)

find_package(Threads REQUIRED)
find_path(PQ_INCLUDE_DIR libpq-fe.h PATH_SUFFIXES postgresql REQUIRED)
find_library(PQ_LIBRARY pq REQUIRED)
find_package(nlohmann_json 3 REQUIRED)
find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h REQUIRED)
find_library(HIREDIS_LIBRARY hiredis REQUIRED)
find_path(HTTPLIB_INCLUDE_DIR httplib.h REQUIRED)

add_library(dts_logic src/common/task_logic.cpp)
target_link_libraries(dts_logic PUBLIC nlohmann_json::nlohmann_json)

add_library(dts_common src/common/redis_client.cpp src/common/task_queue.cpp src/common/task_store.cpp)
target_include_directories(dts_common PUBLIC ${HIREDIS_INCLUDE_DIR} ${PQ_INCLUDE_DIR})
target_link_libraries(dts_common PUBLIC dts_logic ${HIREDIS_LIBRARY} ${PQ_LIBRARY} Threads::Threads)

add_executable(scheduler_api src/api/main.cpp)
target_include_directories(scheduler_api PRIVATE ${HTTPLIB_INCLUDE_DIR})
target_link_libraries(scheduler_api PRIVATE dts_common)

add_executable(scheduler_worker src/worker/main.cpp)
target_link_libraries(scheduler_worker PRIVATE dts_common)

add_executable(test_task_logic tests/test_task_logic.cpp)
target_link_libraries(test_task_logic PRIVATE dts_logic)

enable_testing()
add_test(NAME task_logic COMMAND test_task_logic)
