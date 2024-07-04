cmake_minimum_required(VERSION 3.13)
project(redis-starter-cpp)
set(CMAKE_CXX_STANDARD 20) # Enable the C++20 standard

set(SOURCE_FILES "src/Server.cpp")
set(CMAKE_CXX_FLAGS "-pthread")

add_executable(server ${SOURCE_FILES})