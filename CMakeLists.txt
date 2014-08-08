cmake_minimum_required(VERSION 2.6)
project(object-archive)

find_package(GTest REQUIRED)

if(ENABLE_THREADS)
  find_package(Boost 1.55.0 REQUIRED COMPONENTS filesystem iostreams serialization system thread)
  add_definitions(-DENABLE_THREADS)
  set(THREAD_LIB pthread)
elseif(ENABLE_MPI)
  find_package(Boost 1.55.0 REQUIRED COMPONENTS filesystem iostreams mpi serialization system)
  add_definitions(-DENABLE_MPI)
  set(MPI_LIB mpi mpi_cxx)
else()
  find_package(Boost 1.55.0 REQUIRED COMPONENTS filesystem iostreams serialization system)
endif()

set(CMAKE_CXX_COMPILER "clang++")
include_directories(include)
include_directories(${Boost_INCLUDE_DIRS})

set(CMAKE_CXX_FLAGS "-std=c++11 -Wall -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -ffunction-sections -fdata-sections")
set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gc-sections")

if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type selected, default to Release.")
  set(CMAKE_BUILD_TYPE "Release")
endif()

add_subdirectory(test)
