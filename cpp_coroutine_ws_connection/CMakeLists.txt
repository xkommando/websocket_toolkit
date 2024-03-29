cmake_minimum_required(VERSION 3.10)

project(ws_toolkit VERSION 0.1 LANGUAGES CXX)

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_EXTENSIONS OFF)

set(BOOST_ROOT "/home/bc/lib/boost_183/")
set(BOOST_INCLUDE_DIRS "${BOOST_ROOT}/include")
set(BOOST_LIBRARY_DIRS "${BOOST_ROOT}/lib")
find_package(Boost 1.83.0 COMPONENTS unit_test_framework REQUIRED)

set(Boost_USE_STATIC_LIBS ON)
link_directories(${Boost_LIBRARY_DIRS})
include_directories(SYSTEM ${Boost_INCLUDE_DIR} )

set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL REQUIRED)

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fcoroutines -fconcepts -pthread -fexceptions ")
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -pedantic -Wno-format-y2k -Wshadow -Wcast-qual -Wno-parentheses -Wuninitialized -Wmaybe-uninitialized ")


if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Build debug version")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -O0 -Og -fno-inline -fno-omit-frame-pointer -D_FORTIFY_SOURCE=2 -fstack-protector-strong -D_GLIBCXX_ASSERTIONS")
    option(ENABLE_ASAN "Build with address sanitizer" OFF)
    
    if(ENABLE_ASAN)
        message(STATUS "Instrumenting with Address Sanitizer")
        set(CMAKE_BUILD_TYPE "Debug")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope ")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope")
    endif()

    option(ENABLE_ASAN "Build with thread sanitizer" OFF)
    if(ENABLE_ASAN)
        message(STATUS "Instrumenting with Thread Sanitizer")
        set(CMAKE_BUILD_TYPE "Debug")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=thread ")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread ")
    endif()

else()
    message(STATUS "Build release version")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -O4 -flto  -fno-stack-protector ")
endif()


add_subdirectory(include)

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif(BUILD_TESTS)
