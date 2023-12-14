
file(GLOB ws_toolkit_src
        "${PROJECT_SOURCE_DIR}/include/*.h")

include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/contrib/)
include_directories(ws_toolkit ${PROJECT_SOURCE_DIR}/include)

add_library(ws_toolkit SHARED ${ws_toolkit_src})
target_compile_features(ws_toolkit PUBLIC cxx_std_20)
set_target_properties(ws_toolkit PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(ws_toolkit ${Boost_LIBRARIES}
                                OpenSSL::Crypto)
