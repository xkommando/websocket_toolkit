

include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/contrib/)
include_directories(ws_toolkit ${PROJECT_SOURCE_DIR}/include)
add_executable(ws_toolkit_test 
				test_entrance.cpp
				test_blocking_queues.cpp
				test_websocket_client.cpp)

target_link_libraries(ws_toolkit_test   ws_toolkit
                                        OpenSSL::SSL
                                        OpenSSL::Crypto
                                        ${Boost_LIBRARIES})
