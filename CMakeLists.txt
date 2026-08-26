cmake_minimum_required(VERSION 3.16)
project(websocketclient CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost 1.71 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

add_library(websocketclient STATIC
    src/CLogger.cpp
    src/ISslContext.cpp
    src/CWebsocketClient.cpp
)
target_include_directories(websocketclient PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(websocketclient PUBLIC
    Boost::headers
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
)

option(WEBSOCKETCLIENT_BUILD_EXAMPLES "Build example programs" ON)
option(WEBSOCKETCLIENT_BUILD_TESTS "Build tests" ON)

if(WEBSOCKETCLIENT_BUILD_EXAMPLES)
    add_executable(echo_client examples/echo_client.cpp)
    target_link_libraries(echo_client PRIVATE websocketclient)
endif()

if(WEBSOCKETCLIENT_BUILD_TESTS)
    enable_testing()
    add_executable(websocketclient_test tests/websocketclient_test.cpp)
    target_link_libraries(websocketclient_test PRIVATE websocketclient)
    add_test(NAME websocketclient_test COMMAND websocketclient_test)
endif()

install(TARGETS websocketclient ARCHIVE DESTINATION lib)
install(DIRECTORY include/ DESTINATION include)
