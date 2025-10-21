cmake_minimum_required(VERSION 4.0)

project(powersync)
find_package(CURL)

include(FetchContent)
FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz)
FetchContent_MakeAvailable(json)

add_custom_target(
    powersync_c_rust
    COMMAND cargo build -p libpowersync
)

add_library(powersync_c SHARED IMPORTED)
add_dependencies(powersync_c powersync_c_rust)
target_link_libraries(powersync_c INTERFACE CURL::libcurl)
set_target_properties(powersync_c PROPERTIES
    IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/target/debug/libpowersync_c.a"
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/libpowersync/include"
)

add_executable(cpp_queries)
target_sources(cpp_queries PRIVATE "examples/cpp_queries.cpp")
target_link_libraries(cpp_queries PRIVATE powersync_c nlohmann_json::nlohmann_json)
target_compile_features(cpp_queries PRIVATE cxx_std_17)
