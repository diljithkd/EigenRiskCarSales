cmake_minimum_required(VERSION 3.10)
project(car_sales_project LANGUAGES CXX)

find_package(spdlog REQUIRED)

add_executable(car_sales_app
    src/main.cpp
    src/car_sales_processor.cpp
)

target_compile_features(car_sales_app PRIVATE cxx_std_17)

target_include_directories(car_sales_app PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(car_sales_app PRIVATE spdlog::spdlog)
