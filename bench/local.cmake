# ---------------------------------------------------------------------------
# IMLAB
# ---------------------------------------------------------------------------

add_executable(bm_foo ${CMAKE_SOURCE_DIR}/bench/bm_foo.cc)

target_link_libraries(
    bm_foo
    imlab
    benchmark
    gflags
    Threads::Threads)
    
add_executable(csv_generator_tpch ${CMAKE_SOURCE_DIR}/bench/csv_generator_tpch.cc)
target_link_libraries(
    csv_generator_tpch
    imlab
)

add_executable(csv_generator_flight ${CMAKE_SOURCE_DIR}/bench/csv_generator_flight.cc)
target_link_libraries(
    csv_generator_flight
    imlab
)