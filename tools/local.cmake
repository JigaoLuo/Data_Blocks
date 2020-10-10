# ---------------------------------------------------------------------------
# IMLAB
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

file(GLOB_RECURSE TOOLS_SRC "tools/*.cc")

# ---------------------------------------------------------------------------
# Executables
# ---------------------------------------------------------------------------

add_executable(imlabdb tools/imlabdb.cc)
target_link_libraries(imlabdb imlab gflags Threads::Threads)
# install(TARGETS imlabdb DESTINATION bin)
# commented by Jigao

# add_executable(buffer_test tools/buffer_test.cc)
# target_link_libraries(buffer_test imlab gflags Threads::Threads Boost::filesystem)

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_cpplint_target(lint_tools "${TOOLS_SRC}")
list(APPEND lint_targets lint_tools)

