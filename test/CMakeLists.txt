add_executable(run_tests.bin EXCLUDE_FROM_ALL
  object_archive.cpp
)

target_link_libraries(run_tests.bin gtest gtest_main
  object_archive
)

add_custom_target(test COMMAND run_tests.bin
                       DEPENDS run_tests.bin)
