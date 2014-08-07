if(ENABLE_THREADS)
  add_executable(run_tests_threads.bin EXCLUDE_FROM_ALL
    object_archive.cpp
    object_archive_threads.cpp
  )

  target_link_libraries(run_tests_threads.bin gtest gtest_main
    ${Boost_LIBRARIES}
    ${THREAD_LIB}
  )

  add_custom_target(test COMMAND run_tests_threads.bin
                         DEPENDS run_tests_threads.bin)
else()
  add_executable(run_tests.bin EXCLUDE_FROM_ALL
    object_archive.cpp
  )

  target_link_libraries(run_tests.bin gtest gtest_main
    ${Boost_LIBRARIES}
  )

  add_custom_target(test COMMAND run_tests.bin
                         DEPENDS run_tests.bin)
endif()
