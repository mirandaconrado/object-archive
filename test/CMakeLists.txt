if(ENABLE_THREADS)
  add_executable(run_tests_threads.bin EXCLUDE_FROM_ALL
    object_archive.cpp
    threads_object_archive.cpp
  )

  target_link_libraries(run_tests_threads.bin gtest gtest_main
    ${Boost_LIBRARIES}
    ${THREAD_LIB}
  )

  add_custom_target(test COMMAND run_tests_threads.bin
                         DEPENDS run_tests_threads.bin)
elseif(ENABLE_MPI)
  add_executable(run_tests_mpi.bin EXCLUDE_FROM_ALL
    mpi_object_archive.cpp
    object_archive.cpp
    test_mpi_main.cpp
  )

  target_link_libraries(run_tests_mpi.bin gtest
    mpi_handler
    ${Boost_LIBRARIES}
    ${MPI_LIB}
  )

  add_custom_target(test COMMAND mpirun -np 2 run_tests_mpi.bin
                         DEPENDS run_tests_mpi.bin)
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
