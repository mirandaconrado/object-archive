#include "test_mpi.hpp"

#include <gtest/gtest.h>

boost::mpi::environment env;
boost::mpi::communicator world;

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  if (world.rank() != 0) {
    // Places an empty listener to disable output
    testing::TestEventListeners& listeners =
      testing::UnitTest::GetInstance()->listeners();
    delete listeners.Release(listeners.default_result_printer());
    listeners.Append(new testing::EmptyTestEventListener);
  }

  return RUN_ALL_TESTS();
}
