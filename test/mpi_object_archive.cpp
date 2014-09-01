#include "mpi_object_archive.hpp"

#include <gtest/gtest.h>

#include "test_mpi.hpp"

TEST(MPIObjectArchiveTest, Remove) {
  MPIObjectArchive<size_t> ar(world);
  world.barrier();

  ar.insert(world.rank(), world.rank()+5);
  world.barrier();

  if (world.rank() == 0)
    ar.remove(world.size()-1);
  else
    ar.remove(world.rank()-1);
  world.barrier();

  ar.mpi_process();
  world.barrier();

  EXPECT_FALSE(ar.is_available(world.rank()));

  world.barrier();
}

TEST(MPIObjectArchiveTest, InsertLoad) {
  MPIObjectArchive<size_t> ar(world);
  world.barrier();

  ar.insert(world.rank(), world.rank()+5);

  for (int i = 0; i < world.size(); i++) {
    if (i != world.rank()) {
      int val;
      ar.load(i, val);
      EXPECT_EQ(i+5, val);
    }
  }

  world.barrier();
}

TEST(MPIObjectArchiveTest, RecordEverything) {
  MPIObjectArchive<size_t>* ar = new MPIObjectArchive<size_t>(world,
      [](size_t const&, boost::mpi::communicator& world)
      { return world.rank() == 0; });
  world.barrier();

  if (world.rank() != 0)
    ar->insert(world.rank(), world.rank()+5);

  world.barrier();

  // Loops around to ensure there's enough time for processes to communicate
  for (int i = 0; i < 1000; i++)
    ar->mpi_process();
  world.barrier();

  if (world.rank() != 0)
    delete ar;
  world.barrier();

  if (world.rank() == 0)
    for (int i = 1; i < world.size(); i++) {
      int val;
      ar->load(i, val);
      EXPECT_EQ(i+5, val);
    }

  world.barrier();
}

TEST(MPIObjectArchiveTest, RecordEverythingFail) {
  MPIObjectArchive<size_t>* ar = new MPIObjectArchive<size_t>(world,
      [](size_t const&, boost::mpi::communicator& world)
      { return world.rank() == 0; });
  world.barrier();

  if (world.rank() != 0)
    ar->insert(world.rank(), world.rank()+5);
  world.barrier();

  ar->mpi_process();
  // Don't stop here and let the data source die, so we can check if we
  // correctly handle failed requests

  if (world.rank() != 0)
    delete ar;
  world.barrier();

  if (world.rank() == 0)
    for (int i = 1; i < world.size(); i++) {
      int val;
      ar->load(i, val);
      EXPECT_NE(i+5, val);
    }

  world.barrier();
}
