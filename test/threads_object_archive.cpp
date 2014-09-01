#include "object_archive.hpp"

#include <gtest/gtest.h>

#include <boost/thread.hpp>

class ThreadsObjectArchiveTest: public ::testing::Test {
  protected:
    boost::filesystem::path filename;

    virtual void SetUp() {
      filename = boost::filesystem::temp_directory_path();
      filename += '/';
      filename += boost::filesystem::unique_path();
    }

    virtual void TearDown() {
      boost::filesystem::remove(filename);
    }
};

void worker(ObjectArchive<size_t>* ar, bool odd) {
  for (size_t i = 0; i < 1000; i++) {
    if ((i%2 == 0 && !odd) || (i%2 == 1 && odd)) {
      ar->insert(i, i);
    }
    else {
      while (!ar->is_available(i));
      size_t val;
      ar->load(i, val);
      EXPECT_EQ(i, val);
    }
  }
}

TEST_F(ThreadsObjectArchiveTest, InsertLoad) {
  ObjectArchive<size_t> ar;

  boost::thread t1(worker, &ar, true);
  boost::thread t2(worker, &ar, false);

  t1.join();
  t2.join();
}
