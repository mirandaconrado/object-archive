/*
The MIT License (MIT)

Copyright (c) 2014 Conrado Miranda

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include "object_archive.hpp"

#include <gtest/gtest.h>

#include <boost/thread.hpp>

class ObjectArchiveThreadsTest: public ::testing::Test {
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

TEST_F(ObjectArchiveThreadsTest, InsertLoad) {
  ObjectArchive<size_t> ar;

  boost::thread t1(worker, &ar, true);
  boost::thread t2(worker, &ar, false);

  t1.join();
  t2.join();
}
