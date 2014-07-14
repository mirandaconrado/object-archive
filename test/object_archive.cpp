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

#include <boost/filesystem.hpp>

class ObjectArchiveTest: public ::testing::Test {
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

TEST_F(ObjectArchiveTest, Empty) {
  {
    ObjectArchive ar(filename.string(), 0);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);
  EXPECT_EQ(0, fs.tellp());
}

TEST_F(ObjectArchiveTest, StringConstructor) {
  {
    ObjectArchive ar(filename.string(), "0.001k");
    EXPECT_EQ(0, ar.insert(0, 0));
  }

  {
    std::fstream fs(filename.string(),
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    fs.seekp(0, std::ios_base::end);
    EXPECT_EQ(0, fs.tellp());
  }

  std::size_t s1, s2;
  {
    ObjectArchive ar(filename.string(), "0.05k");
    std::size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 2; val = "3";
    s2 = ar.insert(id, val);
  }

  {
    std::fstream fs(filename.string(),
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    fs.seekp(0, std::ios_base::end);
    EXPECT_EQ((1+2*3)*sizeof(std::size_t)+s1+s2, fs.tellp());
  }
}

TEST_F(ObjectArchiveTest, Insert) {
  std::size_t s1, s2;
  {
    ObjectArchive ar(filename.string(), 100);
    std::size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 2; val = "3";
    s2 = ar.insert(id, val);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);
  EXPECT_EQ((1+2*3)*sizeof(std::size_t)+s1+s2, fs.tellp());
}

TEST_F(ObjectArchiveTest, InsertOverwrite) {
  std::size_t s1;
  {
    ObjectArchive ar(filename.string(), 100);
    std::size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 0; val = "3";
    ar.insert(id, val);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);
  EXPECT_EQ((1+1*3)*sizeof(std::size_t)+s1, fs.tellp());
}

TEST_F(ObjectArchiveTest, InsertSmallBuffer) {
  std::size_t s1, s2;
  {
    ObjectArchive ar(filename.string(), 50);
    std::size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 2; val = "3";
    s2 = ar.insert(id, val);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);
  EXPECT_EQ((1+2*3)*sizeof(std::size_t)+s1+s2, fs.tellp());
}

TEST_F(ObjectArchiveTest, InvalidInsert) {
  {
    ObjectArchive ar(filename.string(), 1);
    std::size_t id;
    std::string val;
    id = 0; val = "1";
    EXPECT_EQ(0, ar.insert(id, val));
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);
  EXPECT_EQ(0, fs.tellp());
}

TEST_F(ObjectArchiveTest, Reopen) {
  {
    ObjectArchive ar(filename.string(), 100);
    std::size_t id;
    std::string val;
    id = 0; val = "1";
    ar.insert(id, val);
    id = 2; val = "3";
    ar.insert(id, val);
  }

  ObjectArchive ar(filename.string(), 100);
  auto available = ar.available_objects();
  if (*available.begin() == 0)
    EXPECT_EQ(2, *++available.begin());
  else if (*available.begin() == 2)
    EXPECT_EQ(0, *++available.begin());
}

TEST_F(ObjectArchiveTest, Remove) {
  std::size_t s1, s2;
  {
    ObjectArchive ar(filename.string(), 100);
    std::size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 2; val = "3";
    s2 = ar.insert(id, val);
  }

  {
    std::fstream fs(filename.string(),
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    fs.seekp(0, std::ios_base::end);
    EXPECT_EQ((1+2*3)*sizeof(std::size_t)+s1+s2, fs.tellp());
  }

  {
    ObjectArchive ar(filename.string(), 2);
    ar.remove(0);
  }

  {
    std::fstream fs(filename.string(),
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    fs.seekp(0, std::ios_base::end);
    EXPECT_EQ((1+1*3)*sizeof(std::size_t)+s2, fs.tellp());
  }

  ObjectArchive ar(filename.string(), 2);
  auto available = ar.available_objects();
  EXPECT_EQ(2, *available.begin());
}

TEST_F(ObjectArchiveTest, Load) {
  std::size_t s1, s2;
  {
    ObjectArchive ar(filename.string(), 50);
    std::size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 2; val = "3";
    s2 = ar.insert(id, val);

    id = 0;
    EXPECT_EQ(s1, ar.load(id, val));
    EXPECT_EQ(std::string("1"), val);
    id = 2;
    EXPECT_EQ(s1, ar.load(id, val));
    EXPECT_EQ(std::string("3"), val);
  }

  {
    ObjectArchive ar(filename.string(), 100);
    std::size_t id;
    std::string val;
    id = 0;
    EXPECT_EQ(s1, ar.load(id, val));
    EXPECT_EQ(std::string("1"), val);
    id = 2;
    EXPECT_EQ(s2, ar.load(id, val));
    EXPECT_EQ(std::string("3"), val);
  }
}

TEST_F(ObjectArchiveTest, InvalidLoad) {
  {
    ObjectArchive ar(filename.string(), 50);
    std::size_t id;
    std::string val;
    id = 0; val = "1";
    ar.insert(id, val);
    id = 2; val = "3";
    ar.insert(id, val);
  }

  {
    ObjectArchive ar(filename.string(), 1);
    std::size_t id;
    std::string val;
    id = 0;
    EXPECT_EQ(0, ar.load(id, val));
  }
}
