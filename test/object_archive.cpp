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
    ObjectArchive<size_t> ar(filename.string(), 0);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);
  EXPECT_EQ(0, fs.tellp());
}

TEST_F(ObjectArchiveTest, StringConstructor) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar(filename.string(), "0.05k");
    size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 2; val = "3";
    s2 = ar.insert(id, val);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);

  size_t total_size = 0;
  total_size += sizeof(size_t)*(1+2*2);
  total_size += s1+s2;
  total_size += ObjectArchive<size_t>::serialize_key(0).size();
  total_size += ObjectArchive<size_t>::serialize_key(2).size();
  EXPECT_EQ(total_size, fs.tellp());
}

TEST_F(ObjectArchiveTest, Insert) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 2; val = "3";
    s2 = ar.insert(id, val);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);

  size_t total_size = 0;
  total_size += sizeof(size_t)*(1+2*2);
  total_size += s1+s2;
  total_size += ObjectArchive<size_t>::serialize_key(0).size();
  total_size += ObjectArchive<size_t>::serialize_key(2).size();
  EXPECT_EQ(total_size, fs.tellp());
}

TEST_F(ObjectArchiveTest, InsertOverwrite) {
  size_t s1;
  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 0; val = "3";
    s1 = ar.insert(id, val);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);

  size_t total_size = 0;
  total_size += sizeof(size_t)*(1+1*2);
  total_size += s1;
  total_size += ObjectArchive<size_t>::serialize_key(0).size();
  EXPECT_EQ(total_size, fs.tellp());
}

TEST_F(ObjectArchiveTest, InsertOverwriteReopen) {
  size_t s1;
  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
  }

  {
    std::fstream fs(filename.string(),
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    fs.seekp(0, std::ios_base::end);

    size_t total_size = 0;
    total_size += sizeof(size_t)*(1+1*2);
    total_size += s1;
    total_size += ObjectArchive<size_t>::serialize_key(0).size();
    EXPECT_EQ(total_size, fs.tellp());
  }

  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
    std::string val;
    id = 0;
    EXPECT_EQ(s1, ar.load(id, val));
    EXPECT_EQ(std::string("1"), val);
    val = "3";
    s1 = ar.insert(id, val);
  }

  {
    std::fstream fs(filename.string(),
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    fs.seekp(0, std::ios_base::end);

    size_t total_size = 0;
    total_size += sizeof(size_t)*(1+1*2);
    total_size += s1;
    total_size += ObjectArchive<size_t>::serialize_key(0).size();
    EXPECT_EQ(total_size, fs.tellp());
  }

  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
    std::string val;
    id = 0;
    EXPECT_EQ(s1, ar.load(id, val));
    EXPECT_EQ(std::string("3"), val);
  }
}

TEST_F(ObjectArchiveTest, InsertSmallBuffer) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar(filename.string(), 50);
    size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 2; val = "3";
    s2 = ar.insert(id, val);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);

  size_t total_size = 0;
  total_size += sizeof(size_t)*(1+2*2);
  total_size += s1;
  total_size += s2;
  total_size += ObjectArchive<size_t>::serialize_key(0).size();
  total_size += ObjectArchive<size_t>::serialize_key(2).size();
  EXPECT_EQ(total_size, fs.tellp());
}

TEST_F(ObjectArchiveTest, InsertTooLarge) {
  size_t s1;
  {
    ObjectArchive<size_t> ar(filename.string(), 1);
    size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);

  size_t total_size = 0;
  total_size += sizeof(size_t)*(1+1*2);
  total_size += s1;
  total_size += ObjectArchive<size_t>::serialize_key(0).size();
  EXPECT_EQ(total_size, fs.tellp());
}

TEST_F(ObjectArchiveTest, Reopen) {
  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
    std::string val;
    id = 0; val = "1";
    ar.insert(id, val);
    id = 2; val = "3";
    ar.insert(id, val);
  }

  ObjectArchive<size_t> ar(filename.string(), 100);
  auto available = ar.available_objects();
  if (**available.begin() == 0)
    EXPECT_EQ(2, **++available.begin());
  else if (**available.begin() == 2)
    EXPECT_EQ(0, **++available.begin());
}

TEST_F(ObjectArchiveTest, Remove) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
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

    size_t total_size = 0;
    total_size += sizeof(size_t)*(1+2*2);
    total_size += s1;
    total_size += s2;
    total_size += ObjectArchive<size_t>::serialize_key(0).size();
    total_size += ObjectArchive<size_t>::serialize_key(2).size();
    EXPECT_EQ(total_size, fs.tellp());
  }

  {
    ObjectArchive<size_t> ar(filename.string(), 2);
    ar.remove(0);
  }

  {
    std::fstream fs(filename.string(),
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    fs.seekp(0, std::ios_base::end);

    size_t total_size = 0;
    total_size += sizeof(size_t)*(1+1*2);
    total_size += s2;
    total_size += ObjectArchive<size_t>::serialize_key(2).size();
    EXPECT_EQ(total_size, fs.tellp());
  }

  ObjectArchive<size_t> ar(filename.string(), 2);
  auto available = ar.available_objects();
  EXPECT_EQ(2, **available.begin());
}

TEST_F(ObjectArchiveTest, Load) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar(filename.string(), 50);
    size_t id;
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
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
    std::string val;
    id = 0;
    EXPECT_EQ(s1, ar.load(id, val));
    EXPECT_EQ(std::string("1"), val);
    id = 2;
    EXPECT_EQ(s2, ar.load(id, val));
    EXPECT_EQ(std::string("3"), val);
  }
}

TEST_F(ObjectArchiveTest, LoadTooLarge) {
  size_t s1;
  {
    ObjectArchive<size_t> ar(filename.string(), 50);
    size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
  }

  {
    ObjectArchive<size_t> ar(filename.string(), 1);
    size_t id;
    std::string val;
    id = 0;
    EXPECT_EQ(s1, ar.load(id, val));
    EXPECT_EQ(std::string("1"), val);
  }
}

TEST_F(ObjectArchiveTest, DontKeepInBuffer) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val, false);
    id = 2; val = "3";
    s2 = ar.insert(id, val, false);
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);

  size_t total_size = 0;
  total_size += sizeof(size_t)*(1+2*2);
  total_size += s1;
  total_size += s2;
  total_size += ObjectArchive<size_t>::serialize_key(0).size();
  total_size += ObjectArchive<size_t>::serialize_key(2).size();
  EXPECT_EQ(total_size, fs.tellp());

  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
    std::string val;
    id = 0;
    EXPECT_EQ(s1, ar.load(id, val, false));
    EXPECT_EQ(std::string("1"), val);
    id = 2;
    EXPECT_EQ(s2, ar.load(id, val, false));
    EXPECT_EQ(std::string("3"), val);
  }
}

TEST_F(ObjectArchiveTest, Flush) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
    id = 2; val = "3";
    s2 = ar.insert(id, val);

    ar.flush();

    id = 0;
    EXPECT_EQ(s1, ar.load(id, val));
    EXPECT_EQ(std::string("1"), val);
    id = 2;
    EXPECT_EQ(s2, ar.load(id, val));
    EXPECT_EQ(std::string("3"), val);
  }
}

TEST_F(ObjectArchiveTest, Clear) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    size_t id;
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

    size_t total_size = 0;
    total_size += sizeof(size_t)*(1+2*2);
    total_size += s1+s2;
    total_size += ObjectArchive<size_t>::serialize_key(0).size();
    total_size += ObjectArchive<size_t>::serialize_key(2).size();
    EXPECT_EQ(total_size, fs.tellp());
  }

  {
    ObjectArchive<size_t> ar(filename.string(), 100);
    ar.clear();
  }

  {
    std::fstream fs(filename.string(),
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    fs.seekp(0, std::ios_base::end);

    size_t total_size = sizeof(size_t);
    EXPECT_EQ(total_size, fs.tellp());
  }
}
