#include "object_archive.hpp"

#include <gtest/gtest.h>

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

TEST_F(ObjectArchiveTest, ChangeKey) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

    size_t old_id, new_id;
    std::string old_val, new_val;
    old_id = 0; old_val = "1";
    new_id = 2; new_val = "3";

    s1 = ar.insert(old_id, old_val);
    ar.change_key(old_id, new_id);
    s2 = ar.load(new_id, new_val);

    EXPECT_EQ(s1, s2);
    EXPECT_EQ(old_val, new_val);
  }
}

TEST_F(ObjectArchiveTest, Clear) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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
    total_size += ObjectArchive<size_t>::serialize((size_t)0).size();
    total_size += ObjectArchive<size_t>::serialize((size_t)2).size();
    EXPECT_EQ(total_size, fs.tellp());
  }

  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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

TEST_F(ObjectArchiveTest, DontKeepInBuffer) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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
  total_size += ObjectArchive<size_t>::serialize((size_t)0).size();
  total_size += ObjectArchive<size_t>::serialize((size_t)2).size();
  EXPECT_EQ(total_size, fs.tellp());

  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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

TEST_F(ObjectArchiveTest, Empty) {
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());

    EXPECT_EQ(0, ar.get_buffer_size());
    EXPECT_EQ(0, ar.get_max_buffer_size());
  }

  std::fstream fs(filename.string(),
      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  fs.seekp(0, std::ios_base::end);
  EXPECT_EQ(sizeof(size_t), fs.tellp());
}

TEST_F(ObjectArchiveTest, Flush) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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

TEST_F(ObjectArchiveTest, Insert) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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
  total_size += ObjectArchive<size_t>::serialize((size_t)0).size();
  total_size += ObjectArchive<size_t>::serialize((size_t)2).size();
  EXPECT_EQ(total_size, fs.tellp());
}

TEST_F(ObjectArchiveTest, InsertOverwrite) {
  size_t s1;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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
  total_size += ObjectArchive<size_t>::serialize((size_t)0).size();
  EXPECT_EQ(total_size, fs.tellp());
}

TEST_F(ObjectArchiveTest, InsertOverwriteReopen) {
  size_t s1;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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
    total_size += ObjectArchive<size_t>::serialize((size_t)0).size();
    EXPECT_EQ(total_size, fs.tellp());
  }

  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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
    total_size += ObjectArchive<size_t>::serialize((size_t)0).size();
    EXPECT_EQ(total_size, fs.tellp());
  }

  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(50);

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
  total_size += ObjectArchive<size_t>::serialize((size_t)0).size();
  total_size += ObjectArchive<size_t>::serialize((size_t)2).size();
  EXPECT_EQ(total_size, fs.tellp());
}

TEST_F(ObjectArchiveTest, InsertTooLarge) {
  size_t s1;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(1);

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
  total_size += ObjectArchive<size_t>::serialize((size_t)0).size();
  EXPECT_EQ(total_size, fs.tellp());
}

TEST_F(ObjectArchiveTest, IsAvailable) {
  ObjectArchive<size_t> ar;
  ar.init(filename.string());
  ar.set_buffer_size(100);

  size_t id;
  std::string val;
  id = 0; val = "1";
  ar.insert(id, val);

  EXPECT_TRUE(ar.is_available(id));
}

TEST_F(ObjectArchiveTest, Load) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(50);

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
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(50);

    size_t id;
    std::string val;
    id = 0; val = "1";
    s1 = ar.insert(id, val);
  }

  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(1);

    size_t id;
    std::string val;
    id = 0;
    EXPECT_EQ(s1, ar.load(id, val));
    EXPECT_EQ(std::string("1"), val);
  }
}

TEST_F(ObjectArchiveTest, Remove) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

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
    total_size += ObjectArchive<size_t>::serialize((size_t)0).size();
    total_size += ObjectArchive<size_t>::serialize((size_t)2).size();
    EXPECT_EQ(total_size, fs.tellp());
  }

  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(2);

    ar.remove(0);
  }

  {
    std::fstream fs(filename.string(),
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    fs.seekp(0, std::ios_base::end);

    size_t total_size = 0;
    total_size += sizeof(size_t)*(1+1*2);
    total_size += s2;
    total_size += ObjectArchive<size_t>::serialize((size_t)2).size();
    EXPECT_EQ(total_size, fs.tellp());
  }

  ObjectArchive<size_t> ar;
  ar.init(filename.string());
  ar.set_buffer_size(2);

  auto available = ar.available_objects();
  EXPECT_EQ(2, **available.begin());
}

TEST_F(ObjectArchiveTest, Reopen) {
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size(100);

    size_t id;
    std::string val;
    id = 0; val = "1";
    ar.insert(id, val);
    id = 2; val = "3";
    ar.insert(id, val);
  }

  ObjectArchive<size_t> ar;
  ar.init(filename.string());
  ar.set_buffer_size(100);

  auto available = ar.available_objects();
  if (**available.begin() == 0)
    EXPECT_EQ(2, **++available.begin());
  else if (**available.begin() == 2)
    EXPECT_EQ(0, **++available.begin());
}

TEST_F(ObjectArchiveTest, StringConstructor) {
  size_t s1, s2;
  {
    ObjectArchive<size_t> ar;
    ar.init(filename.string());
    ar.set_buffer_size("0.05k");
    EXPECT_EQ(0, ar.get_buffer_size());
    EXPECT_EQ(50, ar.get_max_buffer_size());

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
  total_size += ObjectArchive<size_t>::serialize((size_t)0).size();
  total_size += ObjectArchive<size_t>::serialize((size_t)2).size();
  EXPECT_EQ(total_size, fs.tellp());
}
