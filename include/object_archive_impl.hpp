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

// Each archive has a entries with the following format:
// 1) Number of entries (size_t);
// 2.1) Size of the key (size_t);
// 2.2) Size of the object (size_t);
// 2.3) Key as serialized by boost;
// 2.4) Object as serialized by boost.

#ifndef __OBJECT_ARCHIVE_IMPL_HPP__
#define __OBJECT_ARCHIVE_IMPL_HPP__

#include "object_archive.hpp"

#include <algorithm>
#include <boost/filesystem.hpp>

template <class Key>
ObjectArchive<Key>::ObjectArchive():
  must_rebuild_file_(false),
  max_buffer_size_(0),
  buffer_size_(0) {
    init();
    set_buffer_size(0);
}

template <class Key>
ObjectArchive<Key>::~ObjectArchive() {
  internal_flush();
  stream_.close();
  if (temporary_file_)
    boost::filesystem::remove(filename_);
}

template <class Key>
std::string ObjectArchive<Key>::serialize_key(Key const& key) {
  std::stringstream stream;
  boost::archive::binary_oarchive ofs(stream);
  ofs << key;

  return stream.str();
}

template <class Key>
Key ObjectArchive<Key>::deserialize_key(std::string const& key_string) {
  std::stringstream stream(key_string);
  boost::archive::binary_iarchive ifs(stream);

  Key key;
  ifs >> key;

  return key;
}

template <class Key>
void ObjectArchive<Key>::init() {
  std::string filename;
  filename = boost::filesystem::temp_directory_path().string();
  filename += '/';
  filename += boost::filesystem::unique_path().string();

  init(filename);

  temporary_file_ = true;
}

template <class Key>
void ObjectArchive<Key>::init(std::string const& filename) {
  internal_flush();

  stream_.close();
  if (temporary_file_)
    boost::filesystem::remove(filename_);

  filename_ = filename;
  temporary_file_ = false;

  buffer_size_ = 0;
  objects_.clear();
  LRU_.clear();

  stream_.open(filename, std::ios_base::in | std::ios_base::out |
                         std::ios_base::binary);
  stream_.seekg(0, std::ios_base::end);

  // If the file seems ok and has entries, use it. Otherwise, overwrite.
  if (stream_.good() && stream_.tellg() > 0) {
    stream_.seekg(0);

    size_t n_entries;
    stream_.read((char*)&n_entries, sizeof(size_t));

    for (size_t i = 0; i < n_entries; i++) {
      size_t key_size;
      size_t data_size;
      stream_.read((char*)&key_size, sizeof(size_t));
      stream_.read((char*)&data_size, sizeof(size_t));

      std::string key_string;
      key_string.resize(key_size);
      stream_.read(&key_string[0], key_size);

      ObjectEntry entry;
      entry.key = deserialize_key(key_string);
      entry.index_in_file = stream_.tellg();
      entry.size = data_size;
      entry.modified = false;
      objects_[entry.key] = entry;

      stream_.seekg(data_size, std::ios_base::cur);
    }
  }
  else {
    stream_.open(filename, std::ios_base::in | std::ios_base::out |
        std::ios_base::binary | std::ios_base::trunc);
  }
}

template <class Key>
void ObjectArchive<Key>::set_buffer_size(size_t max_buffer_size) {
  unload(max_buffer_size);
  max_buffer_size_ = max_buffer_size;
}

template <class Key>
void ObjectArchive<Key>::set_buffer_size(std::string const& max_buffer_size) {
  size_t length = max_buffer_size.size();
  double buffer_size = atof(max_buffer_size.c_str());

  bool changed = false;
  for (size_t i = 0; i< length && !changed; i++) {
    switch (max_buffer_size[i]) {
      case 'k':
      case 'K':
        buffer_size *= 1e3;
        changed = true;
        break;
      case 'm':
      case 'M':
        buffer_size *= 1e6;
        changed = true;
        break;
      case 'g':
      case 'G':
        buffer_size *= 1e9;
        changed = true;
        break;
    }
  }

  set_buffer_size(buffer_size);
}

#if BOOST_OS_LINUX
#include <sys/sysinfo.h>

template <class Key>
void ObjectArchive<Key>::set_buffer_size_scale(float max_buffer_size) {
  struct sysinfo info;
  if (sysinfo(&info) == 0) {
    unsigned long freeram = info.freeram;
    set_buffer_size(freeram * max_buffer_size);
  }
}
#endif

template <class Key>
void ObjectArchive<Key>::remove(Key const& key) {
  auto it = objects_.find(key);
  if (it == objects_.end())
    return;

  ObjectEntry& entry = it->second;
  if (entry.data.size())
    buffer_size_ -= entry.size;
  objects_.erase(key);
  LRU_.remove(&entry);
  must_rebuild_file_ = true;
}

template <class Key>
template <class T>
size_t ObjectArchive<Key>::insert(Key const& key, T const& obj,
    bool keep_in_buffer) {
  std::stringstream stream;
  boost::archive::binary_oarchive ofs(stream);
  ofs << obj;
  return insert_raw(key, stream.str(), keep_in_buffer);
}

template <class Key>
size_t ObjectArchive<Key>::insert_raw(Key const& key, std::string const& data,
    bool keep_in_buffer) {
  return insert_raw(key, std::string(data), keep_in_buffer);
}

template <class Key>
size_t ObjectArchive<Key>::insert_raw(Key const& key, std::string&& data,
    bool keep_in_buffer) {
  size_t size = data.size();
  if (size > max_buffer_size_)
    keep_in_buffer = false;

  remove(key);

  if (size + buffer_size_ > max_buffer_size_ && keep_in_buffer)
    unload(max_buffer_size_ - size);

  buffer_size_ += size;

  ObjectEntry& entry = objects_[key];
  entry.key = key;
  entry.data.swap(data);
  entry.size = size;
  entry.modified = true;

  touch_LRU(&entry);

  if (!keep_in_buffer)
    write_back(key);

  return size;
}

template <class Key>
template <class T>
size_t ObjectArchive<Key>::load(Key const& key, T& obj, bool keep_in_buffer) {
  std::string s;
  size_t ret = load_raw(key, s, keep_in_buffer);
  if (ret == 0) return 0;
  std::stringstream stream(s);
  boost::archive::binary_iarchive ifs(stream);
  ifs >> obj;
  return ret;
}

template <class Key>
size_t ObjectArchive<Key>::load_raw(Key const& key, std::string& data,
    bool keep_in_buffer) {
  auto it = objects_.find(key);
  if (it == objects_.end())
    return 0;

  ObjectEntry& entry = it->second;

  size_t size = entry.size;
  if (size > max_buffer_size_)
    keep_in_buffer = false;

  // If the result isn't in the buffer, we must read it.
  if (entry.data.size() == 0) {
    // Only check for size if we have to load.
    if (size + buffer_size_ > max_buffer_size_)
      unload(max_buffer_size_ - size);

    stream_.seekg(entry.index_in_file);
    std::string& buf = entry.data;
    buf.resize(size);
    stream_.read(&buf[0], size);
    buffer_size_ += size;

    entry.modified = false;
  }

  touch_LRU(&entry);

  if (!keep_in_buffer) {
    if (!entry.modified)
      data.swap(entry.data);
    else
      data = entry.data;

    write_back(key);
  }
  else
    data = entry.data;

  return size;
}

template <class Key>
void ObjectArchive<Key>::unload(size_t desired_size) {
  while (buffer_size_ > desired_size)
    write_back(LRU_.back()->key);
}

template <class Key>
bool ObjectArchive<Key>::is_available(Key const& key) const {
  if (objects_.find(key) == objects_.end())
    return false;
  return true;
}

template <class Key>
std::list<Key const*> ObjectArchive<Key>::available_objects() const {
  std::list<Key const*> list;
  for (auto& it : objects_)
    list.push_front(&it.second.key);

  return list;
}

template <class Key>
void ObjectArchive<Key>::flush() {
  internal_flush();
  init(filename_);
}

template <class Key>
void ObjectArchive<Key>::clear() {
  objects_.clear();
  LRU_.clear();
  must_rebuild_file_ = true;
  flush();
}

template <class Key>
void ObjectArchive<Key>::internal_flush() {
  unload();

  if (!must_rebuild_file_)
    return;

  must_rebuild_file_ = false;

  boost::filesystem::path temp_filename;
  temp_filename = boost::filesystem::temp_directory_path();
  temp_filename += '/';
  temp_filename += boost::filesystem::unique_path();
  std::fstream temp_stream(temp_filename.string(),
      std::ios_base::in | std::ios_base::out |
      std::ios_base::binary | std::ios_base::trunc);

  size_t n_entries = objects_.size();
  temp_stream.write((char*)&n_entries, sizeof(size_t));

  char* temp_buffer = new char[max_buffer_size_];

  for (auto& it : objects_) {
    ObjectEntry& entry = it.second;

    std::string key_str = serialize_key(entry.key);

    size_t key_size = key_str.size();
    size_t data_size = entry.size;

    temp_stream.write((char*)&key_size, sizeof(size_t));
    temp_stream.write((char*)&data_size, sizeof(size_t));

    temp_stream.write((char*)&key_str[0], key_size);

    stream_.seekg(entry.index_in_file);
    size_t size = data_size;

    // Only uses the allowed buffer memory.
    for (;
         size > max_buffer_size_;
         size -= max_buffer_size_) {
      stream_.read(temp_buffer, max_buffer_size_);
      temp_stream.write(temp_buffer, max_buffer_size_);
    }
    stream_.read(temp_buffer, size);
    temp_stream.write(temp_buffer, size);
  }

  delete[] temp_buffer;

  stream_.close();
  temp_stream.close();

  boost::filesystem::remove(filename_);
  boost::filesystem::rename(temp_filename, filename_);
}

template <class Key>
bool ObjectArchive<Key>::write_back(Key const& key) {
  auto it = objects_.find(key);
  if (it == objects_.end())
    return false;

  ObjectEntry& entry = it->second;

  if (entry.modified) {
    entry.index_in_file = stream_.tellp();
    stream_.write((char*)&entry.data[0], entry.size);
    entry.modified = false;
    must_rebuild_file_ = true;
  }

  entry.data.clear();
  buffer_size_ -= entry.size;
  LRU_.remove(&entry);

  return true;
}

template <class Key>
void ObjectArchive<Key>::touch_LRU(ObjectEntry const* entry) {
  LRU_.remove(entry);
  LRU_.push_front(entry);
}

#endif
