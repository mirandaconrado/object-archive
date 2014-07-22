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

// Each archive has a header with the following format:
// 1) Number of entries;
// 2) For each entry, the following values:
// 2.1) The id of the object;
// 2.2) The position of the object inside the file;
// 2.3) The size of the object.
//
// Each entry is of type size_t and the header isn't taken into account for the
// object position.

#include "object_archive.hpp"

#include <algorithm>
#include <boost/filesystem.hpp>

ObjectArchive::ObjectArchive(std::string const& filename,
    size_t max_buffer_size):
  must_rebuild_file_(false),
  max_buffer_size_(max_buffer_size),
  buffer_size_(0) {
    init(filename);

    if (max_buffer_size_ < 1)
      max_buffer_size_ = 1;
}

ObjectArchive::ObjectArchive(std::string const& filename,
    std::string const& max_buffer_size):
  ObjectArchive(filename, 0) {
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

    max_buffer_size_ = buffer_size;

    if (max_buffer_size_ < 1)
      max_buffer_size_ = 1;
}

ObjectArchive::~ObjectArchive() {
  flush();
}

void ObjectArchive::init(std::string const& filename) {
  flush();

  filename_ = filename;

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
      size_t id, pos, size;
      stream_.read((char*)&id, sizeof(size_t));
      stream_.read((char*)&pos, sizeof(size_t));
      stream_.read((char*)&size, sizeof(size_t));

      ObjectEntry entry;
      entry.index_in_file = pos;
      entry.size = size;
      entry.modified = false;
      objects_[id] = entry;
    }

    header_offset_ = stream_.tellg();
  }
  else {
    stream_.open(filename, std::ios_base::in | std::ios_base::out |
        std::ios_base::binary | std::ios_base::trunc);
    header_offset_ = 0;
  }
}

void ObjectArchive::remove(size_t id) {
  auto it = objects_.find(id);
  if (it == objects_.end())
    return;

  ObjectEntry& entry = it->second;
  if (entry.data.size())
    buffer_size_ -= entry.size;
  objects_.erase(id);
  LRU_.remove(id);
  must_rebuild_file_ = true;
}

size_t ObjectArchive::insert_raw(size_t id, std::string&& data,
    bool keep_in_buffer) {
  size_t size = data.size();
  if (size > max_buffer_size_)
    keep_in_buffer = false;

  remove(id);

  if (size + buffer_size_ > max_buffer_size_ && keep_in_buffer)
    unload(max_buffer_size_ - size);

  buffer_size_ += size;

  ObjectEntry& entry = objects_[id];
  entry.data.swap(data);
  entry.size = size;
  entry.modified = true;
  touch_LRU(id);

  if (!keep_in_buffer)
    write_back(id);

  return size;
}

size_t ObjectArchive::load_raw(size_t id, std::string& data,
    bool keep_in_buffer) {
  auto it = objects_.find(id);
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

  touch_LRU(id);

  if (!keep_in_buffer) {
    data.swap(entry.data);
    write_back(id);
  }
  else
    data = entry.data;

  return size;
}

void ObjectArchive::unload(size_t desired_size) {
  while (buffer_size_ > desired_size)
    write_back(LRU_.back());
}

std::list<size_t> ObjectArchive::available_objects() const {
  std::list<size_t> list;
  for (auto& it : objects_)
    list.push_front(it.first);

  return list;
}

void ObjectArchive::flush() {
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

  size_t pos = (1+3*objects_.size()) * sizeof(size_t);
  for (auto& it : objects_) {
    size_t id, size;
    ObjectEntry& entry = it.second;
    id = it.first;
    size = entry.size;
    temp_stream.write((char*)&id, sizeof(size_t));
    temp_stream.write((char*)&pos, sizeof(size_t));
    temp_stream.write((char*)&size, sizeof(size_t));
    pos += size;
  }

  char* temp_buffer = new char[max_buffer_size_];

  for (auto& it : objects_) {
    ObjectEntry& entry = it.second;
    stream_.seekg(entry.index_in_file);
    size_t size = entry.size;

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

  init(filename_);
}

bool ObjectArchive::write_back(size_t id) {
  auto it = objects_.find(id);
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
  LRU_.remove(id);

  return true;
}

void ObjectArchive::touch_LRU(size_t id) {
  LRU_.remove(id);
  LRU_.push_front(id);
}
