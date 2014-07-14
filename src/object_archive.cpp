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

// Each batch has a header with the following format:
// 1) Number of entries;
// 2) For each entry, the following values:
// 2.1) The hash of the task which provided the result;
// 2.2) The position of the result inside the file;
// 2.3) The size of the result.
//
// Each entry is of type std::size_t and the header isn't taken into account for
// the maximum file size or entry position.

#include "object_archive.hpp"

#include <algorithm>
#include <boost/filesystem.hpp>

ObjectArchive::ObjectArchive(std::string const& filename,
    std::size_t max_buffer_size):
  filename_(filename),
  modified_(false),
  must_rebuild_file_(false),
  max_buffer_size_(max_buffer_size),
  buffer_size_(0),
  header_offset_(0),
  stream_(filename, std::ios_base::in | std::ios_base::out |
                    std::ios_base::binary) {
    stream_.seekg(0, std::ios_base::end);

    // If the file seems ok and has entries, use it. Otherwise, overwrite.
    if (stream_.good() && stream_.tellg() > 0) {
      stream_.seekg(0);

      std::size_t n_entries;
      stream_.read((char*)&n_entries, sizeof(std::size_t));

      for (std::size_t i = 0; i < n_entries; i++) {
        std::size_t id, pos, size;
        stream_.read((char*)&id, sizeof(std::size_t));
        stream_.read((char*)&pos, sizeof(std::size_t));
        stream_.read((char*)&size, sizeof(std::size_t));
        index_file_[id] = pos;
        sizes_[id] = size;
      }

      header_offset_ = stream_.tellg();
    }
    else {
      stream_.open(filename, std::ios_base::in | std::ios_base::out |
                             std::ios_base::binary | std::ios_base::trunc);
    }

    if (max_buffer_size_ < 1)
      max_buffer_size_ = 1;
}

ObjectArchive::ObjectArchive(std::string const& filename,
    std::string const& max_buffer_size):
  ObjectArchive(filename, 0) {
    std::size_t length = max_buffer_size.size();
    double buffer_size = atof(max_buffer_size.c_str());

    bool changed = false;
    for (std::size_t i = 0; i< length && !changed; i++) {
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
  defrag();
}

void ObjectArchive::remove(std::size_t id) {
  auto it = sizes_.find(id);
  if (it == sizes_.end())
    return;

  if (buffer_.find(id) != buffer_.end())
    buffer_size_ -= it->second;
  index_file_.erase(id);
  sizes_.erase(id);
  buffer_.erase(id);
  modified_ = true;
}

std::size_t ObjectArchive::internal_insert(std::size_t id,
    std::string const& val) {
  std::size_t size = val.size();
  if (size > max_buffer_size_)
    return 0;

  if (size + buffer_size_ > max_buffer_size_)
    unload();

  buffer_size_ += size;
  index_file_.erase(id); // The value inside the file isn't valid anymore
  sizes_[id] = size;
  buffer_[id] = val;
  modified_ = true;

  return size;
}

std::size_t ObjectArchive::internal_load(std::size_t id, std::string& val) {
  auto it = sizes_.find(id);
  if (it == sizes_.end())
    return 0;

  std::size_t size = it->second;
  if (size > max_buffer_size_)
    return 0;

  // If the result isn't in the buffer, we must read it.
  if (buffer_.find(id) == buffer_.end()) {
    // Only check for size if we have to load.
    if (size + buffer_size_ > max_buffer_size_)
      unload();

    stream_.seekg(index_file_.at(id)+header_offset_);
    std::string& buf = buffer_[id];
    buf.resize(size);
    stream_.read(&buf[0], size);
  }

  val = buffer_[id];
  return size;
}

void ObjectArchive::unload() {
  if (modified_) {
    stream_.seekp(0, std::ios_base::end);
    for (auto& it : buffer_) {
      std::size_t id = it.first;
      // If the object isn't valid inside the file, write it to the end.
      if (index_file_.find(id) == index_file_.end()) {
        index_file_[id] = stream_.tellp();
        index_file_[id] -= header_offset_;
        stream_.write((char*)&it.second[0], sizes_[id]);
      }
    }
    modified_ = false;
    must_rebuild_file_ = true;
  }

  buffer_.clear();
  buffer_size_ = 0;
}

std::set<std::size_t> ObjectArchive::available_objects() const {
  std::set<std::size_t> set;
  for (auto& it : index_file_)
    set.insert(it.first);

  for (auto& it : buffer_)
    set.insert(it.first);

  return set;
}

void ObjectArchive::defrag() {
  unload();

  if (!must_rebuild_file_)
    return;

  boost::filesystem::path temp_filename;
  temp_filename = boost::filesystem::temp_directory_path();
  temp_filename += '/';
  temp_filename += boost::filesystem::unique_path();
  std::fstream temp_stream(temp_filename.string(),
      std::ios_base::in | std::ios_base::out |
      std::ios_base::binary | std::ios_base::trunc);

  std::size_t n_entries = index_file_.size();
  temp_stream.write((char*)&n_entries, sizeof(size_t));
  std::size_t pos = 0;
  for (auto& it : index_file_) {
    size_t id, size;
    id = it.first;
    size = sizes_[id];
    temp_stream.write((char*)&id, sizeof(size_t));
    temp_stream.write((char*)&pos, sizeof(size_t));
    temp_stream.write((char*)&size, sizeof(size_t));
    pos += size;
  }

  char* temp_buffer = new char[max_buffer_size_];

  for (auto& it : index_file_) {
    std::size_t id = it.first;
    stream_.seekg(it.second+header_offset_);
    std::size_t size = sizes_.at(id);
    // Only uses the allowed buffer memory.
    for (size = sizes_.at(id);
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

  stream_.open(filename_, std::ios_base::in | std::ios_base::out |
                          std::ios_base::binary);
}
