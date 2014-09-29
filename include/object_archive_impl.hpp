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
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/zlib.hpp>

#if ENABLE_THREADS
#define OBJECT_ARCHIVE_MUTEX_GUARD \
  boost::lock_guard<boost::recursive_mutex> __mutex_guard(mutex_)
#else
#define OBJECT_ARCHIVE_MUTEX_GUARD do { } while(0)
#endif

template <class Key>
ObjectArchive<Key>::ObjectArchive():
  must_rebuild_file_(false),
  max_buffer_size_(0),
  buffer_size_(0),
  temporary_file_(false) {
    init();
    set_buffer_size(0);
}

template <class Key>
ObjectArchive<Key>::~ObjectArchive() {
  OBJECT_ARCHIVE_MUTEX_GUARD;

  if (!temporary_file_)
    internal_flush();
  stream_.close();
  if (temporary_file_)
    boost::filesystem::remove(filename_);
}

template <class Key>
template <class T>
std::string ObjectArchive<Key>::serialize(T const& val) {
  std::stringstream stream;
  {
    boost::iostreams::filtering_stream<boost::iostreams::output> filtering;
    filtering.push(boost::iostreams::zlib_compressor());
    filtering.push(stream);
    boost::archive::binary_oarchive ofs(filtering);
    ofs << val;
  }

  return stream.str();
}

template <class Key>
template <class T1, class T2>
std::string ObjectArchive<Key>::serialize(T2 const& val) {
  std::stringstream stream;
  {
    boost::iostreams::filtering_stream<boost::iostreams::output> filtering;
    filtering.push(boost::iostreams::zlib_compressor());
    filtering.push(stream);
    boost::archive::binary_oarchive ofs(filtering);
    ofs.register_type<T1>();
    ofs << val;
  }

  return stream.str();
}

template <class Key>
template <class T>
void ObjectArchive<Key>::deserialize(std::string const& str, T& val) {
  std::stringstream stream(str);
  boost::iostreams::filtering_stream<boost::iostreams::input> filtering;
  filtering.push(boost::iostreams::zlib_decompressor());
  filtering.push(stream);
  boost::archive::binary_iarchive ifs(filtering);

  ifs >> val;
}

template <class Key>
void ObjectArchive<Key>::init() {
  std::string filename;
  filename = boost::filesystem::temp_directory_path().string();
  filename += '/';
  filename += boost::filesystem::unique_path().string();

  init(filename, true);
}

template <class Key>
void ObjectArchive<Key>::init(std::string const& filename,
    bool temporary_file) {
  OBJECT_ARCHIVE_MUTEX_GUARD;

  internal_flush();

  stream_.close();
  if (temporary_file_)
    boost::filesystem::remove(filename_);

  filename_ = filename;
  temporary_file_ = temporary_file;

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

      Key key;
      deserialize(key_string, key);

      ObjectEntry entry;
      entry.index_in_file = stream_.tellg();
      entry.size = data_size;
      entry.modified = false;
      auto it = objects_.emplace(key, entry).first;
      it->second.key = &it->first;

      stream_.seekg(data_size, std::ios_base::cur);
    }
  }
  else {
    stream_.close();
    stream_.open(filename, std::ios_base::in | std::ios_base::out |
        std::ios_base::binary | std::ios_base::trunc);
    // Consistency on crash if there's no previous flush.
    // Assumes there are no entries.
    size_t n_entries = 0;
    stream_.write((char*)&n_entries, sizeof(size_t));
  }
}

template <class Key>
void ObjectArchive<Key>::set_buffer_size(size_t max_buffer_size) {
  max_buffer_size_ = max_buffer_size;
  unload(max_buffer_size);
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
size_t ObjectArchive<Key>::get_max_buffer_size() const {
  return max_buffer_size_;
}

template <class Key>
size_t ObjectArchive<Key>::get_buffer_size() const {
  return buffer_size_;
}

template <class Key>
void ObjectArchive<Key>::remove(Key const& key) {
  if (!is_available(key))
    return;

  OBJECT_ARCHIVE_MUTEX_GUARD;

  auto it = objects_.find(key);
  if (it == objects_.end())
    return;

  ObjectEntry entry = it->second;
  if (entry.data.size())
    buffer_size_ -= entry.size;
  objects_.erase(key);
  LRU_.remove(&entry);
  must_rebuild_file_ = true;
}

template <class Key>
void ObjectArchive<Key>::change_key(Key const& old_key, Key const& new_key) {
  if (!is_available(old_key))
    return;

  OBJECT_ARCHIVE_MUTEX_GUARD;

  auto it = objects_.find(old_key);
  if (it == objects_.end())
    return;

  ObjectEntry entry = it->second;

  objects_.erase(old_key);
  LRU_.remove(&entry);

  auto it2 = objects_.emplace(new_key, entry).first;
  it2->second.key = &it2->first;
  touch_LRU(&it2->second);

  must_rebuild_file_ = true;
}

template <class Key>
template <class T>
size_t ObjectArchive<Key>::insert(Key const& key, T const& obj,
    bool keep_in_buffer) {
  return insert_raw(key, serialize(obj), keep_in_buffer);
}

template <class Key>
size_t ObjectArchive<Key>::insert_raw(Key const& key, std::string const& data,
    bool keep_in_buffer) {
  // Makes sure we call the local method, not its virtualization
  return ObjectArchive<Key>::insert_raw(key, std::string(data), keep_in_buffer);
}

template <class Key>
size_t ObjectArchive<Key>::insert_raw(Key const& key, std::string&& data,
    bool keep_in_buffer) {
  size_t size = data.size();
  if (size > max_buffer_size_)
    keep_in_buffer = false;

  // Makes sure we call the local method, not its virtualization
  ObjectArchive<Key>::remove(key);

  if (size + buffer_size_ > max_buffer_size_ && keep_in_buffer)
    unload(max_buffer_size_ - size);

  OBJECT_ARCHIVE_MUTEX_GUARD;

  buffer_size_ += size;

  ObjectEntry entry;
  entry.data.swap(data);
  entry.size = size;
  entry.modified = true;
  auto it = objects_.emplace(key, entry).first;
  it->second.key = &it->first;

  touch_LRU(&it->second);

  if (!keep_in_buffer)
    write_back(it);

  return size;
}

template <class Key>
template <class T>
size_t ObjectArchive<Key>::load(Key const& key, T& obj, bool keep_in_buffer) {
  std::string s;
  size_t ret = load_raw(key, s, keep_in_buffer);
  if (ret == 0) return 0;
  deserialize(s, obj);
  return ret;
}

template <class Key>
size_t ObjectArchive<Key>::load_raw(Key const& key, std::string& data,
    bool keep_in_buffer) {
  if (!is_available(key))
    return 0;

  OBJECT_ARCHIVE_MUTEX_GUARD;

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
    if (size + buffer_size_ > max_buffer_size_ && keep_in_buffer)
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

    write_back(it);
  }
  else
    data = entry.data;

  return size;
}

template <class Key>
void ObjectArchive<Key>::unload(size_t desired_size) {
  OBJECT_ARCHIVE_MUTEX_GUARD;
  while (buffer_size_ > desired_size)
    write_back(*LRU_.back()->key);
}

template <class Key>
bool ObjectArchive<Key>::is_available(Key const& key) {
  OBJECT_ARCHIVE_MUTEX_GUARD;
  if (objects_.count(key) == 0)
    return false;
  return true;
}

template <class Key>
std::list<Key const*> ObjectArchive<Key>::available_objects() {
  std::list<Key const*> list;

  OBJECT_ARCHIVE_MUTEX_GUARD;
  for (auto& it : objects_)
    list.push_front(it.second.key);

  return list;
}

template <class Key>
void ObjectArchive<Key>::flush() {
  OBJECT_ARCHIVE_MUTEX_GUARD;

  internal_flush();
  init(filename_);
}

template <class Key>
void ObjectArchive<Key>::clear() {
  OBJECT_ARCHIVE_MUTEX_GUARD;

  auto key_list = available_objects();
  for (auto& it : key_list)
    remove(*it);

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

  size_t local_max_buffer_size = (max_buffer_size_ == 0 ? 1 : max_buffer_size_);

  char* temp_buffer = new char[local_max_buffer_size];

  for (auto& it : objects_) {
    ObjectEntry& entry = it.second;

    std::string key_str = serialize(it.first);

    size_t key_size = key_str.size();
    size_t data_size = entry.size;

    temp_stream.write((char*)&key_size, sizeof(size_t));
    temp_stream.write((char*)&data_size, sizeof(size_t));

    temp_stream.write((char*)&key_str[0], key_size);

    stream_.seekg(entry.index_in_file);
    size_t size = data_size;

    // Only uses the allowed buffer memory.
    for (;
         size > local_max_buffer_size;
         size -= local_max_buffer_size) {
      stream_.read(temp_buffer, local_max_buffer_size);
      temp_stream.write(temp_buffer, local_max_buffer_size);
    }
    stream_.read(temp_buffer, size);
    temp_stream.write(temp_buffer, size);
  }

  delete[] temp_buffer;

  stream_.close();
  temp_stream.close();

  boost::filesystem::remove(filename_);
  boost::filesystem::copy(temp_filename, filename_);
  boost::filesystem::remove(temp_filename);
}

template <class Key>
bool ObjectArchive<Key>::write_back(Key const& key) {
  auto it = objects_.find(key);
  if (it == objects_.end())
    return false;

  return write_back(it);
}

template <class Key>
bool ObjectArchive<Key>::write_back(
    typename std::unordered_map<Key, ObjectEntry>::iterator const& it) {
  ObjectEntry& entry = it->second;

  if (entry.modified) {
    stream_.seekp(0, std::ios_base::end);
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
