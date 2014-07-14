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

// This file defines a class to store many objects into a file, but a buffer is
// provided for speed.
//
// The object are read from file as needed and, when the buffer is full, it's
// cleared. Although LRU policy would be prefered, it's much more complicated
// and the OS tends to buffer part of the file.
//
// New objects are stored in the buffer until the archive is unloaded, when they
// are saved into its file. When the archive is destroyed, its file is updated
// to take into account the modifications during use (removes and inserts).
// Hence, if a crash that doesn't destroy the archive occurs, the objects aren't
// saved even if the user called the method unload()!
//
// To make sure that the objects are written, the user can call defrag(), which
// will completely rebuild the file.
//
// Each object is referenced by an id, which by default is the hash of its key
// unless it's an std::size_t. Hash collisions are NOT handled! The object must
// be serializable through boost.
//
// Note: the maximum buffer size provided isn't the maximum size that will
// actually be used, as there is a minor overhead for bookkeeping.
//
// Example:
// ObjectArchive ar("path/to/file", "1.5G");
// ar.insert("filename", filedata);
// ...
// ar.load("filename", filedata);

#ifndef __OBJECT_ARCHIVE_HPP__
#define __OBJECT_ARCHIVE_HPP__

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <fstream>
#include <functional>
#include <list>
#include <map>
#include <set>
#include <sstream>

class ObjectArchive {
  public:
    // Loads the archive at the given path and associates the maximum buffer
    // size. If the archive doesn't exist, create it. The minimum buffer size
    // allowed is 1 byte, which is forced if 0 is given.
    ObjectArchive(std::string const& filename, std::size_t max_buffer_size);

    // Same as the other constructor, but the string holds the number of bytes
    // for the buffer, possibly with modifiers K, M or G. If more than one
    // modifier is found, then the first one is used.
    ObjectArchive(std::string const& filename,
        std::string const& max_buffer_size);

    // Unloads the buffer, saving it to file, and defragments the file, as some
    // object may have been removed.
    ~ObjectArchive();

    // Removes an object entry if it's present.
    void remove(std::size_t id);

    // Stores an object and associates it with an id. Returns the total size
    // stored, which is 0 if the object is larger than the buffer. If the buffer
    // isn't able to fit the argument in its free space, the archive is
    // unloaded.
    template <class T1, class T2>
    std::size_t insert(T1 const& id, T2 const& obj) {
      return insert(std::hash<T1>()(id), obj);
    }

    template <class T>
    std::size_t insert(std::size_t const& id, T const& obj) {
      std::stringstream stream;
      boost::archive::binary_oarchive ofs(stream);
      ofs << obj;
      return internal_insert(id, stream.str());
    }

    // Loads the object associated with the id and stores at val. Returns the
    // total size of the object, which is 0 if the object is larger than the
    // buffer or isn't found. If the buffer isn't able to fit the object in its
    // free space, the archive is unloaded.
    template <class T1, class T2>
    std::size_t load(T1 const& id, T2& obj) {
      return load(std::hash<T1>()(id), obj);
    }

    template <class T>
    std::size_t load(std::size_t const& id, T& obj) {
      std::string s;
      std::size_t ret = internal_load(id, s);
      if (ret == 0) return 0;
      std::stringstream stream(s);
      boost::archive::binary_iarchive ifs(stream);
      ifs >> obj;
      return ret;
    }

    // Saves all entries into the file and frees the buffer.
    void unload();

    // Gets a list of all the results stored in this archive.
    std::set<std::size_t> available_objects() const;

    // If results were added or removed, defragments the file and writes the new
    // header. This ensures that all objects are saved and the program can
    // crash.
    void defrag();

  private:
    std::size_t internal_insert(std::size_t id, std::string const& val);
    std::size_t internal_load(std::size_t id, std::string& val);

    std::string filename_;
    bool modified_, must_rebuild_file_;
    std::map<std::size_t,std::size_t>
      index_file_, // Index of the objects in the file
      sizes_; // Size of each object entry
    std::size_t max_buffer_size_, // Argument provided at creation
      buffer_size_, // Current buffer size
      header_offset_; // Offset to the file positions caused by the header

    std::fstream stream_;
    std::map<std::size_t, std::string> buffer_;
};

#endif
