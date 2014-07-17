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
// The object are read from file as needed and, when the buffer is full, they
// are removed in a LRU fashion.
//
// New objects are stored in the buffer until the archive is flushed, when they
// are saved into its file and the buffer cleared, or when some buffer slots are
// freed. When the archive is destroyed, its file is updated to take into
// account the modifications during use (removes and inserts). Hence, if a crash
// that doesn't destroy the archive occurs, the objects aren't saved!
//
// To make sure that the objects are written, the user can call flush(), which
// will completely rebuild the file.
//
// Each object is referenced by an id, which by default is the hash of its key
// unless it's an size_t. Hash collisions are NOT handled! The object must be
// serializable through boost.
//
// Note: the maximum buffer size provided isn't the maximum size that will
// actually be used, as there is a minor overhead for bookkeeping.
//
// Example:
// ObjectArchive ar("path/to/file", "1.5G");
// ar.insert("filename", filedata);
// [do some stuff]
// ar.load("filename", filedata);
// [filedata has the previous value again]
// ar.remove("filename");
// [filedata keeps its value]

#ifndef __OBJECT_ARCHIVE_HPP__
#define __OBJECT_ARCHIVE_HPP__

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <fstream>
#include <functional>
#include <list>
#include <map>
#include <sstream>

class ObjectArchive {
  public:
    // Loads the archive at the given path and associates the maximum buffer
    // size. If the archive doesn't exist, create it. The minimum buffer size
    // allowed is 1 byte, which is forced if 0 is given.
    ObjectArchive(std::string const& filename, size_t max_buffer_size);

    // Same as the other constructor, but the string holds the number of bytes
    // for the buffer, possibly with modifiers K, M or G. If more than one
    // modifier is found, then the first one is used.
    ObjectArchive(std::string const& filename,
        std::string const& max_buffer_size);

    // Unloads the buffer using method flush().
    ~ObjectArchive();

    // Initializes the archive using a new file as backend.
    void init(std::string const& filename);

    // Removes an object entry if it's present.
    void remove(size_t id);

    // Stores an object and associates it with an id and returns the total size
    // stored.
    // If the object is larger than the buffer's maximum size, it isn't
    // kept in memory. The user can choose to not add the object to buffer,
    // which is useful if it won't be used again.
    template <class T1, class T2>
    size_t insert(T1 const& id, T2 const& obj,
        bool keep_in_buffer = true) {
      return insert(std::hash<T1>()(id), obj, keep_in_buffer);
    }

    template <class T>
    size_t insert(size_t const& id, T const& obj, bool keep_in_buffer = true) {
      std::stringstream stream;
      boost::archive::binary_oarchive ofs(stream);
      ofs << obj;
      return internal_insert(id, stream.str(), keep_in_buffer);
    }

    // Loads the object associated with the id and stores at val. Returns the
    // total size of the object, which is 0 if the object isn't found.
    // If the object is larger than the buffer's maximum size, it isn't
    // kept in memory. The user can choose to not add the object to buffer,
    // which is useful if it won't be used again.
    template <class T1, class T2>
    size_t load(T1 const& id, T2& obj, bool keep_in_buffer = true) {
      return load(std::hash<T1>()(id), obj, keep_in_buffer);
    }

    template <class T>
    size_t load(size_t const& id, T& obj, bool keep_in_buffer = true) {
      std::string s;
      size_t ret = internal_load(id, s, keep_in_buffer);
      if (ret == 0) return 0;
      std::stringstream stream(s);
      boost::archive::binary_iarchive ifs(stream);
      ifs >> obj;
      return ret;
    }

    // Saves the least recently used entries so that the buffer size is at most
    // the value given in the argument. By default, frees the full buffer. If
    // the argument is larger than the current buffer, does nothing.
    void unload(size_t desired_size = 0);

    // Gets a list of all the results stored in this archive.
    std::list<size_t> available_objects() const;

    // Flushs the archive, guaranteeing that the data is saved to a file, which
    // can be used later or continue to be used. The buffer is empty after this
    // method, but the archive can still be used.
    void flush();

  private:
    size_t internal_insert(size_t id, std::string&& data,
        bool keep_in_buffer = true);
    size_t internal_load(size_t id, std::string& data,
        bool keep_in_buffer = true);

    // Writes a file back to disk, freeing its buffer space. Returns if the
    // object id is inside the buffer.
    bool write_back(size_t id);

    // Puts the id in the front of the list, saying it was the last one used.
    void touch_LRU(size_t id);

    // Holds the entry for one object with all the information required to
    // manage it.
    struct ObjectEntry {
      std::string data; // Data for the object
      size_t index_in_file; // Index for finding it inside a file
      size_t size; // Total object size. data.size() == size if loaded
      bool modified; // If modified, the file must be written back to disk
    };
    std::map<size_t, ObjectEntry> objects_;

    std::list<size_t> LRU_; // Most recent elements are on the front

    std::string filename_;

    // Inserting or removing files changes the header and it must be rebuilt
    bool must_rebuild_file_;

    size_t max_buffer_size_, // Argument provided at creation
      buffer_size_, // Current buffer size
      header_offset_; // Offset to the file positions caused by the header

    std::fstream stream_;
};

#endif
