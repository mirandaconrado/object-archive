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
// Each object is referenced by a key, whose type must be hashable and
// comparable, as it's used inside as index to an unordered_map. Both the key
// and the object must be serializable through boost.
//
// Note: the maximum buffer size provided isn't the maximum size that will
// actually be used, as there is an overhead for bookkeeping.
//
// Example:
// ObjectArchive<std::string> ar("path/to/file", "1.5G");
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
#include <sstream>
#include <unordered_map>

template <class Key>
class ObjectArchive {
  public:
    // Creates an archive with a temporary file as backend, which is deleted on
    // destruction. To use a permanent record, call the method init().
    ObjectArchive();

    // Unloads the buffer using method flush().
    ~ObjectArchive();

    // Passes the key through boost serialize, making it easier to store/load.
    static std::string serialize_key(Key const& key);
    static Key deserialize_key(std::string const& key_string);

    // Initializes the archive using a temporary file as backend. As the names
    // are random, it's possible to have a collision!
    void init();

    // Initializes the archive using a new file as backend.
    void init(std::string const& filename);

    // Resets the buffer size to a certain number of bytes.
    void set_buffer_size(size_t max_buffer_size);

    // Same as the other, but the string holds the number of bytes for the
    // buffer, possibly with modifiers K, M or G. If more than one modifier is
    // found, then the first one is used.
    void set_buffer_size(std::string const& max_buffer_size);

    // Removes an object entry if it's present.
    void remove(Key const& key);

    // Stores an object and associates it with an id and returns the total size
    // stored.
    // If the object is larger than the buffer's maximum size, it isn't
    // kept in memory. The user can choose to not add the object to buffer,
    // which is useful if it won't be used again.
    template <class T>
    size_t insert(Key const& key, T const& obj, bool keep_in_buffer = true);

    // Stores an object that has already been serialized.
    // If the object is larger than the buffer's maximum size, it isn't
    // kept in memory. The user can choose to not add the object to buffer,
    // which is useful if it won't be used again.
    size_t insert_raw(Key const& key, std::string const& data,
        bool keep_in_buffer = true);
    size_t insert_raw(Key const& key, std::string&& data,
        bool keep_in_buffer = true);

    // Loads the object associated with the id and stores at val. Returns the
    // total size of the object, which is 0 if the object isn't found.
    // If the object is larger than the buffer's maximum size, it isn't
    // kept in memory. The user can choose to not add the object to buffer,
    // which is useful if it won't be used again.
    template <class T>
    size_t load(Key const& key, T& obj, bool keep_in_buffer = true);

    // Loads the raw serialized data of an object.
    // If the object is larger than the buffer's maximum size, it isn't
    // kept in memory. The user can choose to not add the object to buffer,
    // which is useful if it won't be used again.
    size_t load_raw(Key const& key, std::string& data,
        bool keep_in_buffer = true);

    // Saves the least recently used entries so that the buffer size is at most
    // the value given in the argument. By default, frees the full buffer. If
    // the argument is larger than the current buffer, does nothing.
    void unload(size_t desired_size = 0);

    // Checks if there exists an object with this key.
    bool is_available(Key const& key) const;

    // Gets a list of all the results stored in this archive.
    std::list<Key const*> available_objects() const;

    // Flushs the archive, guaranteeing that the data is saved to a file, which
    // can be used later or continue to be used. The buffer is empty after this
    // method, but the archive can still be used.
    void flush();

    // Removes every object entry from the archive and flushes it.
    void clear();

  private:
    // Same as external flush, but the archive can't be used anymore.
    void internal_flush();

    // Writes a file back to disk, freeing its buffer space. Returns if the
    // object id is inside the buffer.
    bool write_back(Key const& key);

    // Puts the id in the front of the list, saying it was the last one used.
    struct ObjectEntry;
    void touch_LRU(ObjectEntry const* entry);

    // Holds the entry for one object with all the information required to
    // manage it.
    struct ObjectEntry {
      Key key;
      std::string data; // Data for the object
      size_t index_in_file; // Index for finding it inside a file
      size_t size; // Total object size. data.size() == size if loaded
      bool modified; // If modified, the file must be written back to disk
    };
    std::unordered_map<Key, ObjectEntry> objects_;

    std::list<ObjectEntry const*> LRU_; // Most recent elements are on the front

    // Inserting or removing files changes the header and it must be rebuilt
    bool must_rebuild_file_;

    size_t max_buffer_size_, // Argument provided at creation
      buffer_size_; // Current buffer size

    std::string filename_;
    bool temporary_file_;
    std::fstream stream_;
};

#include "object_archive_impl.hpp"

#endif
