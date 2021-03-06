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
// The default buffer size is zero, so no objects are kept in memory, and a
// temporary file is used as backend. For permanent storage, the user must
// provide its own filename to use.
//
// Note: the maximum buffer size provided isn't the maximum size that will
// actually be used, as there is an overhead for bookkeeping.
//
// Threading support: to allow the archive to be used by multiple threads, set
// ENABLE_THREADS. This should place mutex at the right places for consistency.
//
// Example:
// ObjectArchive<std::string> ar;
// ar.init("path/to/file");
// ar.set_buffer_size("1.5G");
//
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
#include <boost/predef.h>
#if ENABLE_THREADS
#include <boost/thread.hpp>
#endif
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
    virtual ~ObjectArchive();

    // Passes an object through boost serialize, making it easier to handle.
    template <class T> static std::string serialize(T const& val);
    template <class T> static void deserialize(std::string const& str, T& val);

    // If trying to serialize a pointer of a Base class, that has virtual
    // methods, poiting to a Derived object, serialization fails because it
    // doesn't recognize the type. In this case, this method deals with this.
    // Calls like serialize<Derived>(value);
    template <class T1, class T2>
    static std::string serialize(T2 const& val);

    // Initializes the archive using a temporary file as backend. As the names
    // are random, it's possible to have a collision!
    void init();

    // Initializes the archive using a new file as backend. If the file is
    // temporary, it's deleted during destruction.
    void init(std::string const& filename, bool temporary_file = false);

    // Resets the buffer size to a certain number of bytes.
    void set_buffer_size(size_t max_buffer_size);

    // Same as the other, but the string holds the number of bytes for the
    // buffer, possibly with modifiers K, M or G. If more than one modifier is
    // found, then the first one is used.
    void set_buffer_size(std::string const& max_buffer_size);

#if BOOST_OS_LINUX
    // Sets the buffer size to a percentage of the FREE memory available in the
    // system. Currently only Linux is supported.
    void set_buffer_size_scale(float max_buffer_size);
#endif

    // Provides information about the maximum and current buffer sizes.
    size_t get_max_buffer_size() const;
    size_t get_buffer_size() const;

    // Removes an object entry if it's present.
    virtual void remove(Key const& key);

    // Changes the key associated with a data.
    virtual void change_key(Key const& old_key, Key const& new_key);

    // Stores an object and associates it with an id and returns the total size
    // stored.
    // If the object is larger than the buffer's maximum size, it isn't
    // kept in memory. The user can choose not to add the object to buffer,
    // which is useful if it won't be used again.
    template <class T>
    size_t insert(Key const& key, T const& obj, bool keep_in_buffer = true);

    // Stores an object that has already been serialized.
    // If the object is larger than the buffer's maximum size, it isn't
    // kept in memory. The user can choose not to add the object to buffer,
    // which is useful if it won't be used again.
    size_t insert_raw(Key const& key, std::string const& data,
        bool keep_in_buffer = true);
    virtual size_t insert_raw(Key const& key, std::string&& data,
        bool keep_in_buffer = true);

    // Loads the object associated with the id and stores at val. Returns the
    // total size of the object, which is 0 if the object isn't found.
    // If the object is larger than the buffer's maximum size, it isn't
    // kept in memory. The user can choose not to add the object to buffer,
    // which is useful if it won't be used again.
    template <class T>
    size_t load(Key const& key, T& obj, bool keep_in_buffer = true);

    // Loads the raw serialized data of an object.
    // If the object is larger than the buffer's maximum size, it isn't
    // kept in memory. The user can choose not to add the object to buffer,
    // which is useful if it won't be used again.
    virtual size_t load_raw(Key const& key, std::string& data,
        bool keep_in_buffer = true);

    // Saves the least recently used entries so that the buffer size is at most
    // the value given in the argument. By default, frees the full buffer. If
    // the argument is larger than the current buffer, does nothing.
    void unload(size_t desired_size = 0);

    // Checks if there exists an object with this key.
    bool is_available(Key const& key);

    // Gets a list of all the results stored in this archive.
    std::list<Key const*> available_objects();

    // Flushs the archive, guaranteeing that the data is saved to a file, which
    // can be used later or continue to be used. The buffer is empty after this
    // method, but the archive can still be used.
    void flush();

    // Removes every object entry from the archive and flushes it.
    void clear();

  private:
    // Not implemented
    ObjectArchive(ObjectArchive const& other);
    ObjectArchive const& operator=(ObjectArchive const& other);

  protected:
    // Holds the entry for one object with all the information required to
    // manage it.
    struct ObjectEntry {
      Key const* key;
      std::string data; // Data for the object
      size_t index_in_file; // Index for finding it inside a file
      size_t size; // Total object size. data.size() == size if loaded
      bool modified; // If modified, the file must be written back to disk
    };

    // Same as external flush, but the archive can't be used anymore.
    void internal_flush();

    // Writes a file back to disk, freeing its buffer space. Returns if the
    // object id is inside the buffer.
    bool write_back(Key const& key);
    bool write_back(
        typename std::unordered_map<Key, ObjectEntry>::iterator const& it);

    // Puts the id in the front of the list, saying it was the last one used.
    void touch_LRU(ObjectEntry const* entry);

    std::unordered_map<Key, ObjectEntry> objects_;

    std::list<ObjectEntry const*> LRU_; // Most recent elements are on the front

    // Inserting or removing files changes the header and it must be rebuilt
    bool must_rebuild_file_;

    size_t max_buffer_size_, // Argument provided at creation
      buffer_size_; // Current buffer size

    std::string filename_;
    bool temporary_file_;
    std::fstream stream_;

#if ENABLE_THREADS
    boost::recursive_mutex mutex_;
#endif
};

#include "object_archive_impl.hpp"

#endif
