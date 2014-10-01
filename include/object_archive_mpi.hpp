// This file defines an implementation of the ObjectArchive that allows very
// loose consistency to be kept between archives in a MPI environment.
//
// The three basic operations (insert, load and remove) are communicated to
// object instances in other MPI nodes, so data can be transparently shared.
// However, the methods to check if a value is present (is_available and
// available_objects) aren't mapped through MPI, as their returned values may
// become incorrect right after the call. Hence they only provide local values.
// In case local copies of data inserted in remotes is desired, the MPI archive
// can be instructed to store them based on the key.
//
// The archives require 6 different tags to communicate among themselves, which
// should be the same for all but are personalizable by the user, and uses
// boost::mpi. The communicator given to the constructor must be kept valid
// until the archive is destroyed.
//
// A filter can also be provided, which chooses whether values inserted in
// remote nodes should also be stored locally based on the key. This can be
// useful to store values to be used in a latter time when the other node may
// not be available.
//
// All messages are processed using a MPIHandler, which must be provided at
// construction. The handler's method run() inside the handler is automatically
// called at some places for consistency.
//
// Threading support: to allow the archive to be used by multiple threads, set
// ENABLE_THREADS. This should place mutex at the right places for consistency.
//
// Example:
// boost::mpi::communicator world;
// MPIHandler handler;
// MPIObjectArchive<std::string> ar(world, handler);
// ar.init("path/to/file");
// ar.set_buffer_size("1.5G");
//
// ar.insert("filename", filedata);
// [do some stuff]
// ar.load("filename", filedata);
// [filedata has the previous value again]
// ar.remove("filename");
// [filedata keeps its value]

#ifndef __OBJECT_ARCHIVE_MPI_HPP__
#define __OBJECT_ARCHIVE_MPI_HPP__

#include <boost/function.hpp>
#include <boost/mpi.hpp>
#include <map>
#include <vector>
#include <unordered_map>

#include "mpi_handler.hpp"
#include "object_archive.hpp"

template <class Key>
class MPIObjectArchive: public ObjectArchive<Key> {
  public:
    // Type of the filter for storing remote data. It receives the key inserted
    // remotely and the MPI communicator, and returns true if the remote data
    // should be stored locally.
    typedef boost::function<bool (Key const&,
        boost::mpi::communicator&)> filter_type;

    // Tags that the archives use to communicate. The user can provide his own
    // values as long as they are different and aren't used in any other place.
    struct Tags {
      int alive = 0;
      int invalidated = 1;
      int inserted = 2;
      int change_key = 3;
      int request = 4;
      int response = 5;
      int request_data = 6;
      int response_data = 7;
    };

    // Constructs with the default tags. If the filter returns true for a given
    // key inserted in a remote node, this archive has a copy of the value
    // inserted.
    MPIObjectArchive(boost::mpi::communicator& world, MPIHandler& handler);

    // Same as the other constructor, but user-provided tags are used.
    MPIObjectArchive(Tags const& tags, boost::mpi::communicator& world,
        MPIHandler& handler);

    ~MPIObjectArchive();

    // Removes an object entry if it's present.
    virtual void remove(Key const& key);

    // Changes the key associated with a data.
    virtual void change_key(Key const& old_key, Key const& new_key);

    // Stores an object that has already been serialized.
    virtual size_t insert_raw(Key const& key, std::string&& data,
        bool keep_in_buffer = true);

    // Loads the object associated with the id and stores at val. Returns the
    // total size of the object, which is 0 if the object isn't found.
    virtual size_t load_raw(Key const& key, std::string& data,
        bool keep_in_buffer = true);

    // Sets or clears the filter of remote insertions.
    void set_insert_filter(filter_type filter);
    void clear_insert_filter();

  private:
    struct KeyPair {
      Key old_key;
      Key new_key;
      template<class Archive>
      void serialize(Archive& ar, const unsigned int version) {
        ar & old_key;
        ar & new_key;
      }
    };

    // Message that requests a given object associated with the given key to a
    // remote node.
    struct Request {
      Key key;
      int counter; // This counter is required in case of multiple requests with
                   // the same key. Although the code should work without it,
                   // the tests seem to get stuck. TODO: check why it gets stuck

      template<class Archive>
      void serialize(Archive& ar, const unsigned int version) {
        ar & key;
        ar & counter;
      }

      bool operator<(Request const& other) const {
        return key < other.key || (key == other.key && counter < other.counter);
      }
    };

    // Response to a data request saying whether it was found in the responding
    // node.
    struct Response {
      Request request;
      bool found;

      template<class Archive>
      void serialize(Archive& ar, const unsigned int version) {
        ar & request;
        ar & found;
      }
    };

    // If the data was found, it's sent back to the original node.
    struct ResponseData {
      Request request;
      bool valid;
      std::string data;

      template<class Archive>
      void serialize(Archive& ar, const unsigned int version) {
        ar & request;
        ar & valid;
        ar & data;
      }
    };

    // Processes the MPI tags other nodes might have sent
    bool process_alive(int source, int tag);
    bool process_invalidated(int source, int tag);
    bool process_inserted(int source, int tag);
    bool process_change_key(int source, int tag);
    bool process_request(int source, int tag);
    bool process_response(int source, int tag);
    bool process_request_data(int source, int tag);
    bool process_response_data(int source, int tag);

    // Gets the data associated with a given request, returning an empty
    // optional if it's not found. If source is a given node, then n_waiting
    // should be 1. If source is any_source, then n_waiting indicates how many
    // negative responses must be found before aborting.
    boost::optional<std::string> get_response(int source, int n_waiting,
        Request& request);

    // Sends an specific value with a given tag to every other node. If
    // check_alive is true, the message is only sent to alive nodes.
    template <class T>
    void broadcast_others(int tag, T const& val, bool check_alive = true);

    // Tags to be used by archive
    Tags tags_;

    boost::mpi::communicator& world_;

    // Handler for MPI messages
    MPIHandler& handler_;

    // When a remote node inserts a value, it notifies everyone. If this
    // function returns true, the local archive gets a copy.
    filter_type remote_insert_filter_;

    // Vector of alive nodes. By default, considers itself dead
    std::vector<bool> alive_;

    // Incrementing counter for requests
    int request_counter_;

    // Stores data associated with every request made. This should only be used
    // inside get_response and allows multiple requests to occur at the same
    // time. This can happen even in single-threaded programs, as multiple
    // requests can happen if record_everything_ is true.

    // Map between requests and their pointers, which are keys to the other maps
    std::map<Request, Request*> alive_requests_;
    // Source of the request
    std::unordered_map<Request*, int> requests_source_;
    // Number of responses to wait
    std::unordered_map<Request*, int> requests_waiting_;
    // Rank of the node that got the data
    std::unordered_map<Request*, int> requests_found_;
    // Valid flags obtained in response of the data request
    std::unordered_map<Request*, bool> responses_data_valid_;
    // Data obtained in response of the data request
    std::unordered_map<Request*, std::string> responses_data_;
};

#include "object_archive_mpi_impl.hpp"

#endif
