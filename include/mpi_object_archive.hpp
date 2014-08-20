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
// Threading support: to allow the archive to be used by multiple threads, set
// ENABLE_THREADS. This should place mutex at the right places for consistency.
//
// Example:
// boost::mpi::communicator world;
// MPIObjectArchive<std::string> ar(world);
// ar.init("path/to/file");
// ar.set_buffer_size("1.5G");
//
// ar.insert("filename", filedata);
// [do some stuff]
// ar.load("filename", filedata);
// [filedata has the previous value again]
// ar.remove("filename");
// [filedata keeps its value]

#ifndef __MPI_OBJECT_ARCHIVE_HPP__
#define __MPI_OBJECT_ARCHIVE_HPP__

#include <boost/function.hpp>
#include <boost/mpi.hpp>
#include <map>
#include <vector>
#include <unordered_map>

#include "object_archive.hpp"

template <class Key>
class MPIObjectArchive: public ObjectArchive<Key> {
  private:
    typedef boost::function<bool (Key const&)> filter_type;

  public:
    // Tags that the archives use to communicate. The user can provide his own
    // values as long as they are different and aren't used in any other place.
    struct Tags {
      int alive = 0;
      int invalidated = 1;
      int inserted = 2;
      int request = 3;
      int response = 4;
      int request_data = 5;
      int response_data = 6;
    };

    // Constructs with the default tags. If the filter returns true for a given
    // key inserted in a remote node, this archive has a copy of the value
    // inserted.
    MPIObjectArchive(boost::mpi::communicator& world,
        filter_type remote_insert_filter =
        filter_type([](Key const&) { return false; }));

    // Same as the other constructor, but user-provided tags are used.
    MPIObjectArchive(Tags const& tags, boost::mpi::communicator& world,
        filter_type remote_insert_filter =
        filter_type([](Key const&) { return false; }));

    ~MPIObjectArchive();

    // Removes an object entry if it's present.
    virtual void remove(Key const& key);

    // Stores an object that has already been serialized.
    virtual size_t insert_raw(Key const& key, std::string&& data,
        bool keep_in_buffer = true);

    // Loads the object associated with the id and stores at val. Returns the
    // total size of the object, which is 0 if the object isn't found.
    virtual size_t load_raw(Key const& key, std::string& data,
        bool keep_in_buffer = true);

    // Processes MPI messages from other nodes. This is called automatically on
    // the other methods, but may be called directly if the node is idle.
    void mpi_process();

  private:
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
    void process_alive(int source, bool alive);
    void process_invalidated(int source, Key const& key);
    void process_inserted(int source, Key const& key);
    void process_request(int source, Request const& request);
    void process_request_data(int source, Request const& request);

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

    // Tries to receive a value from a given combination of source and tag. If
    // the source dies while waiting, returns an empty optional. This should be
    // used everywhere instead of simply receiving, as it call mpi_processes to
    // avoid deadlocks.
    template <class T>
    boost::optional<boost::mpi::status>
    non_blocking_recv(int source, int tag, T& value);

    Tags tags_; // Tags to be used by archive
    boost::mpi::communicator& world_;
    // When a remote node inserts a value, it notifies everyone. If this
    // function returns true, the local archive gets a copy.
    filter_type remote_insert_filter_;
    std::vector<bool> alive_; // By default, considers itself dead
    int request_counter_; // Incrementing counter for requests

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

#include "mpi_object_archive_impl.hpp"

#endif
