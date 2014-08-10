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

#ifndef __MPI_OBJECT_ARCHIVE_HPP__
#define __MPI_OBJECT_ARCHIVE_HPP__

// Can't use threads and MPI
#undef ENABLE_THREADS

#include <boost/mpi.hpp>
#include <map>
#include <vector>
#include <unordered_map>

#include "object_archive.hpp"

template <class Key>
class MPIObjectArchive: public ObjectArchive<Key> {
  public:
    struct Tags {
      int alive = 0;
      int invalidated = 1;
      int inserted = 2;
      int request = 3;
      int response = 4;
      int response_data = 5;
    };

    MPIObjectArchive(boost::mpi::communicator* world,
        bool record_everything = false);

    MPIObjectArchive(Tags const& tags, boost::mpi::communicator* world,
        bool record_everything = false);

    ~MPIObjectArchive();

    virtual void remove(Key const& key);

    virtual size_t insert_raw(Key const& key, std::string&& data,
        bool keep_in_buffer = true);

    virtual size_t load_raw(Key const& key, std::string& data,
        bool keep_in_buffer = true);

    void mpi_process();

  private:
    struct Request {
      Key key;
      int counter;

      template<class Archive>
      void serialize(Archive& ar, const unsigned int version) {
        ar & key;
        ar & counter;
      }

      bool operator<(Request const& other) const {
        return key < other.key || (key == other.key && counter < other.counter);
      }
    };

    struct Response {
      Request request;
      bool found;

      template<class Archive>
      void serialize(Archive& ar, const unsigned int version) {
        ar & request;
        ar & found;
      }
    };

    struct ResponseData {
      Request request;
      std::string data;

      template<class Archive>
      void serialize(Archive& ar, const unsigned int version) {
        ar & request;
        ar & data;
      }
    };

    void process_alive(int source, bool alive);
    void process_invalidated(int source, Key const& key);
    void process_inserted(int source, Key const& key);
    void process_request(int source, Request const& request);
    boost::optional<std::string> get_response(int source, int n_waiting,
        Request& request);

    template <class T>
    void broadcast_others(int tag, T const& val, bool check_alive = true);

    template <class T>
    boost::mpi::status non_blocking_recv(int source, int tag, T& value);

    Tags tags_;
    boost::mpi::communicator* world_;
    bool record_everything_;
    std::vector<bool> alive_; // By default, considers itself dead
    int request_counter_;

    std::map<Request, Request*> alive_requests_;
    std::unordered_map<Request*, int> requests_source_;
    std::unordered_map<Request*, int> requests_waiting_;
    std::unordered_map<Request*, int> requests_found_;
    std::unordered_map<Request*, std::string> responses_data_;
};

#include "mpi_object_archive_impl.hpp"

#endif
