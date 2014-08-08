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
#include <vector>

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
    struct Response {
      Key key;
      bool found;

      template<class Archive>
      void serialize(Archive& ar, const unsigned int version) {
        ar & key;
        ar & found;
      }
    };

    template <class T>
    void broadcast_others(int tag, T const& val);

    Tags tags_;
    boost::mpi::communicator* world_;
    bool record_everything_;
    std::vector<bool> alive_; // By default, considers itself dead
};

#include "mpi_object_archive_impl.hpp"

#endif
