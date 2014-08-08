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
