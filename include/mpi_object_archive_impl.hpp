#ifndef __MPI_OBJECT_ARCHIVE_IMPL_HPP__
#define __MPI_OBJECT_ARCHIVE_IMPL_HPP__

#include "mpi_object_archive.hpp"

template <class Key>
MPIObjectArchive<Key>::MPIObjectArchive(boost::mpi::communicator* world,
    bool record_everything):
  MPIObjectArchive(Tags(), world, record_everything) { }

template <class Key>
MPIObjectArchive<Key>::MPIObjectArchive(Tags const& tags,
    boost::mpi::communicator* world, bool record_everything):
  ObjectArchive<Key>(),
  tags_(tags),
  world_(world),
  record_everything_(record_everything),
  alive_(world->size(), false) {
    broadcast_others(tags_.alive, true);
  }

template <class Key>
MPIObjectArchive<Key>::~MPIObjectArchive() {
  broadcast_others(tags_.alive, false);
}

template <class Key>
void MPIObjectArchive<Key>::remove(Key const& key) {
  mpi_process();

  broadcast_others(tags_.invalidated, key);

  ObjectArchive<Key>::remove(key);
}

template <class Key>
size_t MPIObjectArchive<Key>::insert_raw(Key const& key, std::string&& data,
    bool keep_in_buffer) {
  mpi_process();

  size_t size = ObjectArchive<Key>::insert_raw(key,
      std::forward<std::string>(data), true);

  broadcast_others(tags_.inserted, key);

  if (!keep_in_buffer)
    this->write_back(key);

  return size;
}

template <class Key>
size_t MPIObjectArchive<Key>::load_raw(Key const& key, std::string& data,
    bool keep_in_buffer) {
  mpi_process();

  size_t size = ObjectArchive<Key>::load_raw(key, data, keep_in_buffer);

  // Object not found here!
  if (size == 0) {
    broadcast_others(tags_.request, key);

    int n_waiting_response = world_->size();

    for (auto it : alive_)
      if (!it)
        n_waiting_response--;

    while (n_waiting_response > 0) {
      Response response;
      boost::mpi::status status = world_->recv(boost::mpi::any_source,
          tags_.response, response);

      if (response.key != key)
        continue;

      if (response.found) {
        world_->recv(status.source(), tags_.response_data, data);
        ObjectArchive<Key>::insert_raw(key, data, true);
        size = ObjectArchive<Key>::load_raw(key, data, keep_in_buffer);
        return size;
      }

      n_waiting_response--;
    }
  }

  return size;
}

template <class Key>
void MPIObjectArchive<Key>::mpi_process() {
  bool stop = false;
  while (!stop) {
    auto status_opt = world_->iprobe();
    if (status_opt) {
      auto status = status_opt.get();
      if (status.tag() == tags_.alive) {
        bool alive;
        world_->recv(status.source(), status.tag(), alive);
        alive_[status.source()] = alive;
      }
      else if (status.tag() == tags_.invalidated) {
        Key key;
        world_->recv(status.source(), status.tag(), key);
        ObjectArchive<Key>::remove(key);
      }
      else if (status.tag() == tags_.inserted && record_everything_) {
        Key key;
        world_->recv(status.source(), status.tag(), key);
        world_->send(status.source(), tags_.request, key);

        Response response;
        do {
          world_->recv(status.source(), tags_.response, response);
        } while (response.key != key);

        if (response.found) {
          std::string data;
          world_->recv(status.source(), tags_.response_data, data);
          ObjectArchive<Key>::insert_raw(key, data, false);
        }
      }
      else if (status.tag() == tags_.request) {
        Key key;
        world_->recv(status.source(), status.tag(), key);

        Response response;
        response.key = key;
        response.found = this->is_available(key);

        auto response_req = world_->isend(status.source(), tags_.response,
            response);

        if (response.found) {
          std::string data;
          load_raw(key, data, false);
          world_->send(status.source(), tags_.response_data, data);
        }

        response_req.wait();
      }
    }
    else
      stop = true;
  }
}

template <class Key>
template <class T>
void MPIObjectArchive<Key>::broadcast_others(int tag, T const& val) {
  std::vector<boost::mpi::request> reqs(world_->size());
  for (int i = 0; i < world_->size(); i++)
    if (alive_[i])
      reqs[i] = world_->isend(i, tags_.invalidated, val);
  boost::mpi::wait_all(reqs.begin(), reqs.end());
}

#endif
