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
  alive_(world->size(), false),
  request_counter_(0) {
    broadcast_others(tags_.alive, true, false);
    mpi_process();
  }

template <class Key>
MPIObjectArchive<Key>::~MPIObjectArchive() {
  mpi_process();
  broadcast_others(tags_.alive, false, false);
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
    int current_request_counter = request_counter_++;

    Request request;
    request.key = key;
    request.counter = current_request_counter;
    broadcast_others(tags_.request, request);

    int n_waiting_response = world_->size();

    for (int i = 0; i < alive_.size(); i++)
      if (!alive_[i])
        n_waiting_response--;

    while (n_waiting_response > 0) {
      Response response;
      boost::mpi::status status = non_blocking_recv(boost::mpi::any_source,
          tags_.response, response);

      if (response.request.counter != current_request_counter ||
          response.request.key != key)
        continue;

      if (response.found) {
        non_blocking_recv(status.source(), tags_.response_data, data);
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
        process_alive(status.source(), alive);
      }
      else if (status.tag() == tags_.invalidated) {
        Key key;
        world_->recv(status.source(), status.tag(), key);
        process_invalidated(status.source(), key);
      }
      else if (status.tag() == tags_.inserted) {
        Key key;
        world_->recv(status.source(), status.tag(), key);
        process_inserted(status.source(), key);
      }
      else if (status.tag() == tags_.request) {
        Request request;
        world_->recv(status.source(), status.tag(), request);
        process_request(status.source(), request);
      }
      else
        stop = true;
    }
    else
      stop = true;
  }
}

template <class Key>
void MPIObjectArchive<Key>::process_alive(int source, bool alive) {
  bool old_alive;

  old_alive = alive_[source];
  alive_[source] = alive;

  // Became alive
  if (alive && !old_alive)
    world_->send(source, tags_.alive, true);
}

template <class Key>
void MPIObjectArchive<Key>::process_invalidated(int source, Key const& key) {
  ObjectArchive<Key>::remove(key);
}

template <class Key>
void MPIObjectArchive<Key>::process_inserted(int source, Key const& key) {
  if (record_everything_) {
    int current_request_counter = request_counter_++;

    Request request;
    request.key = key;
    request.counter = current_request_counter;

    world_->send(source, tags_.request, request);

    // Avoid non-processed responses from another request
    Response response;
    do {
      non_blocking_recv(source, tags_.response, response);
    } while (response.request.counter != current_request_counter ||
             response.request.key != key);

    if (response.found) {
      std::string data;
      non_blocking_recv(source, tags_.response_data, data);
      ObjectArchive<Key>::insert_raw(key, data, false);
    }
  }
}

template <class Key>
void MPIObjectArchive<Key>::process_request(int source,
    Request const& request) {
  Response response;
  response.request = request;
  response.found = this->is_available(request.key);

  auto response_req = world_->isend(source, tags_.response, response);

  if (response.found) {
    std::string data;
    ObjectArchive<Key>::load_raw(request.key, data, false);
    world_->send(source, tags_.response_data, data);
  }

  response_req.wait();
}

template <class Key>
template <class T>
void MPIObjectArchive<Key>::broadcast_others(int tag, T const& val,
    bool check_alive) {
  std::vector<boost::mpi::request> reqs(world_->size());
  for (int i = 0; i < world_->size(); i++)
    if (alive_[i] || (!check_alive && i != world_->rank()))
      reqs[i] = world_->isend(i, tag, val);

  boost::mpi::wait_all(reqs.begin(), reqs.end());
}

template <class Key>
template <class T>
boost::mpi::status MPIObjectArchive<Key>::non_blocking_recv(int source, int tag,
    T& value) {
  boost::mpi::request req = world_->irecv(source, tag, value);

  while (1) {
    auto status_opt = req.test();
    if (status_opt)
      return status_opt.get();

    mpi_process();
  }
}

#endif
