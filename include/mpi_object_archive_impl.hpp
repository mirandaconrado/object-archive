// The communication logic between archives is as follows:
// 1) When they are constructed, they alert everyone they became alive. When
// destructed, they say they aren't dead anymore.
// 1.1) If a node receives an alive message from a previously dead node, they
// answer back saying that they are alive too.
// 1.2) If a node receives a dead message from a previously alive node, they
// check every request to see if that node is involved. If it is, reduces the
// number of waiting nodes as it won't answer the request.
// 2) When an object is removed, an "invalidated" tag is broadcasted.
// 3) When an object is inserted, an "inserted" tag is broadcasted. Remote nodes
// do nothing if record_everything is false, as inserting call the remove method
// before, so the value was already invalidated.
// 4) When an object is requested and isn't found in the local archive, a
// "request" tag is broadcasted.
// 4.1) If a remote node finds the object in its archive, it sends a "response"
// message and a "response_data" with the data.
// 4.2) If a remote doesn't find the object, it sends a negative "response".

#ifndef __MPI_OBJECT_ARCHIVE_IMPL_HPP__
#define __MPI_OBJECT_ARCHIVE_IMPL_HPP__

#include <functional>

#include "mpi_object_archive.hpp"

template <class Key>
MPIObjectArchive<Key>::MPIObjectArchive(boost::mpi::communicator& world,
    MPIHandler& handler, filter_type remote_insert_filter):
  MPIObjectArchive(Tags(), world, handler, remote_insert_filter) { }

template <class Key>
MPIObjectArchive<Key>::MPIObjectArchive(Tags const& tags,
    boost::mpi::communicator& world, MPIHandler& handler,
    filter_type remote_insert_filter):
  ObjectArchive<Key>(),
  tags_(tags),
  world_(world),
  handler_(handler),
  remote_insert_filter_(remote_insert_filter),
  alive_(world.size(), false),
  request_counter_(0) {
    handler.insert(tags_.alive,
        std::bind(&MPIObjectArchive<Key>::process_alive, this,
          std::placeholders::_1, tags.alive));
    handler.insert(tags_.invalidated,
        std::bind(&MPIObjectArchive<Key>::process_invalidated, this,
          std::placeholders::_1, tags.invalidated));
    handler.insert(tags_.inserted,
        std::bind(&MPIObjectArchive<Key>::process_inserted, this,
          std::placeholders::_1, tags.inserted));
    handler.insert(tags_.request,
        std::bind(&MPIObjectArchive<Key>::process_request, this,
          std::placeholders::_1, tags.request));
    handler.insert(tags_.request_data,
        std::bind(&MPIObjectArchive<Key>::process_request_data, this,
          std::placeholders::_1, tags.request_data));

    broadcast_others(tags_.alive, true, false);
    handler_.run();
  }

template <class Key>
MPIObjectArchive<Key>::~MPIObjectArchive() {
  handler_.run();
  broadcast_others(tags_.alive, false, false);
}

template <class Key>
void MPIObjectArchive<Key>::remove(Key const& key) {
  handler_.run();

  OBJECT_ARCHIVE_MUTEX_GUARD;

  broadcast_others(tags_.invalidated, key);

  ObjectArchive<Key>::remove(key);
}

template <class Key>
size_t MPIObjectArchive<Key>::insert_raw(Key const& key, std::string&& data,
    bool keep_in_buffer) {
  handler_.run();

  OBJECT_ARCHIVE_MUTEX_GUARD;

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
  handler_.run();

  OBJECT_ARCHIVE_MUTEX_GUARD;

  size_t size = ObjectArchive<Key>::load_raw(key, data, keep_in_buffer);

  // Object not found here! Let's find it somewhere else.
  if (size == 0) {
    int current_request_counter = request_counter_++;

    Request request;
    request.key = key;
    request.counter = current_request_counter;
    broadcast_others(tags_.request, request);

    int n_waiting_response = world_.size();

    for (int i = 0; i < alive_.size(); i++)
      if (!alive_[i])
        n_waiting_response--;

    auto response_data = get_response(boost::mpi::any_source,
        n_waiting_response, request);

    if (response_data) {
        ObjectArchive<Key>::insert_raw(key, response_data.get(), true);
        size = ObjectArchive<Key>::load_raw(key, data, keep_in_buffer);
        return size;
    }
  }

  return size;
}

template <class Key>
bool MPIObjectArchive<Key>::process_alive(int source, int tag) {
  bool alive;
  world_.recv(source, tag, alive);

  bool old_alive;

  old_alive = alive_[source];
  alive_[source] = alive;

  if (alive && !old_alive) // Became alive
    world_.send(source, tags_.alive, true); // Tells this one is alive too
  else if (old_alive && !alive) // Died
    for (auto& it : requests_source_) // Check requests for dependencies
      if (it.second == source || it.second == boost::mpi::any_source)
        requests_waiting_[it.first]--;

  return true;
}

template <class Key>
bool MPIObjectArchive<Key>::process_invalidated(int source, int tag) {
  Key key;
  world_.recv(source, tag, key);
  ObjectArchive<Key>::remove(key);

  return true;
}

template <class Key>
bool MPIObjectArchive<Key>::process_inserted(int source, int tag) {
  Key key;
  world_.recv(source, tag, key);
  ObjectArchive<Key>::remove(key);

  if (remote_insert_filter_(key, world_)) {
    int current_request_counter = request_counter_++;

    Request request;
    request.key = key;
    request.counter = current_request_counter;

    world_.send(source, tags_.request, request);

    // Abool non-processed responses from another request
    auto response_data = get_response(source, 1, request);

    if (response_data) {
      ObjectArchive<Key>::insert_raw(key, response_data.get(), false);
    }
  }

  return true;
}

template <class Key>
bool MPIObjectArchive<Key>::process_request(int source, int tag) {
  Request request;
  world_.recv(source, tag, request);

  Response response;
  response.request = request;
  response.found = this->is_available(request.key);

  world_.send(source, tags_.response, response);

  return true;
}

template <class Key>
bool MPIObjectArchive<Key>::process_request_data(int source, int tag) {
  Request request;
  world_.recv(source, tag, request);

  ResponseData response_data;
  response_data.request = request;
  response_data.valid = this->is_available(request.key);
  if (response_data.valid)
    ObjectArchive<Key>::load_raw(request.key, response_data.data, false);
  world_.send(source, tags_.response_data, response_data);

  return true;
}

template <class Key>
boost::optional<std::string> MPIObjectArchive<Key>::get_response(int source,
    int n_waiting, Request& request) {
  boost::optional<std::string> ret;

  alive_requests_[request] = &request;
  requests_source_[&request] = source;
  requests_waiting_[&request] = n_waiting;

  // Loops trying to get an response and processing everyone until ours is
  // found or nobody got the data.
  while (requests_waiting_[&request] > 0 &&
         requests_found_.count(&request) == 0) {
    Response response;
    auto status = non_blocking_recv(source, tags_.response, response);
    if (!status)
      continue;

    if (alive_requests_.count(response.request) > 0) {
      Request* req = alive_requests_[response.request];
      requests_waiting_[req]--;

      if (response.found)
        requests_found_[req] = status->source();
    }
  }

  // If found somewhere, gets the data.
  if (requests_found_.count(&request) > 0) {
    int source = requests_found_[&request];

    world_.send(source, tags_.request_data, request);

    while (responses_data_valid_.count(&request) == 0) {
      ResponseData response_data;
      auto status = non_blocking_recv(source, tags_.response_data,
          response_data);
      if (!status)
        continue;

      if (alive_requests_.count(response_data.request) > 0) {
        Request* req = alive_requests_[response_data.request];
        requests_found_[req] = status->source();
        responses_data_valid_[req] = response_data.valid;
        if (response_data.valid)
          responses_data_[req] = response_data.data;
      }
    }

    if (responses_data_valid_[&request])
      ret = boost::optional<std::string>(std::move(responses_data_[&request]));
  }

  alive_requests_.erase(request);
  requests_source_.erase(&request);
  requests_waiting_.erase(&request);
  requests_found_.erase(&request);
  responses_data_valid_.erase(&request);
  responses_data_.erase(&request);

  return ret;
}

template <class Key>
template <class T>
void MPIObjectArchive<Key>::broadcast_others(int tag, T const& val,
    bool check_alive) {
  std::vector<boost::mpi::request> reqs(world_.size());
  for (int i = 0; i < world_.size(); i++)
    if (alive_[i] || (!check_alive && i != world_.rank()))
      reqs[i] = world_.isend(i, tag, val);

  boost::mpi::wait_all(reqs.begin(), reqs.end());
}

template <class Key>
template <class T>
boost::optional<boost::mpi::status>
MPIObjectArchive<Key>::non_blocking_recv(int source, int tag, T& value) {
  boost::mpi::request req = world_.irecv(source, tag, value);

  while (1) {
    auto status_opt = req.test();
    if (status_opt) // If we got an answer, returns
      return status_opt;

    // If the source is a specific node and it died, returns empty.
    if (source != boost::mpi::any_source && !alive_[source]) {
      req.cancel();
      return boost::optional<boost::mpi::status>();
    }

    if (source == boost::mpi::any_source) {
      bool found_one_alive = false;
      for (int i = 0; i < world_.size(); i++)
        if (alive_[i]) {
          found_one_alive = true;
          break;
        }

      if (!found_one_alive) { // Nobody can send the data
        req.cancel();
        return boost::optional<boost::mpi::status>();
      }
    }

    handler_.run();
  }
}

#endif
