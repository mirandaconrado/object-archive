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

#ifndef __OBJECT_ARCHIVE_MPI_IMPL_HPP__
#define __OBJECT_ARCHIVE_MPI_IMPL_HPP__

#include <functional>

#include "object_archive_mpi.hpp"

template <class Key>
MPIObjectArchive<Key>::MPIObjectArchive(boost::mpi::communicator& world,
    MPIHandler& handler):
  MPIObjectArchive(Tags(), world, handler) { }

template <class Key>
MPIObjectArchive<Key>::MPIObjectArchive(Tags const& tags,
    boost::mpi::communicator& world, MPIHandler& handler):
  ObjectArchive<Key>(),
  tags_(tags),
  world_(world),
  handler_(handler),
  alive_(world.size(), false),
  request_counter_(0) {
    clear_insert_filter();

    handler.insert(tags_.alive,
        std::bind(&MPIObjectArchive<Key>::process_alive, this,
          std::placeholders::_1, tags.alive));
    handler.insert(tags_.invalidated,
        std::bind(&MPIObjectArchive<Key>::process_invalidated, this,
          std::placeholders::_1, tags.invalidated));
    handler.insert(tags_.inserted,
        std::bind(&MPIObjectArchive<Key>::process_inserted, this,
          std::placeholders::_1, tags.inserted));
    handler.insert(tags_.change_key,
        std::bind(&MPIObjectArchive<Key>::process_change_key, this,
          std::placeholders::_1, tags.change_key));
    handler.insert(tags_.request,
        std::bind(&MPIObjectArchive<Key>::process_request, this,
          std::placeholders::_1, tags.request));
    handler.insert(tags_.response,
        std::bind(&MPIObjectArchive<Key>::process_response, this,
          std::placeholders::_1, tags.response));
    handler.insert(tags_.request_data,
        std::bind(&MPIObjectArchive<Key>::process_request_data, this,
          std::placeholders::_1, tags.request_data));
    handler.insert(tags_.response_data,
        std::bind(&MPIObjectArchive<Key>::process_response_data, this,
          std::placeholders::_1, tags.response_data));

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
void MPIObjectArchive<Key>::change_key(Key const& old_key, Key const& new_key) {
  handler_.run();

  OBJECT_ARCHIVE_MUTEX_GUARD;

  KeyPair pair;
  pair.old_key = old_key;
  pair.new_key = new_key;

  broadcast_others(tags_.change_key, pair);

  ObjectArchive<Key>::change_key(old_key, new_key);
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
void MPIObjectArchive<Key>::set_insert_filter(filter_type filter) {
  remote_insert_filter_ = filter;
}

template <class Key>
void MPIObjectArchive<Key>::clear_insert_filter() {
  remote_insert_filter_ =
    [](Key const&, boost::mpi::communicator&) { return false; };
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
bool MPIObjectArchive<Key>::process_change_key(int source, int tag) {
  KeyPair pair;
  world_.recv(source, tag, pair);

  ObjectArchive<Key>::change_key(pair.old_key, pair.new_key);
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
bool MPIObjectArchive<Key>::process_response(int source, int tag) {
  Response response;
  world_.recv(source, tag, response);

  if (alive_requests_.count(response.request) > 0) {
    Request* req = alive_requests_[response.request];
    requests_waiting_[req]--;

    if (response.found)
      requests_found_[req] = source;
  }

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
bool MPIObjectArchive<Key>::process_response_data(int source, int tag) {
  ResponseData response_data;
  world_.recv(source, tag, response_data);

  if (alive_requests_.count(response_data.request) > 0) {
    Request* req = alive_requests_[response_data.request];
    requests_found_[req] = source;
    responses_data_valid_[req] = response_data.valid;
    if (response_data.valid)
      responses_data_[req] = response_data.data;
  }

  return true;
}

template <class Key>
boost::optional<std::string> MPIObjectArchive<Key>::get_response(int source,
    int n_waiting, Request& request) {
  boost::optional<std::string> ret;

  alive_requests_[request] = &request;
  requests_source_[&request] = source;
  requests_waiting_[&request] = n_waiting;

  // Loops trying to get a response and processing everyone until ours is found
  // or nobody got the data.
  while (requests_waiting_[&request] > 0 &&
         requests_found_.count(&request) == 0)
    handler_.run();

  // If found somewhere, gets the data.
  if (requests_found_.count(&request) > 0) {
    int source = requests_found_[&request];

    world_.send(source, tags_.request_data, request);

    // Runs until gets a response or source dies.
    while (responses_data_valid_.count(&request) == 0 && alive_[source])
      handler_.run();

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

#endif
