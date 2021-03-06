Object Archive
==============

Simple C++ class to manage multiple objects storage. Requires C++11 and boost.
It has "mpi-handler" as a submodule.

When storing multiple objects, the most basic approaches are storing each object
in a file, which requires the user to handle multiple files and possibly lots of
small ones, or storing all of them into a single file, which makes it hard to
keep track of where each file is located.

This library takes the latter approach, using a single file for all objects, and
handles the object positioning for the user. Moreover, a buffer with user-chosen
size is provided to allow further flexibility.

Each key and object stored must be serializable through boost and the key must
be hashable and comparable. To free part of the buffer, the method `unload()` is
provided and, to ensure the objects are written to disk, the method `flush()`
may be used. Destruction if an ObjectArchive automatically ensures the files are
stored in disk.

The default buffer size is zero, so no objects are kept in memory, and a
temporary file is used as backend. For permanent storage, the user must provide
its own filename to use.

Example of use:
```
ObjectArchive<std::string> ar;
ar.init("path/to/file");
ar.set_buffer_size("1.5G");

ar.insert("filename", filedata);
[do some stuff]
ar.load("filename", filedata);
[filedata has the previous value again]
ar.remove("filename");
[filedata keeps its value]
```

Threading
---------

If the archive will be used by multiple threads, ENABLE_THREADS should be set
during compilation.

MPI
---------

A class named MPIObjectArchive is provided and can be used like ObjectArchive,
but provides very loose consistency between archives in a MPI environment. This
allows the user to load objects without having to worry whether they are in the
current node or not.

The methods `insert`, `load` and `remove` are used just as in the original
ObjectArchive, but transfers the data between nodes to keep the consistency.
Loaded data are stored locally for caching. The user can call `mpi_process` when
the node is idle to process messages from other nodes.

The communication is based on MPI tags, and the user can provide his own values
so that they don't conflict with the rest of the application. The user can also
provide a filter that chooses whether values inserted in remote nodes should
also be stored locally based on the key. This can be useful to store values to
be used in a latter time when the other node may not be available.

All messages are processed using a MPIHandler, which must be provided at
construction. The handler's method `run()` inside the handler is automatically
called at some places for consistency.

Example of use:
```
boost::mpi::communicator world;
MPIHandler handler;
MPIObjectArchive<std::string> ar(world, handler);
ar.init("path/to/file");
ar.set_buffer_size("1.5G");

ar.insert("filename", filedata);
[do some stuff]
ar.load("filename", filedata);
[filedata has the previous value again]
ar.remove("filename");
[filedata keeps its value]
```
