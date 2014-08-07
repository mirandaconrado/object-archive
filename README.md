object-archive
==============

Simple C++ class to manage multiple objects storage. Requires boost and C++11.

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
