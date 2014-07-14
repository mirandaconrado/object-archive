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

Each object stored must be serializable through boost and the identifier must be
hashable. No hash collision prevention is provided! To free the buffer, a method
`unload()` is provided and, to ensure the objects are written to disk, a method
`defrag()` may be used. Destruction if an ObjectArchive automatically ensures
the files are stored in disk.

Current drawbacks are:

1. if an object is larger than the buffer size, it can't be stored or loaded.
This is easy to change, but protects against size bugs;
2. when the buffer needs space, it clears itself. A better approach would be to
use LRU policy, but it's much more complicated (somewhat fine timing is
required) and the OS tends to buffer part of the file, so it may be only a
memory copy.

Example of use:
```
ObjectArchive ar("path/to/file", "1.5G");
ar.insert("filename", filedata);
...
ar.load("filename", filedata);
...
ar.remove("filename");
```
