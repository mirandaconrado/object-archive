#!/bin/sh

if [ ! -d build_normal ];
then
  mkdir build_normal;
  cd build_normal;
  cmake .. -DCMAKE_BUILD_TYPE=Debug
else
  cd build_normal;
fi;

make test
if [ $? != 0 ]
then
  exit
fi
cd ..

if [ ! -d build_threads ];
then
  mkdir build_threads;
  cd build_threads;
  cmake .. -DCMAKE_BUILD_TYPE=Debug -DENABLE_THREADS=True
else
  cd build_threads;
fi;

make test
if [ $? != 0 ]
then
  exit
fi
cd ..

if [ ! -d build_mpi ];
then
  mkdir build_mpi;
  cd build_mpi;
  cmake .. -DCMAKE_BUILD_TYPE=Debug -DENABLE_MPI=True
else
  cd build_mpi;
fi;

make test
if [ $? != 0 ]
then
  exit
fi
cd ..
