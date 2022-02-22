#!/bin/bash

mkdir -p build

cd build

cmake -D VNX_ROCKSDB_BUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fmax-errors=1" $@ ..

make -j8

