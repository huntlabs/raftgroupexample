#!/bin/sh
ulimit -c unlimited
killall -9 raftgroupexample
rm -rf entry.log*
rm -rf snap.log*
rm -rf raftgroupexample.log
./raftgroupexample 1 &
./raftgroupexample 2 &
./raftgroupexample 3 &
./raftgroupexample 4 &
