# Replica

## Introduction
Implementation of the Replica. Currently, the only functionality the Replica can perform is to heartbeat the LFD and receive a membership message. 

## Table of contents
<!--ts-->
   * [Dependencies](#dependencies)
   * [Running the code](#running)
   * [TODO](#todo)
<!--te-->

<a name="dependencies"></a>
## Dependencies
```
Python 3
```

<a name="running"></a>
## Running the code
- Start up the test LFD:
```
sudo python3 test_lfd.py
```
- Start up the replica(s):
```
python3 replica.py -i <heartbeat interval>
```
 The default port is 10000.

<a name="todo"></a>
## TODO
- Replica-to-replica communication
- Replica-to-client communication
- Total ordering
- Etc.
