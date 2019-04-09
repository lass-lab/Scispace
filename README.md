# Scispace

The prototype of a collaboration friendly file system, to empower collaborators with several features, such as selective data export, unified data visualization and scientific search and discovery services on top of unified file system interface.

The library names and versions may differ based on your operating system. 

Pre-requisites
--------------

1. FUSE Library
   ```
   https://github.com/libfuse/libfuse
   ```
2. Grpc & Protobuf
   ```
   http://grpc.io
   ```
   Protobuf may be included in grpc
   ```
   https://developers.google.com/protocol-buffers
   ```
3. SQLite3 Library
   ``` 
   sudo apt-get install libsqlite3-dev
   ```
   or
   ```
   sudo yum install libsqlite3x-devel.x86_64

   ```
4. HDF5 Library
   ```
   https://support.hdfgroup.org/HDF5

   ```
5. C++ Boost Library

6. Extended Attribute attr & xattr Library
   ```
   ubuntu : libattr1-dev
   CentOS : libattr-devel.x86\_64
   ```
7. Lustre Environment

   lustre/lustreapi.h is needed for Data Transfer Nodes (DTUs) (DTU targets for LustreFS)

8. NUMA Library

   NUMA library such as libnuma-dev should be available for DTU


Setup Configurations
--------------------
1. Client
   
   - Add the network mounted data transfer nodes for unification
   ```
   loc.push_back("/mnt/DTN0");
   loc.push_back("/mnt/DTN1");
   ```
   - Add attributes of HDF5 files for auto extraction and indexing
   ```
   attrs.push_back("platform");
   attrs.push_back("processing_level");
   attrs.push_back("instrument");
   attrs.push_back("start_center_longitude");
   attrs.push_back("start_center_latitude");
   ```

   - Configure path for search ouput file
   ```
   out_path="/var/scifs/search_output.txt";
   ```

2. Data Transfer Node
   
   - Configure directory for actual data located in the data transfer node
   ```
   string SciFS_dir_location ("lustre1/scifs/DTN0/");
   ```
   - Configure Paths for Data transfer units
   ```
   string DTU_module_location ("/var/scifs/lads/src/");
   string DTU_dir_location ("/lustre1/scifs/DTU/");
   ```
3. Metadata Export Utility (MEU)

   - Configure directory path for fsscan utility to scan the contents and to add an extended attribute to tag synced files and directories
     ```
     loc.push_back("/home/foo/scifs/1");
     ```
   - Configure data transfer node address to sync metadata of files and directories stored via local namespace

3. Execute database scripts

   - Run create_mdsdb.sh to create database responsible for managing collaboration file system metadata such namespace controls, data transfer nodes informations, and file to path mappings etc.
   
   - Run create_sdsdb.sh to create scientific discovery script to create database responsible for managing all attribute definitions, and file to attribute mappings etc.
   

Compilation
-----------

1. Run Makefile

   - Compile server and client executable
   ```
   make
   ```

2. Run make clean

   - Removes the all executables
   ```
   make clean
   ```


Data Transfer Units
-------------------

1. Make directory for source/sink
  
   ```
   Make ./lads/src/src directory in source side
   Make ./lads/src/sink directory in sink side
   Then all files in ./lads/src/src will be sent to ./lads/src/sink
   ```

   As DTU is optimized for PFS by being aware of file chunks layout, it assumes all files are striped preliminarily

   Also, configure source and sink sides' core number in ./lads/src/source.c and ./lads/src/sink.c

2. make
   Currently, make DTU seperatly

   ```
   in ./lads/ directory
   1. ./autogen.sh
   2. ./configure --enable-debugger LDFLAGS='-pthread -lnuma' --enable-lustre-support
   3. make
   ```

Run
----
* SciFS client

   ```
   ./scifs [fuse options] [mount point]
   ```

* Distributed Metadata Managers

   - Iterate this on each data transfer node (DTN)
   ```
   ./create_msddb.sh
   ./create_sdsdb.sh
   ./mmu
   ```

Utilities for Scientific Discovery and Services
-----------------------------------------------
1. tag\_user
2. tag\_auto
3. search
4. dtu

Limitations
-----------

1. DTU
   - Make issue (SciFS and DTU's Makefiles are not unified yet)
   - Directory granularity issue (DTU can send file in unit of directory, but not implemented directory option now)

2. No integration of template namespace and metadata export utility in beta version. 

3. We do not have a single configuration file stating directory mount points etc rather you need to add it manually in beta version. 
