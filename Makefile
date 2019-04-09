CC=g++
CCFLAGS=-c -Wall -std=c++11 -I/usr/local/include -I./proto -lhdf5
LDFLAGS=-L/usr/local/lib -lgrpc++_unsecure -lgrpc -lprotobuf -lsqlite3 -lhdf5 -lpthread
FUSELDFLAGS=`pkg-config fuse3 --libs`
FUSECFLAGS=`pkg-config fuse3 --cflags`
SRC=tag_manual.c tag_auto.c search.c dtu.c


all : scifs mmu utils fsscan

fsscan	: fsscan.o
	$(CC) ./fsscan.o ./proto/unimd.grpc.pb.o ./proto/unimd.pb.o $(FUSELDFLAGS) $(LDFLAGS) -o fsscan

fsscan.o : protos
	$(CC) $(CCFLAGS) $(FUSECFLAGS) -o ./fsscan.o ./src/fsscan/fsscan.cc

scifs : scifs.o protoobj
	$(CC) ./scifs.o ./proto/mmu.grpc.pb.o ./proto/mmu.pb.o $(FUSELDFLAGS) $(LDFLAGS) -o scifs

scifs.o : protos
	$(CC) $(CCFLAGS) $(FUSECFLAGS) -o ./scifs.o ./src/scifs/scifs.cc

mmu : mmu.o protoobj
	$(CC) ./mmu.o ./proto/mmu.grpc.pb.o ./proto/mmu.pb.o $(LDFLAGS) -o mmu

mmu.o : protos
	$(CC) $(CCFLAGS) -o ./mmu.o ./src/mmu/mmu.cc

utils : utils.o
	$(CC) -o tag_manual ./tag_manual.o
	$(CC) -o tag_auto ./tag_auto.o
	$(CC) -o search ./search.o
	$(CC) -o dtu ./dtu.o

utils.o : $(SRC)
	$(CC) -c $(SRC)

protoobj : protos
	$(CC) $(CCFLAGS) -o ./proto/mmu.grpc.pb.o ./proto/mmu.grpc.pb.cc
	$(CC) $(CCFLAGS) -o ./proto/mmu.pb.o ./proto/mmu.pb.cc

protos :
	protoc -I ./proto/ --grpc_out=./proto --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` ./proto/mmu.proto
	protoc -I ./proto/ --cpp_out=./proto ./proto/mmu.proto

test: test.o protoobj
	$(CC) ./test.o ./proto/mmu.grpc.pb.o ./proto/mmu.pb.o $(FUSELDFLAGS) $(LDFLAGS) -o test

test.o: protos
	$(CC) $(CCFLAGS) $(FUSECFLAGS) -o ./test.o ./src/scifs/test.cc

clean :
	[ -x ./fsscan ] && rm fsscan
	[ -x ./fsscan.o ] && rm fsscan.o
	[ -x ./mmu ] && rm mmu
	[ -x ./scifs ] && rm scifs
	[ -f ./scifs.o ] && rm scifs.o
	[ -f ./mmu.o ] && rm mmu.o
	[ -f ./proto/mmu.grpc.pb.o ] && rm ./proto/mmu.grpc.pb.o
	[ -f ./proto/mmu.pb.o ] && rm ./proto/mmu.pb.o
	[ -f ./proto/mmu.grpc.pb.cc ] && rm ./proto/mmu.grpc.pb.cc
	[ -f ./proto/mmu.grpc.pb.h ] && rm ./proto/mmu.grpc.pb.h
	[ -f ./proto/mmu.pb.cc ] && rm ./proto/mmu.pb.cc
	[ -f ./proto/mmu.pb.h ] && rm ./proto/mmu.pb.h
	[ -f ./tag_manual ] && rm ./tag_manual
	[ -f ./tag_manual.o ] && rm ./tag_manual.o
	[ -f ./tag_auto ] && rm ./tag_auto
	[ -f ./tag_auto.o ] && rm ./tag_auto.o
	[ -f ./search ] && rm ./search
	[ -f ./search.o ] && rm ./search.o
	[ -f ./dtu ] && rm ./dtu
	[ -f ./dtu.o ] && rm ./dtu.o
	[ -f ./search_output.txt ] && rm ./search_output.txt
