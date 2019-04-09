
#define FUSE_USE_VERSION 30

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite()/utimensat() */
#define _XOPEN_SOURCE 700
#endif

#include <cstdlib>
#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/wait.h>
//#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
//#endif

#include <map>
#include <string>
#include <algorithm>
#include <vector>
#include <list>
#include <thread>

#include <grpc/grpc.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "mmu.grpc.pb.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <boost/tokenizer.hpp>

#define RPCALL ((AgMetaDataClient*)(fuse_get_context()->private_data))

#define LOCAL_DC 0  // It should be moved to config file

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::Status;
using mmu::Path;
using mmu::Location;
using mmu::AgMetaData;
using mmu::Batch;
using mmu::Attr;
using mmu::LocPath;
using mmu::DTU_Path;
using mmu::Sdata;

using std::vector;
using std::string;
using std::list;
using std::map;
using std::pair;

/* ------ Configurations ------- */
#define CLICACHE false
#define BATCH_MODE false
#define DMD true


/* ------ Global Values ------ */
int BATCH_LIMIT = 1200;
int CACHE_SIZE = 6000;

bool auto_flag=false;
string out_path;
list<string> mknod_list;
vector<string> attrs;

//////// temp
////int fd_global;
////////

class AgMetaDataClient {
  public:
    
  int get_locations_size() {
    return (int)locations.size();
  }

  int compute_location (string path)  
  {
//    std::cout << " Compute Location " <<std::endl;
    int loc_ = 0 ; size_t loc_db =  std::hash<string>{}(path); 
//    std :: cout << " hash_computation " << loc_db << std::endl;
    
//    std::cout << "path is : " << path << std::endl;

    //printf("[In compute_location] locations.size : %d\n", locations.size());
    loc_ =  loc_db % get_locations_size();

    return loc_;
  }

  AgMetaDataClient(std::shared_ptr<Channel> channel, vector<string> loc) : stub_(AgMetaData::NewStub(channel)) {
    locations = loc;
  }
  
  AgMetaDataClient(std::shared_ptr<Channel> channel, std::shared_ptr<Channel> channel1, vector<string> loc) : stub_(AgMetaData::NewStub(channel)), stub_1(AgMetaData::NewStub(channel1)) {
    locations = loc;
  }

  AgMetaDataClient(std::shared_ptr<Channel> channel, std::shared_ptr<Channel> channel1, std::shared_ptr<Channel> channel2, vector<string> loc) : stub_(AgMetaData::NewStub(channel)), stub_1(AgMetaData::NewStub(channel1)),stub_2(AgMetaData::NewStub(channel2)) {
    locations = loc;
  }   

  AgMetaDataClient(std::shared_ptr<Channel> channel, std::shared_ptr<Channel> channel1, std::shared_ptr<Channel> channel2, std::shared_ptr<Channel> channel3, vector<string> loc) : stub_(AgMetaData::NewStub(channel)), stub_1(AgMetaData::NewStub(channel1)), stub_2(AgMetaData::NewStub(channel2)), stub_3(AgMetaData::NewStub(channel3)) {
    locations = loc;
  }

/////IndexAFile/////
  int IndexAFile(const char* path){
    ClientContext context;
    mmu::Empty empty;
    LocPath lp;
    lp.set_path(path);

    for(int i=0; i<(int)attrs.size(); i++)
      lp.add_attrs(attrs[i]);

    Status status;
    int res = ResolvePath(path);
    lp.set_loc(locations[res]);
    if ( res == 0 )
      status = stub_->IndexAFile(&context, lp, &empty);
    else if ( res == 1 )
      status = stub_1->IndexAFile(&context, lp, &empty);
    else if ( res == 2 )
      status = stub_2->IndexAFile(&context, lp, &empty);
    else if ( res == 3 )
      status = stub_3->IndexAFile(&context, lp, &empty);
    
    return 0;
  }

/////RemoveIndex/////
  int RemoveIndex(const char* path){
    ClientContext context;
    mmu::Empty empty;

    Path path_;
    path_.set_path(path);

    Status status;
    int res = ResolvePath(path);

    if ( res == 0 )
      status = stub_->RemoveIndex(&context, path_, &empty);
    else if ( res == 1 )
      status = stub_1->RemoveIndex(&context, path_, &empty);
    else if ( res == 2 )
      status = stub_2->RemoveIndex(&context, path_, &empty);
    else if ( res == 3) 
      status = stub_3->RemoveIndex(&context, path_, &empty);

    return 0;
  }

/////AutoIndex/////
  void Thread_AutoIndex(int location, LocPath attr){
    ClientContext context;
    mmu::Empty out;
    Status status;

    if (location == 0)
      status = stub_->AutoIndex(&context, attr, &out);
    else if (location == 1)
      status = stub_1->AutoIndex(&context, attr, &out);
    else if (location == 2)
      status = stub_2->AutoIndex(&context, attr, &out);
    else if (location == 3)
      status = stub_3->AutoIndex(&context, attr, &out);
  }


  int AutoIndex(const char* path, const char* key, const char *value){
    LocPath attr;

    for(int i=0; i<(int)attrs.size(); i++)
      attr.add_attrs(attrs[i]);
    

    if(auto_flag == false)
      auto_flag = true;

    std::thread t1 ( [=] {Thread_AutoIndex(0, attr);} );
    std::thread t2 ( [=] {Thread_AutoIndex(1, attr);} );
    //std::thread t3 ( [=] {Thread_AutoIndex(2, attr);} );
    //std::thread t4 ( [=] {Thread_AutoIndex(3, attr);} );

    t1.join();
    t2.join();
    //t3.join();
    //t4.join();

    return 0;
  }

////////////////////
// Search by xattr//
  void Thread_Search(int location, Sdata sdata, Sdata *ret){
    ClientContext context;

    if ( location == 0 )
      Status status = stub_->DoSearch(&context, sdata, ret);
    else if ( location == 1 )
      Status status = stub_1->DoSearch(&context, sdata, ret);
    else if ( location == 2 )
      Status status = stub_2->DoSearch(&context, sdata, ret);
    else if ( location == 3 )
      Status status = stub_3->DoSearch(&context, sdata, ret);
      
  }

  int Search(const char* path, const char* key, const char* value){

    ClientContext context1, context2;  ///// One context variable makes error!
                                      ///// Make it as the number of DCs.
    Sdata sdata;
    Sdata ret0, ret1, ret2, ret3;
//    Status status0, status1;
    string searchstr(value);

//    std::cout << searchstr << std::endl;

    boost::char_separator<char> sep("\\");
    boost::tokenizer<boost::char_separator<char>> tokens(searchstr, sep);
    boost::tokenizer<boost::char_separator<char>>::iterator tok_iter = tokens.begin();

    sdata.set_key(*tok_iter);
    tok_iter++;
    sdata.set_condition(*tok_iter);
    tok_iter++;
    sdata.set_value(*tok_iter);

    std::thread t1 ( [&] {Thread_Search(0, sdata, &ret0);} );
    std::thread t2 ( [&] {Thread_Search(1, sdata, &ret1);} );
    //std::thread t3 ( [&] {Thread_Search(2, sdata, &ret2);} );
    //std::thread t4 ( [&] {Thread_Search(3, sdata, &ret3);} );

    t1.join();
    t2.join();
    //t3.join();
    //t4.join();

//    if( !status0.ok() | !status1.ok() )
//      return -1;  

    std::ofstream outFile(out_path);
    if(!outFile.is_open()){
      std::cout << "File open error\n";
      return -1;
    }

    for(int j=0; j < ret0.path_size(); j++)
        outFile<< ret0.path(j) <<std::endl;

    for(int j=0; j < ret1.path_size(); j++)
        outFile<< ret1.path(j) <<std::endl;

    outFile.close();

    return 0;
  }

//////////////////
  int UserIndex(const char* path, const char* key, const char* value){
    string tagstr(value);

//    std::cout << tagstr << std::endl;

    boost::char_separator<char> sep("\\");
    boost::tokenizer<boost::char_separator<char>> tokens(tagstr, sep);
    boost::tokenizer<boost::char_separator<char>>::iterator tok_iter = tokens.begin();

    ClientContext context;
    mmu::Empty empty;

    Attr attr;
    attr.set_key(*tok_iter);
    tok_iter++;
    attr.set_type(*tok_iter);
    tok_iter++;
    
    attr.set_val(*tok_iter);
    attr.set_path(path);

    Status status;
    int res=ResolvePath(path);
    
    if ( res == 0 )
      status = stub_->UserIndex(&context, attr, &empty);
    else if ( res == 1 )
      status = stub_1->UserIndex(&context, attr, &empty);
    else if ( res == 2 )
      status = stub_2->UserIndex(&context, attr, &empty);
    else if ( res == 3 )
      status = stub_3->UserIndex(&context, attr, &empty);
    else
      return -1;
    
    if(status.ok())
      return 0;
    else
      return -1;
  }

  ////////// Trigger DTU src on source side's MMU with given verb uri
  int IncurDTU_src(const char* path, string uri, int loc_data){
//    std::cout << "\n[IncurDTU_src] received uri : " << uri << std::endl;
    ClientContext context1, context2;
    mmu::Empty empty;

    DTU_Path path_;
    path_.set_path(path);
    path_.set_uri(uri);

    Status status1, status2;

    ////// TODO : modify hard code.... make stubs as array and change if&else if to for
    if( loc_data == 0 )
      status1 = stub_->DTU_src(&context1, path_, &empty);
    else if( loc_data == 1 )
      status1 = stub_1->DTU_src(&context1, path_, &empty);
    else if( loc_data == 2 )
      status1 = stub_2->DTU_src(&context1, path_, &empty);
    else if( loc_data == 3 )
      status1 = stub_3->DTU_src(&context1, path_, &empty);
    //else if( loc_data == 2 )
    //  status1 = stub_2->DTU_src(&context1, path_, &empty);

    int loc_shard = compute_location(string(path));
    mmu::Path path_dtu;   ////// path : transferred file's path in SciFS
                          ////// loc : file's destination DTN's index
                          //////       Or -1 when the transmission is finished
    path_dtu.set_path(path);
    path_dtu.set_loc(-1);
  
    ///////// Unset the duringDTU flag
    if( loc_shard == 0 )
      status2 = stub_->DTU_mark(&context2, path_dtu, &empty);
    else if( loc_shard == 1 )
      status2 = stub_1->DTU_mark(&context2, path_dtu, &empty);
    else if( loc_shard == 2 )
      status2 = stub_2->DTU_mark(&context2, path_dtu, &empty);
    else if( loc_shard == 3 )
      status2 = stub_3->DTU_mark(&context2, path_dtu, &empty);
    //else if( loc_shard == 2 )
    //  status2 = stub_2->DTU_mark(&context2, path_dtu, &empty);

    if (status1.ok())
      return 0;
    else
      return -1;
  }

  ///////// Trigger DTU sink on sink's MMU and return the verb uri
  string IncurDTU_sink(const char* path, int DTN_index, int *loc_data) {
//    std::cout << "\n[IncurDTU] dest DTN : " << DTN_index << "\n" << std::endl;
    ClientContext context1, context2;
    mmu::Empty empty;

    DTU_Path path_;
    path_.set_path(path);

    Status status1, status2;
    int idx = ResolvePath(path);
    *loc_data = idx;

    /* In here, location DB will come */
    int loc_shard = compute_location(string(path));

    mmu::Path path_dtu;
    path_dtu.set_path(path);
    path_dtu.set_loc(DTN_index);

    ///////// Change the location of file and set the DTU flag to 1
    ////// TODO : modify hard code.... make stubs as array and change if&else if to for
    if ( loc_shard == 0 ) 
      status1 = stub_->DTU_mark (&context1, path_dtu, &empty);
    else if ( loc_shard == 1 )
      status1 = stub_1->DTU_mark (&context1, path_dtu, &empty);
    else if ( loc_shard == 2 )
      status1 = stub_2->DTU_mark (&context1, path_dtu, &empty);
    else if ( loc_shard == 3 )
      status1 = stub_3->DTU_mark (&context1, path_dtu, &empty);

    ////// TODO : modify hard code.... make stubs as array and change if&else if to for
    if( DTN_index == 0 )
      status2 = stub_->DTU_sink(&context2, path_, &path_);
    else if( DTN_index == 1 ) 
      status2 = stub_1->DTU_sink(&context2, path_, &path_);
    else if( DTN_index == 2 ) 
      status2 = stub_2->DTU_sink(&context2, path_, &path_);
    else if( DTN_index == 3 ) 
      status2 = stub_3->DTU_sink(&context2, path_, &path_);
      //std::cout << "returned uri : " << path_.uri() << std::endl;
    
    return path_.uri();
  }


  string MakePath(const char* path, int n) 
  {
    return locations[n] + string(path);
  }

  int ResolvePath(const char* path) 
  {
    if(CLICACHE) {
      auto it = client_cache.find(string(path));
      if(it!=client_cache.end()) {
        return it->second;
      }
    }
    ClientContext context;
    Path path_;
    Location loc;
    path_.set_path(path);
    
    ///// Decide the location of location shard
    int loc_db = compute_location(string(path));
    //path_.set_loc(loc_db);


    /////////// For testing single mmu
    //loc_db = 0;
    ///////////

    Status status;
    if (loc_db == 0)
      status = stub_->ResolvePath(&context, path_, &loc);
    else if (loc_db == 1)
      status = stub_1->ResolvePath(&context, path_, &loc);
    else if (loc_db == 2)
      status = stub_2->ResolvePath(&context, path_, &loc);
    else if (loc_db == 3)
      status = stub_3->ResolvePath(&context, path_, &loc);

    if (status.ok()) {
      if (CLICACHE){
        if (client_cache.size() > (unsigned)CACHE_SIZE)
          client_cache.clear();
        client_cache.insert(pair<string,int>(string(path),loc.loc()));
      }
      return loc.loc();
    }
    else {
      return -1;
    }
  }

  void Thread_Batch(int location){
    ClientContext context;
    Status status;
    mmu::Empty empty_;
    Batch batch_;
    string batch_sql = "INSERT INTO locations (path, locationID, duringDTU) VALUES ";
    string batch_value ("");

    for (auto it = io_batch[location].cbegin(); it !=  io_batch[location].cend(); it++)
      batch_value += "('" + string(it->first) + "', " + std::to_string(it->second) + ",0),";

    batch_sql = batch_sql + batch_value;
	  batch_sql.replace(batch_sql.end()-1,batch_sql.end(),";");

    batch_.set_batch_sql(batch_sql);

    if ( location == 0 )
      status = stub_->CreateBatchMetadata(&context, batch_, &empty_);
    else if ( location == 1 )
      status = stub_1->CreateBatchMetadata(&context, batch_, &empty_);
    else if ( location == 2 )
      status = stub_2->CreateBatchMetadata(&context, batch_, &empty_);
    else if ( location == 3 )
      status = stub_3->CreateBatchMetadata(&context, batch_, &empty_);

    if (status.ok()) {
      io_batch[location].clear();
      std::cout << " *** BATCH PUSHED *** " << std::endl;
    }

  }

  int CreateMetadata(const char* path, int loc_desire) 
  {
    if (BATCH_MODE) {		
      int loc_id = compute_location(string(path));

      io_batch[loc_id].insert (pair<string,int>(string(path), loc_id));
      client_cache.insert(pair<string,int>(string(path),loc_id));

      int batch_size = 0;
      for (int i = 0; i < get_locations_size(); i++)
        batch_size += io_batch[i].size();

      if ( batch_size >= (unsigned)BATCH_LIMIT ) {
        std::thread t1 ( [&] { Thread_Batch(0); } );
        std::thread t2 ( [&] { Thread_Batch(1); } );
        //std::thread t3 ( [&] { Thread_Batch(2); } );
        //std::thread t4 ( [&] { Thread_Batch(3); } );

        t1.join();
        t2.join();
        //t3.join();
        //t4.join();
      }

      return loc_id;
    }
    else {
      std:: cout << " \n *** SINGLE IO INTITATED " << path << std::endl;
      ClientContext context;
      Path path_;
      mmu::Empty empty_;
      Location loc;
      path_.set_path(path);
      int ret = -1;

      int loc_db = compute_location(string(path));
      
      ///////// For testing single mmu
      //loc_db = 0;
      /////////

      path_.set_loc(loc_desire);
      ret = loc_desire;

      if (loc_desire == -1) {
        //path_.set_loc(loc_db);  /////// This is for batch mode comparison
        path_.set_loc(LOCAL_DC);  // Parameter is the local DC index.
                                /// For node02, cause it is a client belong to DC1,
                                /// so location becomes 1.
        //ret = loc_db;
        ret = LOCAL_DC;
      }

      Status status;
      if (loc_db == 0)
        status = stub_->CreateDistributedMetadata(&context, path_, &empty_);
      else if (loc_db == 1)
        status = stub_1->CreateDistributedMetadata(&context, path_, &empty_);
      else if (loc_db == 2) 
        status = stub_2->CreateDistributedMetadata(&context, path_, &empty_);
      else if (loc_db == 3)
        status = stub_3->CreateDistributedMetadata(&context, path_, &empty_);

      if(status.ok()) 
        return ret;
      else 
       return -1;
    }
  }

  int RemoveMetadata(const char* path) 
  {
    if (BATCH_MODE) {
      auto it = client_cache.find(string(path));
      if (it!=client_cache.end())
        return 0;
      else
        return -1;
        
    }
    //else {
    ClientContext context;
    Path path_;
    mmu::Empty empty_;
    path_.set_path(path);
    Status status;

    int loc_db = compute_location(string(path));

    if (loc_db == 0) 
      status = stub_->RemoveMetadata(&context, path_, &empty_);
    else if (loc_db == 1) 
      status = stub_1->RemoveMetadata(&context, path_, &empty_);
    else if (loc_db == 2) 
      status = stub_2->RemoveMetadata(&context, path_, &empty_);
    else if (loc_db == 3) 
      status = stub_3->RemoveMetadata(&context, path_, &empty_);
        
    if (status.ok()) return 0;
    else return -1;
    //}
  }

  private:

  std::unique_ptr<AgMetaData::Stub> stub_;
  std::unique_ptr<AgMetaData::Stub> stub_1;
  std::unique_ptr<AgMetaData::Stub> stub_2;
  std::unique_ptr<AgMetaData::Stub> stub_3;

  vector<string> locations;
  map<string, int> client_cache;
  map<string, int> *io_batch = new map<string, int> [4];
  int tagger = 0 ;

};


static int scifs_getattr(const char *path, struct stat *stbuf, fuse_file_info *fi)
{
  int res;
  string rpath;

  res = RPCALL->ResolvePath(path);
  if (res == -1) res = 0;
  rpath = RPCALL->MakePath(path, res);

  res = lstat(rpath.c_str(), stbuf);
  if (res == -1)
    return -errno;

  return 0;
}

static int scifs_access(const char *path, int mask)
{
  int res;
  string rpath;
  res = RPCALL->ResolvePath(path);
  if (res == -1) res = 0;
  rpath = RPCALL->MakePath(path,res);
  res = access(rpath.c_str(), mask);
  if (res == -1)
    return -errno;

  return 0;
}

static int scifs_readlink(const char *path, char *buf, size_t size)
{
    int res;
    string rpath;
    res = RPCALL->ResolvePath(path);
    if (res == -1) res = 0;
    rpath = RPCALL->MakePath(path,res);
    res = readlink(rpath.c_str(), buf, size - 1);
    if (res == -1)
        return -errno;

    buf[res] = '\0';
    return 0;
}


static int scifs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
        off_t offset, struct fuse_file_info *fi,
        enum fuse_readdir_flags flags)
{
  DIR *dp;
  struct dirent *de;

  (void) offset;
  (void) fi;
  (void) flags;

  for(int i=0; i < RPCALL->get_locations_size(); i++) {
    string rpath = RPCALL->MakePath(path, i);
    dp = opendir(rpath.c_str());
    if (dp == NULL)
      return -errno;
    while ((de = readdir(dp)) != NULL) {
      struct stat st;
      memset(&st, 0, sizeof(st));
      st.st_ino = de->d_ino;
      st.st_mode = de->d_type << 12;
      if(de->d_type == DT_DIR && i>0) continue;
      if (filler(buf, de->d_name, &st, 0,(enum fuse_fill_dir_flags)0))
        break;
    }

    closedir(dp);
  }

  return 0;
}

static int scifs_mknod(const char *path, mode_t mode, dev_t rdev)
{
  int res;
  string rpath;
  res = RPCALL->ResolvePath(path);
  if (res == -1){
    res = RPCALL->CreateMetadata(path, -1);
  } 
  else {
    puts("DUPLICATE PATH IN MKNOD");
    //return res;
  }
  if (res == -1) {
    // err
    return -EAGAIN;
  }
  rpath = RPCALL->MakePath(path,res);

  /* On Linux this could just be 'mknod(path, mode, rdev)' but this
     is more portable */
  if (S_ISREG(mode)) {
    res = open(rpath.c_str(), O_CREAT | O_EXCL | O_WRONLY, mode);
    if (res >= 0)
      res = close(res);

    if( auto_flag == true ){
      mknod_list.push_back(string(path));
      //std::cout << "mknod_list_push_back\n";
    }
  }
  else if (S_ISFIFO(mode))
    res = mkfifo(rpath.c_str(), mode);
  else
    res = mknod(rpath.c_str(), mode, rdev);
  if (res == -1) {
    res = RPCALL->RemoveMetadata(path);
    if(res == -1) puts("POSSIBLE INCONSISTENCY");
    return -errno;
  }

  return 0;
}

static int scifs_mkdir(const char *path, mode_t mode)
{
  int res;
  string rpath;
    
  int i;
  for (i=0; i < RPCALL->get_locations_size(); i++) {
    rpath = RPCALL->MakePath(path,i);
    res = mkdir(rpath.c_str(), mode);
    if (res == -1) 
      break;
  }

  if (res == -1) {
    for (i--;i>=0;i--) {
      rpath = RPCALL->MakePath(path,i);
      rmdir(rpath.c_str());
    }
    return -errno;
  }
  return 0;
}

static int scifs_unlink(const char *path)
{
  int res;
  string rpath;
  res = RPCALL->ResolvePath(path);

  if (res == -1)
    return -EAGAIN;
  rpath = RPCALL->MakePath(path,res);

  res = RPCALL->RemoveMetadata(path);
  if (res == -1){
    //err
    puts("metadata inconsistency alrt");
  }

  res = unlink(rpath.c_str());
  if (res == -1)
    return -errno;

  res = RPCALL->RemoveIndex(path);

  return 0;
}

static int scifs_rmdir(const char *path)
{
  int res;
  string rpath;
 // std::cout << "\n REMOVE INVOKED ::" << path << std::endl;
 
  for (int i=0; i<RPCALL->get_locations_size(); i++) 
  {
    rpath = RPCALL->MakePath(path,i);
   // std::cout << " AFTER MAKEPATH " <<std::endl;
    res = rmdir(rpath.c_str());
   // std::cout << " AFTER RMDIR " <<std::endl;
    if (res == -1)
      return -errno;
  }
  return 0;
}

static int scifs_symlink(const char *from, const char *to)
{
  int res;
  string rto;
  res = RPCALL->CreateMetadata(to, -1);
  if(res==-1) return -EAGAIN;
  rto = RPCALL->MakePath(to,res);

  res = symlink(from, rto.c_str());
  if (res == -1)
    return -errno;

  return 0;
}

static int scifs_rename(const char *from, const char *to, unsigned int flags)
{
    int res;
    if (flags)
      return -EINVAL;

    string rfrom;
    res = RPCALL->ResolvePath(from);
    if(res == -1) return -EAGAIN;
    rfrom = RPCALL->MakePath(from, res);
    string rto;
    res = RPCALL->CreateMetadata(to, -1);
    if(res == -1) return -EAGAIN;
    rto = RPCALL->MakePath(to, res);

	res = rename(rfrom.c_str(), rto.c_str());
	if (res == -1)
		return -errno;
    res = RPCALL->RemoveMetadata(from);
    if(res == -1) puts("POSSIBLE INCONSISTENCY");
	return 0;
}

static int scifs_link(const char *from, const char *to)
{
	int res;
    string rfrom;
    res = RPCALL->ResolvePath(from);
    if(res == -1) return -EAGAIN;
    rfrom = RPCALL->MakePath(from, res);
    string rto;
    res = RPCALL->CreateMetadata(to, -1);
    if(res == -1) return -EAGAIN;
    rto = RPCALL->MakePath(to, res);

	res = link(rfrom.c_str(), rto.c_str());
	if (res == -1)
		return -errno;

	return 0;
}

static int scifs_chmod(const char *path, mode_t mode, fuse_file_info *fi)
{
	int res;
    string rpath;
    res = RPCALL->ResolvePath(path);
    if(res == -1) return -EAGAIN;
    rpath = RPCALL->MakePath(path, res);
	res = chmod(rpath.c_str(), mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int scifs_chown(const char *path, uid_t uid, gid_t gid, fuse_file_info *fi)
{
  int res;
  string rpath;
  res = RPCALL->ResolvePath(path);
  if(res == -1) return -EAGAIN;
  rpath = RPCALL->MakePath(path, res);

  res = lchown(rpath.c_str(), uid, gid);
  if (res == -1)
    return -errno;

  return 0;
}

static int scifs_truncate(const char *path, off_t size, fuse_file_info *fi)
{
  int res;
  string rpath;
  res = RPCALL->ResolvePath(path);
  if(res == -1) return -EAGAIN;
  rpath = RPCALL->MakePath(path, res);

  res = truncate(rpath.c_str(), size);
  if (res == -1)
    return -errno;

  return 0;
}

#ifdef HAVE_UTIMENSAT
static int scifs_utimens(const char *path, const struct timespec ts[2])
{
	int res;
    string rpath;
    res = RPCALL->ResolvePath(path);
    if(res == -1) return -EAGAIN;
    rpath = RPCALL->MakePath(path, res);
	/* don't use utime/utimes since they follow symlinks */
	res = utimensat(0, rpath.c_str(), ts, AT_SYMLINK_NOFOLLOW);
	if (res == -1)
		return -errno;

	return 0;
}
#endif

static int scifs_open(const char *path, struct fuse_file_info *fi)
{
  int res;
  string rpath;
  res = RPCALL->ResolvePath(path);
  if(res == -1) return -EAGAIN;
  rpath = RPCALL->MakePath(path, res);

  res = open(rpath.c_str(), fi->flags);

  //////// For test
  ////fd_global = res;
  ////////

  if (res == -1)
    return -errno;

  close(res);
  return 0;
}

static int scifs_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
  int fd;
  int res;

  string rpath;
  res = RPCALL->ResolvePath(path);
  ////res = 0;
  if(res == -1) return -EAGAIN;
  rpath = RPCALL->MakePath(path, res);
  (void) fi;

  fd = open(rpath.c_str(), O_RDONLY);
  if (fd == -1)
    return -errno;

  res = pread(fd, buf, size, offset);
  ////res = pread(fd_global, buf, size, offset);
  if (res == -1)
    res = -errno;

  close(fd);
  return res;
}

static int scifs_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
  int fd;
  int res;

  string rpath;
  res = RPCALL->ResolvePath(path);
  ////res = 0;
  if(res == -1) return -EAGAIN;
  rpath = RPCALL->MakePath(path, res);
  (void) fi;

  fd = open(rpath.c_str(), O_WRONLY);
  if (fd == -1)
    return -errno;

  //std::cout << size << std::endl;

  res = pwrite(fd, buf, size, offset);
  ////res = pwrite(fd_global, buf, size, offset);
  if (res == -1)
    res = -errno;

  close(fd);
  return res;
}

static int scifs_statfs(const char *path, struct statvfs *stbuf)
{
  int res;
  // need care
  string rpath;
  res = RPCALL->ResolvePath(path);
  if(res == -1) return -EAGAIN;
  rpath = RPCALL->MakePath(path, res);
  res = statvfs(rpath.c_str(), stbuf);

  if (res == -1)
    return -errno;

  return 0;
}

static int scifs_release(const char *path, struct fuse_file_info *fi)
{
  /* Just a stub. This method is optional and can safely be left
     unimplemented */

 (void) path;
 (void) fi;

 // std::cout<<"%#%#%#%#RELEASE#%#%#%#%\n";

  string s(path);
  if(!mknod_list.empty()){
    list<string>::iterator li;

    for(li=mknod_list.begin(); li!=mknod_list.end(); ){
      //std::cout << "release: " << *li;
      if( *li == s ){
        //std::cout<< "Mknod-Release: "<< path << std::endl;
        int res = RPCALL->IndexAFile(path); 
        mknod_list.erase(li++);
      }
      li++;
    }
  }

  (void) path;
  (void) fi;

  return 0;
}

static int scifs_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	/* Just a stub.	 This method is optional and can safely be left
	   unimplemented */

	(void) path;
	(void) isdatasync;
	(void) fi;
	return 0;
}

#ifdef HAVE_POSIX_FALLOCATE
static int scifs_fallocate(const char *path, int mode,
			off_t offset, off_t length, struct fuse_file_info *fi)
{
	int fd;
	int res;

	(void) fi;

	if (mode)
		return -EOPNOTSUPP;

    string rpath;
    res = RPCALL->ResolvePath(path);
    if(res == -1) return -EAGAIN;
    rpath = RPCALL->MakePath(path, res);
	fd = open(rpath.c_str(), O_WRONLY);
	if (fd == -1)
		return -errno;

	res = -posix_fallocate(fd, offset, length);

	close(fd);
	return res;
}
#endif

//#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int scifs_setxattr(const char *path, const char *name, const char *value,
			size_t size, int flags)
{
    int res, temp, count = 0;
    string rpath;

    if( strcmp(name,"user.tag")==0 )
      temp = RPCALL->UserIndex(path, name, value);
    else if( strcmp(name, "user.auto")==0 )
      temp = RPCALL->AutoIndex(path, name, value);
    else if( strcmp(name, "user.search")==0 ) 
      temp = RPCALL->Search(path, name, value);
    else if ( strcmp(name, "user.dtu") == 0 ) {
      /*int pid;
      pid = fork();
      if (pid == 0) {
        std::system("/var/scifs/lads/src/zs_sink");
      }
      else if (pid > 0) {
        int status;
        sleep(1);
        std::cout << "\n\nURI is \n\n" << std::ifstream("/var/scifs/lads/src/uri").rdbuf() << std::endl;
        pid_t ws = waitpid(pid, &status, 0);
      }*/
      //int DTU_index = atoi(value);
      int DTU_index, src_loc;
      std::stringstream aux;
      string uri;

      aux << value;
      aux >> DTU_index;

      //std::cout << "\nDTU in agu, " << path << "," << DTU_index << std::endl;
      uri = RPCALL->IncurDTU_sink(path, DTU_index, &src_loc);
      temp = RPCALL->IncurDTU_src(path, uri, src_loc);
    }
      
    else{
      res = RPCALL->ResolvePath(path);
      if(res == -1) return -EAGAIN;
      rpath = RPCALL->MakePath(path, res);

      #ifdef HAVE_SETXATTR
  	  res = lsetxattr(rpath.c_str(), name, value, size, flags);
      #endif
    }

  	if (res == -1)
	  	return -errno;

	return 0;
}

static int scifs_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
    int res;
    string rpath;
    res = RPCALL->ResolvePath(path);
    ////res = 0;
    if(res == -1) res = 0;
    rpath = RPCALL->MakePath(path, res);
    #ifdef HAVE_SETXATTR
	res = lgetxattr(rpath.c_str(), name, value, size);
    #endif
	if (res == -1)
		return -errno;
	return res;
}

static int scifs_listxattr(const char *path, char *list, size_t size)
{
    int res; 
    string rpath;
    res = RPCALL->ResolvePath(path);
    if(res == -1) return -EAGAIN;
    rpath = RPCALL->MakePath(path, res);
//	res = llistxattr(rpath.c_str(), list, size);
	if (res == -1)
		return -errno;
	return res;
}

static int scifs_removexattr(const char *path, const char *name)
{
    int res;
    string rpath;
    res = RPCALL->ResolvePath(path);
    if(res == -1) return -EAGAIN;
    rpath = RPCALL->MakePath(path, res);
//	res = lremovexattr(rpath.c_str(), name);
	if (res == -1)
		return -errno;
	return 0;
}
//#endif /* HAVE_SETXATTR */

static void* scifs_init(struct fuse_conn_info *conn, fuse_config *config)
{
  int i;
  vector<string> loc;

  conn->max_write = 524288;
  /* configurations */
  //**/loc.push_back("/mnt/DTN2");
  //**/loc.push_back("/mnt/DTN3");
  /**/loc.push_back("/mnt/DTN4");
  /**/loc.push_back("/mnt/DTN5");
  /**///loc.push_back("/home/scifs/iStore-DMD/3");

  /**/attrs.push_back("platform");
  /**/attrs.push_back("processing_level");
  /**/attrs.push_back("instrument");
  /**/attrs.push_back("start_center_longitude");
  /**/attrs.push_back("start_center_latitude");

  /**/out_path="/var/scifs/search_output.txt";
  /* configurations */

  AgMetaDataClient *ptr = new AgMetaDataClient(grpc::CreateChannel("10.149.0.3:50051", grpc::InsecureChannelCredentials()), grpc::CreateChannel("10.149.0.4:50051", grpc::InsecureChannelCredentials()), loc);
  //AgMetaDataClient *ptr = new AgMetaDataClient(grpc::CreateChannel("10.149.0.3:50051", grpc::InsecureChannelCredentials()), grpc::CreateChannel("10.149.0.4:50051", grpc::InsecureChannelCredentials()), grpc::CreateChannel("10.149.0.5:50051", grpc::InsecureChannelCredentials()), grpc::CreateChannel("10.149.0.6:50051", grpc::InsecureChannelCredentials()), loc);

  DIR *dp;
  struct dirent *de;
  
  for (i = 0; i < loc.size(); i++) {
    dp = opendir(loc[i].c_str());

    while ((de = readdir(dp)) != NULL) {
      //printf("%s\n", de->d_name);
      string s = "/";
      s += de->d_name;
      int res = ptr->CreateMetadata(s.c_str(), i);
    }
  }

  //int res = RPCALL->CreateMetadata("/");
//  AgMetaDataClient *ptr = new AgMetaDataClient(grpc::CreateChannel("10.149.0.4:50051", grpc::InsecureChannelCredentials()), loc);
  return ptr;
}

static void scifs_destroy(void* private_data)
{
  delete (AgMetaDataClient*)private_data;
}

static struct fuse_operations scifs_oper = {};


void set_operations()
{
  scifs_oper.init           = scifs_init;//
  scifs_oper.destroy        = scifs_destroy;
  scifs_oper.getattr        = scifs_getattr;//
  scifs_oper.access	  = scifs_access;
  scifs_oper.readlink       = scifs_readlink;
  scifs_oper.readdir        = scifs_readdir;
  scifs_oper.mknod	  = scifs_mknod;
  scifs_oper.mkdir	  = scifs_mkdir;
  scifs_oper.symlink        = scifs_symlink;
  scifs_oper.unlink	  = scifs_unlink;
  scifs_oper.rmdir	  = scifs_rmdir;
  scifs_oper.rename	  = scifs_rename;
  scifs_oper.link	          = scifs_link;
  scifs_oper.chmod	  = scifs_chmod;//
  scifs_oper.chown	  = scifs_chown;//
  scifs_oper.truncate	  = scifs_truncate;//
#ifdef HAVE_UTIMENSAT
  scifs_oper.utimens	  = scifs_utimens;
#endif
  scifs_oper.open		  = scifs_open;
  scifs_oper.read		  = scifs_read;
  scifs_oper.write	  = scifs_write;
  scifs_oper.statfs	  = scifs_statfs;
  scifs_oper.release	  = scifs_release;
  scifs_oper.fsync	  = scifs_fsync;
#ifdef HAVE_POSIX_FALLOCATE
  scifs_oper.fallocate	  = scifs_fallocate;
#endif
//#ifdef HAVE_SETXATTR
  scifs_oper.setxattr	  = scifs_setxattr;
  scifs_oper.getxattr	  = scifs_getxattr;
  scifs_oper.listxattr	  = scifs_listxattr;
  scifs_oper.removexattr	  = scifs_removexattr;
//#endif

}

int main(int argc, char *argv[])
{
  umask(0);
  set_operations();
  if(BATCH_MODE)
  {
    std::cout << " \nMETDATA_MODE: BATCH " <<std ::endl;
  }
  else
  {
    std::cout << "\nMETADATA_MODE: SINGLE_IO " <<std ::endl; 
  }
  return fuse_main(argc, argv, &scifs_oper, NULL);
}
