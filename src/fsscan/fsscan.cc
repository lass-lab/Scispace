/*
This is free and open source. Anyone is allowed to modify this file.

Signed off by:
Awais Khan
awais@sogang.ac.kr
*/

#include <iostream>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <vector>
#include <string.h>
#include <sys/types.h>
#include <attr/xattr.h>

using namespace std;

#include <grpc/grpc.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "unimd.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::Status;
using unimd::Path;
using unimd::Location;
using unimd::UniMetaData;
using unimd::Batch;

using std::vector;
using std::string;
using std::map;
using std::pair;


//#define PRINT_LOG_

#define XATTR_SIZE 10000

class SyncFiles {
    public:
    
    int get_locations_size() {
        return (int)locations.size();
    }

    SyncFiles(std::shared_ptr<Channel> channel, vector<string> loc) : stub_(UniMetaData::NewStub(channel)) 
    {
        locations = loc;
    }

    string MakePath(const char* path, int n) 
    {
        return locations[n] + string(path);
    }

    int SyncMeta(std::vector<string> files, int loc_id) 
    {
        #ifdef PRINT_LOG_
        cout << " Sync MetaData of " << files.size() << " files " <<endl;
        #endif
 
        //int loc_id=0;

        for (unsigned int cnt=0; cnt < files.size(); cnt++)
        {
	    //loc_id = (++tagger)%get_locations_size();
            batch_value += "('" + files[cnt] + "', " + to_string(loc_id) + "),";
        }           
	 #ifdef PRINT_LOG_
	cout << "after loop " << endl;
	#endif
        batch_sql = "INSERT INTO locations (path, locationID) VALUES " + batch_value;
        batch_sql.replace(batch_sql.end()-1,batch_sql.end(),";");
        Batch batch_; unimd::Empty empty_;
        batch_.set_batch_sql(batch_sql);
        ClientContext context;
        Status status = stub_->CreateBatchMetadata(&context, batch_, &empty_);
        if(status.ok())
        {
           batch_sql.clear();
           batch_value.clear();
           #ifdef PRINT_LOG_
           std::cout << " *** BATCH PUSHED *** " << std::endl;
           #endif
        }
               return loc_id;
    }

    private:
    std::unique_ptr<UniMetaData::Stub> stub_;
    vector<string> locations;
    string batch_sql; 
    string batch_value;
    int tagger = 0 ;
};

int switch_syncflag(const char* file)
{
     char const *value = "synced";
     int xatr = setxattr(file, "user.flg", value, strlen(value), 0);
	#ifdef PRINT_LOG_     
cout << " extended attr : " << xatr << " for file " << file <<endl;
	#endif

     return xatr;
}

int view_syncflag(const char* file)
{
     char value[XATTR_SIZE];

     int xatr = getxattr(file, "user.flg", value, XATTR_SIZE);
	#ifdef PRINT_LOG_
     cout << " getxattr : " << xatr << "for file: " << file << "value " << value <<endl;
	#endif

     return xatr;

}

int remove_syncflag(const char* file)
{
	int xatr=0;
	return xatr;
}

int scanloc (string dir, vector<string> &files)
{
    DIR *dp;
    struct dirent *dirp;
    string parent;

    if((dp  = opendir(dir.c_str())) == NULL) 
    {

        cout << "Error(" << errno << ") opening " << dir << endl;
        return errno;
    }

    while ((dirp = readdir(dp)) != NULL) 
    { 
        if(dirp->d_type == DT_DIR && strcmp(dirp->d_name, ".") != 0 && strcmp(dirp->d_name, "..") != 0)
        {
            #ifdef PRINT_LOG_
            cout << "Dir:" << dir << "/" << dirp->d_name  << endl;
            #endif
            string parent = dir + "/" + string(dirp->d_name);
            scanloc(parent, files); //recursive call
            files.push_back("/"+string(dirp->d_name));
        }
        else if(strcmp(dirp->d_name, ".") != 0 && strcmp(dirp->d_name, "..") != 0)
        {
            #ifdef PRINT_LOG_
            cout << "\tFile:"  << dirp->d_name << endl;
            #endif
	    string fp = dir + "/" + dirp->d_name;
	    if(view_syncflag(fp.c_str()) == -1)
	    {
		switch_syncflag(fp.c_str());
            	files.push_back("/"+string(dirp->d_name));
	    }
        }
    }
    closedir(dp);
    return 0;
}



int main()
{
    vector<string> loc;
     loc.push_back("/home/foo/scifs/1");
     loc.push_back("/home/foo/scifs/2");

    SyncFiles *ptr = new SyncFiles(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()), loc);


    for (unsigned int i=0; i< loc.size(); i++)
    {
    string dir = loc[i];
    vector<string> files = vector<string>();
    scanloc(dir, files);
    #ifdef PRINT_LOG_
    cout << " Loc- " << (i+1) << " size: " << files.size() << endl;
    #endif
    if(files.size() > 0)
     	{
	ptr->SyncMeta(files, i);
     	files.clear();
	}
	else
	{
	cout << " No new files to sync at location " << (i+1) << endl;
	}
    }

   // string fp = "/home/khan/scifs/1/10";
    //view_syncflag(fp.c_str());
    return 0;
}
