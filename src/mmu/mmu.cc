#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <list>

#include <grpc/grpc.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc++/security/server_credentials.h>
#include "mmu.grpc.pb.h"
#include "../dsp/solver.cc"

#include <sqlite3.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <dirent.h>
//#include "H5Cpp.h"
#include "hdf5.h"

#define PRINT_LOG_
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

using mmu::Batch;
using mmu::Path;
using mmu::Location;
using mmu::AgMetaData;
using mmu::Attr;
using mmu::Sdata;
using mmu::LocPath;
using mmu::DTU_Path;

using std::vector;

///////// Config files ///////////
string SciFS_dir_location ("/lustre1/scifs/nfs_dtn0/");
string DTU_module_location ("/var/scifs/lads/src/");
string DTU_dir_location ("/lustre1/scifs/DTU/");
//////////////////////////////////
int tmp_count = 0;

class AgMetaDataImpl final : public AgMetaData::Service {
    private:
    sqlite3 *db, *adb;
    int tagger = 0;
    int AID = 0;

    
    public:
    explicit AgMetaDataImpl(const std::string& db_path, const std::string& adb_path) {
        int res;
        res = sqlite3_open(db_path.c_str(), &db);
	      Dsp t1(10, 50, 59);   
      	if (res) {
            std::cout << "Can't open db" << std::endl;
            exit(1);
        } else {
            std::cout << "DB opened" << std::endl;
        }
	    
        res = sqlite3_open(adb_path.c_str(), &adb);
        if(res){
          std::cout << "Can't open attr db" << std::endl;
          exit(1);
        }
        else
          std::cout << "Attr DB opened" << std::endl;
    }

    ~AgMetaDataImpl() {
        sqlite3_close(db);
        sqlite3_close(adb);
    }

    int H5Tag(std::string fullpath, std::string path, const LocPath *lp){
      hid_t file, attr, filetype, memtype, space;
      herr_t status;
      size_t sdim;
      char *rdata;
      double *fdata;
      int *idata;
      int aid, res, cls;
      std::string type;

      file = H5Fopen(fullpath.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
      if( file<0 ){
        std::cout<<"file open error\n";
        return -1;
      }

      //for(std::list<std::string>::iterator i=Attr_list.begin(); i!=Attr_list.end(); i++){
      for( int i = 0; i < lp->attrs_size(); i++){
        //tag the file using Attr_list
        attr=H5Aopen(file, lp->attrs(i).c_str(), H5P_DEFAULT);

        filetype = H5Aget_type(attr);
        cls = H5Tget_class(filetype);//get the attr type

        switch(cls){
          case H5T_STRING://type="text"

          sdim=H5Tget_size(filetype);
          sdim++;

          space = H5Aget_space(attr);

          rdata = (char*)malloc(sdim*sizeof(char));

          memtype = H5Tcopy(H5T_C_S1);
          status = H5Tset_size(memtype, sdim);

          status = H5Aread(attr, memtype, rdata);
            
          //insert into DB
          aid=FindAID(lp->attrs(i), "text");
          res=UpdateIndex(path, "text", aid, string(rdata));
            
          free(rdata);
          break;

          case H5T_FLOAT:{//type="float"
            space = H5Aget_space(attr);
            fdata = (double*)malloc(sizeof(double));
            status = H5Aread(attr, H5T_NATIVE_DOUBLE, fdata);

            ////////////// CCGrid version ////////////////
            ///////// For distributing data types ////////
            int int_value, int_flag=0;

            std::string attr_except[5];
            attr_except[0] = "equatorCrossingLongitude";
            attr_except[1] = "northernmost_latitude";
            attr_except[2] = "southernmost_latitude";
            attr_except[3] = "easternmost_longitude";
            attr_except[4] = "westernmost_longitude";

            for (int j=0;j<5;j++) {
              if (lp->attrs(i) == attr_except[j]) {
                int_value = (int)(*fdata);
                int_flag = 1;
              }
            }
            if (int_flag) {
              aid=FindAID(lp->attrs(i), "integer");
              res=UpdateIndex(path, "integer", aid, std::to_string(int_value));
            }

            //////////////////////////////////////////////
            else {          
							//insert into DB
							aid=FindAID(lp->attrs(i), "float");
							res=UpdateIndex(path, "float", aid, std::to_string(*fdata));
							free(fdata);
            }
          }
          break;

          case H5T_INTEGER://type="integer"
          space = H5Aget_space(attr);
          idata = (int*)malloc(sizeof(int));
          status = H5Aread(attr, H5T_NATIVE_INT, idata);

          //insert into DB
          aid=FindAID(lp->attrs(i), "integer");
          res=UpdateIndex(path, "integer", aid, std::to_string(*idata));
          free(idata);
          break;

          default:break;
        }//switch

        status=H5Aclose(attr);
        status=H5Tclose(filetype);
      }//for Attr_list

      tmp_count ++;

      status=H5Fclose(file);
      return 0;
    }

    Status IndexAFile(ServerContext *context, const LocPath *lp, mmu::Empty* empty) override{

      //std::cout << "IndexAFile started " <<std::endl;

      //std::string fullpath = lp->loc() + "/" + lp->path();
      std::string fullpath = SciFS_dir_location + lp->path();

      std::cout << "INDEX_MKNOD" << std::endl;

      int res=H5Tag(fullpath, lp->path(), lp);
      std::cout << "tmp_count : " << tmp_count << std::endl;

      if( res<0 )
        return Status::CANCELLED;
      

      //std::cout << "indexAFile end " << std::endl;
      return Status::OK;
    }

    int FindAID(std::string key, std::string type){
        int find_aid=-1;

        sqlite3_stmt *stmt;
        const char *query = "SELECT AID FROM Adef WHERE Aname=?;";
        sqlite3_prepare_v2(adb, query, strlen(query), &stmt, NULL);
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
        int res = sqlite3_step(stmt);
        if(res == SQLITE_ROW) find_aid = sqlite3_column_int(stmt, 0);
        
        if( find_aid < 0 ) std::cout << "find_aid FAILED : ";
        sqlite3_finalize(stmt);

//        std::cout << find_aid << std::endl;

        if( find_aid < 0 ){ // insert attr->key() to Adef with new AID
          const char *temp = "SELECT count() FROM Adef;";
          sqlite3_prepare_v2(adb, temp, strlen(temp), &stmt, NULL);
          int tmp = sqlite3_step(stmt);
          if(tmp == SQLITE_ROW) AID=sqlite3_column_int(stmt, 0);
          sqlite3_finalize(stmt);

          find_aid = ++AID;
          std::cout << "Insert new AID : "<< find_aid << std::endl;
          sqlite3_stmt *stmt1;
          const char *query1 = "INSERT INTO Adef (AID, Aname, Atype) VALUES (?, ?, ?);";
          sqlite3_prepare_v2(adb, query1, strlen(query1), &stmt1, NULL);
          sqlite3_bind_int(stmt1, 1, find_aid);
          sqlite3_bind_text(stmt1, 2, key.c_str(), -1, SQLITE_STATIC);
          sqlite3_bind_text(stmt1, 3, type.c_str(), -1, SQLITE_STATIC);
          int res1 = sqlite3_step(stmt1);
          sqlite3_finalize(stmt1);
          if(res1 != SQLITE_DONE ) std::cout<<"RES1FAILED\n";
        }
        return find_aid;
    }

    int FindRemoveIndex(std::string path, std::string type, int aid){
        int res, find=-1;
        sqlite3_stmt *stmt;
        const char *query;
        
        if( type == "float" )
          query = "SELECT * FROM Ftable WHERE path=? AND AID=?;";
        else if( type == "text" )
          query = "SELECT * FROM Ttable WHERE path=? AND AID=?;";
        else if( type == "integer" )
          query = "SELECT * FROM Itable WHERE path=? AND AID=?;";
        else
          return -1;

        sqlite3_prepare_v2(adb, query, strlen(query), &stmt, NULL);
        sqlite3_bind_text(stmt, 1, path.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 2, aid);
        res = sqlite3_step(stmt);
        if(res == SQLITE_ROW) find = sqlite3_column_int(stmt, 1);
        sqlite3_finalize(stmt);

        if(find>0){
          //drop the previous attr entry
          const char *dquery;

          if( type == "float" )
            dquery = "DELETE FROM Ftable WHERE path=? AND AID=?;";
          else if( type == "text" )
            dquery = "DELETE FROM Ttable WHERE path=? AND AID=?;";
          else if( type == "integer" )
            dquery = "DELETE FROM Itable WHERE path=? AND AID=?;";
          else return -1;

          sqlite3_prepare_v2(adb, dquery, strlen(dquery), &stmt, NULL);
          sqlite3_bind_text(stmt, 1, path.c_str(), -1, SQLITE_STATIC);
          sqlite3_bind_int(stmt, 2, aid);
          res = sqlite3_step(stmt);
          sqlite3_finalize(stmt);

          if(res != SQLITE_DONE)
            return -2;
        }

        return 0;
    }

    int UpdateIndex(std::string path, std::string type, int find_aid, std::string value ){
      
      int res, fr;
      const char *query;

      fr=FindRemoveIndex(path, type, find_aid);

      if( fr == -1 ){
        std::cout << "wrong attr type\n";
        return -1;
      }
      else if( fr == -2){
        std::cout << "DELETE error\n";
        return -1;
      }

      if( type == "float" )
        query = "INSERT INTO Ftable (path, AID, Avalue) VALUES (?, ?, ?);";
      else if( type == "text" )
        query = "INSERT INTO Ttable (path, AID, Avalue) VALUES (?, ?, ?);";
      else if( type == "integer" )
        query = "INSERT INTO Itable (path, AID, Avalue) VALUES (?, ?, ?);";
      else{
        std::cout << "::::ERROR : wrong attr type::::\n";
        return -1;
      }
      sqlite3_stmt *stmt;
      sqlite3_prepare_v2(adb, query, strlen(query), &stmt, NULL);
      sqlite3_bind_text(stmt, 1, path.c_str(), -1, SQLITE_STATIC);
      sqlite3_bind_int(stmt, 2, find_aid);
      sqlite3_bind_text(stmt, 3, value.c_str(), -1, SQLITE_STATIC);
      res = sqlite3_step(stmt);
      sqlite3_finalize(stmt);

      if( res == SQLITE_DONE ){
        std::cout << "::::Update Index :: done::::" << std::endl;
        return 0;
      }
      else{
        std::cout << "::::Update Index :: failed:::::" << std::endl;
        return -1;
      }
    }

    bool _RemoveIndex (string path) {

      int res;

      sqlite3_stmt *stmt;
      const char *fquery = "DELETE FROM Ftable WHERE path = ?;";
      sqlite3_prepare_v2(adb, fquery, strlen(fquery), &stmt, NULL);
      sqlite3_bind_text(stmt, 1, path.c_str(), -1, SQLITE_STATIC);
      res = sqlite3_step(stmt);
      sqlite3_finalize(stmt);
      
      if(res != SQLITE_DONE)
        return false;

      const char *tquery = "DELETE FROM Ttable WHERE path = ?;";
      sqlite3_prepare_v2(adb, tquery, strlen(tquery), &stmt, NULL);
      sqlite3_bind_text(stmt, 1, path.c_str(), -1, SQLITE_STATIC);
      res=sqlite3_step(stmt);
      sqlite3_finalize(stmt);

      if(res != SQLITE_DONE)
        return false;

      const char *iquery = "DELETE FROM Itable WHERE path = ?;";
      sqlite3_prepare_v2(adb, iquery, strlen(iquery), &stmt, NULL);
      sqlite3_bind_text(stmt, 1, path.c_str(), -1, SQLITE_STATIC);
      res=sqlite3_step(stmt);
      sqlite3_finalize(stmt);

      if(res != SQLITE_DONE)
        return false;

      return true;
    }

    Status RemoveIndex(ServerContext* context, const Path *path, mmu::Empty *empty) override {
      int res;

      /*sqlite3_stmt *stmt;
      const char *fquery = "DELETE FROM Ftable WHERE path = ?;";
      sqlite3_prepare_v2(adb, fquery, strlen(fquery), &stmt, NULL);
      sqlite3_bind_text(stmt, 1, path->path().c_str(), -1, SQLITE_STATIC);
      res = sqlite3_step(stmt);
      sqlite3_finalize(stmt);
      
      if(res != SQLITE_DONE)
        return Status::CANCELLED;

      const char *tquery = "DELETE FROM Ttable WHERE path = ?;";
      sqlite3_prepare_v2(adb, tquery, strlen(tquery), &stmt, NULL);
      sqlite3_bind_text(stmt, 1, path->path().c_str(), -1, SQLITE_STATIC);
      res=sqlite3_step(stmt);
      sqlite3_finalize(stmt);

      if(res != SQLITE_DONE)
        return Status::CANCELLED;

      const char *iquery = "DELETE FROM Itable WHERE path = ?;";
      sqlite3_prepare_v2(adb, iquery, strlen(iquery), &stmt, NULL);
      sqlite3_bind_text(stmt, 1, path->path().c_str(), -1, SQLITE_STATIC);
      res=sqlite3_step(stmt);
      sqlite3_finalize(stmt);

      if(res != SQLITE_DONE)
        return Status::CANCELLED;
      */
    
      res = _RemoveIndex(path->path());

      if (res)
        return Status::OK;
      else
        return Status::CANCELLED;
    }


    Status AutoIndex(ServerContext* context, const LocPath *loc, mmu::Empty *out) override{
      DIR *dir = opendir(SciFS_dir_location.c_str());
      struct dirent *de;
      int res;

      //read directory and get file name
      while ( (de = readdir(dir)) != NULL){
        if( de->d_name[0]!='.'){
  
        //Open HDF5 file and get attribute value
        std::string fullpath = SciFS_dir_location + "/" + std::string(de->d_name);
        std::string path = "/" + std::string(de->d_name);

        res=H5Tag(fullpath, path, loc);
        }//if '.'
      }//while readdir

      return Status::OK;
    }

    Status DoSearch(ServerContext* context, const Sdata* sdata, Sdata* ret) override{
        int aid=-1;
        std::string type;

        std::cout << sdata->key() << sdata->condition() << sdata->value() << std::endl;

        sqlite3_stmt *stmt;
        const char *query = "SELECT AID, Atype FROM Adef WHERE Aname=?;";
        sqlite3_prepare_v2(adb, query, strlen(query), &stmt, NULL);
        sqlite3_bind_text(stmt, 1, sdata->key().c_str(), -1, SQLITE_STATIC);

        int res = sqlite3_step(stmt);
        if(res == SQLITE_ROW){
            aid = sqlite3_column_int(stmt, 0);
            type=std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1)));
        }
        if(aid<0){
            std::cout << "There is no AID for " << sdata->key() << std::endl;
            return Status::CANCELLED;
        }
        sqlite3_finalize(stmt);

//        std::cout << aid << " " << type << std::endl;

        const char *query1;
        if( type == "float"){
          std::cout << "float" << std::endl;

          if( sdata->condition() == "=" )
            query1="SELECT path FROM Ftable WHERE AID=? AND Avalue = ?;";
          else if( sdata->condition() == ">" )
            query1="SELECT path FROM Ftable WHERE AID=? AND Avalue > ?;";
          else if( sdata->condition() == "<" )
            query1="SELECT path FROM Ftable WHERE AID=? AND Avalue < ?;";
          else if( sdata->condition() == ">=" )
            query1="SELECT path FROM Ftable WHERE AID=? AND Avalue >= ?;";
          else if( sdata->condition() == "<=" )
            query1="SELECT path FROM Ftable WHERE AID=? AND Avalue <= ?;";
          else if( sdata->condition() == "between" )
            query1="SELECT path FROM Ftable WHERE AID=? AND Avalue BETWEEN ? AND ?;";
          else{
            std::cout << "condition err\n";
            return Status::CANCELLED;
          }        
        }
        else if( type == "text" ){
          std::cout << "text" << std::endl;

          if( sdata->condition() == "=" )
            query1="SELECT path FROM Ttable WHERE AID=? AND Avalue = ?;";
          else if( sdata->condition() == "like" )
            query1="SELECT path FROM Ttable WHERE AID=? AND Avalue like ?;";
          else{
            std::cout << "condition err\n";
            return Status::CANCELLED;
          }
        }
        else if( type == "integer" ){
          std::cout << "integer" << std::endl;
          if( sdata->condition() == "=" )
            query1="SELECT path FROM Itable WHERE AID=? AND Avalue = ?;";
          else if( sdata->condition() == ">" )
            query1="SELECT path FROM Itable WHERE AID=? AND Avalue > ?;";
          else if( sdata->condition() == "<" )
            query1="SELECT path FROM Itable WHERE AID=? AND Avalue < ?;";
          else if( sdata->condition() == ">=" )
            query1="SELECT path FROM Itable WHERE AID=? AND Avalue >= ?;";
          else if( sdata->condition() == "<=" )
            query1="SELECT path FROM Itable WHERE AID=? AND Avalue <= ?;";
          else if( sdata->condition() == "between" )
            query1="SELECT path FROM Itable WHERE AID=? AND Avalue BETWEEN ? AND ?;";
          else{
            std::cout << "condition err\n";
            return Status::CANCELLED;
          }        
        }

        sqlite3_prepare_v2(adb, query1, strlen(query1), &stmt, NULL);
        sqlite3_bind_int(stmt, 1, aid);
        sqlite3_bind_text(stmt, 2, sdata->value().c_str(), -1, SQLITE_STATIC);
        if( sdata->condition() == "between")
          sqlite3_bind_text(stmt, 3, sdata->bvalue().c_str(), -1, SQLITE_STATIC);
        int res1=sqlite3_step(stmt);

        while(res1 == SQLITE_ROW){
          //std::cout<<sqlite3_column_text(stmt, 0) << std::endl;
        
          ret->add_path(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));

          res1=sqlite3_step(stmt);
        }    
        sqlite3_finalize(stmt);

        return Status::OK;
    }

    Status UserIndex(ServerContext* context, const Attr* attr, mmu::Empty* empty) override {
        int find_aid=-1, res=-1;

//      #ifdef PRINT_LOG_
          std::cout << "CREATE UserIndex:::" << attr->path() << std::endl;
//      #endif

        std::cout << attr->key() << std::endl;
        std::cout << attr->type() <<std::endl;

        find_aid = FindAID(attr->key(), attr->type());

        //insert path, find_aid, attr->value to each proper Data table
        res = UpdateIndex(attr->path(), attr->type(), find_aid, attr->val());

        if( res < 0 )
          return Status::CANCELLED;
        else
          return Status::OK;
    }

    /////// DTU function in sink side
    Status DTU_sink(ServerContext* context, const mmu::DTU_Path* path, mmu::DTU_Path* ret_uri) override {
      int pid;
      pid = ::fork();

      if (pid == 0) {
        string dtu_sink_query;
        dtu_sink_query = DTU_module_location + "zs_sink";
        dtu_sink_query += (" -d " + SciFS_dir_location);
        dtu_sink_query = ("nice -n -5 " + dtu_sink_query);

        std::cout << "\n[DTU_sink] Calling sink lads : " << dtu_sink_query << std::endl;

        std::system(dtu_sink_query.c_str());  //// DTU call (sink side)

        exit(0);

        return Status::OK;
      }
      else {
        sleep(1);  // For passing verb URI by file

        string uri_location = DTU_module_location + "uri";
        std::ifstream ifs(uri_location.c_str());

        string uri;

        std::getline(ifs, uri);

        ret_uri->set_uri(uri.c_str());
        std::cout << "\n[DTU_sink] URI : " << ret_uri->uri() << std::endl;

        return Status::OK;
      }
    }

    /////// DTU function in source side
    Status DTU_src(ServerContext* context, const mmu::DTU_Path* path, mmu::Empty* empty) override {
      int pid, res;
      pid = ::fork();

      if (pid == 0) {
        string dtu_src_query;
        dtu_src_query = DTU_module_location + "zs_src";
        dtu_src_query += (" -d " + DTU_dir_location);
        dtu_src_query += (" -h " + path->uri());
        dtu_src_query = ("nice -n -5 " + dtu_src_query);

        string fpath, fpath_new;
        fpath = SciFS_dir_location + path->path();
        fpath_new = DTU_dir_location + path->path();

        std::cout << "\n[DTU_src] Calling source lads : " << dtu_src_query << std::endl;
        std::cout << "Old path : " << fpath << "\nNew path : " << fpath_new << std::endl;

        ::link(fpath.c_str(), fpath_new.c_str());

        std::system(dtu_src_query.c_str());  //// DTU call

        ////// Delete indices of transferred file
        res = _RemoveIndex(path->path()); 

        ::remove(fpath.c_str());
        ::remove(fpath_new.c_str());
        
        std::cout << "[DTU_src] DTU done" << std::endl;
        exit(0);

        return Status::OK;
      }
      else {
        sleep(1);

        return Status::OK;
      }
    }

    Status DTU_mark (ServerContext* context, const Path *path, mmu::Empty *empty) override {  

      int res;

      //////// DTU working done
      /////// Unset the duringDTU flag in location shard
      if (path->loc() == -1) {
        sqlite3_stmt *stmt;
        const char *fquery = "UPDATE locations SET duringDTU=? WHERE path = ?;";
        sqlite3_prepare_v2(db, fquery, strlen(fquery), &stmt, NULL);
        sqlite3_bind_int(stmt, 1, 0);
        sqlite3_bind_text(stmt, 2, (path->path()).c_str(), -1, SQLITE_STATIC);
        res = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
      
        if (res != SQLITE_DONE)
          return Status::CANCELLED;
        else
          return Status::OK;
      }

      ///////// DTU working is starting
      //////// Change the location of file and set the duringDTU flag
      else {
        sqlite3_stmt *stmt;
        const char *fquery = "UPDATE locations SET locationID=?, duringDTU=? WHERE path = ?;";
        sqlite3_prepare_v2(db, fquery, strlen(fquery), &stmt, NULL);
        sqlite3_bind_int(stmt, 1, path->loc());
        sqlite3_bind_int(stmt, 2, 1);
        sqlite3_bind_text(stmt, 3, (path->path()).c_str(), -1, SQLITE_STATIC);
        res = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
      
        if (res != SQLITE_DONE)
          return Status::CANCELLED;
        else
          return Status::OK;
      }
    }

    Status CreateMetadata(ServerContext* context, const Path* path, Location* loc) override 
    {
      #ifdef PRINT_LOG_
              std::cout<<"CREATE METADATA:::"<<path->path()<<":::"<<std::endl;
      #endif
      std::  cout << " Tagger " << tagger << std::endl;
      int locID = (++tagger)%2;
      std :: cout << " Locations " << locID << " tagger "  << tagger << std ::endl;
      sqlite3_stmt *stmt;
      const char *query = "INSERT INTO Locations (path, locationID, duringDTU) VALUES (?, ?, ?);";
      std :: cout << query << std::endl;
      sqlite3_prepare_v2(db, query, strlen(query), &stmt, NULL);
      sqlite3_bind_text(stmt, 1, path->path().c_str(), -1, SQLITE_STATIC);
      sqlite3_bind_int(stmt, 2, locID);
      sqlite3_bind_int(stmt, 3, 0);
      int res = sqlite3_step(stmt);
      sqlite3_finalize(stmt);
      if(res == SQLITE_DONE)
      {
        loc->set_loc(locID);
        #ifdef PRINT_LOG_
          std::cout << "::::create done:::"<<locID<<":::" << std::endl;
        #endif
          return Status::OK;
      }
      else
      {
        #ifdef PRINT_LOG_
          std::cout << "::::cannot create" << std::endl;
        #endif
        return Status::CANCELLED;
      }
    }

    Status CreateDistributedMetadata(ServerContext* context, const Path* path, mmu::Empty* empty){
      #ifdef PRINT_LOG_
      std::cout<<" Create Metadata using Distributed Metadata Method "<<path->path()<<":::"<<std::endl;
      #endif

      sqlite3_stmt *stmt;
      const char *query = "INSERT INTO Locations (path, locationID, duringDTU) VALUES (?, ?, ?);";
      sqlite3_prepare_v2(db, query, strlen(query), &stmt, NULL);
      sqlite3_bind_text(stmt, 1, path->path().c_str(), -1, SQLITE_STATIC);
      sqlite3_bind_int(stmt, 2, path->loc());
      sqlite3_bind_int(stmt, 3, 0);
      
      int res = sqlite3_step(stmt);
      sqlite3_finalize(stmt);
      if(res == SQLITE_DONE) {
        #ifdef PRINT_LOG_
        std::cout << ":: Entry Created ::: "<< path->loc()<<" ::: " << std::endl;
        #endif
        return Status::OK;
      }
      else {
        #ifdef PRINT_LOG_
        std::cout << " Error! Entry Failed ... " << std::endl;
        #endif
        return Status::CANCELLED;
      }
    }

    Status CreateBatchMetadata(ServerContext* context, const Batch* batch_sql, mmu::Empty* empty) override 
    {
        #ifdef PRINT_LOG_
                std::cout<<" CREATE BATCH METADATA: \n"<<batch_sql->batch_sql() <<std::endl;
        #endif

        sqlite3_stmt *stmt;
        sqlite3_prepare_v2(db,batch_sql->batch_sql().c_str(), strlen(batch_sql->batch_sql().c_str()), &stmt, NULL);
        int res = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        if(res == SQLITE_DONE)
        {
//            #ifdef PRINT_LOG_
                        std::cout << " MD_BACTH SUCCESSFULLY INSERTED " << std::endl;
  //          #endif
                return Status::OK;
        }
        else
        {
            #ifdef PRINT_LOG_
                        std::cout << "::::cannot create" << std::endl;
            #endif
            return Status::CANCELLED;
        }
    }

    Status ResolvePath(ServerContext* context, const Path* path, Location* loc) override {
      std::cout << "RESOLVE PATH::::"<<path->path()<<":::"<<std::endl;
#ifdef PRINT_LOG_
      std::cout << "RESOLVE PATH::::"<<path->path()<<":::"<<std::endl;
#endif
      int resolved_loc = -1;
      int DTU_flag = 0;

      /////* Check status of DTU *//////
      sqlite3_stmt *stmt_dtu;
      const char *query_dtu = "SELECT duringDTU FROM Locations WHERE path = ?;";
      sqlite3_prepare_v2(db, query_dtu, strlen(query_dtu), &stmt_dtu, NULL);
      sqlite3_bind_text(stmt_dtu, 1, path->path().c_str(), -1, SQLITE_STATIC);
      int res_dtu = sqlite3_step(stmt_dtu);

      if (res_dtu == SQLITE_ROW)
        DTU_flag = sqlite3_column_int(stmt_dtu, 0);

      sqlite3_finalize(stmt_dtu);

      /////// Case that requested file is in transmission
      if (DTU_flag == 1) {
        std::cout << "::::Access while DTU" << std::endl;
        return Status::CANCELLED;
      }
      /////* Check status of DTU *//////

      /////* retreive from db */////
      sqlite3_stmt *stmt;
      const char *query = "SELECT locationID FROM Locations WHERE path = ?;";
      sqlite3_prepare_v2(db, query, strlen(query), &stmt, NULL);
      sqlite3_bind_text(stmt, 1, path->path().c_str(), -1, SQLITE_STATIC);
      int res = sqlite3_step(stmt);
      if(res==SQLITE_ROW) resolved_loc = sqlite3_column_int(stmt, 0);
      sqlite3_finalize(stmt);
      // if doest exists send error
      if (resolved_loc < 0){
#ifdef PRINT_LOG_
        std::cout << "::::No MD" << std::endl;
#endif
        return Status::CANCELLED;
      }
      //////* retreive from db */////

#ifdef PRINT_LOG_
      std::cout << "::::MD HIT:::"<<resolved_loc<<":::" << std::endl;
#endif
      loc->set_loc(resolved_loc);
      return Status::OK;
    }

    Status RemoveMetadata(ServerContext* context, const Path* path, mmu::Empty* empty_) override {
#ifdef PRINT_LOG_
        std::cout<<"REMOVE METADATA::::"<<path->path()<<":::"<<std::endl;
#endif
        sqlite3_stmt *stmt;
        const char *query = "DELETE FROM Locations WHERE path = ?;";
        sqlite3_prepare_v2(db, query, strlen(query), &stmt, NULL);
        sqlite3_bind_text(stmt, 1, path->path().c_str(), -1 ,SQLITE_STATIC);
        int res = sqlite3_step(stmt);
        if(res==SQLITE_DONE){
#ifdef PRINT_LOG_
          std::cout << "::::SUCCESSFUL DELETE" <<std::endl;
#endif
          return Status::OK;
        }
#ifdef PRINT_LOG_
        std::cout << "::::DELETE FAIL" <<std::endl;
#endif
        return Status::CANCELLED;
    }
};

void RunServer() 
{
    std::string server_address("0.0.0.0:50051");
    std::string db_path("mmu.db");
    std::string adb_path("attr.db");
    AgMetaDataImpl service(db_path, adb_path);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService((&service));
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server Listening on " << server_address << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
    
    RunServer();

    return 0;
}
