#ifndef DSP_H
#define DSP_H

 struct data_center
  {
    char name[20];
    int loc_id;
    int str;
    int cmp;
    int ntw;
    int a_str;
    int status;
    int dc_v;
    int chunkcapc;
    float jet;
  };
  
  struct data_gen
  {
    int dg_id;
    char fname[20];
    int fsz;
  };
          
class Dsp
{
     private :
          int hour;
          int minute;
          int second;
          
          struct data_center dclst[2];
          struct data_center optm_dc;
          int dc_count = 2;
          struct data_gen dgen[1];
          
     public :
          //with default value
          Dsp(const int h = 0, const int m  = 0, const int s = 0);
          
          //	setter function
          void setTime(const int h, const int m, const int s);
          
          // Print a description of object in " hh:mm:ss"
          void print() const;
          
          //compare two time object
          bool equals(const Dsp&);
          
          // fetch network bandwidth between data generator and data center
          int get_NetworkBandwidth(char *, char *);
          
          // total time of data center given dg name
          float get_TotalTime(struct data_center *, char *,int);

          // total time of data center given dg name with Chunking functionality
          float get_TotalTimeChunks(struct data_center *, char *,int);
          
          // main algorithm for dsp-solver
          struct data_center findOptimalData_Center_Static(struct data_center *, int, int, char*);

          // main algorithm for dsp-solver with Dynamic functionality
          struct data_center findOptimalData_Center_Dynamic(struct data_center *, int, int, char*);
          
          // read data centers from the file
          int parse_DataCenters();
          
          void printdata_center();

};
 
#endif