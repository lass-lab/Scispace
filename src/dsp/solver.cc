

#include <iostream>
#include <fstream>
#include <iomanip>
#include <string.h>
#include "dsp.h"
using namespace std;

Dsp :: Dsp(const int h, const int m, const int s) 
: hour(h), minute (m), second(s)
{}

void Dsp :: setTime(const int h, const int m, const int s) 
{
	hour = h;
	minute = m;
	second = s;     
}		

void Dsp :: print() const
{   
	cout << setw(2) << setfill('0') << hour << ":"
	<< setw(2) << setfill('0') << minute << ":"
	<< setw(2) << setfill('0') << second << "\n";	

}

bool Dsp :: equals(const Dsp &otherTime)
{
	if(hour == otherTime.hour 
		&& minute == otherTime.minute 
		&& second == otherTime.second)
		return true;
	else
		return false;
}

int Dsp :: get_NetworkBandwidth(char *toFind_dg_name, char *toFind_dc_name)
{
	char dg_name[100];char dc_name[100]; int network_bandwidth=-1;
	int dg_count = 0; char buffer[100]; char *temp = NULL;
	ifstream myfile ("./src/dsp/dg.txt");
	FILE *fp = fopen("./src/dsp/dg.txt","r");
	if (myfile.is_open())
	{
		//cout<<" data generator file opened successfully. \n";
	}
	else
	{
		cout <<" data generator file not found. \n";
	}

	while(fgets(buffer,sizeof(buffer),fp))
	{
		if(dg_count > 0)
		{
			int space = 0 ;
			temp = strtok(buffer," ");
			while(temp != NULL)
			{
				if(space == 0 )
				{
					strcpy(dg_name,temp);
				}
				if(space == 1)
				{
					strcpy(dc_name,temp);

				}
				if(space == 2)
				{
					network_bandwidth = atoi(temp);
				}
				temp = strtok(NULL," ");
				space++;
			}
					//check if its a match
			if(strcmp(dg_name, toFind_dg_name) == 0 && strcmp(dc_name, toFind_dc_name) ==0)
			{
				myfile.close();
				return network_bandwidth;
			}
		}
		dg_count++;
	}
	myfile.close();
	return network_bandwidth;
}

float Dsp :: get_TotalTime(struct data_center *dc, char *dg_name, int size)
{
	dc->ntw = get_NetworkBandwidth(dg_name,dc->name);
	float sum = 0 ;
	sum = (size/dc->str) + (size/dc->cmp) + (size/dc->ntw);
	return sum;
}

float Dsp :: get_TotalTimeChunks(struct data_center *dc, char *dg_name, int size)
{
	dc->ntw = get_NetworkBandwidth(dg_name,dc->name);
	/*float sum = 0 ;
	sum = (size/dc->str) + (size/dc->ntw);*/
	if(dc->str > dc->ntw)
	{	cout << "optimal datacenter power dc-str" << dc->str <<endl;
		return dc->str;
	}
	else
	{

		cout << "optimal datacenter power dc-ntw" << dc->ntw <<endl;
		return dc->ntw;
	}
	return dc->str;
}

int Dsp :: parse_DataCenters()
{
	int dc_count = 0 ; char buffer[100]; char* temp;
	struct data_center dcl; 
	int space = 0 ;
	ifstream myfile ("./src/dsp/dc.txt");
	FILE *fp = fopen("./src/dsp/dc.txt","r");
	if (myfile.is_open())
	{
		//cout<<" data center file opened successfully. \n";
	}
	else
	{
		cout <<" data center file not found. \n";
	}

	while(fgets(buffer,sizeof(buffer),fp))
	{
		if(dc_count > 0)
		{
			space = 0 ;
			temp = strtok(buffer," ");
			while(temp != NULL)
			{
				if(space == 0 )
				{
					strcpy(dcl.name,temp);	
				}

				if(space == 1 )
				{
					dcl.str = atoi(temp);

				}

				if(space == 2 )
				{
					dcl.cmp = atoi(temp);
				}

				if(space == 4 )
				{
					dcl.ntw = atoi(temp);
				}

				if(space == 3 )
				{
					dcl.a_str = atoi(temp);	
				}

				if(space == 5)
				{
					dcl.dc_v = atoi(temp);
				}
				temp = strtok(NULL," ");
				space++;
			}
			dclst[dc_count - 1].loc_id = dc_count -1 ;
			strcpy(dclst[dc_count - 1].name,dcl.name);
			dclst[dc_count -1].str = dcl.str;
			dclst[dc_count -1].cmp = dcl.cmp;
			dclst[dc_count -1].ntw = dcl.ntw;
			dclst[dc_count -1].a_str = dcl.a_str;
		}

		dc_count++;
	}
	myfile.close();
	return dc_count;
}

struct data_center Dsp :: findOptimalData_Center_Static(struct data_center *dcptr, int dc_count, int f_size, char* dg_name)
{
	//cout <<"\n\t DSP-Engine-findOptimalData_Center \n";
	struct data_center dc[dc_count], opt_dc;
	for(int d = 0 ; d < dc_count; d++)
	{
		strcpy(dc[d].name,dclst[d].name);
		dc[d].loc_id = dclst[d].loc_id;
		dc[d].str =  dclst[d].str;
		dc[d].cmp =  dclst[d].cmp;
		dc[d].ntw =  dclst[d].ntw;
		dc[d].a_str =  dclst[d].a_str;
		dc[d].status = 0 ;

		if(dc[d].a_str > f_size)
		{
				dc[d].dc_v = 1; // pass first check f_size
			}
			else
			{
				dc[d].dc_v = 0; // failed first check f_size	
			}

			if(dc[d].status == 0)
			{
				dc[d].dc_v = dc[d].dc_v + 1 ; // pass second check dc_status idle/free
			}
			
			if(dc[d].dc_v == 2)
			{
				strcpy(opt_dc.name,dc[d].name);
				opt_dc.loc_id = dc[d].loc_id;
				opt_dc.str =  dc[d].str;
				opt_dc.cmp =  dc[d].cmp;
				opt_dc.ntw =  dc[d].ntw;
				opt_dc.a_str =  dc[d].a_str;
			}	
	}  //first loop ends here
	
	float opt_total_time = get_TotalTime(&opt_dc, dg_name,f_size);
	opt_dc.jet = opt_total_time;

	for(int j = 0; j < dc_count; j++)
	{

		if(dc[j].dc_v == 2) 
		{	
			if(opt_total_time > (get_TotalTime(&dc[j], dg_name,f_size)))
			{
				strcpy(opt_dc.name,dc[j].name);
				opt_dc.loc_id = dc[j].loc_id;
				opt_dc.str =  dc[j].str;
				opt_dc.cmp =  dc[j].cmp;
				opt_dc.ntw =  dc[j].ntw;
				opt_dc.a_str =  dc[j].a_str;
				opt_dc.dc_v = dc[j].dc_v;
				opt_total_time = get_TotalTime(&opt_dc, dg_name,f_size);
				opt_dc.jet = opt_total_time;
			}
		}
	}
	return opt_dc;
}

struct data_center Dsp :: findOptimalData_Center_Dynamic(struct data_center *dcptr, int dc_count, int f_size, char* dg_name)
{
	//cout <<"\n\t DSP-Engine-findOptimalData_Center \n";
	struct data_center dc[dc_count], opt_dc;
	for(int d = 0 ; d < dc_count; d++)
	{
		strcpy(dc[d].name,dclst[d].name);
		dc[d].loc_id = dclst[d].loc_id;
		dc[d].str =  dclst[d].str;
		dc[d].cmp =  dclst[d].cmp;
		dc[d].ntw =  dclst[d].ntw;
		dc[d].a_str =  dclst[d].a_str;
		dc[d].status = 0 ;

		if(dc[d].a_str > f_size)
		{
				dc[d].dc_v = 1; // pass first check f_size
		}
		else
		{
			dc[d].dc_v = 0; // failed first check f_size	
		}

		if(dc[d].status == 0)
		{
			dc[d].dc_v = dc[d].dc_v + 1 ; // pass second check dc_status idle or free
		}
		
		if(dc[d].dc_v == 2)			// copying in optimal dc
		{
			strcpy(opt_dc.name,dc[d].name);
			opt_dc.loc_id = dc[d].loc_id;
			opt_dc.str =  dc[d].str;
			opt_dc.cmp =  dc[d].cmp;
			opt_dc.ntw =  dc[d].ntw;
			opt_dc.a_str =  dc[d].a_str;
		}	
	}  // data center filteration and validation ends here!
	
	float opt_total_time = get_TotalTimeChunks(&opt_dc, dg_name,f_size);
	opt_dc.jet = opt_total_time;

	for(int j = 0; j < dc_count; j++)
	{

		if(dc[j].dc_v == 2) 
		{	
			if(opt_total_time > (get_TotalTime(&dc[j], dg_name,f_size)))
			{
				strcpy(opt_dc.name,dc[j].name);
				opt_dc.loc_id = dc[j].loc_id;
				opt_dc.str =  dc[j].str;
				opt_dc.cmp =  dc[j].cmp;
				opt_dc.ntw =  dc[j].ntw;
				opt_dc.a_str =  dc[j].a_str;
				opt_dc.dc_v = dc[j].dc_v;
				opt_total_time = get_TotalTimeChunks(&opt_dc, dg_name,f_size);
				opt_dc.jet = opt_total_time;
			}
		}
	}
	return opt_dc;
}

void Dsp :: printdata_center()
{
	for(int i = 0; i < dc_count ; i++)
	{
		cout << dclst[i].loc_id << " "  <<dclst[i].name << " "<< dclst[i].str << " " << dclst[i].cmp << " " << dclst[i].ntw << " " << dclst[i].a_str << "\n ";
	}
}
