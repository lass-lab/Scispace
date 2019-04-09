#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <attr/xattr.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

/* USAGE : ./search attr.name [ < | > | = | LIKE | NOT ] attr.value */

int main(int argc, char* argv[]){
  int res;
  FILE *fd;
  char sum[128]={0,}, line[128];
  char aname[32], cond[16], aval[32], bval[32];
/*
  if(argc < 4){
    printf("USAGE : ./search attr.name [ < | > | = | LIKE | NOT ] attr.value\n");
    return -1;
  }

  strcpy(sum, argv[1]);
  strcat(sum, "\\");
  strcat(sum, argv[2]);
  strcat(sum, "\\");
  strcat(sum, argv[3]);
*/

  printf("Enter for the searching query < attr.name condition attr.value >\n");

  printf("Attr.name : ");
  scanf("%s", aname);

  printf("Search.operator : ");
  scanf("%s", cond);

  printf("Attr.value : ");
  scanf("%s", aval);

  if(strcmp(cond, "between")==0){
    printf("to : ");
    scanf("%s", bval);
  }

  strcpy(sum, aname);
  strcat(sum, "\\");
  strcat(sum, cond);
  strcat(sum, "\\");
  strcat(sum, aval);
  strcat(sum, "\\");

  if(strcmp(cond, "between")==0){
    strcat(sum, bval);
    strcat(sum, "\\");
  }

//  fgets(sum, 128, stdin);

//  sum[strlen(sum)-1]='\\';

  printf("Search for %s\n", sum);

  res = lsetxattr(".", "user.search", sum, strlen(sum), 0);

  fd = fopen("../search_output.txt", "r");
  if (fd ==NULL ){ printf("File open error\n"); }
  else{
  while(fgets(line, 128, fd))
    printf("%s", line);
  
  fclose(fd);
  }

  if(errno==ENOTSUP)
    printf("ENOTSUP\n");

  return res;
}
