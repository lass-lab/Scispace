#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <attr/xattr.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>

/* USAGE : ./search attr.name [ < | > | = | LIKE | NOT ] attr.value */

int main(int argc, char* argv[]){
  int res;
  FILE *fd;
  char sum[128], line[128];

  struct timeval mytime, after;
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

  printf("Enter for the searching query < attr.name\\condition\\attr.value >\n");
  printf(" if you want to use between, then append [\\attr.value2]\n");

  fgets(sum, 128, stdin);

  sum[strlen(sum)-1]='\\';

  printf("Search for %s\n", sum);

  gettimeofday(&mytime, NULL);

  res = lsetxattr(".", "user.search", sum, strlen(sum), 0);

  gettimeofday(&after, NULL);

  fd = fopen("../search_output.txt", "r");
  if (fd ==NULL ){ printf("File open error\n"); }
  else{
  while(fgets(line, 128, fd))
    printf("%s", line);
  
  fclose(fd);
  }

  if(errno==ENOTSUP)
    printf("ENOTSUP\n");

  
  printf("%ld %ld\n", mytime.tv_sec, mytime.tv_usec);
  printf("%ld %ld\n", after.tv_sec, after.tv_usec);

  return res;
}
