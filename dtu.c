#include <stdio.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

int main(int argc, char* argv[]){
  int res, tmp;
  char buf[3], sum[128] = {0,};

  if( argc < 3 ){
    printf("Usage : dtu file_name DTN_number\n");
    return -1;
  }

  strcat(sum, argv[2]);

  printf("File name: %s\nDTN index: %s\n", argv[1], sum);

  res = lsetxattr(argv[1], "user.dtu", sum, strlen(sum), 0);

  if(errno==ENOTSUP)
    printf("ENOTSUP\n");

  printf("res: %d errno: %d\n", res, errno);
/*
  res = lgetxattr(argv[1], argv[2], buf, strlen(argv[3]));

  printf("val : %s\n", buf);
  */
  return res;
}
