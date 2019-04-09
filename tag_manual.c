#include <stdio.h>
#include <sys/types.h>
#include <attr/xattr.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

/* USAGE : ./setmeta file.name attr.name attr.type attr.value */

int main(int argc, char* argv[]){
  int res, tmp;
  char buf[3], sum[128];

  if( argc < 5 ){
    printf("Usage : ./setmeta file.name attr.name attr.type attr.value\n");
    printf("    attr.type:\n");
    printf("              < text | float | integer >\n");
    return -1;
  }

  strcpy(sum, argv[2]);
  strcat(sum, "\\");
  strcat(sum, argv[3]);
  strcat(sum, "\\");
  strcat(sum, argv[4]);

  printf("File name: %s\nTag: %s\n", argv[1], sum);

  res = lsetxattr(argv[1], "user.tag", sum, strlen(sum), 0);

  if(errno==ENOTSUP)
    printf("ENOTSUP\n");

  printf("res: %d errno: %d\n", res, errno);
/*
  res = lgetxattr(argv[1], argv[2], buf, strlen(argv[3]));

  printf("val : %s\n", buf);
  */
  return res;
}
