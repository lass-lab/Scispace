#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#define NUM_NAME 6000
#define NUM_THREAD 16

char file_name[NUM_NAME][18] = {0,};
void func(void *idx);

int main()
{
  int i;
  char name[] = "../scifs/mdtest";
  pthread_t tid[NUM_THREAD];
  struct timeval time_start, time_end;

  for (i=0;i<NUM_NAME;i++) {
    char number[4] = {0,};
    memcpy(file_name[i], name, strlen(name));
    sprintf(number, "%d", i);
    memcpy(file_name[i] + strlen(name), number, 4);
  }

  gettimeofday(&time_start, NULL);
  for (i=0; i < NUM_THREAD; i++)
    pthread_create(&tid[i], NULL, func, (void*)i);

  for (i=0; i < NUM_THREAD; i++)
    pthread_join(tid[i], NULL);
  gettimeofday(&time_end, NULL);

  printf("time : %ld (usec)\n", (time_end.tv_sec - time_start.tv_sec)*1000000 \
                        + time_end.tv_usec - time_start.tv_usec);

  return 0;
}

void func(void *idx) {
  int i;
  for (i=0; i < NUM_NAME/NUM_THREAD; i++) {
    FILE *fp = fopen( file_name[(int)idx * (NUM_NAME/NUM_THREAD) + i], "w");
    fclose(fp);
  }
}
