#ifndef UTIL_H
#define UTIL_H

#include <stdio.h>
#include "zs.h"

#define ZDB_WARN	(1 << 0)	/* Warn, but do not abort */
#define ZDB_ABORT	(1 << 1)	/* Warn, then abort */
#define ZDB_MASTER	(1 << 2)	/* Master thread */
#define ZDB_COMM	(1 << 3)	/* Comm threads */
#define ZDB_IO		(1 << 4)	/* I/O threads */
#define ZDB_FILE	(1 << 5)	/* File related messages */
#define ZDB_BLOCK	(1 << 6)	/* Block related messages */

#define ZDB_ALL		(~0)		/* Print everything */

#define ZDB_DEFAULT	(ZDB_WARN|ZDB_ABORT)

#define ZS_DEBUG	(0)		/* 1 enables debuggin, 0 disables */

#if ZS_DEBUG == 1
#define zdb(lvl,zs,who,id,fmt,args...)				\
  do {								\
      if ((lvl) & (zs)->verbose)				\
	  fprintf(stderr, "%s: %s %2d: %s:%d: " fmt "\n",	\
                  zs_role((zs)), (who), (id),			\
                  __func__, __LINE__, ##args );			\
  } while (0)

#define zwarn(zs,who,id,fmt,args...)				\
  do {								\
      zdb(ZDB_WARN,zs,who,id,fmt,args);				\
  } while (0)

#define zabort(zs,who,id,fmt,args...)				\
  do {								\
      zdb(ZDB_ABORT,zs,who,id,fmt,args);			\
      abort();							\
  } while (0)
#else
#define zdb(lvl,zs,who,id,fmt,args...)	do { } while (0)
#define zwarn(lvl,zs,who,id,fmt,args...)	do { } while (0)
#define zabort(lvl,zs,who,id,fmt,args...)	do { } while (0)
#endif /* ZS_DEBUG */
#endif /* UTIL_H */
