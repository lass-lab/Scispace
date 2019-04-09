#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <sys/time.h>
#include <sched.h>
#include <numa.h>

#include "zs.h"
#include "ssd.h"

#define NODE_MASTER 0

zs_globals_t *zs = NULL;

void pin_to_core(size_t core, pthread_t tid) 
{
  cpu_set_t cpuset;
  CPU_ZERO (&cpuset);
  CPU_SET (core, &cpuset);
  pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
}

static void
print_usage(char *procname)
{
	fprintf(stderr, "usage: %s -h <server_uri> [-c <comm>] [-i <io>] "
	                "[-o <ost>] [-r <mtu>] [-R <blocks>] [-S] "
	                "[-N <ssd_blocks>] [-l <ssd_path>]\n", procname);
	fprintf(stderr, "where:\n");
	fprintf(stderr, "\t-h\tThe server's CCI URI\n");
	fprintf(stderr, "\t-d\tInput directory (default %s)\n", ZS_SRC_DIR);
	fprintf(stderr, "\t-c\tNumber of comm threads (default %d)\n",
	        ZS_NUM_COMM);
	fprintf(stderr, "\t-C\tNumber of open connections (default %d\n",
	        ZS_NUM_CONN);
	fprintf(stderr, "\t-i\tNumber of io threads (default %d)\n", ZS_NUM_IO);
	fprintf(stderr, "\t-s\tNumber of ssd threads (default %d\n",
	        ZS_NUM_SSD);
	fprintf(stderr, "\t-o\tNumber of OST queues (default %d)\n",
	        ZS_NUM_OSTS);
	fprintf(stderr, "\t-r\tRMA MTU (default %d)\n", ZS_RMA_MTU);
	fprintf(stderr, "\t-R\tNumber of RMA blocks (default %d)\n",
	        ZS_RMA_BLOCKS);
	fprintf(stderr, "\t-S\tEnable to use SSD buffer "
	                "(default off, off/on (0/1))\n");
	fprintf(stderr, "\t-N\tNumber of SSD blocks (default %d)\n",
	        ZS_SSD_BLOCKS);
	fprintf(stderr, "\t-l\tPath to the SSD log directory (default: %s)\n",
	        ZS_SRC_SSD_LOG);
	exit(EXIT_FAILURE);
}

extern void *master(void *args);
extern void *comm(void *args);
extern void *io(void *args);
extern void *ssd(void *args);

int
main(int argc, char *argv[])
{
	int ret = 0, c = 0, i = 0;

  /* Change the constants to adequate number considering NUMA architecture
   * E.g. Node 0's cores : 0, 2, 4, 6, 8, 10
   *      Node 1's cores : 1, 2, 3, 4, 5, 6
   *      Node 2's cores : 12, 14, 16, 18, 20, 22
   *      Node 3's cores : 13, 15, 17, 19, 21, 23

   *      Then, idx_core_0 <- 0, idx_core_1 <- 1, idx_core_2 <- 12, idx_core_3 <- 13
   */
  int idx_core_0 = 0, idx_core_1 = 1, idx_core_2 = 12, idx_core_3 = 13;

  struct timeval time_start, time_end;

  ///////// NUMA variables
  int num_cpus = numa_num_task_cpus();
  struct bitmask *bm;
  struct bitmask *node_to_alloc = numa_allocate_nodemask();

  bm = numa_bitmask_alloc(num_cpus);
  for (i = 0; i <= numa_max_node(); i++) {
    numa_node_to_cpus (i, bm);
  }
  numa_bitmask_free(bm);

  numa_bitmask_setbit(node_to_alloc, NODE_MASTER);
  numa_bind(node_to_alloc);
  numa_bitmask_free(node_to_alloc);

  /*numa_set_localalloc();*/

	zs = calloc(1, sizeof(*zs));
	if (!zs) {
		ZS_DEBUG ("No memory for zs\n");
		goto out;
	}
	zs->num_comms = ZS_NUM_COMM;
	zs->num_ios = ZS_NUM_IO;
	zs->num_ssds = ZS_NUM_SSD;
	zs->num_osts = ZS_NUM_OSTS;
	zs->rma_mtu = ZS_RMA_MTU;
	zs->rma_cnt = ZS_RMA_BLOCKS;
	zs->ssd_cnt = ZS_SSD_BLOCKS;
	zs->dir_name = strdup(ZS_SRC_DIR);
	zs->ssd_log_file_name = strdup(ZS_SRC_SSD_LOG);
	zs->ssd_buffer_flag = 0;
	zs->num_conns = ZS_NUM_CONN;

	ret = fd_tree_init(&zs->ftree);
	if (ret)
		goto out;

	//while((c = getopt(argc, argv, "h:d:c:i:o:r:R:")) != -1) {
	while((c = getopt(argc, argv, "h:d:c:C:i:o:s:r:R:S:N:")) != -1) {
		switch (c) {
		case 'h':
			zs->server_uri = strdup(optarg);
			break;
		case 'd':
			free(zs->dir_name);
			zs->dir_name = strdup(optarg);
			break;
		case 'c':
			zs->num_comms = strtol(optarg, NULL, 0);
			break;
		case 'C':
			zs->num_conns = strtol(optarg, NULL, 0);
			break;
		case 'i':
			zs->num_ios = strtol(optarg, NULL, 0);
			break;
		case 's':
			zs->num_ssds = strtol(optarg, NULL, 0);
			break;
		case 'o':
			zs->num_osts = strtol(optarg, NULL, 0);
			break;
		case 'r':
			zs->rma_mtu = strtol(optarg, NULL, 0);
			break;
		case 'R':
			zs->rma_cnt = strtol(optarg, NULL, 0);
			break;
		case 'S':
			zs->ssd_buffer_flag = strtol(optarg, NULL, 0);
			break;
		case 'N':
			zs->ssd_cnt = strtol(optarg, NULL, 0);
			break;
		case 'l':
			zs->ssd_log_file_name = strdup(optarg);
			break;
		default:
			print_usage(argv[0]);
		}
	}

	assert(zs->num_comms == 1); /* for now */
	assert(zs->num_ssds == 1); 

	if (!zs->server_uri) {
		ZS_DEBUG ("%s must set -h <server_uri>\n", argv[0]);
		print_usage(argv[0]);
	}

	ret = pthread_mutex_init(&zs->master.lock, NULL);
	if (ret) {
		ZS_DEBUG ("pthread_mutex_init(master) failed with %s\n",
		  	  strerror(ret));
		goto out;
	}

	TAILQ_INIT(&zs->master.q);

	ret = pthread_cond_init(&zs->master.wait.cv, NULL);
	if (ret) {
		ZS_DEBUG ("pthread_cond_init(master) failed with %s\n",
			  strerror(ret));
		goto out;
	}

	zs->comm = calloc(zs->num_comms, sizeof(*zs->comm));
	if (!zs->comm) {
		ZS_DEBUG ("No memory for zs->comm\n");
		goto out;
	}

	for (i = 0; i < zs->num_comms; i++) {
		zs->comm[i] = calloc(1, sizeof(*zs->comm[0]));
		if (!zs->comm[i]) {
			ZS_DEBUG ("No memory for zs->comm[%d]\n", i);
			goto out;
		}

		TAILQ_INIT(&zs->comm[i]->q);
	}

	zs->io = calloc(zs->num_ios, sizeof(*zs->io));
	if (!zs->io) {
		ZS_DEBUG ("No memory for zs->io\n");
		goto out;
	}

	for (i = 0; i < zs->num_ios; i++) {
		zs->io[i] = calloc(1, sizeof(*zs->io[0]));
		if (!zs->io[i]) {
			ZS_DEBUG ("No memory for zs->io[%d]\n", i);
			goto out;
		}

		ret = pthread_cond_init(&zs->io[i]->wait.cv, NULL);
		if (ret) {
			ZS_DEBUG ("pthread_cond_init(io %d) failed with %s\n",
					i, strerror(ret));
			goto out;
		}
	}

	if (zs->ssd_buffer_flag == 1) {
		zs->ssd = calloc(zs->num_ssds, sizeof(zs->ssd));
		if (!zs->ssd) {
			ZS_DEBUG ("No memory for zs->ssd\n");
			goto out;
		}	

		for (i = 0; i < zs->num_ssds; i++) {
			zs->ssd[i] = calloc(1, sizeof(*zs->ssd[0]));
			if (!zs->ssd[i]) {
				ZS_DEBUG ("No memory for zs->ssd[%d]\n", i);
				goto out;	
			}
	
			ret = pthread_cond_init(&zs->ssd[i]->wait.cv, NULL);
			if (ret) {
				ZS_DEBUG ("pthread_cond_init(ssd %d) failed with %s\n", 
				          i, strerror(ret));
				goto out;
			}
		}
		zs->ssdq = calloc(1, sizeof(*zs->ssdq));
		TAILQ_INIT(&zs->ssdq->q);

		ret = pthread_mutex_init(&zs->ssdq->lock, NULL);
		if (ret) {
			ZS_DEBUG ("pthread_mutex_init(ssd %d) failed with %s\n", 
			          0, strerror(ret));
			goto out;
		}
	}

	zs->osts = calloc(zs->num_osts, sizeof(*zs->osts));
	if (!zs->osts) {
		ZS_DEBUG ("No memory for zs->osts\n");
		goto out;
	}

	for (i = 0; i < zs->num_osts; i++) {
		zs->osts[i] = calloc(1, sizeof(*zs->osts[0]));
		if (!zs->osts[i]) {
			ZS_DEBUG ("No memory for zs->osts[%d]\n", i);
			goto out;
		}

		TAILQ_INIT(&zs->osts[i]->q);

		ret = pthread_mutex_init(&zs->osts[i]->lock, NULL);
		if (ret) {
			ZS_DEBUG ("pthread_mutex_init(osts %d) failed with %s\n",
					i, strerror(ret));
			goto out;
		}
	}

	zs->lock = calloc(1, sizeof(*zs->lock));
	if (!zs->lock) {
		ZS_DEBUG ("No memory for zs->lock\n");
		goto out;
	}

	ret = pthread_mutex_init(zs->lock, NULL);
	if (ret) {
		ZS_DEBUG ("pthread_mutex_init(zs->lock) failed with %s\n",
				strerror(ret));
		goto out;
	}

	waitq_init(&zs->start, zs->lock);

	ids_init(zs->rma_cnt, &zs->ids);
	if (zs->ssd_buffer_flag == 1)
		ids_init(zs->ssd_cnt, &zs->ids_ssd);

	zs->role = ZS_SRC;

	if (zs->ssd_buffer_flag == 1) {
		zs->ssd_fd = open (zs->ssd_log_file_name,
		                   O_CREAT | O_RDWR, 0666);
		if (zs->ssd_fd == -1)
			fprintf (stderr, "Cannot open %s\n",
			         zs->ssd_log_file_name);
		assert(zs->ssd_fd != -1);

        	posix_fallocate(zs->ssd_fd, 0, zs->ssd_cnt* ZS_RMA_MTU);

		zs->ssd_mmap_addr = mmap (NULL,
		                          zs->ssd_cnt * ZS_RMA_MTU,
		                          PROT_WRITE | PROT_READ,
		                          MAP_SHARED, zs->ssd_fd, 0);	


		if (zs->ssd_mmap_addr == MAP_FAILED) {
			ZS_DEBUG ("mmap failed\n");
			goto out;
		}

		zs->ssd_offset = 0;
	}

	/* start the threads */

  gettimeofday(&time_start, NULL);
  
	ret = pthread_create(&zs->master.tid, NULL, master, (void*) zs);

  pin_to_core(idx_core_0, zs->master.tid);
  idx_core_0 += 2;
  //idx_core_0 ++;

	if (ret) {
		ZS_DEBUG ("pthread_create(master) failed with %s\n",
				strerror(ret));
		goto out;
	}

  //idx_core_odd = 3;
	for (i = 0; i < zs->num_comms; i++) {
		ret = pthread_create(&zs->comm[i]->tid, NULL, comm, (void*)zs);
    pin_to_core(idx_core_0, zs->comm[i]->tid);
    idx_core_0 += 2;
    //idx_core_0 ++;

		if (ret) {
			pthread_mutex_lock(zs->lock);
			zs->done++;
			pthread_mutex_unlock(zs->lock);
		}
	}

	for (i = 0; i < zs->num_ios; i++) {
		ret = pthread_create(&zs->io[i]->tid, NULL, io, (void*)zs);
    //pin_to_core(idx_core_2, zs->io[i]->tid);
    //idx_core_2 += 2;
    //idx_core_3 ++;

    /////// For dividing I/O threads into two
    ////// Currently, scheduling is done with local CPU socket (NUMA node 0 and 2)
    ////// Inside socket, I/O threads are placed equally to two nodes
    if (i < zs->num_ios/2) { 
      pin_to_core(idx_core_0, zs->io[i]->tid);
      idx_core_0 += 2;
    }
    else {
      pin_to_core(idx_core_2, zs->io[i]->tid);
      idx_core_2 += 2;
    }

    /////// For 12 I/O threads
    /*if (i < zs->num_ios/3) { 
      pin_to_core(idx_core_1, zs->io[i]->tid);
      idx_core_1 += 2;
    } 
    else if (i < (zs->num_ios/3)*2){
      pin_to_core(idx_core_2, zs->io[i]->tid);
      idx_core_2 += 2;
    } 
    else {
      pin_to_core(idx_core_3, zs->io[i]->tid);
      idx_core_3 += 2;
    }*/

    /////// For dividing I/O threads into three
    /*if (i < 4) {
      pin_to_core(idx_core_2, zs->io[i]->tid);
      idx_core_2 += 2;
    }
    else if (i < 6) {
      pin_to_core(idx_core_3, zs->io[i]->tid);
      idx_core_3 += 2;
    }
    else {
      pin_to_core(idx_core_1, zs->io[i]->tid);
      idx_core_1 += 2;
    }*/

    /////// For dividing I/O threads into four
    /*if (i < 1) {
      pin_to_core(idx_core_0, zs->io[i]->tid);
      idx_core_0 += 2;
    }
    else if (i < 2) {
      pin_to_core(idx_core_1, zs->io[i]->tid);
      idx_core_1 += 2;
    }
    else if (i < 3) {
      pin_to_core(idx_core_2, zs->io[i]->tid);
      idx_core_2 += 2;
    }
    else {
      pin_to_core(idx_core_3, zs->io[i]->tid);
      idx_core_3 += 2;
    }*/

		if (ret) {
			pthread_mutex_lock(zs->lock);
			zs->done++;
			pthread_mutex_unlock(zs->lock);
		}
	}
  printf("For check, idx_core_0 : %d\n", idx_core_0);
  printf("For check, idx_core_1 : %d\n", idx_core_1);
  printf("For check, idx_core_2 : %d\n", idx_core_2);
  printf("For check, idx_core_3 : %d\n", idx_core_3);

	if (zs->ssd_buffer_flag == 1) {
		for (i = 0; i < zs->num_ssds; i++) {
			ret = pthread_create(&zs->ssd[i]->tid, NULL, ssd, (void*)zs);
			if (ret) {
				pthread_mutex_lock(zs->lock);
				zs->done++;
				pthread_mutex_unlock(zs->lock);
			}	
		}
	}	

//	while (!zs->done && zs->ready < (zs->num_comms + zs->num_ios))
	while (!zs->done && zs->ready < (zs->num_comms + zs->num_ios + zs->num_ssds))
		sleep(1);

	/* We are ready or done, wake everyone */
	waitq_broadcast(&zs->start);

	/* If we did not fail, wait on threads */
	if (!zs->done) {
		pthread_join(zs->master.tid, NULL);
		for (i = 0; i < zs->num_comms; i++)
			pthread_join(zs->comm[i]->tid, NULL);
		for (i = 0; i < zs->num_ios; i++)
			pthread_join(zs->io[i]->tid, NULL);
		if (zs->ssd_buffer_flag == 1)
			for (i = 0; i < zs->num_ssds; i++)
				pthread_join(zs->ssd[i]->tid, NULL);
	}
  gettimeofday(&time_end, NULL);

  printf("time : %ld\n", (time_end.tv_sec - time_start.tv_sec)*1000000 \
                        + time_end.tv_usec - time_start.tv_usec);

    out:
	if (zs) {
		free(zs->lock);

		if (zs->io) {
			for (i = 0; i < zs->num_ios; i++) {
				if (zs->io[i])
					pthread_cond_destroy(&zs->io[i]->wait.cv);
				free(zs->io[i]);
			}
			free(zs->io);
		}

		if (zs->ssd_buffer_flag == 1 && zs->ssd) {
			for (i = 0; i < zs->num_ssds; i++) {
				if (zs->ssd[i])
					pthread_cond_destroy(&zs->ssd[i]->wait.cv);
				free(zs->ssd[i]);
			}
			free(zs->ssd);
		}

		if (zs->osts) {
			for (i = 0; i < zs->num_osts; i++)
				free(zs->osts[i]);
			free(zs->osts);
		}

		if (zs->ssd_buffer_flag == 1 && zs->ssdq) {
			free(zs->ssdq);
		}

		if (zs->comm) {
			for (i = 0; i < zs->num_comms; i++)
				free(zs->comm[i]);
			free(zs->comm);
		}

		fd_tree_destroy(zs->ftree);
		free(zs->server_uri);
		free(zs->dir_name);
		//if (zs->ssd_buffer_flag == 1)
		//	unlink(zs->ssd_log_file_name);
		free(zs->ssd_log_file_name);
		if (zs->ssd_buffer_flag == 1) {
			munmap (zs->ssd_mmap_addr, ZS_SSD_BLOCKS * ZS_RMA_MTU);
			close(zs->ssd_fd);
		}
	}
	free(zs);

	return ret;
}
