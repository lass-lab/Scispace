#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
//#include <lustre/liblustreapi.h>
#include <lustre/lustreapi.h>
#include <errno.h>

#include "ost.h"
//#include "zs.h"

#define LOV_EA_SIZE(lum, num) (sizeof(*lum) + num * sizeof(*lum->lmm_objects))
#define LOV_EA_MAX(lum) LOV_EA_SIZE(lum, ZS_MAX_NUM_OSTS)

int
get_file_layout_info(char *path,
                     uint32_t *ost_offset,
                     uint32_t *ost_cnt,
                     uint32_t *ost_mtu,
                     uint32_t *ost_stripe_info)
{
	struct lov_user_md *lump;
	int rc;
	int i;

	//lump = malloc(LOV_EA_MAX(lump));
	lump = calloc(1, (LOV_EA_MAX(lump)));

	if (lump == NULL) {
		ZS_DEBUG ("No memory for lump\n");
		goto out;
	}

	ZS_DEBUG ("%s\n", path);

  rc = llapi_file_get_stripe(path, lump);
  if (rc != 0) {
    ZS_DEBUG ("filename:%s\n", path);
    ZS_DEBUG ("get_stripe failed: %d (%s)\n",errno, strerror(errno));
    return -1;
  }

	*ost_offset = (uint32_t) lump->lmm_objects[0].l_ost_idx;	// the first OST index 
	*ost_cnt = (uint32_t) lump->lmm_stripe_count;
	*ost_mtu = (uint32_t) lump->lmm_stripe_size;

	for (i = 0; i < lump->lmm_stripe_count; i++) {
		ost_stripe_info[i] = lump->lmm_objects[i].l_ost_idx;
	}

	/*
	fprintf(stderr, "Lov magic %u\n", lump->lmm_magic);
	fprintf(stderr, "Lov pattern %u\n", lump->lmm_pattern);
	fprintf(stderr, "Lov object id %llu\n", lump->lmm_object_id);
	fprintf(stderr, "Lov object group %llu\n", lump->lmm_object_gr);
	fprintf(stderr, "Lov stripe size %u\n", lump->lmm_stripe_size);
	fprintf(stderr, "Lov stripe count %hu\n", lump->lmm_stripe_count);
	fprintf(stderr, "Lov stripe offset %u\n", lump->lmm_stripe_offset);

	for (i = 0; i < lump->lmm_stripe_count; i++) {
		printf("Object index %d Objid %llu\n", lump->lmm_objects[i].l_ost_idx, lump->lmm_objects[i].l_object_id);
        }

	int ost_layout[ZS_NUM_OSTS]; 
	int index;

	for (i = 0; i < lump->lmm_stripe_count; i++) {
		ost_layout[i] = lump->lmm_objects[i].l_ost_idx;
		index = ost_layout[i]; 
		global_stat_array[index]++;
	}
	*/

   out:

	free(lump);
	return rc;
}
