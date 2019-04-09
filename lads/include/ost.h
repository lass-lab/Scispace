#ifndef OST_H
#define OST_H

#include <stdint.h>
#include "zs.h"

int get_file_layout_info(char *path, uint32_t *ost_offset, uint32_t *ost_cnt, uint32_t *ost_mtu, uint32_t *ost_stripe_info);

#endif /* OST_H*/
