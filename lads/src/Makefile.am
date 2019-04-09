#
# Copyright © 2013 UT-Battelle, LLC.  All rights reserved.
# $COPYRIGHT
#

bin_PROGRAMS = zs_src zs_sink

COMMON_SOURCES = zs.h               \
                master.c master.h   \
                comm.c comm.h       \
                io.c io.h           \
                fd_tree.c fd_tree.h \
                waitq.c waitq.h     \
                ids.c ids.h         \
                wire.h request.h    \
                ssd.h ssd.c         \
                common.c

if LUSTRE_ENABLED
COMMON_SOURCES += ost.h ost.c
endif

zs_src_SOURCES = source.c $(COMMON_SOURCES)
if LUSTRE_ENABLED
zs_src_LDADD = /usr/lib64/liblustreapi.so
endif
zs_sink_SOURCES = sink.c $(COMMON_SOURCES)
if LUSTRE_ENABLED
zs_sink_LDADD = /usr/lib64/liblustreapi.so
endif
