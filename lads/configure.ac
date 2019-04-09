# -*- shell-script -*-
#
# Copyright © 2013 UT-Battelle, LLC.  All rights reserved.
#
# See COPYING in top-level directory.
# 
# Additional copyrights may follow
# 
# $HEADER$
#

AC_INIT([zs], [0.1], [yk7@ornl.gov])
AC_CONFIG_SRCDIR([src/source.c])

AM_INIT_AUTOMAKE([dist-bzip2 subdir-objects foreign tar-ustar -Wall -Werror])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_RANLIB
AC_PROG_MKDIR_P
AM_PROG_CC_C_O

AC_CONFIG_HEADERS([include/config.h])

CPPFLAGS="$CPPFLAGS -I$srcdir/../include"

# CCIR_CHECK_WITHDIR(with_option_name, dir_value, file_in_dir)
# ----------------------------------------------------
AC_DEFUN([CCI_CHECK_WITHDIR],[
    AC_MSG_CHECKING([--with-$1 value])
    AS_IF([test "$2" = "yes" -o "$2" = "no" -o "x$2" = "x"],
          [AC_MSG_RESULT([simple ok (unspecified)])],
          [AS_IF([test ! -d "$2"],
                 [AC_MSG_RESULT([not found])
                  AC_MSG_WARN([Directory $2 not found])
                  AC_MSG_ERROR([Cannot continue])],
                 [AS_IF([test "x`ls $2/$3 2> /dev/null`" = "x"],
                        [AC_MSG_RESULT([not found])
                         AC_MSG_WARN([Expected file $2/$3 not found])
                         AC_MSG_ERROR([Cannot continue])],
                        [AC_MSG_RESULT([sanity check ok ($2)])]
                       )
                 ]
                )
          ]
         )
])dnl

AC_ARG_WITH([cci], [AS_HELP_STRING([--with-cci=DIR],
            [Specify the location of the CCI installation.])])
CCI_CHECK_WITHDIR([cci],[$with_cci], [include/cci.h])
AC_ARG_WITH([cci-libdir], [AC_HELP_STRING([--with-cci-libdir=DIR],
             [Search for CCI libraries in DIR])])
CCI_CHECK_WITHDIR([cci-libdir],[$with_cci_libdir], [libcci*])

AS_IF([test ! -z "$with_cci" -a "$with_cci" != "yes"],
      [cci_dir="$with_cci"])
AS_IF([test ! -z "$with_cci_libdir" -a "$with_cci_libdir" != "yes"],
      [cci_libdir="$with_cci_libdir"])

CPPFLAGS_save="$CPPFLAGS"
AS_IF([test ! -z "$cci_dir"],
      AC_SUBST([CPPFLAGS],["-I$cci_dir/include $CPPFLAGS_save"]))
LDFLAGS_save="$LDFLAGS"
AS_IF([test ! -z "$cci_libdir"],
      AC_SUBST([LDFLAGS],(["-L$cci_libdir -Wl,-rpath,$cci_libdir $LDFLAGS_save"])))
AS_IF([test ! -z "$cci_dir" -a -z "$cci_libdir"],
      AC_SUBST([LDFLAGS],(["-L$cci_dir/lib -Wl,-rpath,$cci_dir/lib $LDFLAGS_save"])))

AC_CHECK_LIB([cci], [cci_init], , AC_MSG_ERROR([Unable to locate CCI installation]))

# Option to enable Lustre
# -----------------------
AC_DEFUN([LUSTRE_CONFIG],[
    AC_ARG_ENABLE([lustre-support],
                  [AC_HELP_STRING([--enable-lustre-support],
                                  [Enable the Lustre support (default: no)])
                  ],
                  [AC_DEFINE([ENABLE_LUSTRE_SUPPORT], [1], [Enable/disable Lustre support]) lustre=yes],
                  [AC_DEFINE([ENABLE_LUSTRE_SUPPORT], [0], [Enable/disable Lustre support]) lustre=no]
    )
])

LUSTRE_CONFIG

AM_CONDITIONAL([LUSTRE_ENABLED], [test x$lustre = xyes])

AC_CONFIG_FILES([Makefile src/Makefile etc/Makefile])

AC_OUTPUT
