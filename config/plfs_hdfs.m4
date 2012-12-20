AC_DEFUN([AC_PKG_PLFS_HDFS_SETUP], [

    plfs_hdfs_javadir=/usr/lib/jvm

    AC_ARG_WITH([javadir],
        [AS_HELP_STRING([--with-javadir=JAVADIR],
            [give the path to java. @<:@default=/usr/lib/jvm@:>@])],
        [AS_IF([test "x$withval" = "x" -o ! -d "$withval"],
               [AC_MSG_ERROR([Bad JAVADIR value, please give a valid dir])],
               [plfs_hdfs_javadir="$withval"])] )

    AS_IF([test ! -d "$plfs_hdfs_javadir"],
          [AC_MSG_ERROR([Bad JAVADIR, give valid dir])])

    plfs_hdfs_java_inc_dir=$plfs_hdfs_javadir/include

    AC_ARG_WITH([java-inc-dir],
        [AS_HELP_STRING([--with-java-inc-dir=JAVAINCDIR],
            [give the path to java inc dir. @<:@default=JAVADIR/include@:>@])],
        [AS_IF([test "x$withval" = "x" -o ! -d "$withval"],
               [AC_MSG_ERROR([Bad JAVAINCDIR value, please give a valid dir])],
               [plfs_hdfs_java_inc_dir="$withval"])] )

    AS_IF([test ! -f "$plfs_hdfs_java_inc_dir/jni.h"],
          [AC_MSG_ERROR([Cannot find jni.h in JAVAINCDIR: give valid dir])])

    #XXX: sun java requires this, trying to avoid hardwiring linux
    plfs_hdfs_uname=`uname | tr A-Z a-z`
    plfs_hdfs_java_inc_md_dir=$plfs_hdfs_java_inc_dir/$plfs_hdfs_uname

    AC_ARG_WITH([java-inc-md-dir],
        [AS_HELP_STRING([--with-java-inc-md-dir=JAVAINCMDDIR],
         [give the path to java md inc dir. @<:@default=JAVAINCDIR/UNAME@:>@])],
        [AS_IF([test "x$withval" = "x" -o ! -d "$withval"],
            [AC_MSG_ERROR([Bad JAVAINCMDDIR value, please give a valid dir])],
               [plfs_hdfs_java_inc_md_dir="$withval"])] )

    AS_IF([test ! -d "$plfs_hdfs_java_inc_md_dir"],
          [AC_MSG_ERROR([Cannot find jni.h in JAVAINCMDDIR: give valid dir])])

    CFLAGS="$CFLAGS -I$plfs_hdfs_java_inc_dir -I$plfs_hdfs_java_inc_md_dir"
    CXXFLAGS="$CXXFLAGS -I$plfs_hdfs_java_inc_dir -I$plfs_hdfs_java_inc_md_dir"

    plfs_hdfs_java_lib_dir=$plfs_hdfs_javadir/jre/lib/amd64/server

    AC_ARG_WITH([java-lib-dir],
        [AS_HELP_STRING([--with-java-lib-dir=JAVALIBDIR],
         [give the path to java libjvm dir. ])],
        [AS_IF([test "x$withval" = "x" -o ! -d "$withval"],
            [AC_MSG_ERROR([Bad JAVALIBDIR value, please give a valid dir])],
               [plfs_hdfs_java_lib_dir="$withval"])] )

    LDFLAGS="$LDFLAGS -Wl,-R$plfs_hdfs_java_lib_dir -L$plfs_hdfs_java_lib_dir"

    AC_SEARCH_LIBS([JNI_GetCreatedJavaVMs], [jvm],,
                   [AC_MSG_ERROR([Cannot link with jvm])])

    plfs_hdfs_hadoopdir=/usr/hadoop

    AC_ARG_WITH([hadoopdir],
        [AS_HELP_STRING([--with-hadoopdir=HADOOPDIR],
            [give the path to hadoop. @<:@default=/usr/hadoop@:>@])],
        [AS_IF([test "x$withval" = "x" -o ! -d "$withval"],
               [AC_MSG_ERROR([Bad HADOOPDIR value, please give a valid dir])],
               [plfs_hdfs_hadoopdir="$withval"])] )

    AS_IF([test ! -d "$plfs_hdfs_hadoopdir"],
          [AC_MSG_ERROR([Bad HADOOPDIR, give valid dir])])

    plfs_hdfs_hadoop_inc_dir=$plfs_hdfs_hadoopdir/hdfs/src/c++/libhdfs

    AC_ARG_WITH([hadoop-inc-dir],
        [AS_HELP_STRING([--with-hadoop-inc-dir=HADOOPINCDIR],
            [give the path to hadoop inc dir. @<:@default=HADOOPDIR/hdfs/src/c++/libhdfs@:>@])],
        [AS_IF([test "x$withval" = "x" -o ! -d "$withval"],
               [AC_MSG_ERROR([Bad HADOOPINCDIR value, give a valid dir])],
               [plfs_hdfs_hadoop_inc_dir="$withval"])] )

    AS_IF([test ! -f "$plfs_hdfs_hadoop_inc_dir/hdfs.h"],
          [AC_MSG_ERROR([Cannot find hdfs.h in HADOOPINCDIR: give valid dir])])

    CFLAGS="$CFLAGS -I$plfs_hdfs_hadoop_inc_dir"
    CXXFLAGS="$CXXFLAGS -I$plfs_hdfs_hadoop_inc_dir"

    CFLAGS="$CFLAGS -DUSE_HDFS"
    CXXFLAGS="$CXXFLAGS -DUSE_HDFS"

    plfs_hdfs_hadoop_lib_dir=$plfs_hdfs_hadoopdir/hdfs/src/c++/install/lib

    AC_ARG_WITH([hadoop-lib-dir],
        [AS_HELP_STRING([--with-hadoop-lib-dir=HADOOPLIBDIR],
         [give the path to hadoop libhdfs dir. ])],
        [AS_IF([test "x$withval" = "x" -o ! -d "$withval"],
            [AC_MSG_ERROR([Bad HADOOPLIBDIR value, please give a valid dir])],
               [plfs_hdfs_hadoop_lib_dir="$withval"])] )

    LDFLAGS="$LDFLAGS -L$plfs_hdfs_hadoop_lib_dir"

    AC_SEARCH_LIBS([hdfsConnect], [hdfs],,
                   [AC_MSG_ERROR([Cannot link with hdfs])])
])dnl


AC_DEFUN([AC_PKG_PLFS_HDFS],
[
    AC_MSG_CHECKING([whether PLFS HDFS support is enabled])
    AC_ARG_ENABLE([hdfs],
        AC_HELP_STRING([--enable-hdfs], 
            [enable PLFS HDFS support (default: disable)]),,[enable_hdfs=no])

    AS_IF([test "x$enable_hdfs" = "xno"], 
          [AC_MSG_RESULT([no])],
          [AC_MSG_RESULT([yes]) AC_PKG_PLFS_HDFS_SETUP ] )
])dnl
