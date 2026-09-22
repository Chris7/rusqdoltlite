
bin_PROGRAMS = blockcachevfsd $(bcvtclsqlite)
EXTRA_PROGRAMS = bcvtclsqlite

blockcachevfsd_CFLAGS = -DSQLITE_ENABLE_DESERIALIZE
blockcachevfsd_SOURCES = src/blockcachevfs.c src/blockcachevfsd.c src/simplexml.c src/sqlite3.c src/bcvutil.c src/bcvmodule.c src/bcvlog.c src/bcvencrypt.c

bcvtclsqlite_CFLAGS = -DTCLSH_INIT_PROC=bcvtest_init -DSTATIC_BUILD -DSQLITE_ENABLE_DESERIALIZE
bcvtclsqlite_SOURCES = src/blockcachevfs.c src/sqlite3.c src/tclsqlite.c src/blockcachevfstest.c src/simplexml.c src/bcvutil.c src/bcvmodule.c src/bcvlog.c src/bcvencrypt.c
