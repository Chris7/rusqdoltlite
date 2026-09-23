

TOP = ../trunk/src

LIBS = -lpthread -ldl -lcurl -lssl -lcrypto 
#LIBS += -fsanitize=address -fsanitize=undefined

CC = gcc 
CFLAGS = -Wall -g -O0 
CFLAGS += -I$(HOME)/tcl/include/ -DTCLSH_INIT_PROC=bcvtest_init
CFLAGS += -DSQLITE_OMIT_LOOKASIDE 
CFLAGS += -DSQLITE_DIRECT_OVERFLOW_READ
CFLAGS += -DSQLITE_DEBUG
#CFLAGS += -DNDEBUG
CFLAGS += -DHAVE_USLEEP
CFLAGS += -I$(TOP)
#CFLAGS += -fsanitize=thread 
#CFLAGS += -fsanitize=address -fsanitize=undefined
#CFLAGS += -fprofile-arcs -ftest-coverage -DNDEBUG

# CFLAGS += -Wno-unused-function
CFLAGS += -Wno-unused-but-set-variable -Wno-deprecated-declarations
CFLAGS += -Wno-int-to-pointer-cast

HDR = $(TOP)/bcv_int.h \
      $(TOP)/bcvutil.h \
      $(TOP)/bcvmodule.h \
      $(TOP)/blockcachevfs.h

DAEMON_OBJ = sqlite3.o blockcachevfsd.o simplexml.o bcvutil.o bcvmodule.o blockcachevfs.o bcvlog.o bcvencrypt.o
TCL_OBJ = sqlite3.o tclsqlite.o blockcachevfstest.o bcvutil_test.o simplexml.o bcvmodule.o blockcachevfs.o bcvlog.o bcvencrypt.o

TT3_OBJ = sqlite3.o threadtest3.o bcvutil.o simplexml.o bcvmodule.o blockcachevfs.o bcvlog.o bcvencrypt.o

CS_OBJ = sqlite3.o bcvutil.o simplexml.o bcvmodule.o blockcachevfs.o cloudsql.o bcvlog.o bcvencrypt.o

# TT3_OBJ += blockcachevfs.o


# all: blockcachevfsd bcvshell bcvtclsqlite
all: bcvtclsqlite blockcachevfsd cloudsql

blockcachevfsd: $(DAEMON_OBJ)
	$(CC) $(CFLAGS) $(DAEMON_OBJ) -o $@ $(LIBS) 

bcvtclsqlite: $(TCL_OBJ)
	$(CC) $(CFLAGS) $(TCL_OBJ) -o $@ $(LIBS) -ltcl

threadtest3: $(TT3_OBJ)
	$(CC) $(CFLAGS) $(TT3_OBJ) -o $@ $(LIBS)

cloudsql: $(CS_OBJ)
	$(CC) $(CFLAGS) $(CS_OBJ) -o $@ $(LIBS)

%.o: $(TOP)/%.c $(HDR)
	$(CC) $(CFLAGS) -c $< -o $@

bcvutil_test.o: $(TOP)/bcvutil.c $(HDR)
	$(CC) $(CFLAGS) -DSQLITE_BCV_CURL_HANDLE_CONFIG=tclTestCurlConfig -c $< -o $@

%.o: $(TOP)/../test/%.c $(HDR)
	$(CC) $(CFLAGS) -c $< -o $@

%.o: $(TOP)/../www/%.c $(HDR)
	$(CC) $(CFLAGS) -c $< -o $@


clean:
	rm -f *.o
	rm -f blockcachevfsd
	rm -f bcvshell
	rm -f bcvtclsqlite
	rm -f threadtest3


