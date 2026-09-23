/*
** 2010-07-22
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** The code in this file runs a few multi-threaded test cases using the
** SQLite library. It can be compiled to an executable on unix using the
** following command:
**
**   gcc -O2 threadtest3.c sqlite3.c -ldl -lpthread -lm
**
** Even though threadtest3.c is the only C source code file mentioned on
** the compiler command-line, #include macros are used to pull in additional
** C code files named "tt3_*.c".
**
** After compiling, run this program with an optional argument telling
** which test to run.  All tests are run if no argument is given.  The
** argument can be a glob pattern to match multiple tests.  Examples:
**
**        ./a.out                 -- Run all tests
**        ./a.out walthread3      -- Run the "walthread3" test
**        ./a.out 'wal*'          -- Run all of the wal* tests
**        ./a.out --help          -- List all available tests
**
** The exit status is non-zero if any test fails.
*/

/* 
** The "Set Error Line" macro.
*/
#define SEL(e) ((e)->iLine = ((e)->rc ? (e)->iLine : __LINE__))

/* Database functions */
#define opendb(w,x,y,z)         (SEL(w), opendb_x(w,x,y,z))
#define closedb(y,z)            (SEL(y), closedb_x(y,z))

/* Functions to execute SQL */
#define sql_script(x,y,z)       (SEL(x), sql_script_x(x,y,z))
#define integrity_check(x,y)    (SEL(x), integrity_check_x(x,y))
#define quick_check(x,y)        (SEL(x), quick_check_x(x,y))
#define execsql_i64(x,y,...)    (SEL(x), execsql_i64_x(x,y,__VA_ARGS__))
#define execsql_text(x,y,z,...) (SEL(x), execsql_text_x(x,y,z,__VA_ARGS__))
#define execsql(x,y,...)        (SEL(x), (void)execsql_i64_x(x,y,__VA_ARGS__))
#define sql_script_printf(x,y,z,...) (                \
    SEL(x), sql_script_printf_x(x,y,z,__VA_ARGS__)    \
) 

/* Thread functions */
#define launch_thread(w,x,y,z)     (SEL(w), launch_thread_x(w,x,y,z))
#define join_all_threads(y,z)      (SEL(y), join_all_threads_x(y,z))

/* Timer functions */
#define setstoptime(y,z)        (SEL(y), setstoptime_x(y,z))
#define timetostop(z)           (SEL(z), timetostop_x(z))

/* Report/clear errors. */
#define test_error(z, ...)      test_error_x(z, sqlite3_mprintf(__VA_ARGS__))
#define clear_error(y,z)        clear_error_x(y, z)

/* File-system operations */
#define filesize(y,z)           (SEL(y), filesize_x(y,z))
#define filecopy(x,y,z)         (SEL(x), filecopy_x(x,y,z))

/* BCVFS functions */
#define bcvfs_prefetch(w,x,y,z)   (SEL(w), bcvfs_prefetch_x(w,x,y,z))

#define PTR2INT(x) ((int)((intptr_t)x))
#define INT2PTR(x) ((void*)((intptr_t)x))

/*
** End of test code/infrastructure interface macros.
*************************************************************************/




#include <sqlite3.h>
#include <bcvutil.h>
#include <bcv_int.h>
#include <blockcachevfs.h>

#ifdef _WIN32
# include <stdio.h>
# include <string.h>
# include <assert.h>
# include <process.h>
# include <windows.h>
# include <sys/types.h> 
# include <sys/stat.h> 
# include <errno.h>
# include <fcntl.h>
# include <io.h>
#else
# include <stdlib.h>
# include <unistd.h>
# include <stdio.h>
# include <pthread.h>
# include <assert.h>
# include <sys/types.h> 
# include <sys/stat.h> 
# include <string.h>
# include <fcntl.h>
# include <errno.h>

# define O_BINARY 0
#endif

#ifdef _WIN32
# include <windows.h>
# define BCV_PATH_SEPARATOR "\\"
# define osMkdir(x,y) mkdir(x)
#else
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
# include <pthread.h>
# define BCV_PATH_SEPARATOR "/"
# define osMkdir(x,y) mkdir(x,y)
#endif

/*
 * This code implements the MD5 message-digest algorithm.
 * The algorithm is due to Ron Rivest.  This code was
 * written by Colin Plumb in 1993, no copyright is claimed.
 * This code is in the public domain; do with it what you wish.
 *
 * Equivalent code is available from RSA Data Security, Inc.
 * This code has been tested against that, and is equivalent,
 * except that you don't need to include two pages of legalese
 * with every copy.
 *
 * To compute the message digest of a chunk of bytes, declare an
 * MD5Context structure, pass it to MD5Init, call MD5Update as
 * needed on buffers full of bytes, and then call MD5Final, which
 * will fill a supplied 16-byte array with the digest.
 */

/*
 * If compiled on a machine that doesn't have a 32-bit integer,
 * you just set "uint32" to the appropriate datatype for an
 * unsigned 32-bit integer.  For example:
 *
 *       cc -Duint32='unsigned long' md5.c
 *
 */
#ifndef uint32
#  define uint32 unsigned int
#endif

struct MD5Context {
  int isInit;
  uint32 buf[4];
  uint32 bits[2];
  union {
    unsigned char in[64];
    uint32 in32[16];
  } u;
};
typedef struct MD5Context MD5Context;

/*
 * Note: this code is harmless on little-endian machines.
 */
static void byteReverse (unsigned char *buf, unsigned longs){
  uint32 t;
  do {
    t = (uint32)((unsigned)buf[3]<<8 | buf[2]) << 16 |
          ((unsigned)buf[1]<<8 | buf[0]);
    *(uint32 *)buf = t;
    buf += 4;
  } while (--longs);
}
/* The four core functions - F1 is optimized somewhat */

/* #define F1(x, y, z) (x & y | ~x & z) */
#define F1(x, y, z) (z ^ (x & (y ^ z)))
#define F2(x, y, z) F1(z, x, y)
#define F3(x, y, z) (x ^ y ^ z)
#define F4(x, y, z) (y ^ (x | ~z))

/* This is the central step in the MD5 algorithm. */
#define MD5STEP(f, w, x, y, z, data, s) \
  ( w += f(x, y, z) + data,  w = w<<s | w>>(32-s),  w += x )

/*
 * The core of the MD5 algorithm, this alters an existing MD5 hash to
 * reflect the addition of 16 longwords of new data.  MD5Update blocks
 * the data and converts bytes into longwords for this routine.
 */
static void MD5Transform(uint32 buf[4], const uint32 in[16]){
  register uint32 a, b, c, d;

  a = buf[0];
  b = buf[1];
  c = buf[2];
  d = buf[3];

  MD5STEP(F1, a, b, c, d, in[ 0]+0xd76aa478,  7);
  MD5STEP(F1, d, a, b, c, in[ 1]+0xe8c7b756, 12);
  MD5STEP(F1, c, d, a, b, in[ 2]+0x242070db, 17);
  MD5STEP(F1, b, c, d, a, in[ 3]+0xc1bdceee, 22);
  MD5STEP(F1, a, b, c, d, in[ 4]+0xf57c0faf,  7);
  MD5STEP(F1, d, a, b, c, in[ 5]+0x4787c62a, 12);
  MD5STEP(F1, c, d, a, b, in[ 6]+0xa8304613, 17);
  MD5STEP(F1, b, c, d, a, in[ 7]+0xfd469501, 22);
  MD5STEP(F1, a, b, c, d, in[ 8]+0x698098d8,  7);
  MD5STEP(F1, d, a, b, c, in[ 9]+0x8b44f7af, 12);
  MD5STEP(F1, c, d, a, b, in[10]+0xffff5bb1, 17);
  MD5STEP(F1, b, c, d, a, in[11]+0x895cd7be, 22);
  MD5STEP(F1, a, b, c, d, in[12]+0x6b901122,  7);
  MD5STEP(F1, d, a, b, c, in[13]+0xfd987193, 12);
  MD5STEP(F1, c, d, a, b, in[14]+0xa679438e, 17);
  MD5STEP(F1, b, c, d, a, in[15]+0x49b40821, 22);

  MD5STEP(F2, a, b, c, d, in[ 1]+0xf61e2562,  5);
  MD5STEP(F2, d, a, b, c, in[ 6]+0xc040b340,  9);
  MD5STEP(F2, c, d, a, b, in[11]+0x265e5a51, 14);
  MD5STEP(F2, b, c, d, a, in[ 0]+0xe9b6c7aa, 20);
  MD5STEP(F2, a, b, c, d, in[ 5]+0xd62f105d,  5);
  MD5STEP(F2, d, a, b, c, in[10]+0x02441453,  9);
  MD5STEP(F2, c, d, a, b, in[15]+0xd8a1e681, 14);
  MD5STEP(F2, b, c, d, a, in[ 4]+0xe7d3fbc8, 20);
  MD5STEP(F2, a, b, c, d, in[ 9]+0x21e1cde6,  5);
  MD5STEP(F2, d, a, b, c, in[14]+0xc33707d6,  9);
  MD5STEP(F2, c, d, a, b, in[ 3]+0xf4d50d87, 14);
  MD5STEP(F2, b, c, d, a, in[ 8]+0x455a14ed, 20);
  MD5STEP(F2, a, b, c, d, in[13]+0xa9e3e905,  5);
  MD5STEP(F2, d, a, b, c, in[ 2]+0xfcefa3f8,  9);
  MD5STEP(F2, c, d, a, b, in[ 7]+0x676f02d9, 14);
  MD5STEP(F2, b, c, d, a, in[12]+0x8d2a4c8a, 20);

  MD5STEP(F3, a, b, c, d, in[ 5]+0xfffa3942,  4);
  MD5STEP(F3, d, a, b, c, in[ 8]+0x8771f681, 11);
  MD5STEP(F3, c, d, a, b, in[11]+0x6d9d6122, 16);
  MD5STEP(F3, b, c, d, a, in[14]+0xfde5380c, 23);
  MD5STEP(F3, a, b, c, d, in[ 1]+0xa4beea44,  4);
  MD5STEP(F3, d, a, b, c, in[ 4]+0x4bdecfa9, 11);
  MD5STEP(F3, c, d, a, b, in[ 7]+0xf6bb4b60, 16);
  MD5STEP(F3, b, c, d, a, in[10]+0xbebfbc70, 23);
  MD5STEP(F3, a, b, c, d, in[13]+0x289b7ec6,  4);
  MD5STEP(F3, d, a, b, c, in[ 0]+0xeaa127fa, 11);
  MD5STEP(F3, c, d, a, b, in[ 3]+0xd4ef3085, 16);
  MD5STEP(F3, b, c, d, a, in[ 6]+0x04881d05, 23);
  MD5STEP(F3, a, b, c, d, in[ 9]+0xd9d4d039,  4);
  MD5STEP(F3, d, a, b, c, in[12]+0xe6db99e5, 11);
  MD5STEP(F3, c, d, a, b, in[15]+0x1fa27cf8, 16);
  MD5STEP(F3, b, c, d, a, in[ 2]+0xc4ac5665, 23);

  MD5STEP(F4, a, b, c, d, in[ 0]+0xf4292244,  6);
  MD5STEP(F4, d, a, b, c, in[ 7]+0x432aff97, 10);
  MD5STEP(F4, c, d, a, b, in[14]+0xab9423a7, 15);
  MD5STEP(F4, b, c, d, a, in[ 5]+0xfc93a039, 21);
  MD5STEP(F4, a, b, c, d, in[12]+0x655b59c3,  6);
  MD5STEP(F4, d, a, b, c, in[ 3]+0x8f0ccc92, 10);
  MD5STEP(F4, c, d, a, b, in[10]+0xffeff47d, 15);
  MD5STEP(F4, b, c, d, a, in[ 1]+0x85845dd1, 21);
  MD5STEP(F4, a, b, c, d, in[ 8]+0x6fa87e4f,  6);
  MD5STEP(F4, d, a, b, c, in[15]+0xfe2ce6e0, 10);
  MD5STEP(F4, c, d, a, b, in[ 6]+0xa3014314, 15);
  MD5STEP(F4, b, c, d, a, in[13]+0x4e0811a1, 21);
  MD5STEP(F4, a, b, c, d, in[ 4]+0xf7537e82,  6);
  MD5STEP(F4, d, a, b, c, in[11]+0xbd3af235, 10);
  MD5STEP(F4, c, d, a, b, in[ 2]+0x2ad7d2bb, 15);
  MD5STEP(F4, b, c, d, a, in[ 9]+0xeb86d391, 21);

  buf[0] += a;
  buf[1] += b;
  buf[2] += c;
  buf[3] += d;
}

/*
 * Start MD5 accumulation.  Set bit count to 0 and buffer to mysterious
 * initialization constants.
 */
static void MD5Init(MD5Context *ctx){
  ctx->isInit = 1;
  ctx->buf[0] = 0x67452301;
  ctx->buf[1] = 0xefcdab89;
  ctx->buf[2] = 0x98badcfe;
  ctx->buf[3] = 0x10325476;
  ctx->bits[0] = 0;
  ctx->bits[1] = 0;
}

/*
 * Update context to reflect the concatenation of another buffer full
 * of bytes.
 */
static 
void MD5Update(MD5Context *ctx, const unsigned char *buf, unsigned int len){
  uint32 t;

  /* Update bitcount */

  t = ctx->bits[0];
  if ((ctx->bits[0] = t + ((uint32)len << 3)) < t)
    ctx->bits[1]++; /* Carry from low to high */
  ctx->bits[1] += len >> 29;

  t = (t >> 3) & 0x3f;    /* Bytes already in shsInfo->data */

  /* Handle any leading odd-sized chunks */

  if ( t ) {
    unsigned char *p = (unsigned char *)ctx->u.in + t;

    t = 64-t;
    if (len < t) {
      memcpy(p, buf, len);
      return;
    }
    memcpy(p, buf, t);
    byteReverse(ctx->u.in, 16);
    MD5Transform(ctx->buf, (uint32 *)ctx->u.in);
    buf += t;
    len -= t;
  }

  /* Process data in 64-byte chunks */

  while (len >= 64) {
    memcpy(ctx->u.in, buf, 64);
    byteReverse(ctx->u.in, 16);
    MD5Transform(ctx->buf, (uint32 *)ctx->u.in);
    buf += 64;
    len -= 64;
  }

  /* Handle any remaining bytes of data. */

  memcpy(ctx->u.in, buf, len);
}

/*
 * Final wrapup - pad to 64-byte boundary with the bit pattern 
 * 1 0* (64-bit count of bits processed, MSB-first)
 */
static void MD5Final(unsigned char digest[16], MD5Context *ctx){
  unsigned count;
  unsigned char *p;

  /* Compute number of bytes mod 64 */
  count = (ctx->bits[0] >> 3) & 0x3F;

  /* Set the first char of padding to 0x80.  This is safe since there is
     always at least one byte free */
  p = ctx->u.in + count;
  *p++ = 0x80;

  /* Bytes of padding needed to make 64 bytes */
  count = 64 - 1 - count;

  /* Pad out to 56 mod 64 */
  if (count < 8) {
    /* Two lots of padding:  Pad the first block to 64 bytes */
    memset(p, 0, count);
    byteReverse(ctx->u.in, 16);
    MD5Transform(ctx->buf, (uint32 *)ctx->u.in);

    /* Now fill the next block with 56 bytes */
    memset(ctx->u.in, 0, 56);
  } else {
    /* Pad block to 56 bytes */
    memset(p, 0, count-8);
  }
  byteReverse(ctx->u.in, 14);

  /* Append length in bits and transform */
  ctx->u.in32[14] = ctx->bits[0];
  ctx->u.in32[15] = ctx->bits[1];

  MD5Transform(ctx->buf, (uint32 *)ctx->u.in);
  byteReverse((unsigned char *)ctx->buf, 4);
  memcpy(digest, ctx->buf, 16);
  memset(ctx, 0, sizeof(*ctx));    /* In case it is sensitive */
}

/*
** Convert a 128-bit MD5 digest into a 32-digit base-16 number.
*/
static void MD5DigestToBase16(unsigned char *digest, char *zBuf){
  static char const zEncode[] = "0123456789abcdef";
  int i, j;

  for(j=i=0; i<16; i++){
    int a = digest[i];
    zBuf[j++] = zEncode[(a>>4)&0xf];
    zBuf[j++] = zEncode[a & 0xf];
  }
  zBuf[j] = 0;
}

/*
** During testing, the special md5sum() aggregate function is available.
** inside SQLite.  The following routines implement that function.
*/
static void md5step(sqlite3_context *context, int argc, sqlite3_value **argv){
  MD5Context *p;
  int i;
  if( argc<1 ) return;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( p==0 ) return;
  if( !p->isInit ){
    MD5Init(p);
  }
  for(i=0; i<argc; i++){
    const char *zData = (char*)sqlite3_value_text(argv[i]);
    if( zData ){
      MD5Update(p, (unsigned char*)zData, strlen(zData));
    }
  }
}
static void md5finalize(sqlite3_context *context){
  MD5Context *p;
  unsigned char digest[16];
  char zBuf[33];
  p = sqlite3_aggregate_context(context, sizeof(*p));
  MD5Final(digest,p);
  MD5DigestToBase16(digest, zBuf);
  sqlite3_result_text(context, zBuf, -1, SQLITE_TRANSIENT);
}

/*
** End of copied md5sum() code.
**************************************************************************/

typedef sqlite3_int64 i64;

typedef struct Error Error;
typedef struct Sqlite Sqlite;
typedef struct Statement Statement;

typedef struct Threadset Threadset;
typedef struct Thread Thread;

/* Total number of errors in this process so far. */
static int nGlobalErr = 0;

struct Error {
  int rc;
  int iLine;
  char *zErr;
};

struct Sqlite {
  sqlite3 *db;                    /* Database handle */
  Statement *pCache;              /* Linked list of cached statements */
  int nText;                      /* Size of array at aText[] */
  char **aText;                   /* Stored text results */
};

struct Statement {
  sqlite3_stmt *pStmt;            /* Pre-compiled statement handle */
  Statement *pNext;               /* Next statement in linked-list */
};

struct Thread {
  int iTid;                       /* Thread number within test */
  void* pArg;                     /* Pointer argument passed by caller */

#ifdef _WIN32
  uintptr_t winTid;               /* Thread handle */
#else
  pthread_t tid;                  /* Thread id */
#endif
  char *(*xProc)(int, void*);     /* Thread main proc */
  char *zRes;                     /* Value returned by xProc */
  Thread *pNext;                  /* Next in this list of threads */
};

struct Threadset {
  int iMaxTid;                    /* Largest iTid value allocated so far */
  Thread *pThread;                /* Linked list of threads */
};

static void free_err(Error *p){
  sqlite3_free(p->zErr);
  p->zErr = 0;
  p->rc = 0;
}

static void print_err(Error *p){
  if( p->rc!=SQLITE_OK ){
    int isWarn = 0;
    if( p->rc==SQLITE_SCHEMA ) isWarn = 1;
    if( sqlite3_strglob("* - no such table: *",p->zErr)==0 ) isWarn = 1;
    printf("%s: (%d) \"%s\" at line %d\n", isWarn ? "Warning" : "Error",
            p->rc, p->zErr, p->iLine);
    if( !isWarn ) nGlobalErr++;
    fflush(stdout);
  }
}

static void print_and_free_err(Error *p){
  print_err(p);
  free_err(p);
}

static void system_error(Error *pErr, int iSys){
  pErr->rc = iSys;
#if _WIN32
  pErr->zErr = sqlite3_mprintf("%s", strerror(iSys));
#else
  pErr->zErr = (char *)sqlite3_malloc(512);
  strerror_r(iSys, pErr->zErr, 512);
  pErr->zErr[511] = '\0';
#endif
}

static void sqlite_error(
  Error *pErr, 
  Sqlite *pDb, 
  const char *zFunc
){
  pErr->rc = sqlite3_errcode(pDb->db);
  pErr->zErr = sqlite3_mprintf(
      "sqlite3_%s() - %s (%d)", zFunc, sqlite3_errmsg(pDb->db),
      sqlite3_extended_errcode(pDb->db)
  );
}

static void test_error_x(
  Error *pErr,
  char *zErr
){
  if( pErr->rc==SQLITE_OK ){
    pErr->rc = 1;
    pErr->zErr = zErr;
  }else{
    sqlite3_free(zErr);
  }
}

static void clear_error_x(
  Error *pErr,
  int rc
){
  if( pErr->rc==rc ){
    pErr->rc = SQLITE_OK;
    sqlite3_free(pErr->zErr);
    pErr->zErr = 0;
  }
}

static int busyhandler(void *pArg, int n){
  sqlite3_sleep(10);
  return 1;
}

static void opendb_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* OUT: Database handle */
  const char *zFile,              /* Database file name */
  int bDelete                     /* True to delete db file before opening */
){
  if( pErr->rc==SQLITE_OK ){
    int rc;
    int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI;
    if( bDelete ) unlink(zFile);
    rc = sqlite3_open_v2(zFile, &pDb->db, flags, 0);
    if( rc ){
      sqlite_error(pErr, pDb, "open");
      sqlite3_close(pDb->db);
      pDb->db = 0;
    }else{
      sqlite3_create_function(
          pDb->db, "md5sum", -1, SQLITE_UTF8, 0, 0, md5step, md5finalize
      );
      sqlite3_busy_handler(pDb->db, busyhandler, 0);
      sqlite3_exec(pDb->db, "PRAGMA synchronous=OFF", 0, 0, 0);
    }
  }
}

static void closedb_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb                     /* OUT: Database handle */
){
  int rc;
  int i;
  Statement *pIter;
  Statement *pNext;
  for(pIter=pDb->pCache; pIter; pIter=pNext){
    pNext = pIter->pNext;
    sqlite3_finalize(pIter->pStmt);
    sqlite3_free(pIter);
  }
  for(i=0; i<pDb->nText; i++){
    sqlite3_free(pDb->aText[i]);
  }
  sqlite3_free(pDb->aText);
  rc = sqlite3_close(pDb->db);
  if( rc && pErr->rc==SQLITE_OK ){
    pErr->zErr = sqlite3_mprintf("%s", sqlite3_errmsg(pDb->db));
  }
  memset(pDb, 0, sizeof(Sqlite));
}

static void sql_script_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  const char *zSql                /* SQL script to execute */
){
  if( pErr->rc==SQLITE_OK ){
    pErr->rc = sqlite3_exec(pDb->db, zSql, 0, 0, &pErr->zErr);
  }
}

static void sql_script_printf_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  const char *zFormat,            /* SQL printf format string */
  ...                             /* Printf args */
){
  va_list ap;                     /* ... printf arguments */
  va_start(ap, zFormat);
  if( pErr->rc==SQLITE_OK ){
    char *zSql = sqlite3_vmprintf(zFormat, ap);
    pErr->rc = sqlite3_exec(pDb->db, zSql, 0, 0, &pErr->zErr);
    sqlite3_free(zSql);
  }
  va_end(ap);
}

static Statement *getSqlStatement(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  const char *zSql                /* SQL statement */
){
  Statement *pRet;
  int rc;

  for(pRet=pDb->pCache; pRet; pRet=pRet->pNext){
    if( 0==strcmp(sqlite3_sql(pRet->pStmt), zSql) ){
      return pRet;
    }
  }

  pRet = sqlite3_malloc(sizeof(Statement));
  rc = sqlite3_prepare_v2(pDb->db, zSql, -1, &pRet->pStmt, 0);
  if( rc!=SQLITE_OK ){
    sqlite_error(pErr, pDb, "prepare_v2");
    return 0;
  }
  assert( 0==strcmp(sqlite3_sql(pRet->pStmt), zSql) );

  pRet->pNext = pDb->pCache;
  pDb->pCache = pRet;
  return pRet;
}

static sqlite3_stmt *getAndBindSqlStatement(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  va_list ap                      /* SQL followed by parameters */
){
  Statement *pStatement;          /* The SQLite statement wrapper */
  sqlite3_stmt *pStmt;            /* The SQLite statement to return */
  int i;                          /* Used to iterate through parameters */

  pStatement = getSqlStatement(pErr, pDb, va_arg(ap, const char *));
  if( !pStatement ) return 0;
  pStmt = pStatement->pStmt;
  for(i=1; i<=sqlite3_bind_parameter_count(pStmt); i++){
    const char *zName = sqlite3_bind_parameter_name(pStmt, i);
    void * pArg = va_arg(ap, void*);

    switch( zName[1] ){
      case 'i':
        sqlite3_bind_int64(pStmt, i, *(i64 *)pArg);
        break;

      case 'z':
        sqlite3_bind_text(pStmt, i, (char *)pArg, -1, SQLITE_TRANSIENT);
        break;

      default:
        pErr->rc = 1;
        pErr->zErr = sqlite3_mprintf("Cannot discern type: \"%s\"", zName);
        pStmt = 0;
        break;
    }
  }

  return pStmt;
}

static i64 execsql_i64_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  ...                             /* SQL and pointers to parameter values */
){
  i64 iRet = 0;
  if( pErr->rc==SQLITE_OK ){
    sqlite3_stmt *pStmt;          /* SQL statement to execute */
    va_list ap;                   /* ... arguments */
    va_start(ap, pDb);
    pStmt = getAndBindSqlStatement(pErr, pDb, ap);
    if( pStmt ){
      int first = 1;
      while( SQLITE_ROW==sqlite3_step(pStmt) ){
        if( first && sqlite3_column_count(pStmt)>0 ){
          iRet = sqlite3_column_int64(pStmt, 0);
        }
        first = 0;
      }
      if( SQLITE_OK!=sqlite3_reset(pStmt) ){
        sqlite_error(pErr, pDb, "reset");
      }
    }
    va_end(ap);
  }
  return iRet;
}

static char * execsql_text_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  int iSlot,                      /* Db handle slot to store text in */
  ...                             /* SQL and pointers to parameter values */
){
  char *zRet = 0;

  if( iSlot>=pDb->nText ){
    int nByte = sizeof(char *)*(iSlot+1);
    pDb->aText = (char **)sqlite3_realloc(pDb->aText, nByte);
    memset(&pDb->aText[pDb->nText], 0, sizeof(char*)*(iSlot+1-pDb->nText));
    pDb->nText = iSlot+1;
  }

  if( pErr->rc==SQLITE_OK ){
    sqlite3_stmt *pStmt;          /* SQL statement to execute */
    va_list ap;                   /* ... arguments */
    va_start(ap, iSlot);
    pStmt = getAndBindSqlStatement(pErr, pDb, ap);
    if( pStmt ){
      int first = 1;
      while( SQLITE_ROW==sqlite3_step(pStmt) ){
        if( first && sqlite3_column_count(pStmt)>0 ){
          zRet = sqlite3_mprintf("%s", sqlite3_column_text(pStmt, 0));
          sqlite3_free(pDb->aText[iSlot]);
          pDb->aText[iSlot] = zRet;
        }
        first = 0;
      }
      if( SQLITE_OK!=sqlite3_reset(pStmt) ){
        sqlite_error(pErr, pDb, "reset");
      }
    }
    va_end(ap);
  }

  return zRet;
}

static void integrity_check_worker(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  const char *zSql
){
  if( pErr->rc==SQLITE_OK ){
    Statement *pStatement;        /* Statement to execute */
    char *zErr = 0;               /* Integrity check error */

    pStatement = getSqlStatement(pErr, pDb, zSql);
    if( pStatement ){
      sqlite3_stmt *pStmt = pStatement->pStmt;
      while( SQLITE_ROW==sqlite3_step(pStmt) ){
        const char *z = (const char*)sqlite3_column_text(pStmt, 0);
        if( strcmp(z, "ok") ){
          if( zErr==0 ){
            zErr = sqlite3_mprintf("%s", z);
          }else{
            zErr = sqlite3_mprintf("%z\n%s", zErr, z);
          }
        }
      }
      sqlite3_reset(pStmt);

      if( zErr ){
        pErr->zErr = zErr;
        pErr->rc = 1;
      }
    }
  }
}

static void integrity_check_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb                     /* Database handle */
){
  integrity_check_worker(pErr, pDb, "PRAGMA integrity_check");
}
static void quick_check_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb                     /* Database handle */
){
  integrity_check_worker(pErr, pDb, "PRAGMA quick_check");
}

#ifdef _WIN32
static unsigned __stdcall launch_thread_main(void *pArg){
  Thread *p = (Thread *)pArg;
  p->zRes = p->xProc(p->iTid, p->pArg);
  _endthreadex(0);
  return 0; /* NOT REACHED */
}
#else
static void *launch_thread_main(void *pArg){
  Thread *p = (Thread *)pArg;
  p->zRes = p->xProc(p->iTid, p->pArg);
  return 0;
}
#endif

static void launch_thread_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Threadset *pThreads,            /* Thread set */
  char *(*xProc)(int, void*),     /* Proc to run */
  void *pArg                      /* Argument passed to thread proc */
){
  if( pErr->rc==SQLITE_OK ){
    int iTid = ++pThreads->iMaxTid;
    Thread *p;
    int rc;

    p = (Thread *)sqlite3_malloc(sizeof(Thread));
    memset(p, 0, sizeof(Thread));
    p->iTid = iTid;
    p->pArg = pArg;
    p->xProc = xProc;

#ifdef _WIN32
    rc = SQLITE_OK;
    p->winTid = _beginthreadex(0, 0, launch_thread_main, (void*)p, 0, 0);
    if( p->winTid==0 ) rc = errno ? errno : rc;
#else
    rc = pthread_create(&p->tid, NULL, launch_thread_main, (void *)p);
#endif
    if( rc!=0 ){
      system_error(pErr, rc);
      sqlite3_free(p);
    }else{
      p->pNext = pThreads->pThread;
      pThreads->pThread = p;
    }
  }
}

static void join_all_threads_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Threadset *pThreads             /* Thread set */
){
  Thread *p;
  Thread *pNext;
  for(p=pThreads->pThread; p; p=pNext){
#ifndef _WIN32
    void *ret;
#endif
    int rc;
    pNext = p->pNext;

#ifdef _WIN32
    do {
      rc = WaitForSingleObjectEx((HANDLE)p->winTid, INFINITE, TRUE);
    }while( rc==WAIT_IO_COMPLETION );
    CloseHandle((HANDLE)p->winTid);
#else
    rc = pthread_join(p->tid, &ret);
#endif

    if( rc!=0 ){
      if( pErr->rc==SQLITE_OK ) system_error(pErr, rc);
    }else{
      printf("Thread %d says: %s\n", p->iTid, (p->zRes==0 ? "..." : p->zRes));
      fflush(stdout);
    }
    sqlite3_free(p->zRes);
    sqlite3_free(p);
  }
  pThreads->pThread = 0;
}

#ifdef _WIN32
# define THREADTEST3_STAT _stat
#else
# define THREADTEST3_STAT stat
#endif

static i64 filesize_x(
  Error *pErr,
  const char *zFile
){
  i64 iRet = 0;
  if( pErr->rc==SQLITE_OK ){
    struct THREADTEST3_STAT sStat;
    if( THREADTEST3_STAT(zFile, &sStat) ){
      iRet = -1;
    }else{
      iRet = sStat.st_size;
    }
  }
  return iRet;
}

static void filecopy_x(
  Error *pErr,
  const char *zFrom,
  const char *zTo
){
  if( pErr->rc==SQLITE_OK ){
    i64 nByte = filesize_x(pErr, zFrom);
    if( nByte<0 ){
      test_error_x(pErr, sqlite3_mprintf("no such file: %s", zFrom));
    }else{
      i64 iOff;
      char aBuf[1024];
      int fd1;
      int fd2;
      unlink(zTo);

      fd1 = open(zFrom, O_RDONLY|O_BINARY);
      if( fd1<0 ){
        system_error(pErr, errno);
        return;
      }
      fd2 = open(zTo, O_RDWR|O_CREAT|O_EXCL|O_BINARY, 0644);
      if( fd2<0 ){
        system_error(pErr, errno);
        close(fd1);
        return;
      }

      iOff = 0;
      while( iOff<nByte ){
        int nCopy = sizeof(aBuf);
        if( nCopy+iOff>nByte ){
          nCopy = nByte - iOff;
        }
        if( nCopy!=read(fd1, aBuf, nCopy) ){
          system_error(pErr, errno);
          break;
        }
        if( nCopy!=write(fd2, aBuf, nCopy) ){
          system_error(pErr, errno);
          break;
        }
        iOff += nCopy;
      }

      close(fd1);
      close(fd2);
    }
  }
}

static void bcvfs_prefetch_x(
  Error *pErr,
  sqlite3_bcvfs *pFs,
  const char *zCont,
  const char *zDb
){
  int rc = SQLITE_OK;
  sqlite3_prefetch *pPrefetch = 0;

  rc = sqlite3_bcvfs_prefetch_new(pFs, zCont, zDb, &pPrefetch);
  while( rc==SQLITE_OK ){
    rc = sqlite3_bcvfs_prefetch_run(pPrefetch, 4, 1000);
  }
  if( rc!=SQLITE_DONE ){
    test_error(pErr, "error in prefetch: (rc=%d) %s", rc, 
        sqlite3_bcvfs_prefetch_errmsg(pPrefetch)
    );
  }
  sqlite3_bcvfs_prefetch_destroy(pPrefetch);
}

/* 
** Used by setstoptime() and timetostop().
*/
static double timelimit = 0.0;

static double currentTime(void){
  double t;
  static sqlite3_vfs *pTimelimitVfs = 0;
  if( pTimelimitVfs==0 ) pTimelimitVfs = sqlite3_vfs_find(0);
  if( pTimelimitVfs->iVersion>=2 && pTimelimitVfs->xCurrentTimeInt64!=0 ){
    sqlite3_int64 tm;
    pTimelimitVfs->xCurrentTimeInt64(pTimelimitVfs, &tm);
    t = tm/86400000.0;
  }else{
    pTimelimitVfs->xCurrentTime(pTimelimitVfs, &t);
  }
  return t;
}

static void setstoptime_x(
  Error *pErr,                    /* IN/OUT: Error code */
  int nMs                         /* Milliseconds until "stop time" */
){
  if( pErr->rc==SQLITE_OK ){
    double t = currentTime();
    timelimit = t + ((double)nMs)/(1000.0*60.0*60.0*24.0);
  }
}

static int timetostop_x(
  Error *pErr                     /* IN/OUT: Error code */
){
  int ret = 1;
  if( pErr->rc==SQLITE_OK ){
    double t = currentTime();
    ret = (t >= timelimit);
  }
  return ret;
}


/*************************************************************************
**************************************************************************
**************************************************************************
** End infrastructure. Begin tests.
*/

#define TT3_TEST_MODULE "azure?emulator=127.0.0.1:10000&sas=1"
#define TT3_TEST_USER   "devstoreaccount1"
#define TT3_TEST_KEY    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6" \
                        "IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

#define TT3_TEST_BLKSZ  (128*1024)
#define TT3_TEST_NAMESZ (24)

static i64 test_time_zero = 0;

static char *getAzureSas(
  Error *pErr, 
  int bCreate,                    /* For an SAS that can create containers */
  const char *zCont, 
  i64 nSecond
){
  Sqlite db = {0};
  char *zCmd = 0;
  char *zRet = 0;
  FILE *fp = 0;

  opendb(pErr, &db, "", 0);
  if( bCreate ){
    zCmd = execsql_text(pErr, &db, 0, 
        "SELECT "
        "'az storage account generate-sas --output tsv'"
        "||' --account-key '||:z1"
        "||' --account-name '||:z2"
        "||' --permissions acdlpruw'"
        "||' --resource-types sco '"
        "||' --services b '"
        "||' --expiry '"
        "|| strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '+'||:i3||' seconds')",
        TT3_TEST_KEY, TT3_TEST_USER, &nSecond
    );
  }else{
    zCmd = execsql_text(pErr, &db, 0, 
        "SELECT "
        "'az storage container generate-sas --output tsv'"
        "||' --account-key '||:z1"
        "||' --account-name '||:z2"
        "||' --name '||:z3"
        "||' --permissions dlrwac'"
        "||' --expiry '"
        "|| strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '+'||:i3||' seconds')",
        TT3_TEST_KEY, TT3_TEST_USER, zCont, &nSecond
    );
  }

  fp = popen(zCmd, "r");
  if( !fp ){
    test_error(pErr, "failed to run command: %s", zCmd);
  }else{
    int nRet;
    zRet = sqlite3_malloc(2000);
    fgets(zRet, 2000, fp);
    pclose(fp);
    nRet = strlen(zRet);
    while( bcv_isspace(zRet[nRet-1]) ){
      nRet--;
      zRet[nRet] = '\0';
    }
  }
  closedb(pErr, &db);

  return zRet;
}

static int bcvfs_auth_callback(
  void *pCtx, 
  const char *zModule, 
  const char *zUser, 
  const char *zCont, 
  char **pzOut
){
  Error err = {0};
  char *zRet = getAzureSas(&err, 0, zCont, 500);

  if( err.rc ){
    assert( zRet==0 );
    *pzOut = sqlite3_mprintf("error in getAzureSas(): %s", err.zErr);
  }else{
    *pzOut = zRet;
  }
  return err.rc;
}
static void bcvfs_log_callback(void *pCtx, int mLog, const char *zMsg){
  struct LogMaskName {
    u32 mLog;
    const char *zName;
  } aName[] = {
    { SQLITE_BCV_LOG_HTTP, "http" },
    { SQLITE_BCV_LOG_UPLOAD, "upload" },
    { SQLITE_BCV_LOG_CLEANUP, "cleanup" },
    { SQLITE_BCV_LOG_EVENT, "event" },
  };
  int i;
  const char *zName = "unknown";
  const char *zz = (const char*)pCtx;
  i64 ts = sqlite_timestamp() - test_time_zero;
  for(i=0; i<sizeof(aName)/sizeof(aName[0]); i++){
    if( mLog==aName[i].mLog ){
      zName = aName[i].zName;
    }
  }

  printf("%s: % 3lld.%.3lld: %s: %s\n", zz, ts/1000, ts%1000, zName, zMsg);
  fflush(stdout);
}

static void make_dir(Error *pErr, const char *zDir){
  char *zScript = sqlite3_mprintf("rm -rf %s ; mkdir %s ", zDir, zDir);
  int rc = system(zScript);
  if( rc!=0 ){
    test_error(pErr, "error running script: %s", zScript);
  }
  sqlite3_free(zScript);
}

static void setup_container(
  Error *pErr, 
  const char *zCont, 
  const char *zDb1,
  const char *zDb2,
  const char *zDb3
){
  char *zAuth = 0;
  sqlite3_bcv *pBcv = 0;
  int rc = SQLITE_OK;

  zAuth = getAzureSas(pErr, 1, zCont, 10);

  /* Create a remote container named zCont */
  rc = sqlite3_bcv_open(TT3_TEST_MODULE, TT3_TEST_USER, zAuth, zCont, &pBcv);
  if( rc!=SQLITE_OK ){
    test_error(pErr, "failed to open bcv handle");
  }else{
    rc = sqlite3_bcv_create(pBcv, TT3_TEST_NAMESZ, TT3_TEST_BLKSZ);
    if( rc!=SQLITE_OK ){
      test_error(pErr, "failed to create container: %s", zCont);
    }
  }

  /* Upload an empty database named zDb to the container */
  if( pErr->rc==SQLITE_OK ){
    Sqlite db = {0};
    opendb(pErr, &db, "empty.db", 1);
    sql_script(pErr, &db, "PRAGMA user_version = 43");
    closedb(pErr, &db);
  }
  if( pErr->rc==SQLITE_OK ){
    if( zDb1 ) sqlite3_bcv_upload(pBcv, "empty.db", zDb1);
    if( zDb2 ) sqlite3_bcv_upload(pBcv, "empty.db", zDb2);
    if( zDb3 ) sqlite3_bcv_upload(pBcv, "empty.db", zDb3);
  }

  sqlite3_bcv_close(pBcv);
  sqlite3_free(zAuth);
}

#define DAEMONBUFSIZE 1024

typedef struct Daemon Daemon;
struct Daemon {
  FILE *pFd;
  char aBuf[DAEMONBUFSIZE];
};

static Daemon *bcvfs_launch_daemon(
  Error *pErr,
  const char *zDir,
  const char *zConfig
){
  char *zCmd = 0;
  Daemon *pRet = 0;
  
  pRet = sqlite3_malloc(sizeof(Daemon));
  memset(pRet, 0, sizeof(Daemon));
  zCmd = sqlite3_mprintf(
      "./blockcachevfsd daemon -readymessage -autoexit %s %s", zConfig, zDir
  );
  pRet->pFd = popen(zCmd, "r");
  
  fgets(pRet->aBuf, DAEMONBUFSIZE-1, pRet->pFd);
  printf("daemon: %s", pRet->aBuf);

  sqlite3_free(zCmd);
  return pRet;
}

static void bcvfs_wait_daemon(Daemon *pDaemon){
  while( fgets(pDaemon->aBuf, DAEMONBUFSIZE-1, pDaemon->pFd) ){
    printf("daemon: %s", pDaemon->aBuf);
  }
  pclose(pDaemon->pFd);
  sqlite3_free(pDaemon);
}

static sqlite3_bcvfs *bcvfs_create(
  Error *pErr, 
  const char *zDir, 
  const char *zName
){
  sqlite3_bcvfs *pFs = 0;
  char *zErr = 0;
  if( pErr->rc==SQLITE_OK ){
    int rc = sqlite3_bcvfs_create(zDir, zName, &pFs, &zErr);
    if( rc!=SQLITE_OK ){
      test_error(pErr, "sqlite3_bcvfs_create() failed: %s", zErr);
    }else{
      u32 mLog = SQLITE_BCV_LOG_HTTP | SQLITE_BCV_LOG_EVENT;
      mLog |= SQLITE_BCV_LOG_UPLOAD;
      mLog |= SQLITE_BCV_LOG_CLEANUP;
      sqlite3_bcvfs_auth_callback(pFs, 0, bcvfs_auth_callback);
      // sqlite3_bcvfs_log_callback(pFs, (void*)zName, mLog, bcvfs_log_callback);
    }

  }
  return pFs;
}

static void bcvfs_attach(
  Error *pErr,
  sqlite3_bcvfs *pFs,
  const char *zCont,
  const char *zAlias
){
  if( pErr->rc==SQLITE_OK ){
    char *zErr = 0;
    int rc = sqlite3_bcvfs_attach(
        pFs, TT3_TEST_MODULE, TT3_TEST_USER, zCont, zAlias, 0, &zErr
    );
    if( rc!=SQLITE_OK ){
      test_error(pErr, "sqlite3_bcvfs_attach() failed: %s", zErr);
      sqlite3_free(zErr);
    }
  }
}

static char *bcvfs1_writer(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int iIter = 0;

  opendb(&err, &db, "file:/c1/test.db?vfs=bcvfs", 0);
  while( !timetostop(&err) ){
    iIter++;
    if( iIter%10 ){
      sql_script(&err, &db, 
          "WITH s(i) AS ("
          "  SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<10"
          ")"
          "INSERT INTO t1 SELECT NULL, randomblob(700) FROM s;"
      );
    }else{
      sql_script(&err, &db, "DELETE FROM t1");
    }
    sql_script(&err, &db, "PRAGMA bcv_upload");
    sqlite3_sleep(10);
  }
  closedb(&err, &db);
  print_and_free_err(&err);

  return sqlite3_mprintf("bcvfs1_writer: %d iterations of loop", iIter);
}

static char *bcvfs1_reader(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */

  int nTotal = 0;
  int nDist = 0;
  i64 nPrev = -1;

  opendb(&err, &db, "file:/c1/test.db?vfs=bcvfs", 0);
  while( !timetostop(&err) ){
    i64 iVal;
    integrity_check(&err, &db);
    iVal = execsql_i64(&err, &db, "SELECT count(*) FROM t1");
    if( iVal!=nPrev ){
      nDist++;
      nPrev = iVal;
    }
    nTotal++;
    sqlite3_sleep(10);
  }
  closedb(&err, &db);
  print_and_free_err(&err);

  return sqlite3_mprintf("bcvfs1_reader: %d/%d changes", nDist, nTotal);
}

static void bcvfs1(int nMs){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  sqlite3_bcvfs *pFs = 0;

  setup_container(&err, "cont1", "test.db", 0, 0);
  make_dir(&err, "testdir");
  pFs = bcvfs_create(&err, "testdir", "bcvfs");
  bcvfs_attach(&err, pFs, "cont1", "c1");

  opendb(&err, &db, "file:/c1/test.db?vfs=bcvfs", 0);
  sql_script(&err, &db,
      "CREATE TABLE t1(x INTEGER PRIMARY KEY, y);"
      "CREATE INDEX i1 ON t1(y);"
  );
  closedb(&err, &db);

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, bcvfs1_writer, 0);
  launch_thread(&err, &threads, bcvfs1_reader, 0);
  launch_thread(&err, &threads, bcvfs1_reader, 0);
  join_all_threads(&err, &threads);

  sqlite3_bcvfs_destroy(pFs);
  print_and_free_err(&err);
}

typedef struct PingTest PingTest;
struct PingTest {
  int bOdd;
  const char *zPath;
};

static char *bcvfs_ping_1(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  PingTest *p = (PingTest*)pArg;
  int nLoop = 0;

  opendb(&err, &db, p->zPath, 0);
  while( !timetostop(&err) ){
    i64 iVal;
    char *zSql = 0;
    do {
      sql_script(&err, &db, "PRAGMA bcv_poll");
      iVal = execsql_i64(&err, &db, "PRAGMA user_version");
      if( timetostop(&err) ) goto ping_1_out;
    }while( (iVal%2)!=p->bOdd );

    integrity_check(&err, &db);
    zSql = sqlite3_mprintf(
        "PRAGMA user_version = %lld;"
        "INSERT INTO t1 VALUES(NULL, randomblob(700));"
        , iVal+1
    );
    sql_script(&err, &db, zSql);
    sqlite3_free(zSql);
    sql_script(&err, &db, "PRAGMA bcv_upload;");
    if( err.rc && err.zErr && 0==memcmp("HTTP/1.1 412", err.zErr, 12) ){
      clear_error(&err, SQLITE_ERROR);
    }
    nLoop++;
  }

 ping_1_out:
  closedb(&err, &db);
  print_and_free_err(&err);
  return sqlite3_mprintf("bcvfs1_ping_1: %d loops", nLoop);
}

static void ping1(int nMs){
  PingTest aPing[2] = {
    { 0, "file:/c1/test.db?vfs=bcvfs" },
    { 1, "file:/c2/test.db?vfs=bcvfs" },
  };
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  sqlite3_bcvfs *pFs = 0;

  setup_container(&err, "cont1", "test.db", 0, 0);
  make_dir(&err, "testdir");
  pFs = bcvfs_create(&err, "testdir", "bcvfs");
  bcvfs_attach(&err, pFs, "cont1", "c1");
  opendb(&err, &db, "file:/c1/test.db?vfs=bcvfs", 0);
  sql_script(&err, &db,
      "CREATE TABLE t1(x INTEGER PRIMARY KEY, y);"
      "CREATE INDEX i1 ON t1(y);"
      "PRAGMA user_version=44;"
      "PRAGMA bcv_upload;"
  );
  closedb(&err, &db);
  bcvfs_attach(&err, pFs, "cont1", "c2");

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, bcvfs_ping_1, (void*)&aPing[0]);
  launch_thread(&err, &threads, bcvfs_ping_1, (void*)&aPing[1]);
  join_all_threads(&err, &threads);

  sqlite3_bcvfs_destroy(pFs);
  print_and_free_err(&err);
}

static void ping2(int nMs){
  PingTest aPing[2] = {
    { 0, "file:/c1/test.db?vfs=bcvfs1" },
    { 1, "file:/c2/test.db?vfs=bcvfs2" },
  };
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  sqlite3_bcvfs *pFs1 = 0;
  sqlite3_bcvfs *pFs2 = 0;

  setup_container(&err, "cont1", "test.db", 0, 0);
  make_dir(&err, "testdir1");
  make_dir(&err, "testdir2");
  pFs1 = bcvfs_create(&err, "testdir1", "bcvfs1");
  pFs2 = bcvfs_create(&err, "testdir2", "bcvfs2");

  bcvfs_attach(&err, pFs1, "cont1", "c1");
  opendb(&err, &db, "file:/c1/test.db?vfs=bcvfs1", 0);
  sql_script(&err, &db,
      "CREATE TABLE t1(x INTEGER PRIMARY KEY, y);"
      "CREATE INDEX i1 ON t1(y);"
      "PRAGMA user_version=44;"
      "PRAGMA bcv_upload;"
  );
  closedb(&err, &db);
  bcvfs_attach(&err, pFs2, "cont1", "c2");

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, bcvfs_ping_1, (void*)&aPing[0]);
  launch_thread(&err, &threads, bcvfs_ping_1, (void*)&aPing[1]);
  join_all_threads(&err, &threads);

  sqlite3_bcvfs_destroy(pFs1);
  sqlite3_bcvfs_destroy(pFs2);
  print_and_free_err(&err);
}

static char *bcvfs_cache_1(int iTid, void *pArg){
  int iIter = 0;
  Error err = {0};
  Sqlite db = {0};                /* SQLite database connection */

  opendb(&err, &db, "file:/c1/test.db?vfs=bcvfs", 0);
  assert( err.rc==SQLITE_OK );
  sql_script(&err, &db, "PRAGMA cache_size = 10");

  while( !timetostop(&err) ){
    quick_check(&err, &db);
    iIter++;
  }
  print_and_free_err(&err);
  closedb(&err, &db);

  return sqlite3_mprintf("bcvfs1_cache_1: %d iterations of loop", iIter);
}

static void cache1(int nMs){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  sqlite3_bcvfs *pFs = 0;

  setup_container(&err, "cont1", "test.db", 0, 0);
  make_dir(&err, "testdir");
  pFs = bcvfs_create(&err, "testdir", "bcvfs");
  bcvfs_attach(&err, pFs, "cont1", "c1");

  opendb(&err, &db, "file:/c1/test.db?vfs=bcvfs", 0);
  sql_script(&err, &db,
      "CREATE TABLE t1(x INTEGER PRIMARY KEY, y);"
      "CREATE INDEX i1 ON t1(y);"
      "WITH s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<800) "
      "INSERT INTO t1 SELECT NULL, randomblob(700) FROM s;"
      "PRAGMA bcv_upload;"
  );
  closedb(&err, &db);
  sqlite3_bcvfs_destroy(pFs);

  make_dir(&err, "testdir");
  pFs = bcvfs_create(&err, "testdir", "bcvfs");
  bcvfs_attach(&err, pFs, "cont1", "c1");
  sqlite3_bcvfs_config(pFs, SQLITE_BCV_CACHESIZE, TT3_TEST_BLKSZ*10);

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, bcvfs_cache_1, 0);
  launch_thread(&err, &threads, bcvfs_cache_1, 0);
  join_all_threads(&err, &threads);

  sqlite3_bcvfs_destroy(pFs);
  print_and_free_err(&err);
}

static char *bcvfs_prefetch_writer(int iTid, void *pArg){
  int iIter = 0;
  Error err = {0};
  Sqlite db1 = {0};                /* SQLite database connection */
  Sqlite db2 = {0};                /* SQLite database connection */
  sqlite3_bcvfs *pFs = (sqlite3_bcvfs*)pArg;

  opendb(&err, &db1, "file:/c1/test.db1?vfs=bcvfs", 0);
  opendb(&err, &db2, "file:/c1/test.db2?vfs=bcvfs", 0);
  sql_script(&err, &db1, "PRAGMA cache_size = 10");
  sql_script(&err, &db2, "PRAGMA cache_size = 10");

  sqlite3_bcvfs_register_vtab(db1.db);

  while( !timetostop(&err) ){
    sql_script(&err, &db1, 
        "INSERT INTO t1 VALUES(NULL, randomblob(650));"
        "PRAGMA bcv_upload;"
    );
    sql_script(&err, &db2, 
        "INSERT INTO t1 VALUES(NULL, randomblob(650));"
        "PRAGMA bcv_upload;"
    );
    iIter++;
  }
  closedb(&err, &db1);
  closedb(&err, &db2);

  print_and_free_err(&err);
  return sqlite3_mprintf(
      "bcvfs_prefetch_writer (%d): %d iterations of loop",
      pFs==0 ? 2 : 1, iIter
  );
}

/*
** If (iTid%2)==0:
**
**   1) prefetch test.db1.
**   2) quick-check test.db2.
**
** If (iTid%2)==1:
**
**   1) prefetch test.db2.
**   2) quick-check test.db1.
*/
static char *bcvfs_prefetch_1(int iTid, void *pArg){

  int iIter = 0;
  Error err = {0};
  Sqlite db = {0};                /* SQLite database connection */
  sqlite3_bcvfs *pFs = (sqlite3_bcvfs*)pArg;

  struct Filenames {
    const char *zPrefetch;
    const char *zQuickcheck;
  } aF[] = {
    {"test.db1", "file:/c1/test.db2?vfs=bcvfs"},
    {"test.db2", "file:/c1/test.db1?vfs=bcvfs"},
  };
  struct Filenames *p = &aF[iTid % 2];

  opendb(&err, &db, p->zQuickcheck, 0);
  sql_script(&err, &db, "PRAGMA cache_size = 10");

  for(iIter=0; !timetostop(&err); iIter++){
    int rc = 0;
    sqlite3_prefetch *pPre = 0;
    sqlite3_bcvfs_prefetch_new(pFs, "c1", p->zPrefetch, &pPre);

    do {
      rc = sqlite3_bcvfs_prefetch_run(pPre, 8, 1000);
    }while( rc==SQLITE_OK );
    if( rc!=SQLITE_DONE ){
      test_error(&err, "prefetch failed (%d) - %s", 
          rc, sqlite3_bcvfs_prefetch_errmsg(pPre)
      );
    }
    sqlite3_bcvfs_prefetch_destroy(pPre);

    quick_check(&err, &db);
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf("bcvfs1_prefetch_1(): %d iterations of loop", iIter);
}

/*
** Test case for sqlite3_bcvfs_prefetch() interface.
**
** There are two databases in the container - test.db1 and test.db2. Both
** are 7 blocks in size. Three threads:
**
**  Thread 1 runs this loop:
**       1) prefetch test.db1.
**       2) quick-check test.db2.
**
**  Thread 2 runs this loop:
**       1) prefetch test.db2.
**       2) quick-check test.db1.
**
** Thread 3 runs:
**       1) Add a row to test.db1,
**       2) Upload test.db1,
**       3) Add a row to test.db2,
**       4) Upload test.db2.
*/
static void prefetch1(int nMs){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  sqlite3_bcvfs *pFs = 0;

  setup_container(&err, "cont1", "test.db1", "test.db2", 0);
  make_dir(&err, "testdir");
  pFs = bcvfs_create(&err, "testdir", "bcvfs");
  sqlite3_bcvfs_config(pFs, SQLITE_BCV_CACHESIZE, TT3_TEST_BLKSZ*10);
  bcvfs_attach(&err, pFs, "cont1", "c1");

  opendb(&err, &db, "file:/c1/test.db1?vfs=bcvfs", 0);
  sql_script(&err, &db,
      "CREATE TABLE t1(x INTEGER PRIMARY KEY, y);"
      "WITH s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<1000) "
      "INSERT INTO t1 SELECT NULL, randomblob(700) FROM s;"
      "PRAGMA bcv_upload;"
  );
  closedb(&err, &db);

  opendb(&err, &db, "file:/c1/test.db2?vfs=bcvfs", 0);
  sql_script(&err, &db,
      "CREATE TABLE t1(x INTEGER PRIMARY KEY, y);"
      "WITH s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<1000) "
      "INSERT INTO t1 SELECT NULL, randomblob(700) FROM s;"
      "PRAGMA bcv_upload;"
  );
  closedb(&err, &db);

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, bcvfs_prefetch_1, (void*)pFs);
  launch_thread(&err, &threads, bcvfs_prefetch_1, (void*)pFs);
  launch_thread(&err, &threads, bcvfs_prefetch_writer, 0);
  join_all_threads(&err, &threads);

  sqlite3_bcvfs_destroy(pFs);
  print_and_free_err(&err);
}

static char *bcvfs_dmn_prefetch_1(int iTid, void *pArg){
  int iIter = 0;
  Error err = {0};
  Sqlite db1 = {0};                /* SQLite database connection */
  Sqlite db2 = {0};                /* SQLite database connection */
  sqlite3_bcvfs *pFs = (sqlite3_bcvfs*)pArg;

  opendb(&err, &db1, "file:/c1/test.db1?vfs=bcvfsd", 0);
  opendb(&err, &db2, "file:/c1/test.db2?vfs=bcvfsd", 0);
  sql_script(&err, &db1, "PRAGMA cache_size = 10");
  sql_script(&err, &db2, "PRAGMA cache_size = 10");

  sqlite3_bcvfs_register_vtab(db1.db);

  while( !timetostop(&err) ){
    bcvfs_prefetch(&err, pFs, "c1", "test.db1");
    bcvfs_prefetch(&err, pFs, "c1", "test.db2");
    iIter++;
  }
  closedb(&err, &db1);
  closedb(&err, &db2);

  print_and_free_err(&err);
  return sqlite3_mprintf(
      "bcvfs_dmn_prefetch_1 (%d): %d iterations of loop",
      pFs==0 ? 2 : 1, iIter
  );
}

static void dmn_prefetch1(int nMs){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  sqlite3_bcvfs *pFs = 0;
  sqlite3_bcvfs *pFsD = 0;
  Daemon *pDaemon = 0;

  setup_container(&err, "cont1", "test.db1", "test.db2", 0);
  make_dir(&err, "testdir");
  pFs = bcvfs_create(&err, "testdir", "bcvfs");
  sqlite3_bcvfs_config(pFs, SQLITE_BCV_CACHESIZE, TT3_TEST_BLKSZ*10);
  bcvfs_attach(&err, pFs, "cont1", "c1");

  opendb(&err, &db, "file:/c1/test.db1?vfs=bcvfs", 0);
  sql_script(&err, &db,
      "CREATE TABLE t1(x INTEGER PRIMARY KEY, y);"
      "WITH s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<1000) "
      "INSERT INTO t1 SELECT NULL, randomblob(700) FROM s;"
      "PRAGMA bcv_upload;"
  );
  closedb(&err, &db);

  opendb(&err, &db, "file:/c1/test.db2?vfs=bcvfs", 0);
  sql_script(&err, &db,
      "CREATE TABLE t1(x INTEGER PRIMARY KEY, y);"
      "WITH s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<1000) "
      "INSERT INTO t1 SELECT NULL, randomblob(700) FROM s;"
      "PRAGMA bcv_upload;"
  );
  closedb(&err, &db);

  make_dir(&err, "testdir_d");
  pDaemon = bcvfs_launch_daemon(&err, "testdir_d", "-cachesize 1310720");
  pFsD = bcvfs_create(&err, "testdir_d", "bcvfsd");
  bcvfs_attach(&err, pFsD, "cont1", "c1");

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, bcvfs_dmn_prefetch_1, (void*)pFsD);
  launch_thread(&err, &threads, bcvfs_dmn_prefetch_1, (void*)pFsD);
  join_all_threads(&err, &threads);

  sqlite3_bcvfs_destroy(pFs);
  sqlite3_bcvfs_destroy(pFsD);
  bcvfs_wait_daemon(pDaemon);
  print_and_free_err(&err);
}


static char *dmn1_reader(int iTid, void *pArg){
  int iIter = 0;
  Error err = {0};
  Sqlite db = {0};                /* SQLite database connection */

  opendb(&err, &db, "file:/c1/test.db1?vfs=bcvfsd", 0);
  sql_script(&err, &db, "PRAGMA cache_size = 10");

  while( !timetostop(&err) ){
    sql_script(&err, &db, "SELECT * FROM t1");
    quick_check(&err, &db);
    iIter++;
    sqlite3_sleep(10);
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf(
      "dmn1_reader: %d iterations of loop", iIter
  );
}

static char *dmn1_daemon_logger(int iTid, void *pArg){
  Daemon *pDaemon = (Daemon*)pArg;
  while( fgets(pDaemon->aBuf, DAEMONBUFSIZE-1, pDaemon->pFd) ){
    printf("daemon: %s", pDaemon->aBuf);
  }
  return 0;
}

static void dmn1(int nMs){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  sqlite3_bcvfs *pFs = 0;
  sqlite3_bcvfs *pFsD = 0;
  Daemon *pDaemon = 0;

  /* Create one local VFS to write to the database file. */
  setup_container(&err, "cont1", "test.db1", 0, 0);
  make_dir(&err, "testdir");
  pFs = bcvfs_create(&err, "testdir", "bcvfs");
  bcvfs_attach(&err, pFs, "cont1", "c1");

  /* Populate database file */
  opendb(&err, &db, "file:/c1/test.db1?vfs=bcvfs", 0);
  sql_script(&err, &db,
      "CREATE TABLE t1(x INTEGER PRIMARY KEY, y);"
      "CREATE INDEX i1 ON t1(y);"
      "WITH s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<1000) "
      "INSERT INTO t1 SELECT NULL, randomblob(700) FROM s;"
      "PRAGMA bcv_upload;"
  );
  closedb(&err, &db);

  make_dir(&err, "testdir_d");
  pDaemon = bcvfs_launch_daemon(&err, "testdir_d", "-cachesize 1310720");
  pFsD = bcvfs_create(&err, "testdir_d", "bcvfsd");
  bcvfs_attach(&err, pFsD, "cont1", "c1");

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, dmn1_reader, 0);
  launch_thread(&err, &threads, dmn1_reader, 0);
  launch_thread(&err, &threads, dmn1_daemon_logger, (void*)pDaemon);
  join_all_threads(&err, &threads);

  sqlite3_bcvfs_destroy(pFs);
  sqlite3_bcvfs_destroy(pFsD);
  bcvfs_wait_daemon(pDaemon);
  print_and_free_err(&err);
}

static char *dmn2_reader(int iTid, void *pArg){
  int iIter = 0;
  Error err = {0};
  Sqlite db = {0};                /* SQLite database connection */

  opendb(&err, &db, "file:/c1/test.db1?vfs=bcvfsd", 0);
  sql_script(&err, &db, "PRAGMA cache_size = 10");

  while( !timetostop(&err) ){
    i64 i1, i2; 
    sql_script(&err, &db, "BEGIN");
    i1 = execsql_i64(&err, &db, "SELECT y FROM t1 WHERE x=0");
    i2 = execsql_i64(&err, &db, "SELECT sum(y) FROM t1 WHERE x>0");
    sql_script(&err, &db, "COMMIT");
    if( i1!=i2 ){
      test_error(&err, "sum mismatch: i1=%lld i2=%lld", i1, i2);
      break;
    }
    iIter++;
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf(
      "dmn2_reader: %d iterations of loop", iIter
  );
}

static char *dmn2_writer(int iTid, void *pArg){
  int iIter = 0;
  Error err = {0};
  Sqlite db = {0};                /* SQLite database connection */

  opendb(&err, &db, "file:/c1/test.db1?vfs=bcvfs", 0);
  sql_script(&err, &db, "PRAGMA cache_size = 10");

  while( !timetostop(&err) ){
    sql_script(&err, &db,
        "INSERT INTO t1 VALUES(NULL, random() & 0xFFFFFF, randomblob(5000));"
        "UPDATE t1 SET y = (SELECT sum(y) FROM t1 WHERE x>0) WHERE x=0;"
        "PRAGMA bcv_upload;"
    );
    iIter++;
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf(
      "dmn2_writer: %d iterations of loop", iIter
  );
}

static char *dmn2_poller(int iTid, void *pArg){
  sqlite3_bcvfs *pFs = (sqlite3_bcvfs*)pArg;
  int iIter = 0;
  Error err = {0};

  while( !timetostop(&err) ){
    char *zErr = 0;
    int rc = sqlite3_bcvfs_poll(pFs, "c1", &zErr);
    if( rc!=SQLITE_OK ){
      test_error(&err, "error in sqlite3_bcvfs_poll: %d \"%s\"", rc, zErr);
    }
    iIter++;
  }

  print_and_free_err(&err);
  return sqlite3_mprintf(
      "dmn2_poller: %d iterations of loop", iIter
  );
}

static void dmn2(int nMs){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  sqlite3_bcvfs *pFs = 0;
  sqlite3_bcvfs *pFsD = 0;
  Daemon *pDaemon = 0;

  /* Create one local VFS to write to the database file. */
  setup_container(&err, "cont1", "test.db1", 0, 0);
  make_dir(&err, "testdir");
  pFs = bcvfs_create(&err, "testdir", "bcvfs");
  bcvfs_attach(&err, pFs, "cont1", "c1");

  /* Populate database file */
  opendb(&err, &db, "file:/c1/test.db1?vfs=bcvfs", 0);
  sql_script(&err, &db,
      "CREATE TABLE t1(x INTEGER PRIMARY KEY, y INTEGER, z);"
      "INSERT INTO t1(x, y) VALUES(0, 0);"
      "INSERT INTO t1 VALUES(NULL, random() & 0xFFFFFF, randomblob(5000));"
      "UPDATE t1 SET y = (SELECT sum(y) FROM t1 WHERE x>0) WHERE x=0;"
      "PRAGMA bcv_upload;"
  );
  closedb(&err, &db);

  make_dir(&err, "testdir_d");
  pDaemon = bcvfs_launch_daemon(&err, "testdir_d", "-cachesize 1310720");
  pFsD = bcvfs_create(&err, "testdir_d", "bcvfsd");
  bcvfs_attach(&err, pFsD, "cont1", "c1");

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, dmn2_writer, 0);
  launch_thread(&err, &threads, dmn2_reader, 0);
  launch_thread(&err, &threads, dmn2_poller, (void*)pFsD);
  launch_thread(&err, &threads, dmn1_daemon_logger, (void*)pDaemon);
  join_all_threads(&err, &threads);

  sqlite3_bcvfs_destroy(pFs);
  sqlite3_bcvfs_destroy(pFsD);
  bcvfs_wait_daemon(pDaemon);
  print_and_free_err(&err);
}

int main(int argc, char **argv){
  struct ThreadTest {
    void (*xTest)(int);   /* Routine for running this test */
    const char *zTest;    /* Name of this test */
    int nMs;              /* How long to run this test, in milliseconds */
  } aTest[] = {
    { bcvfs1,   "bcvfs1", 20000 },
    { ping1,    "ping1", 20000 },
    { ping2,    "ping2", 20000 },
    { cache1,   "cache1", 20000 },
    { prefetch1,"prefetch1", 20000 },
    { dmn1,     "dmn1", 20000 },
    { dmn2,     "dmn2", 20000 },
    { dmn_prefetch1, "dmn_prefetch1", 20000 },
  };
  static char *substArgv[] = { 0, "*", 0 };
  int i, iArg;
  int nTestfound = 0;

  sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
  if( argc<2 ){
    argc = 2;
    argv = substArgv;
  }

  /* Loop through the command-line arguments to ensure that each argument
  ** selects at least one test. If not, assume there is a typo on the 
  ** command-line and bail out with the usage message.  */
  for(iArg=1; iArg<argc; iArg++){
    const char *zArg = argv[iArg];
#if 0 
    if( zArg[0]=='-' ){
      if( sqlite3_stricmp(zArg, "-multiplexor")==0 ){
        /* Install the multiplexor VFS as the default */
        int rc = sqlite3_multiplex_initialize(0, 1);
        if( rc!=SQLITE_OK ){
          fprintf(stderr, "Failed to install multiplexor VFS (%d)\n", rc);
          return 253;
        }
      }
      else {
        goto usage;
      }
      continue;
    }
#endif

    for(i=0; i<sizeof(aTest)/sizeof(aTest[0]); i++){
      if( sqlite3_strglob(zArg, aTest[i].zTest)==0 ) break;
    }
    if( i>=sizeof(aTest)/sizeof(aTest[0]) ) goto usage;   
  }

  test_time_zero = sqlite_timestamp();

  for(iArg=1; iArg<argc; iArg++){
    if( argv[iArg][0]=='-' ) continue;
    for(i=0; i<sizeof(aTest)/sizeof(aTest[0]); i++){
      char const *z = aTest[i].zTest;
      if( sqlite3_strglob(argv[iArg],z)==0 ){
        printf("Running %s for %d seconds...\n", z, aTest[i].nMs/1000);
        fflush(stdout);
        aTest[i].xTest(aTest[i].nMs);
        nTestfound++;
      }
    }
  }
  if( nTestfound==0 ) goto usage;

  {
    i64 dummy = 0;
    i64 nByte = 0;
    i64 nAlloc = 0;
    sqlite3_status64(SQLITE_STATUS_MEMORY_USED, &nByte, &dummy, 0);
    sqlite3_status64(SQLITE_STATUS_MALLOC_COUNT, &nAlloc, &dummy, 0);
    printf("unfreed: %d bytes in %d allocations\n", (int)nByte, (int)nAlloc);
  }

  printf("%d errors out of %d tests\n", nGlobalErr, nTestfound);
  return (nGlobalErr>0 ? 255 : 0);

 usage:
  printf("Usage: %s [testname|testprefix*]...\n", argv[0]);
  printf("Available tests are:\n");
  for(i=0; i<sizeof(aTest)/sizeof(aTest[0]); i++){
    printf("   %s\n", aTest[i].zTest);
  }

  return 254;
}
