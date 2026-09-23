
#include <stdio.h>

/*
** Callback for executed SQL script. Print to stdout.
*/
static int csExecCb(
  void *pCtx, 
  int nStr,
  char **azStr,
  char **azCol
){
  if( azStr ){
    int ii;
    const char *zSep = "";
    for(ii=0; ii<nStr; ii++){
      fprintf(stdout, "%s%s", zSep, azStr[ii] ? azStr[ii] : "(null)");
      zSep = "|";
    }
    fprintf(stdout, "\n");
  }
  return 0;
}

#include "blockcachevfs.h"
#include "sqlite3.h"

/*
** This program is hardcoded to access an Azure emulator running on port
** 10000 (the default for Azurite) of the localhost. It is also hardcoded
** to the demo account - the account built-in to the emulator with the
** well-known credentials reproduced below. 
*/ 
#define CS_STORAGE "azure?emulator=127.0.0.1:10000"
#define CS_ACCOUNT "devstoreaccount1"
#define CS_KEY "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

/*
** Authentication callback. A real application would return a different 
** authentication token based on the storage system, account name and 
** container name parameters, but since the credentials used by this
** application are hard coded, it just returns a copy of constant string
** CS_KEY.
**
** Because the API is defined such that this function must return a buffer
** allocated using sqlite3_malloc() or compatible, this implementation
** uses sqlite3_mprintf() to make a copy of the authentication token.
*/
static int csAuthCb(
  void *pCtx,
  const char *zStorage,
  const char *zAccount,
  const char *zContainer,
  char **pzAuthToken
){
  *pzAuthToken = sqlite3_mprintf("%s", CS_KEY);
  return (*pzAuthToken) ? SQLITE_OK : SQLITE_NOMEM;
}

/*
** Open a VFS that uses directory zDir as its cache directory. Then attach
** container zCont. Next, open an SQLite handle on path zPath using the new 
** VFS and execute SQL script zSql.
*/
static int cloudsql(
  const char *zDir,               /* Directory to use for blockcachevfs cache */
  const char *zCont,              /* Container to attach */
  const char *zPath,              /* Path to open */
  const char *zSql                /* SQL to execute */
){
  int rc = SQLITE_OK;             /* Error code */
  char *zErr = 0;                 /* Error message */
  sqlite3_bcvfs *pVfs = 0;        /* VFS handle */
  sqlite3 *db = 0;                /* Database handle open on zPath */

  /* Create a VFS object. Directory zDir must already exist. If it exists
  ** and there is a daemon running in that directory, the new VFS connects
  ** to the daemon for read-only access. Or, if there is no such daemon,
  ** the new VFS will provide read-write daemonless access.  */
  rc = sqlite3_bcvfs_create(zDir, "myvfs", &pVfs, &zErr);

  /* Check if this is a daemon VFS or not */
  if( rc==SQLITE_OK ){
    if( sqlite3_bcvfs_isdaemon(pVfs) ){
      printf("VFS is using a daemon\n");
    }else{
      printf("VFS is in daemonless mode\n");
    }
  }

  /* Configure the authorization callback. */
  if( rc==SQLITE_OK ){
    sqlite3_bcvfs_auth_callback(pVfs, 0, csAuthCb);
  }

  /* Attach the container. Specify the SQLITE_BCV_ATTACH_IFNOT flag so that
  ** it is not an error if the container is already attached. 
  **
  ** There are two reasons the container might already be attached, even
  ** though the VFS was only just created. Firstly, if this VFS is connected
  ** to a running daemon process, then some other client may have already
  ** attached the container to the daemon. Secondly, VFS objects store their
  ** state in the cache directory so that if they are restarted, all
  ** containers are automatically reattached. So if this (or some other
  ** blockcachevfs application) has run before specifying the same 
  ** cache directory, the container may already be attached.  */
  if( rc==SQLITE_OK ){
    rc = sqlite3_bcvfs_attach(pVfs, CS_STORAGE, CS_ACCOUNT, zCont, 0,
        SQLITE_BCV_ATTACH_IFNOT, &zErr
    );
  }

  /* Open a database handle on a cloud database. */
  if( rc==SQLITE_OK ){
    rc = sqlite3_open_v2(zPath, &db, SQLITE_OPEN_READWRITE, "myvfs");
    if( rc!=SQLITE_OK ){
      zErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    }
  }

  /* Enable the virtual table interface. */
  if( rc==SQLITE_OK ){
    rc = sqlite3_bcvfs_register_vtab(db);
  }

  /* Execute the provided SQL script. */
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, zSql, csExecCb, 0, &zErr);
  }

  sqlite3_close(db);
  sqlite3_bcvfs_destroy(pVfs);

  /* Output any error, free any error message and return. */
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "Error: (%d) %s\n", rc, zErr);
  }
  sqlite3_free(zErr);
  return rc;
}

/*
** Usage: cloudsql DIRECTORY CONTAINER DBPATH SQL
*/
int main(int argc, char **argv){
  if( argc!=5 ){
    fprintf(stderr, "Usage: %s DIRECTORY CONTAINER DBPATH SQL\n", argv[0]);
    return -1;
  }
  return cloudsql(argv[1], argv[2], argv[3], argv[4]);
}


