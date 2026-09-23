/*
** 2019-11-25
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
*/
#include <tcl.h>
#include "blockcachevfs.h"
#include "bcvutil.h"
#include "bcv_int.h"
#include "sqlite3.h"
#include <assert.h>
#include <string.h>


struct BcvTestGlobal {
  sqlite3_mem_methods mem;
  int iMemFault;
  int nBenign;
  int ePersist;

  int iIoFault;

  int iSocketFault;
  int (*xRecv)(BCV_SOCKET_TYPE, void *, int);
  int (*xSend)(BCV_SOCKET_TYPE, void *, int);

  /* For curl-config-cb */
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
};
static struct BcvTestGlobal g;

static int bcv_oom_breakpoint(){
  static int nBrk = 0;
  nBrk++;
  return 1;
}

static int bcv_ioerr_breakpoint(){
  static int nBrk = 0;
  nBrk++;
  return 1;
}

static int bcvIsOom(void){
  int ret = 0;
  if( g.ePersist==2 ){
    ret = bcv_oom_breakpoint();
  }else
  if( g.iMemFault && g.nBenign==0 ){
    g.iMemFault--;
    if( g.iMemFault==0 ){
      ret = bcv_oom_breakpoint();
      if( g.ePersist ) g.ePersist = 2;
    }
  }
  return ret;
}

static int bcvIsIoerr(void){
  int ret = 0;
  if( g.iIoFault ){
    g.iIoFault--;
    if( g.iIoFault==0 ){
      ret = bcv_ioerr_breakpoint();
    }
  }
  return ret;
}

static int bcvIsSocketError(void){
  int ret = 0;
  if( g.iSocketFault ){
    g.iSocketFault--;
    if( g.iSocketFault==0 ){
      ret = 1;
    }
  }
  return ret;
}

static void *bcvTestMalloc(int nByte){
  if( bcvIsOom() ) return NULL;
  return g.mem.xMalloc(nByte);
}
static void *bcvTestRealloc(void *pOld, int nByte){
  if( bcvIsOom() ) return NULL;
  return g.mem.xRealloc(pOld, nByte);
}

static void bcvTestBenignStart(void){
  g.nBenign++;
}
static void bcvTestBenignEnd(void){
  g.nBenign--;
  assert( g.nBenign>=0 );
}

static int bcvTestRecv(BCV_SOCKET_TYPE fd, void *a, int n){
  if( bcvIsSocketError() ) return -1;
  return g.xRecv(fd, a, n);
}
static int bcvTestSend(BCV_SOCKET_TYPE fd, void *a, int n){
  if( bcvIsSocketError() ) return -1;
  return g.xSend(fd, a, n);
}

static int test_breakpoint(
  ClientData clientData,
  Tcl_Interp *interp,
  int nArg,
  Tcl_Obj *const apArg[]
){
  return TCL_OK;
}

#if 0
struct ScContext {
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
};
static struct ScContext g_bcv_sc;

static int test_sas_callback(
  void *pCtx, 
  const char *zStorage, 
  const char *zAccount, 
  const char *zContainer,
  char **pzSasToken,
  int *pbReadonly
){
  Tcl_Obj *pEval = Tcl_DuplicateObj(g_bcv_sc.pScript);
  Tcl_Interp *interp = g_bcv_sc.interp;
  int rc = SQLITE_OK;

  Tcl_IncrRefCount(pEval);
  Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(zStorage, -1));
  Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(zAccount, -1));
  Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(zContainer, -1));

  if( Tcl_EvalObjEx(interp, pEval, TCL_GLOBAL_ONLY)!=TCL_OK ){
    rc = SQLITE_ERROR;
  }else{
    char *zToken = sqlite3_mprintf("%s", Tcl_GetStringResult(interp));
    if( zToken==0 ){
      rc = SQLITE_NOMEM;
    }
    *pzSasToken = zToken;
    *pbReadonly = 0;
  }

  return rc;
}
#endif

/*
** sqlite3_bcv_sas_callback ?SCRIPT?
*/
static int test_bcv_sc(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
#if 0
  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?SCRIPT?");
    return TCL_ERROR;
  }

  if( g_bcv_sc.pScript ){
    Tcl_DecrRefCount(g_bcv_sc.pScript);
    g_bcv_sc.pScript = 0;
  }
  g_bcv_sc.interp = 0;
  sqlite3_bcv_sas_callback(0, 0);

  if( objc==2 ){
    g_bcv_sc.interp = interp;
    g_bcv_sc.pScript = Tcl_DuplicateObj(objv[1]);
    Tcl_IncrRefCount(g_bcv_sc.pScript);
    sqlite3_bcv_sas_callback((void*)&g_bcv_sc, test_sas_callback);
  }

  Tcl_ResetResult(interp);
#endif
  return TCL_OK;
}

#if 0
static const char *testGetString(Tcl_Obj *pObj){
  const char *zRet = Tcl_GetString(pObj);
  if( zRet[0]=='\0' ) zRet = 0;
  return zRet;
}
#endif

/*
** sqlite3_bcv_attach ?OPTIONS? DIR MODULE USER CONTAINER AUTH ALIAS 
*/
static int test_bcv_attach(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
#if 0
  const char *zDir;
  const char *zMod;
  const char *zUser;
  const char *zCont;
  const char *zAuth;
  const char *zAlias;
  int rc;
  int flags = 0;
  int i;
  char *zErr = 0;                 /* Error message */
  char **pzErr = &zErr;           /* Argument for sqlite3_bcv_attach() */

  for(i=0; i<objc-7; i++){
    int nStr = 0;
    const char *zStr = Tcl_GetStringFromObj(objv[1+i], &nStr);
    if( nStr>2 && nStr<=5 && memcmp(zStr, "-poll", nStr)==0 ){
      flags |= SQLITE_BCVATTACH_POLL;
    }
    else if( nStr>2 && nStr<=9 && memcmp(zStr, "-readonly", nStr)==0 ){
      flags |= SQLITE_BCVATTACH_READONLY;
    }
    else if( nStr>2 && nStr<=6 && memcmp(zStr, "-noerr", nStr)==0 ){
      pzErr = 0;
    }
    else if( nStr>2 && nStr<=7 && memcmp(zStr, "-secure", nStr)==0 ){
      flags |= SQLITE_BCVATTACH_SECURE;
    }
    else {
      break;
    }
  }

  if( objc-i!=7 ){
    const char *zHelp = "?SWITCHES? DIR MODULE USER CONTAINER AUTH ALIAS"; 
    Tcl_WrongNumArgs(interp, 1, objv, zHelp);
    return TCL_ERROR;
  }

  zAlias = testGetString(objv[objc-1]);
  zAuth = testGetString(objv[objc-2]);
  zCont = testGetString(objv[objc-3]);
  zUser = testGetString(objv[objc-4]);
  zMod = testGetString(objv[objc-5]);
  zDir = testGetString(objv[objc-6]);

  Tcl_ResetResult(interp);
  rc = sqlite3_bcv_attach(zDir, zMod, zUser, zCont, zAuth, zAlias, flags,pzErr);
  assert( rc!=SQLITE_OK || zErr==0 );
  if( rc!=SQLITE_OK ){
    Tcl_SetObjResult(interp, zErr?Tcl_NewStringObj(zErr, -1):Tcl_NewIntObj(rc));
    sqlite3_free(zErr);
  }
  return (rc==SQLITE_OK ? TCL_OK : TCL_ERROR);
#endif
  return TCL_OK;
}

/*
** sqlite3_bcv_detach ?-noerr? DIR CONTAINER
*/
static int test_bcv_detach(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
#if 0
  const char *zDir;
  const char *zCont;
  char *zErr = 0;
  int rc;
  char *zOpt;
  int bNoerr = 0;

  if( objc!=3 && objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?-noerr? DIR CONTAINER");
    return TCL_ERROR;
  }
  if( objc==4 ){
    int nOpt;
    zOpt = Tcl_GetStringFromObj(objv[1], &nOpt);
    if( nOpt>6 || sqlite3_strnicmp("-noerr", zOpt, nOpt) ){
      Tcl_AppendResult(
          interp, "bad option \"", zOpt, "\" - should be \"-noerr\"", (char*)0
      );
      return TCL_ERROR;
    }
    bNoerr = 1;
  }

  zDir = Tcl_GetString(objv[objc-2]);
  zCont = Tcl_GetString(objv[objc-1]);

  Tcl_ResetResult(interp);
  rc = sqlite3_bcv_detach(zDir, zCont, (bNoerr ? 0 : &zErr));
  assert( rc!=SQLITE_OK || zErr==0 );
  if( rc!=SQLITE_OK ){
    Tcl_SetObjResult(interp, zErr?Tcl_NewStringObj(zErr, -1):Tcl_NewIntObj(rc));
    sqlite3_free(zErr);
  }
  return (rc==SQLITE_OK ? TCL_OK : TCL_ERROR);
#endif
  return TCL_OK;
}

#include "blockcachevfs.h"

typedef struct TestBcv TestBcv;
struct TestBcv {
  sqlite3_bcv *pBcv;
  Tcl_Obj *pProgress;
  Tcl_Obj *pLog;
  Tcl_Interp *interp;
};

static void test_bcv_log(
  void *pApp,
  const char *zStr
){
  TestBcv *p = (TestBcv*)pApp;
  Tcl_Obj *pEval;
  Tcl_Obj *pObj;

  pEval = Tcl_DuplicateObj(p->pLog);
  Tcl_IncrRefCount(pEval);

  pObj = Tcl_NewStringObj(zStr, -1);
  if( TCL_OK==Tcl_ListObjAppendElement(p->interp, pEval, pObj) ){
    Tcl_EvalObjEx(p->interp, pEval, 0);
  }
  Tcl_DecrRefCount(pEval);
}

static int test_bcv_progress(
  void *pCtx, 
  sqlite3_int64 nDone, 
  sqlite3_int64 nTotal
){
  TestBcv *p = (TestBcv*)pCtx;
  Tcl_Obj *pEval;
  int rc;
  Tcl_Obj *pRes;
  
  pEval = Tcl_DuplicateObj(p->pProgress);
  Tcl_IncrRefCount(pEval);
  if( Tcl_ListObjAppendElement(p->interp, pEval, Tcl_NewWideIntObj(nDone))
   || Tcl_ListObjAppendElement(p->interp, pEval, Tcl_NewWideIntObj(nTotal))
  ){
    Tcl_DecrRefCount(pEval);
    return 1;
  }

  rc = Tcl_EvalObjEx(p->interp, pEval, 0);
  Tcl_DecrRefCount(pEval);
  pRes = Tcl_GetObjResult(p->interp);
  if( Tcl_GetIntFromObj(p->interp, pRes, &rc) ){
    return 1;
  }

  return rc;
}

/*
** BCV_HANDLE COMMAND ...
*/
static int test_bcv_cmd(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  struct SubCmd {
    const char *zCmd;
    int nArg;
    const char *zHelp;
  } aCmd[] = { 
    { "close",    0, "" },                  /* 0 */
    { "copy",     2, "FROM TO" },           /* 1 */
    { "config",   2, "OPTION VALUE" },      /* 2 */
    { "create",   2, "SZNAME SZBLOCK" },    /* 3 */
    { "delete",   1, "NAME" },              /* 4 */
    { "destroy",  0, "" },                  /* 5 */
    { "download", 2, "REMOTE LOCAL" },      /* 6 */
    { "errcode",  0, "" },                  /* 7 */
    { "errmsg",   0, "" },                  /* 8 */
    { "upload",   2, "LOCAL REMOTE" },      /* 9 */
    { "cleanup",  1, "MS" },                /* 10 */
    { 0, 0 }
  };
  int iCmd = 0;
  int rc = 0;
  TestBcv *p = (TestBcv*)clientData;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND ...ARGS...");
    return TCL_ERROR;
  }
  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[1], aCmd, sizeof(aCmd[0]), "sub-command", 0, &iCmd
  );
  if( rc ) return TCL_ERROR;
  if( objc!=2+aCmd[iCmd].nArg ){
    Tcl_WrongNumArgs(interp, 2, objv, aCmd[iCmd].zHelp);
    return TCL_ERROR;
  }
  Tcl_ResetResult(interp);
  switch( iCmd ){
    case 0: {      /* close */
      Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
      break;
    };
    case 1: {      /* copy */
      const char *zFrom = Tcl_GetString(objv[2]);
      const char *zTo = Tcl_GetString(objv[3]);
      rc = sqlite3_bcv_copy(p->pBcv, zFrom, zTo);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
      break;
    };
    case 2: {      /* config */
      const char *azOpt[] = { 
        "verbose",      /* 0 */
        "progress",     /* 1 */
        "log",          /* 2 */
        "nrequest",     /* 3 */
        "loglevel",     /* 4 */
        "testnokv",     /* 5 */
        "httptimeout",  /* 6 */
        "find_orphans", /* 7 */
        0 
      };
      int iOpt = 0;
      rc = Tcl_GetIndexFromObj(interp, objv[2], azOpt, "option", 0, &iOpt);
      if( rc ) return TCL_ERROR;
      switch( iOpt ){
        case 0: {
          int iVal;
          if( Tcl_GetIntFromObj(interp, objv[3], &iVal) ) return TCL_ERROR;
          rc = sqlite3_bcv_config(p->pBcv, SQLITE_BCVCONFIG_VERBOSE, iVal);
          Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
          break;
        }
        case 1: {
          int nStr = 0;
          if( p->pProgress ){
            Tcl_DecrRefCount(p->pProgress);
          }
          p->pProgress = Tcl_DuplicateObj(objv[3]);
          Tcl_IncrRefCount(p->pProgress);
          Tcl_GetStringFromObj(p->pProgress, &nStr);
          if( nStr==0 ){
            rc = sqlite3_bcv_config(
                p->pBcv, SQLITE_BCVCONFIG_PROGRESS, (void*)0, (void*)0
            );
          }else{
            rc = sqlite3_bcv_config(
                p->pBcv, SQLITE_BCVCONFIG_PROGRESS, (void*)p, test_bcv_progress
            );
          }
          Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
          break;
        }
        case 2: {
          int nStr = 0;
          if( p->pLog ){
            Tcl_DecrRefCount(p->pLog);
          }
          p->pLog = Tcl_DuplicateObj(objv[3]);
          Tcl_IncrRefCount(p->pLog);
          Tcl_GetStringFromObj(p->pLog, &nStr);
          if( nStr ){
            sqlite3_bcv_config(
                p->pBcv, SQLITE_BCVCONFIG_LOG, (void*)p, test_bcv_log
            );
          }else{
            sqlite3_bcv_config(p->pBcv, SQLITE_BCVCONFIG_LOG,(void*)0,(void*)0);
          }
          break;
        }
        case 3: {
          int iVal;
          if( Tcl_GetIntFromObj(interp, objv[3], &iVal) ) return TCL_ERROR;
          rc = sqlite3_bcv_config(p->pBcv, SQLITE_BCVCONFIG_NREQUEST, iVal);
          Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
          break;
        }
        case 4: {
          int iVal;
          if( Tcl_GetIntFromObj(interp, objv[3], &iVal) ) return TCL_ERROR;
          rc = sqlite3_bcv_config(p->pBcv, SQLITE_BCVCONFIG_LOGLEVEL, iVal);
          Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
          break;
        }
        case 5: {
          int iVal;
          if( Tcl_GetIntFromObj(interp, objv[3], &iVal) ) return TCL_ERROR;
          rc = sqlite3_bcv_config(p->pBcv, SQLITE_BCVCONFIG_TESTNOKV, iVal);
          Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
          break;
        }
        case 6: {
          int iVal;
          if( Tcl_GetIntFromObj(interp, objv[3], &iVal) ) return TCL_ERROR;
          rc = sqlite3_bcv_config(p->pBcv, SQLITE_BCVCONFIG_HTTPTIMEOUT, iVal);
          Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
          break;
        }
        case 7: {
          int iVal;
          if( Tcl_GetIntFromObj(interp, objv[3], &iVal) ) return TCL_ERROR;
          rc = sqlite3_bcv_config(p->pBcv, SQLITE_BCVCONFIG_FINDORPHANS, iVal);
          Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
          break;
        }
      }
      break;
    };
    case 3: {      /* create */
      int szName = 0;
      int szBlk = 0;
      if( Tcl_GetIntFromObj(interp, objv[2], &szName)
       || Tcl_GetIntFromObj(interp, objv[3], &szBlk)
      ){
        return TCL_ERROR;
      }
      rc = sqlite3_bcv_create(p->pBcv, szName, szBlk);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
      break;
    };
    case 4: {      /* delete */
      const char *zDelete = Tcl_GetString(objv[2]);
      rc = sqlite3_bcv_delete(p->pBcv, zDelete);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
      break;
    };
    case 5: {      /* destroy */
      rc = sqlite3_bcv_destroy(p->pBcv);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
      break;
    };
    case 6: {      /* download */
      const char *zRemote = (const char*)Tcl_GetString(objv[2]);
      const char *zLocal = (const char*)Tcl_GetString(objv[3]);
      rc = sqlite3_bcv_download(p->pBcv, zRemote, zLocal);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
      break;
    };
    case 7: {      /* errcode */
      rc = sqlite3_bcv_errcode(p->pBcv);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
      break;
    };
    case 8: {      /* errmsg */
      const char *zErr = sqlite3_bcv_errmsg(p->pBcv);
      if( zErr==0 ) zErr = "";
      Tcl_SetObjResult(interp, Tcl_NewStringObj(zErr, -1));
      break;
    };
    case 9: {      /* upload */
      const char *zLocal = (const char*)Tcl_GetString(objv[2]);
      const char *zRemote = (const char*)Tcl_GetString(objv[3]);
      rc = sqlite3_bcv_upload(p->pBcv, zLocal, zRemote);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
      break;
    };
    case 10: {      /* cleanup */
      int nSecond = 0;
      if( Tcl_GetIntFromObj(interp, objv[2], &nSecond) ) return TCL_ERROR;
      rc = sqlite3_bcv_cleanup(p->pBcv, nSecond);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
      break;
    };
    default:
      assert( 0 );
  }

  return TCL_OK;
}

static void del_bcv_cmd(ClientData clientData){
  TestBcv *p = (TestBcv*)clientData;
  sqlite3_bcv_close(p->pBcv);
  if( p->pProgress ) Tcl_DecrRefCount(p->pProgress);
  if( p->pLog ) Tcl_DecrRefCount(p->pLog);
  sqlite3_free(p);
}


/*
** Decode a pointer to an sqlite3 object.
*/
int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite3 **ppDb){
  struct SqliteDb { sqlite3 *db; };
  struct SqliteDb *p;
  Tcl_CmdInfo cmdInfo;
  if( Tcl_GetCommandInfo(interp, zA, &cmdInfo) ){
    p = (struct SqliteDb*)cmdInfo.objClientData;
    *ppDb = p->db;
  }else{
    Tcl_AppendResult(interp, "bad database handle: ", zA, 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** sqlite3_bcv_fcntl DB FCNTL DATABASE
*/
static int test_bcv_fcntl(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
#if 0
  struct Fcntl {
    const char *zName;
    int eType;
  } aFcntl[] = {
    { "n_msg", SQLITE_FCNTL_BCV_N_MSG },
    { "ms_msg", SQLITE_FCNTL_BCV_MS_MSG },
    { 0, 0 }
  };
  int rc;
  int iF = 0;
  sqlite3 *db = 0;
  const char *zDatabase;
  sqlite3_int64 res = 0;

  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB FCNTL DATABASE");
    return TCL_ERROR;
  }
  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[2], aFcntl, sizeof(aFcntl[0]), "FCNTL", 0, &iF
  );
  if( rc || getDbPointer(interp, Tcl_GetString(objv[1]), &db) ){
    return TCL_ERROR;
  }
  zDatabase = Tcl_GetString(objv[3]);

  rc = sqlite3_file_control(db, zDatabase, aFcntl[iF].eType, (void*)&res);
  if( rc==SQLITE_OK ){
    Tcl_SetObjResult(interp, Tcl_NewWideIntObj((Tcl_WideInt)res));
  }else{
    Tcl_AppendResult(interp, "sqlite3_file_control() failed", 0);
  }

#endif
  return TCL_OK;
}

/*
** sqlite3_bcv_register BOOLEAN
*/
static int test_bcv_register(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
#if 0
  int bDefault = 0;
  const char *zName = 0;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "BOOLEAN");
    return TCL_ERROR;
  }
  if( Tcl_GetBooleanFromObj(interp, objv[1], &bDefault) ){
    return TCL_ERROR;
  }

  zName = sqlite3_bcv_register(bDefault);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(zName, -1));
#endif
  return TCL_OK;
}

/*
** sqlite3_bcv_open MODULE USER KEY CONTAINER
*/
static int test_bcv_open(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  const char *zUser = 0;
  const char *zKey = 0;
  const char *zCont = 0;
  const char *zModule = 0;
  char *zCmd = 0;
  int rc;
  sqlite3_bcv *pBcv = 0;
  TestBcv *p = 0;
  unsigned int rnd1;

  if( objc!=5 ){
    Tcl_WrongNumArgs(interp, 1, objv, "MODULE USER KEY CONTAINER");
    return TCL_ERROR;
  }
  zModule = Tcl_GetString(objv[1]);
  zUser = Tcl_GetString(objv[2]);
  zKey = Tcl_GetString(objv[3]);
  zCont = Tcl_GetString(objv[4]);

  if( zUser[0]=='\0' ) zUser = 0;
  if( zKey[0]=='\0' ) zKey = 0;

  Tcl_ResetResult(interp);
  rc = sqlite3_bcv_open(zModule, zUser, zKey, zCont, &pBcv);
  if( rc!=SQLITE_OK ){
    Tcl_AppendResult(interp, sqlite3_bcv_errmsg(pBcv), 0);
    sqlite3_bcv_close(pBcv);
    return TCL_ERROR;
  }

  sqlite3_randomness(sizeof(rnd1), &rnd1);
  zCmd = sqlite3_mprintf("bcv%d", (int)(rnd1&0x7FFFFFFF));
  p = (TestBcv*)sqlite3_malloc(sizeof(TestBcv));
  if( zCmd==0 || p==0 ){
    sqlite3_free(zCmd);
    sqlite3_free(p);
    return TCL_ERROR;
  }
  memset(p, 0, sizeof(TestBcv));
  p->pBcv = pBcv;
  p->interp = interp;

  Tcl_CreateObjCommand(interp, zCmd, test_bcv_cmd, (void*)p, del_bcv_cmd);
  Tcl_AppendResult(interp, zCmd, 0);
  sqlite3_free(zCmd);
  return TCL_OK;
}

static void log_to_stdout(void *pPtr, int rc, const char *zErr){
  fprintf(stdout, "sqlite: (rc=%d) %s\n", rc, zErr);
  fflush(stdout);
}

/*
** bcv_oom_control IMEMFAULT ?PERSIST?
**
**   The argument must be an integer. If it is non-negative, this command sets
**   the value of g.iMemFault to the argument value. In any case the old,
**   possibly overwritten, value of g.iMemFault is returned.
*/
static int test_bcv_oom_control(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  int iNew = 0;
  int bPersist = 0;

  if( objc!=2 && objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "IMEMFAULT ?PERSIST?");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &iNew) ){
    return TCL_ERROR;
  }
  if( objc==3 && Tcl_GetBooleanFromObj(interp, objv[2], &bPersist) ){
    return TCL_ERROR;
  }

  Tcl_SetObjResult(interp, Tcl_NewIntObj(g.iMemFault));
  if( iNew>=0 ){
    g.iMemFault = iNew;
    g.ePersist = bPersist;
  }
  return TCL_OK;
}

/*
** bcv_ioerr_control IFAULT
**
**   The argument must be an integer. If it is non-negative, this command sets
**   the value of g.iIoFault to the argument value. In any case the old,
**   possibly overwritten, value of g.iIoFault is returned.
*/
static int test_bcv_ioerr_control(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  int iNew = 0;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "IMEMFAULT");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &iNew) ){
    return TCL_ERROR;
  }

  Tcl_SetObjResult(interp, Tcl_NewIntObj(g.iIoFault));
  if( iNew>=0 ){
    g.iIoFault = iNew;
  }
  return TCL_OK;
}

/*
** vfs_delete VFSNAME FILE
*/
static int test_vfs_delete(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  const char *zVfs = 0;
  const char *zFile = 0;
  sqlite3_vfs *pVfs = 0;
  int rc;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "VFSNAME FILE");
    return TCL_ERROR;
  }
  zVfs = Tcl_GetString(objv[1]);
  zFile = Tcl_GetString(objv[2]);
  pVfs = sqlite3_vfs_find(zVfs);

  Tcl_ResetResult(interp);
  if( pVfs==0 ){
    Tcl_AppendResult(interp, "no such vfs: ", zVfs, (char*)0);
    return TCL_ERROR;
  }
  rc = pVfs->xDelete(pVfs, zFile, 0);
  if( rc!=SQLITE_OK ){
    char zErr[64];
    sqlite3_snprintf(sizeof(zErr), zErr, "rc=%d", rc);
    Tcl_SetObjResult(interp, Tcl_NewStringObj(zErr, -1));
    return TCL_ERROR;
  }
  return TCL_OK;
}

typedef struct TestBcvfs TestBcvfs;
struct TestBcvfs {
  sqlite3_bcvfs *pFs;
  Tcl_Obj *pLogCallback;
  Tcl_Obj *pAuthCallback;
  Tcl_Interp *interp;
};

static void bcvfsDestroy(ClientData clientData){
  TestBcvfs *pTest = (TestBcvfs*)clientData;
  sqlite3_bcvfs_destroy(pTest->pFs);
  if( pTest->pLogCallback ){
    Tcl_DecrRefCount(pTest->pLogCallback);
  }
  ckfree(pTest);
}

static void bcvfsLogCb(void *pCtx, int mask, const char *zMsg){
  struct MaskBit {
    const char *zName;
    int mask;
  } aBit[] = {
    { "httpretry", SQLITE_BCV_LOG_HTTPRETRY },
    { "http", SQLITE_BCV_LOG_HTTP },
    { "upload", SQLITE_BCV_LOG_UPLOAD },
    { "cleanup", SQLITE_BCV_LOG_CLEANUP },
    { 0, 0 }
  };
  TestBcvfs *pTest = (TestBcvfs*)pCtx;
  Tcl_Interp *interp = pTest->interp;
  int ii;
  Tcl_Obj *pMask = 0;
  Tcl_Obj *pEval;

  for(ii=0; ii<sizeof(aBit)/sizeof(aBit[0]); ii++){
    if( aBit[ii].mask & mask ){
      pMask = Tcl_NewStringObj(aBit[ii].zName, -1);
      break;
    }
  }
  if( pMask==0 ){
    pMask = Tcl_NewIntObj(mask);
  }
  pEval = Tcl_DuplicateObj(pTest->pLogCallback);
  Tcl_IncrRefCount(pEval);
  Tcl_ListObjAppendElement(interp, pEval, pMask);
  Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(zMsg, -1));
  if( Tcl_EvalObjEx(interp, pEval, 0) ){
    Tcl_BackgroundError(interp);
  }
  Tcl_DecrRefCount(pEval);
}

static int bcvfsAuthCb(
  void *pCtx, 
  const char *zStorage,
  const char *zAccount,
  const char *zContainer,
  char **pzAuthToken
){
  TestBcvfs *pTest = (TestBcvfs*)pCtx;
  Tcl_Interp *interp = pTest->interp;
  Tcl_Obj *pEval = 0;
  int rc = TCL_OK;

  pEval = Tcl_DuplicateObj(pTest->pAuthCallback);
  Tcl_IncrRefCount(pEval);
  if( Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(zStorage, -1))
   || Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(zAccount, -1))
   || Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(zContainer, -1))
  ){
    rc = TCL_ERROR;
  }else{
    rc = Tcl_EvalObjEx(interp, pEval, 0);
    if( rc==TCL_OK ){
      *pzAuthToken = sqlite3_mprintf("%s", Tcl_GetStringResult(interp));
      if( *pzAuthToken==0 ){
        rc = SQLITE_NOMEM;
      }
    }
  }
  Tcl_DecrRefCount(pEval);
  return rc;
}

typedef struct BusyHandlerArg BusyHandlerArg;
struct BusyHandlerArg {
  Tcl_Obj *pEval;
  Tcl_Interp *interp;
};
  
static int test_busy_handler(void *pArg, int nPrev){
  BusyHandlerArg *p = (BusyHandlerArg*)pArg;
  Tcl_Obj *pEval = Tcl_DuplicateObj(p->pEval);
  int rc = TCL_OK;
  int iRet = 0;

  Tcl_IncrRefCount(pEval);
  rc = Tcl_ListObjAppendElement(p->interp, pEval, Tcl_NewIntObj(nPrev));
  if( rc==TCL_OK ){
    rc = Tcl_EvalObjEx(p->interp, pEval, TCL_GLOBAL_ONLY);
  }
  if( rc==TCL_OK ){
    Tcl_Obj *pObj = Tcl_GetObjResult(p->interp);
    rc = Tcl_GetIntFromObj(p->interp, pObj, &iRet);
  }

  return (rc==TCL_OK && iRet>0);
}

/*
** PREFETCH_HANDLE COMMAND ...
*/
static int prefetchCmd(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  struct SubCmd {
    const char *zCmd;
    int nArg;
    const char *zHelp;
  } aCmd[] = { 
    { "run",      2, "NREQUEST MS" },       /* 0 */
    { "status",   1, "NAME" },              /* 1 */
    { "errmsg",   0, "" },                  /* 2 */
    { "errcode",  0, "" },                  /* 3 */
    { "destroy",  0, "" },                  /* 4 */
    { 0, 0 }
  };
  int iCmd = 0;
  int rc = 0;
  sqlite3_prefetch *p = (sqlite3_prefetch*)clientData;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND ...ARGS...");
    return TCL_ERROR;
  }
  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[1], aCmd, sizeof(aCmd[0]), "sub-command", 0, &iCmd
  );
  if( rc ) return TCL_ERROR;
  if( objc!=2+aCmd[iCmd].nArg ){
    Tcl_WrongNumArgs(interp, 2, objv, aCmd[iCmd].zHelp);
    return TCL_ERROR;
  }
  Tcl_ResetResult(interp);

  switch( iCmd ){
    case 0: { /* "run" */
      int nRequest = 0;
      int nMs = 0;
      int rc = SQLITE_OK;

      if( Tcl_GetIntFromObj(interp, objv[2], &nRequest)
       || Tcl_GetIntFromObj(interp, objv[3], &nMs)
      ){
        return TCL_ERROR;
      }

      rc = sqlite3_bcvfs_prefetch_run(p, nRequest, nMs);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
      break;
    }

    case 1: { /* "status" */
      struct StatusOption {
        const char *zName;
        int eOp;
      } aOpt[] = {
        { "noutstanding", SQLITE_BCVFS_PFS_NOUTSTANDING },
        { "ndemand", SQLITE_BCVFS_PFS_NDEMAND },
        { "invalid", 1234 },
        { 0, 0 }
      };
      int iOpt = 0;
      i64 iVal = 0;

      int rc = Tcl_GetIndexFromObjStruct(
          interp, objv[2], (void*)aOpt, sizeof(aOpt[0]), "OPTION", 0, &iOpt
      );
      if( rc!=TCL_OK ) return rc;
      rc = sqlite3_bcvfs_prefetch_status(p, iOpt, &iVal);
      if( rc==SQLITE_OK ){
        Tcl_SetObjResult(interp, Tcl_NewWideIntObj(iVal));
      }else{
        Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
        return TCL_ERROR;
      }
      break;
    }

    case 2: { /* "errmsg" */
      const char *zErr = sqlite3_bcvfs_prefetch_errmsg(p);
      Tcl_SetObjResult(interp, Tcl_NewStringObj(zErr, -1));
      break;
    }

    case 3: { /* "errcode" */
      int rc = sqlite3_bcvfs_prefetch_errcode(p);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
      break;
    }

    default:  /* "destroy" */
      Tcl_DeleteCommand(interp, Tcl_GetString(objv[0]));
      break;
  }

  return TCL_OK;
}

static void prefetchDel(ClientData clientData){
  sqlite3_prefetch *p = (sqlite3_prefetch*)clientData;
  sqlite3_bcvfs_prefetch_destroy(p);
}

/*
** $vfs attach ?OPTIONS? STORAGE ACCOUNT CONTAINER
** 
**   where OPTIONS are:
**
**     -auth  AUTHSTRING
**     -alias ALIAS
**
** $vfs detach CONTAINER
**
** $vfs pin ?-noerr? CONTAINER DATABASE
** $vfs unpin ?-noerr? CONTAINER DATABASE
**
** $vfs upload ?-noerr? CONTAINER
**
** $vfs delete CONTAINER DATABASE
**
*/
static int bcvfsCommand(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  const char *azSub[] = {
    "config",           /* 0 */
    "destroy",          /* 1 */
    "log",              /* 2 */
    "attach",           /* 3 */
    "detach",           /* 4 */
    "auth_callback",    /* 5 */
    "poll",             /* 6 */
    "upload",           /* 7 */
    "isdaemon",         /* 8 */
    "copy",             /* 9 */
    "delete",           /* 10 */
    "prefetch_new",     /* 11 */
    "revert",           /* 12 */
    0,                             
  };
  TestBcvfs *pTest = (TestBcvfs*)clientData;
  int iCmd;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND ...");
    return TCL_ERROR;
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], azSub, "sub-command", 0, &iCmd) ){
    return TCL_ERROR;
  }
  switch( iCmd ){
    case 0: assert( !strcmp("config", azSub[iCmd]) ); {
      if( objc!=4 ){
        Tcl_WrongNumArgs(interp, 2, objv, "OPTION VALUE");
        return TCL_ERROR;
      }else{
        struct ConfigurationOption {
          const char *zName;
          int eOp;
        } aOpt[] = {
          { "cachesize", SQLITE_BCV_CACHESIZE },
          { "nrequest", SQLITE_BCV_NREQUEST },
          { "httptimeout", SQLITE_BCV_HTTPTIMEOUT },
          { "curlverbose", SQLITE_BCV_CURLVERBOSE },
          { "httplog_timeout", SQLITE_BCV_HTTPLOG_TIMEOUT },
          { "httplog_nentry", SQLITE_BCV_HTTPLOG_NENTRY },
          { "invalid", 1234 },
          { 0, 0 }
        };
        int iOpt = 0;
        i64 iVal = 0;

        int rc = Tcl_GetIndexFromObjStruct(
            interp, objv[2], (void*)aOpt, sizeof(aOpt[0]), "OPTION", 0, &iOpt 
        );
        if( rc ) return TCL_ERROR;
        if( Tcl_GetWideIntFromObj(interp, objv[3], &iVal) ) return TCL_ERROR;

        rc = sqlite3_bcvfs_config(pTest->pFs, aOpt[iOpt].eOp, iVal);
        if( rc!=SQLITE_OK ){
          char *zErr = sqlite3_mprintf("rc=%d", rc);
          Tcl_SetObjResult(interp, Tcl_NewStringObj(zErr, -1));
          sqlite3_free(zErr);
          return TCL_ERROR;
        }
      }
      break;
    }
    case 1: assert( !strcmp("destroy", azSub[iCmd]) ); {
      if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 2, objv, "");
        return TCL_ERROR;
      }else{
        int rc = sqlite3_bcvfs_destroy(pTest->pFs);
        if( rc==SQLITE_OK ){
          pTest->pFs = 0;
          Tcl_DeleteCommand(interp, Tcl_GetString(objv[0]));
        }else{
          char *zErr = sqlite3_mprintf("rc=%d", rc);
          Tcl_SetObjResult(interp, Tcl_NewStringObj(zErr, -1));
          sqlite3_free(zErr);
          return TCL_ERROR;
        }
      }
      break;
    }
    case 2: assert( !strcmp("log", azSub[iCmd]) ); {
      if( objc!=4 ){
        Tcl_WrongNumArgs(interp, 2, objv, "");
        return TCL_ERROR;
      }else{
        struct MaskBit {
          const char *zName;
          int mask;
        } aBit[] = {
          { "http", SQLITE_BCV_LOG_HTTP },
          { "httpretry", SQLITE_BCV_LOG_HTTPRETRY },
          { "upload", SQLITE_BCV_LOG_UPLOAD },
          { "cleanup", SQLITE_BCV_LOG_CLEANUP },
          { "all", 0xFFFFFF },
          { 0, 0 }
        };
        int mLog = 0;
        int ii;
        int nElem = 0;
        Tcl_Obj **aElem = 0;
        if( Tcl_ListObjGetElements(interp, objv[2], &nElem, &aElem) ){
          return TCL_ERROR;
        }
        for(ii=0; ii<nElem; ii++){
          int iBit = 0;
          int rc = Tcl_GetIndexFromObjStruct(
              interp, aElem[ii], (void*)aBit, sizeof(aBit[0]), "BIT", 0, &iBit 
          );
          if( rc ) return TCL_ERROR;
          mLog |= aBit[iBit].mask;
        }
        if( pTest->pLogCallback ){
          Tcl_DecrRefCount(pTest->pLogCallback);
        }
        pTest->pLogCallback = objv[3];
        pTest->interp = interp;
        Tcl_IncrRefCount(pTest->pLogCallback);
        sqlite3_bcvfs_log_callback(pTest->pFs, (void*)pTest, mLog, bcvfsLogCb);
      }
      break;
    }
    case 3: assert( !strcmp("attach", azSub[iCmd]) ); {
      const char *zAlias = 0;
      const char *zStorage = 0;
      const char *zAccount = 0;
      const char *zContainer = 0;
      int flags = 0;
      char *zErr = 0;
      int rc = SQLITE_OK;
      char **pzErr = &zErr;

      int ii;
      if( objc<5 ){
        Tcl_WrongNumArgs(interp,2, objv, "?OPTIONS? STORAGE ACCOUNT CONTAINER");
        return TCL_ERROR;
      }
      for(ii=2; ii<objc-3; ii++){
        static const char *azOpt[] = {"-alias", "-noerr", "-secure","-ifnot",0};
        int iOpt = 0;
        Tcl_Obj *pArg = objv[ii+1];
        if( Tcl_GetIndexFromObj(interp, objv[ii], azOpt, "OPTION", 0, &iOpt) ){
          return TCL_ERROR;
        }
        if( ii==objc-4 && iOpt==0 ){
          Tcl_ResetResult(interp);
          Tcl_AppendResult(interp,
              "option requires an argument: ", Tcl_GetString(objv[ii]), (char*)0
          );
          return TCL_ERROR;
        }
        assert( iOpt==0 || iOpt==1 || iOpt==2 || iOpt==3 );
        if( iOpt==1 ){
          pzErr = 0;
        }else if( iOpt==2 ){
          flags |= SQLITE_BCV_ATTACH_SECURE;
        }else if( iOpt==3 ){
          flags |= SQLITE_BCV_ATTACH_IFNOT;
        }else{
          zAlias = Tcl_GetString(pArg);
          ii++;
        }
      }

      zStorage = Tcl_GetString(objv[objc-3]);
      zAccount = Tcl_GetString(objv[objc-2]);
      zContainer = Tcl_GetString(objv[objc-1]);
      rc = sqlite3_bcvfs_attach(
          pTest->pFs, zStorage, zAccount, zContainer, zAlias, flags, pzErr
      );
      assert( rc!=SQLITE_OK || zErr==0 );

      Tcl_ResetResult(interp);
      if( rc!=SQLITE_OK ){
        char *z = sqlite3_mprintf(
            "%d%s%s", rc, (zErr && zErr[0])?" ":"", zErr?zErr:""
        );
        Tcl_SetObjResult(interp, Tcl_NewStringObj(z, -1));
        sqlite3_free(zErr);
        sqlite3_free(z);
        return TCL_ERROR;
      }

      break;
    }
    case 4: assert( !strcmp("detach", azSub[iCmd]) ); {
      Tcl_ResetResult(interp);
      if( objc!=3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "CONTAINER");
        return TCL_ERROR;
      }else{
        int rc = SQLITE_OK;
        char *zErr = 0;
        const char *zCont = Tcl_GetString(objv[2]);
        rc = sqlite3_bcvfs_detach(pTest->pFs, zCont, &zErr);
        if( rc!=SQLITE_OK ){
          char *zMsg = sqlite3_mprintf("rc=%d: %s", rc, zErr);
          Tcl_SetObjResult(interp, Tcl_NewStringObj(zMsg, -1));
          sqlite3_free(zErr);
          sqlite3_free(zMsg);
          return TCL_ERROR;
        }
        assert( zErr==0 );
      }
      break;
    }
    case 5: assert( !strcmp("auth_callback", azSub[iCmd]) ); {
      if( objc!=3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "SCRIPT");
        return TCL_ERROR;
      }else{
        if( pTest->pAuthCallback ){
          Tcl_DecrRefCount(pTest->pAuthCallback);
        }
        pTest->pAuthCallback = Tcl_DuplicateObj(objv[2]);
        Tcl_IncrRefCount(pTest->pAuthCallback);
        sqlite3_bcvfs_auth_callback(pTest->pFs, (void*)pTest, bcvfsAuthCb);
        pTest->interp = interp;
      }
      break;
    }
    case 6: assert( !strcmp("poll", azSub[iCmd]) ); {
      if( (objc!=3 && objc!=4) 
       || (objc==4 && sqlite3_stricmp("-noerr", Tcl_GetString(objv[2])))
      ){
        Tcl_WrongNumArgs(interp, 2, objv, "?-noerr? CONTAINER");
        return TCL_ERROR;
      }else{
        char *zErr = 0;
        const char *zCont = Tcl_GetString(objv[objc-1]);
        int rc = sqlite3_bcvfs_poll(pTest->pFs, zCont, (objc==4)?0:&zErr);

        Tcl_ResetResult(interp);
        if( rc!=SQLITE_OK ){
          if( zErr==0 ){
            zErr = sqlite3_mprintf("rc=%d", rc);
          }
          Tcl_SetObjResult(interp, Tcl_NewStringObj(zErr, -1));
          sqlite3_free(zErr);
          return TCL_ERROR;
        }
      }
      break;
    }

    case 7: assert( !strcmp("upload", azSub[iCmd]) ); {
      const char *zContainer = 0;
      char *zErr = 0;
      int rc = SQLITE_OK;
      char **pzErr = &zErr;
      Tcl_Obj *pBusy = 0;
      BusyHandlerArg bh;

      int ii;
      if( objc<3 ){
        Tcl_WrongNumArgs(interp,2, objv, "?OPTIONS? CONTAINER");
        return TCL_ERROR;
      }
      for(ii=2; ii<objc-1; ii++){
        static const char *azOpt[] = {"-noerr", "-busyhandler", 0};
        int iOpt = 0;
        if( Tcl_GetIndexFromObj(interp, objv[ii], azOpt, "OPTION", 0, &iOpt) ){
          return TCL_ERROR;
        }
        if( iOpt==0 ){
          pzErr = 0;
        }else{
          assert( iOpt==1 );
          if( ii==objc-2 ){
            Tcl_ResetResult(interp);
            Tcl_AppendResult(interp, "option requires an argument: ", 
                Tcl_GetString(objv[ii]), (char*)0
            );
            return TCL_ERROR;
          }else{
            if( pBusy ) Tcl_DecrRefCount(pBusy);
            pBusy = Tcl_DuplicateObj(objv[ii+1]);
            ii++;
            Tcl_IncrRefCount(pBusy);
          }
        }
      }
      zContainer = Tcl_GetString(objv[objc-1]);

      bh.pEval = pBusy;
      bh.interp = interp;
      rc = sqlite3_bcvfs_upload(pTest->pFs, zContainer, 
          (pBusy ? test_busy_handler : 0), (void*)&bh, pzErr
      );

      Tcl_ResetResult(interp);
      if( rc!=SQLITE_OK ){
        char *z = sqlite3_mprintf(
            "%d%s%s", rc, (zErr && zErr[0])?" ":"", zErr?zErr:""
        );
        Tcl_SetObjResult(interp, Tcl_NewStringObj(z, -1));
        sqlite3_free(zErr);
        sqlite3_free(z);
        return TCL_ERROR;
      }

      break;
    }

    case 8: assert( !strcmp("isdaemon", azSub[iCmd]) ); {
      int ret;
      if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 2, objv, "");
        return TCL_ERROR;
      }
      ret = sqlite3_bcvfs_isdaemon(pTest->pFs);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(ret));
      break;
    }

    case 9: assert( !strcmp("copy", azSub[iCmd]) ); {
      const char *zCont = 0;
      const char *zFrom = 0;
      const char *zTo = 0;
      char *zErr = 0;
      int rc = SQLITE_OK;

      if( objc!=5 ){
        Tcl_WrongNumArgs(interp, 2, objv, "CONTAINER FROM TO");
        return TCL_ERROR;
      }
      zCont = Tcl_GetString(objv[2]);
      zFrom = Tcl_GetString(objv[3]);
      zTo = Tcl_GetString(objv[4]);

      rc = sqlite3_bcvfs_copy(pTest->pFs, zCont, zFrom, zTo, &zErr);
      if( rc!=SQLITE_OK ){
        char *z = sqlite3_mprintf(
            "%d%s%s", rc, (zErr && zErr[0])?" ":"", zErr?zErr:""
        );
        Tcl_SetObjResult(interp, Tcl_NewStringObj(z, -1));
        sqlite3_free(zErr);
        sqlite3_free(z);
        return TCL_ERROR;
      }
      break;
    }

    case 10: assert( !strcmp("delete", azSub[iCmd]) ); {
      const char *zCont = 0;
      const char *zDb = 0;
      char *zErr = 0;
      int rc = SQLITE_OK;

      if( objc!=4 ){
        Tcl_WrongNumArgs(interp, 2, objv, "CONTAINER DATABASE");
        return TCL_ERROR;
      }
      zCont = Tcl_GetString(objv[2]);
      zDb = Tcl_GetString(objv[3]);

      rc = sqlite3_bcvfs_delete(pTest->pFs, zCont, zDb, &zErr);
      if( rc!=SQLITE_OK ){
        char *z = sqlite3_mprintf(
            "%d%s%s", rc, (zErr && zErr[0])?" ":"", zErr?zErr:""
        );
        Tcl_SetObjResult(interp, Tcl_NewStringObj(z, -1));
        sqlite3_free(zErr);
        sqlite3_free(z);
        return TCL_ERROR;
      }
      break;
    }

    case 11: assert( !strcmp("prefetch_new", azSub[iCmd]) ); {
      const char *zCont = 0;
      const char *zDb = 0;
      const char *zCmd = 0;
      sqlite3_prefetch *pNew = 0;

      if( objc!=5 ){
        Tcl_WrongNumArgs(interp, 2, objv, "CMD CONTAINER DATABASE");
        return TCL_ERROR;
      }
      zCmd = Tcl_GetString(objv[2]);
      zCont = Tcl_GetString(objv[3]);
      zDb = Tcl_GetString(objv[4]);

      sqlite3_bcvfs_prefetch_new(pTest->pFs, zCont, zDb, &pNew);
      Tcl_CreateObjCommand(interp, zCmd, prefetchCmd, (void*)pNew, prefetchDel);
      Tcl_SetObjResult(interp, objv[2]);
      break;
    }

    case 12: assert( !strcmp("revert", azSub[iCmd]) ); {
      const char *zContainer = 0;
      char *zErr = 0;
      int rc = SQLITE_OK;

      if( objc!=3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "CONTAINER");
        return TCL_ERROR;
      }
      zContainer = Tcl_GetString(objv[2]);
      rc = sqlite3_bcvfs_revert(pTest->pFs, zContainer, &zErr);
      assert( zErr==0 || rc!=SQLITE_OK );
      if( rc==SQLITE_OK ){
        Tcl_ResetResult(interp);
      }else{
        char *z = sqlite3_mprintf(
            "%d%s%s", rc, (zErr && zErr[0])?" ":"", zErr?zErr:""
        );
        Tcl_SetObjResult(interp, Tcl_NewStringObj(z, -1));
        sqlite3_free(zErr);
        sqlite3_free(z);
        return TCL_ERROR;
      }
      break;
    }
  }
  return TCL_OK;
}

/*
** sqlite3_bcvfs_create DIR VFSNAME
*/
static int test_bcvfs_create(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  const char *zDir = 0;
  const char *zVfsname = 0;
  sqlite3_bcvfs *pNew = 0;
  TestBcvfs *pTest = 0;
  int rc = SQLITE_OK;
  Tcl_Obj *pCmd;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DIR VFSNAME");
    return TCL_ERROR;
  }

  zDir = Tcl_GetString(objv[1]);
  zVfsname = Tcl_GetString(objv[2]);

  rc = sqlite3_bcvfs_create(zDir, zVfsname, &pNew, 0);
  if( rc!=SQLITE_OK ){
    Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
    return TCL_ERROR;
  }

  pTest = (TestBcvfs*)ckalloc(sizeof(TestBcvfs));
  memset(pTest, 0, sizeof(TestBcvfs));
  pTest->pFs = pNew;

  pCmd = Tcl_ObjPrintf("bcvfs_%s", zVfsname);
  Tcl_CreateObjCommand(interp, 
      Tcl_GetString(pCmd), bcvfsCommand, (void*)pTest, bcvfsDestroy
  );
  Tcl_SetObjResult(interp, pCmd);
  return TCL_OK;
}

typedef struct BcvTfsList BcvTfsList;
struct BcvTfsList {
  int nRef;                       /* Pointers to this structure */
  int rc;
  int nAlloc;
  int nFile;
  char **azFile;
};

/*
** The vtab object for bcv_tfs.
*/
typedef struct bcv_tfs_vtab bcv_tfs_vtab;
struct bcv_tfs_vtab {
  sqlite3_vtab base;              /* Base class */
  sqlite3 *db;                    /* Database handle */
  BcvTfsList *pList;             /* Used by bcv_tfs tables */
};

typedef struct bcv_tfs_cursor bcv_tfs_cursor;
struct bcv_tfs_cursor {
  sqlite3_vtab_cursor base;       /* Base class */
  sqlite3_bcvfs *pFs;
  int iRow;
  BcvTfsList *pList;
};

#define BCV_FILE_VTAB_SCHEMA      \
"CREATE TABLE bcv_tfs("          \
"  name      TEXT,"               \
"  content   BLOB "               \
")"

static int bcvTfsVtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  bcv_tfs_vtab *pNew = 0;
  int rc = SQLITE_OK;

  pNew = (bcv_tfs_vtab*)bcvMallocRc(&rc, sizeof(bcv_tfs_vtab));
  if( pNew ){
    sqlite3_declare_vtab(db, BCV_FILE_VTAB_SCHEMA);
    pNew->db = db;
  }
  *ppVtab = &pNew->base;
  return rc;
}

/*
** Decrement the ref-count for the list object passed as the only argument.
** If this means there are now no references to the object, free it. 
*/
static void bcvTfsListUnref(BcvTfsList *pList){
  if( pList ){
    pList->nRef--;
    if( pList->nRef==0 ){
      int ii;
      for(ii=0; ii<pList->nFile; ii++){
        sqlite3_free(pList->azFile[ii]);
      }
      sqlite3_free(pList->azFile);
      sqlite3_free(pList);
    }
  }
}

static int bcvTfsVtabDisconnect(sqlite3_vtab *pVtab){
  bcv_tfs_vtab *pTab = (bcv_tfs_vtab*)pVtab;
  bcvTfsListUnref(pTab->pList);
  sqlite3_free(pTab);
  return SQLITE_OK;
}

static int bcvTfsVtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCur){
  int rc = SQLITE_OK;
  bcv_tfs_cursor *pNew = 0;
  pNew = (bcv_tfs_cursor*)bcvMallocRc(&rc, sizeof(bcv_tfs_cursor));
  *ppCur = &pNew->base;
  return rc;
}

/*
** Reset the bcv_tfs_cursor object passed as the only argument. So that
** it is in the same state as it was when first returned by xOpen().
*/
static void bcvTfsResetCsr(bcv_tfs_cursor *pCsr){
  bcvTfsListUnref(pCsr->pList);
  pCsr->iRow = 0;
}
static int bcvTfsVtabClose(sqlite3_vtab_cursor *cur){
  bcv_tfs_cursor *pCur = (bcv_tfs_cursor*)cur;
  bcvTfsResetCsr(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int bcvTfsListCb(void *pCtx, int rc, char *z){
  BcvTfsList *pList = (BcvTfsList*)pCtx;
  if( rc!=SQLITE_OK ){
    if( pList->rc==SQLITE_OK ) pList->rc = rc;
  }else if( z ){
    char *zFile = 0;
    if( pList->nFile==pList->nAlloc ){
      i64 nNew = MAX(128, pList->nAlloc*2);
      i64 nByte = nNew*sizeof(char*);
      char **azNew = (char**)sqlite3_realloc64(pList->azFile, nByte);
      if( azNew ){
        pList->azFile = azNew;
        pList->nAlloc = nNew;
      }else{
        pList->rc = SQLITE_NOMEM;
      }
    }
    zFile = bcvStrdupRc(&pList->rc, z);
    if( pList->rc==SQLITE_OK ){
      pList->azFile[pList->nFile] = zFile;
      pList->nFile++;
    }
  }
  return SQLITE_OK;
}

/*
** xBestIndex method for bcv_tfs.
*/
static int bcvTfsVtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = (double)(10*1000*1000);
  pIdxInfo->estimatedRows = 10*1000*1000;
  return SQLITE_OK;
}

int bcvfsGetBcv(sqlite3*, BcvDispatch**, BcvContainer**);
void bcvfsReleaseBcv(sqlite3*, BcvDispatch*, BcvContainer*, int);

static int bcvTfsVtabFilter(
  sqlite3_vtab_cursor *cur, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  bcv_tfs_vtab *pTab = (bcv_tfs_vtab*)cur->pVtab;
  bcv_tfs_cursor *pCur = (bcv_tfs_cursor*)cur;
  int rc = SQLITE_OK;
  BcvContainer *pBcv = 0;
  BcvDispatch *pDisp = 0;

  bcvTfsResetCsr(pCur);
  rc = bcvfsGetBcv(pTab->db, &pDisp, &pBcv);
  if( pBcv ){
    BcvTfsList *pList = (BcvTfsList*)bcvMallocRc(&rc, sizeof(BcvTfsList));
    if( rc==SQLITE_OK ){
      pList->nRef = 1;
      rc = bcvDispatchList(pDisp, pBcv, 0, (void*)pList, bcvTfsListCb);
    }
    if( rc==SQLITE_OK ){
      rc = bcvDispatchRunAll(pDisp);
    }
    if( rc==SQLITE_OK ){
      rc = pList->rc;
    }
    if( rc==SQLITE_OK ){
      pCur->pList = pList;
      bcvTfsListUnref(pTab->pList);
      pTab->pList = pList;
      pList->nRef = 2;
    }else{
      assert( pList==0 || pList->nRef==1 );
      bcvTfsListUnref(pList);
    }
  }
  bcvfsReleaseBcv(pTab->db, pDisp, pBcv, rc!=SQLITE_OK);

  return rc;
}

static int bcvTfsVtabNext(sqlite3_vtab_cursor *cur){
  bcv_tfs_cursor *pCur = (bcv_tfs_cursor*)cur;
  pCur->iRow++;
  return SQLITE_OK;
}

static int bcvTfsVtabEof(sqlite3_vtab_cursor *cur){
  bcv_tfs_cursor *pCur = (bcv_tfs_cursor*)cur;
  return pCur->iRow>=pCur->pList->nFile;
}

static void bcvTfsFetchCb(
  void *pCtx, 
  int rc, char *zETag, 
  const u8 *aData, int nData,
  const u8 *aHdrs, int nHdrs
){
  sqlite3_context *ctx = (sqlite3_context*)pCtx;
  if( rc==SQLITE_OK ){
    sqlite3_result_blob(ctx, aData, nData, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_error(ctx, zETag, -1);
  }
}

static int bcvTfsVtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  bcv_tfs_vtab *pTab = (bcv_tfs_vtab*)cur->pVtab;
  bcv_tfs_cursor *pCur = (bcv_tfs_cursor*)cur;
  const char *zFile = pCur->pList->azFile[pCur->iRow];
  assert( i==0 || i==1 );
  if( i==0 ){
    sqlite3_result_text(ctx, zFile, -1, SQLITE_TRANSIENT);
  }else{
    int rc = SQLITE_OK;
    BcvDispatch *pDisp = 0;
    BcvContainer *pBcv = 0;

    rc = bcvfsGetBcv(pTab->db, &pDisp, &pBcv);
    if( rc==SQLITE_OK ){
      rc = bcvDispatchFetch(pDisp, pBcv, zFile, 0,0,(void*)ctx, bcvTfsFetchCb);
    }
    if( rc==SQLITE_OK ){
      rc = bcvDispatchRunAll(pDisp);
    }
    bcvfsReleaseBcv(pTab->db, pDisp, pBcv, rc!=SQLITE_OK);

    if( rc!=SQLITE_OK ){
      sqlite3_result_error(ctx, 0, 0);
    }
  }
  return SQLITE_OK;
}

static int bcvTfsVtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  bcv_tfs_cursor *pCur = (bcv_tfs_cursor*)cur;
  *pRowid = pCur->iRow;
  return SQLITE_OK;
}

typedef struct FileUpdateCtx FileUpdateCtx;
struct FileUpdateCtx {
  int rc;
};

static void bcvTfsUpdateCb(void *pCtx, int rc, char *zETag){
  FileUpdateCtx *pFu = (FileUpdateCtx*)pCtx;
  pFu->rc = rc;
}


/* 
** This function is the implementation of the xUpdate callback used by 
** bcv_tfs virtual tables. It is invoked by SQLite each time a row is 
** to be inserted, updated or deleted.
**
** A delete specifies a single argument - the rowid of the row to remove.
** 
** Update and insert operations pass:
**
**   1. The "old" rowid, or NULL.
**   2. The "new" rowid.
**   3. Value for the "name" column of the new row.
**   4. Value for the "content" column of the new row.
*/
static int bcvTfsVtabUpdate(
  sqlite3_vtab *tab,
  int nVal,
  sqlite3_value **apVal, 
  sqlite3_int64 *piRowid
){
  bcv_tfs_vtab *pTab = (bcv_tfs_vtab*)tab;
  int rc = SQLITE_OK;
  BcvContainer *pBcv = 0;
  BcvDispatch *pDisp = 0;

  rc = bcvfsGetBcv(pTab->db, &pDisp, &pBcv);
  if( pBcv ){
    FileUpdateCtx fu;
    memset(&fu, 0, sizeof(FileUpdateCtx));
    if( rc==SQLITE_OK && sqlite3_value_type(apVal[0])==SQLITE_INTEGER ){
      i64 iRow = sqlite3_value_int64(apVal[0]);

      bcvDispatchDelete(pDisp, pBcv, 
          pTab->pList->azFile[iRow], 0, &fu, bcvTfsUpdateCb
      );
      if( rc==SQLITE_OK ){
        rc = bcvDispatchRunAll(pDisp);
      }
      if( rc==SQLITE_OK ) rc = fu.rc;
    }
    if( rc==SQLITE_OK && nVal==4 ){
      const char *zName = (const char*)sqlite3_value_text(apVal[2]);
      const void *aData = sqlite3_value_blob(apVal[3]);
      int nData = sqlite3_value_bytes(apVal[3]);

      rc = bcvDispatchPut(
          pDisp, pBcv, zName, 0, aData, nData, (void*)&fu, bcvTfsUpdateCb
      );
    }
    if( rc==SQLITE_OK ){
      rc = bcvDispatchRunAll(pDisp);
    }
    if( rc==SQLITE_OK ){
      rc = fu.rc;
    }
    bcvfsReleaseBcv(pTab->db, pDisp, pBcv, rc!=SQLITE_OK);
  }

  return rc;
}

static int registerBcvTfs(sqlite3 *db){
  static sqlite3_module bcv_tfs = {
    /* iVersion    */ 2,
    /* xCreate     */ 0,
    /* xConnect    */ bcvTfsVtabConnect,
    /* xBestIndex  */ bcvTfsVtabBestIndex,
    /* xDisconnect */ bcvTfsVtabDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ bcvTfsVtabOpen,
    /* xClose      */ bcvTfsVtabClose,
    /* xFilter     */ bcvTfsVtabFilter,
    /* xNext       */ bcvTfsVtabNext,
    /* xEof        */ bcvTfsVtabEof,
    /* xColumn     */ bcvTfsVtabColumn,
    /* xRowid      */ bcvTfsVtabRowid,
    /* xUpdate     */ bcvTfsVtabUpdate,
    /* xBegin      */ 0,
    /* xSync       */ 0,
    /* xCommit     */ 0,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0
  };
  return sqlite3_create_module(db, "bcv_tfs", &bcv_tfs, 0);
}

/*
** sqlite3_bcvfs_register_vtab DB
*/
static int test_bcvfs_register_vtab(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  sqlite3 *db = 0;
  int rc = SQLITE_OK;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ){
    return TCL_ERROR;
  }
  rc = sqlite3_bcvfs_register_vtab(db);
  if( rc==SQLITE_OK ) rc = registerBcvTfs(db);
  return (rc==SQLITE_OK) ? TCL_OK : TCL_ERROR;
}

/*
** sqlite3_extended_errcode DB
*/
static int test_extended_errcode(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  sqlite3 *db = 0;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ){
    return TCL_ERROR;
  }
  Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_extended_errcode(db)));
  return TCL_OK;
}

/*
** Usage:    sqlite3_status  OPCODE  RESETFLAG
**
** Return a list of three elements which are the sqlite3_status() return
** code, the current value, and the high-water mark value.
*/
static int test_status(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc, iValue, mxValue;
  int i, op = 0, resetFlag;
  const char *zOpName;
  static const struct {
    const char *zName;
    int op;
  } aOp[] = {
    { "SQLITE_STATUS_MEMORY_USED",         SQLITE_STATUS_MEMORY_USED         },
    { "SQLITE_STATUS_MALLOC_SIZE",         SQLITE_STATUS_MALLOC_SIZE         },
    { "SQLITE_STATUS_PAGECACHE_USED",      SQLITE_STATUS_PAGECACHE_USED      },
    { "SQLITE_STATUS_PAGECACHE_OVERFLOW",  SQLITE_STATUS_PAGECACHE_OVERFLOW  },
    { "SQLITE_STATUS_PAGECACHE_SIZE",      SQLITE_STATUS_PAGECACHE_SIZE      },
    { "SQLITE_STATUS_SCRATCH_USED",        SQLITE_STATUS_SCRATCH_USED        },
    { "SQLITE_STATUS_SCRATCH_OVERFLOW",    SQLITE_STATUS_SCRATCH_OVERFLOW    },
    { "SQLITE_STATUS_SCRATCH_SIZE",        SQLITE_STATUS_SCRATCH_SIZE        },
    { "SQLITE_STATUS_PARSER_STACK",        SQLITE_STATUS_PARSER_STACK        },
    { "SQLITE_STATUS_MALLOC_COUNT",        SQLITE_STATUS_MALLOC_COUNT        },
  };
  Tcl_Obj *pResult;
  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "PARAMETER RESETFLAG");
    return TCL_ERROR;
  }
  zOpName = Tcl_GetString(objv[1]);
  for(i=0; i<sizeof(aOp)/sizeof(aOp[0]); i++){
    if( strcmp(aOp[i].zName, zOpName)==0 ){
      op = aOp[i].op;
      break;
    }
  }
  if( i>=sizeof(aOp)/sizeof(aOp[0]) ){
    if( Tcl_GetIntFromObj(interp, objv[1], &op) ) return TCL_ERROR;
  }
  if( Tcl_GetBooleanFromObj(interp, objv[2], &resetFlag) ) return TCL_ERROR;
  iValue = 0;
  mxValue = 0;
  rc = sqlite3_status(op, &iValue, &mxValue, resetFlag);
  pResult = Tcl_NewObj();
  Tcl_ListObjAppendElement(0, pResult, Tcl_NewIntObj(rc));
  Tcl_ListObjAppendElement(0, pResult, Tcl_NewIntObj(iValue));
  Tcl_ListObjAppendElement(0, pResult, Tcl_NewIntObj(mxValue));
  Tcl_SetObjResult(interp, pResult);
  return TCL_OK;
}

/*
** bcv_socket_fault_control IFAULT
**
**   The argument must be an integer. If it is non-negative, this command sets
**   the value of g.iSocketFault to the argument value. In any case the old,
**   possibly overwritten, value of g.iSocketFault is returned.
*/
static int test_bcv_socket_fault_control(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  return TCL_OK;
}

/*************************************************************************
** Start of fault-injection VFS shim
*/
static int bcvTestOpen(sqlite3_vfs*, const char*, sqlite3_file*, int , int *);
static int bcvTestDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int bcvTestAccess(sqlite3_vfs*, const char *zName, int, int *pResOut);
static int bcvTestFullPathname(sqlite3_vfs*, const char *zName, int, char*);
static void *bcvTestDlOpen(sqlite3_vfs*, const char *zFilename);
static void bcvTestDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
static void (*bcvTestDlSym(sqlite3_vfs*,void*, const char *zSymbol))(void);
static void bcvTestDlClose(sqlite3_vfs*, void*);
static int bcvTestRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int bcvTestSleep(sqlite3_vfs*, int microseconds);
static int bcvTestCurrentTime(sqlite3_vfs*, double*);
static int bcvTestGetLastError(sqlite3_vfs*, int, char*);
static int bcvTestCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);
#if 0
static int bcvTestSetSystemCall(sqlite3_vfs*, const char*, sqlite3_syscall_ptr);
static sqlite3_syscall_ptr bcvTestGetSystemCall(sqlite3_vfs*, const char*);
static const char *bcvTestNextSystemCall(sqlite3_vfs*, const char *zName);
#endif

typedef struct BcvTestFile BcvTestFile;
struct BcvTestFile {
  sqlite3_file base;
  sqlite3_file *pReal;
};

static int bcvTestClose(sqlite3_file *pFile){
  BcvTestFile *p = (BcvTestFile*)pFile;
  int rc = SQLITE_OK;
  if( p->pReal->pMethods ){
    rc = p->pReal->pMethods->xClose(p->pReal);
    p->pReal->pMethods = 0;
  }
  return rc;
}
static int bcvTestRead(
  sqlite3_file *pFile, 
  void *pBuf, 
  int iAmt, 
  sqlite3_int64 iOfst
){
  BcvTestFile *p = (BcvTestFile*)pFile;
  if( bcvIsIoerr() ){
    return SQLITE_IOERR_READ;
  }
  return p->pReal->pMethods->xRead(p->pReal, pBuf, iAmt, iOfst);
}
static int bcvTestWrite(
  sqlite3_file *pFile, 
  const void *pBuf, 
  int iAmt, 
  sqlite3_int64 iOfst
){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xWrite(p->pReal, pBuf, iAmt, iOfst);
}
static int bcvTestTruncate(sqlite3_file *pFile, sqlite3_int64 size){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xTruncate(p->pReal, size);
}
static int bcvTestSync(sqlite3_file *pFile, int flags){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xSync(p->pReal, flags);
}
static int bcvTestFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xFileSize(p->pReal, pSize);
}
static int bcvTestLock(sqlite3_file *pFile, int eLock){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xLock(p->pReal, eLock);
}
static int bcvTestUnlock(sqlite3_file *pFile, int eLock){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xUnlock(p->pReal, eLock);
}
static int bcvTestCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xCheckReservedLock(p->pReal, pResOut);
}
static int bcvTestFileControl(sqlite3_file *pFile, int op, void *pArg){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xFileControl(p->pReal, op, pArg);
}
static int bcvTestSectorSize(sqlite3_file *pFile){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xSectorSize(p->pReal);
}
static int bcvTestDeviceCharacteristics(sqlite3_file *pFile){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
}
static int bcvTestShmMap(
  sqlite3_file *pFile, 
  int iPg, 
  int pgsz, 
  int flags, 
  void volatile **pp
){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xShmMap(p->pReal, iPg, pgsz, flags, pp);
}
static int bcvTestShmLock(sqlite3_file *pFile, int offset, int n, int flags){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xShmLock(p->pReal, offset, n, flags);
}
static void bcvTestShmBarrier(sqlite3_file *pFile){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xShmBarrier(p->pReal);
}
static int bcvTestShmUnmap(sqlite3_file *pFile, int deleteFlag){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xShmUnmap(p->pReal, deleteFlag);
}
static int bcvTestFetch(
  sqlite3_file *pFile, 
  sqlite3_int64 iOfst, 
  int iAmt, 
  void **pp
){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xFetch(p->pReal, iOfst, iAmt, pp);
}
static int bcvTestUnfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *pPg){
  BcvTestFile *p = (BcvTestFile*)pFile;
  return p->pReal->pMethods->xUnfetch(p->pReal, iOfst, pPg);
}


/*
** VFS methods.
*/
static int bcvTestOpen(
  sqlite3_vfs *pVfs, 
  const char *zName, 
  sqlite3_file *pFile, 
  int flags, int *pFlags
){
  static sqlite3_io_methods bcv_test_methods = {
    3,                            /* iVersion */
    bcvTestClose,
    bcvTestRead,
    bcvTestWrite,
    bcvTestTruncate,
    bcvTestSync,
    bcvTestFileSize,
    bcvTestLock,
    bcvTestUnlock,
    bcvTestCheckReservedLock,
    bcvTestFileControl,
    bcvTestSectorSize,
    bcvTestDeviceCharacteristics,
    bcvTestShmMap,
    bcvTestShmLock,
    bcvTestShmBarrier,
    bcvTestShmUnmap,
    bcvTestFetch,
    bcvTestUnfetch
  };
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  BcvTestFile *p = (BcvTestFile*)pFile;
  int rc;

  memset(p, 0, sizeof(BcvTestFile));
  if( bcvIsIoerr() ){
    return SQLITE_IOERR;
  }
  p->pReal = (sqlite3_file*)&p[1];
  rc = pVfs2->xOpen(pVfs2, zName, p->pReal, flags, pFlags);
  if( rc==SQLITE_OK ){
    p->base.pMethods = &bcv_test_methods;
  }
  return rc;
}
static int bcvTestDelete(sqlite3_vfs *pVfs, const char *zName, int syncDir){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xDelete(pVfs2, zName, syncDir);
}
static int bcvTestAccess(
  sqlite3_vfs *pVfs, 
  const char *zName, 
  int flags, 
  int *pResOut
){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xAccess(pVfs2, zName, flags, pResOut);
}
static int bcvTestFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zName, 
  int nOut, 
  char *zOut
){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xFullPathname(pVfs2, zName, nOut, zOut);
}
static void *bcvTestDlOpen(sqlite3_vfs *pVfs, const char *zFilename){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xDlOpen(pVfs2, zFilename);
}
static void bcvTestDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xDlError(pVfs2, nByte, zErrMsg);
}
static void (*bcvTestDlSym(sqlite3_vfs *pVfs, void *p, const char *zSym))(void){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xDlSym(pVfs2, p, zSym);
}
static void bcvTestDlClose(sqlite3_vfs *pVfs, void *pSym){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  pVfs2->xDlClose(pVfs2, pSym);
}
static int bcvTestRandomness(sqlite3_vfs *pVfs, int nByte, char *zOut){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xRandomness(pVfs2, nByte, zOut);
}
static int bcvTestSleep(sqlite3_vfs *pVfs, int microseconds){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xSleep(pVfs2, microseconds);
}
static int bcvTestCurrentTime(sqlite3_vfs *pVfs, double *pdTime){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xCurrentTime(pVfs2, pdTime);
}
static int bcvTestGetLastError(sqlite3_vfs *pVfs, int a, char *b){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xGetLastError(pVfs2, a, b);
}
static int bcvTestCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *piTime){
  sqlite3_vfs *pVfs2 = (sqlite3_vfs*)pVfs->pAppData;
  return pVfs2->xCurrentTimeInt64(pVfs2, piTime);
}


/*
** bcv_install_vfs_wrapper
*/
static int test_bcv_install_vfs_wrapper(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  static sqlite3_vfs bcv_test_vfs = {
    3,                            /* iVersion */
    0,                            /* szOsFile */
    0,                            /* mxPathname */
    0,                            /* pNext */
    "bcvtest",                    /* zName */
    0,                            /* pAppData */
    bcvTestOpen,
    bcvTestDelete,
    bcvTestAccess,
    bcvTestFullPathname,
    bcvTestDlOpen,
    bcvTestDlError,
    bcvTestDlSym,
    bcvTestDlClose,
    bcvTestRandomness,
    bcvTestSleep,
    bcvTestCurrentTime,
    bcvTestGetLastError,
    bcvTestCurrentTimeInt64,
    0,
    0,
    0
  };

  if( bcv_test_vfs.pAppData==0 ){
    sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
    bcv_test_vfs.pAppData = (void*)pVfs;
    bcv_test_vfs.szOsFile = pVfs->szOsFile + sizeof(BcvTestFile);
    bcv_test_vfs.mxPathname = pVfs->mxPathname;
    sqlite3_vfs_register(&bcv_test_vfs, 1);
  }

  return TCL_OK;
}

int tclTestCurlConfig(CURL *pCurl, int eMethod, const char *zUri){
  int rc = SQLITE_OK;
  if( g.pScript ){
    Tcl_Obj *pEval = Tcl_DuplicateObj(g.pScript);
    Tcl_IncrRefCount(pEval);
    Tcl_ListObjAppendElement(g.interp, pEval, Tcl_NewStringObj("HANDLE", -1));
    Tcl_ListObjAppendElement(g.interp, pEval, Tcl_NewStringObj(
      eMethod==1 ? "GET" :
      eMethod==2 ? "PUT" :
      eMethod==3 ? "HEAD" : "DELETE"
      , -1
    ));
    Tcl_ListObjAppendElement(g.interp, pEval, Tcl_NewStringObj(zUri, -1));

    rc = Tcl_EvalObjEx(g.interp, pEval, 0);
    Tcl_DecrRefCount(pEval);
  }
  return rc;
}

/*
** bcv_curl_config_cb SCRIPT
*/
static int test_bcv_curl_config_cb(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const objv[]
){
  const char *zScript;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SCRIPT");
    return TCL_ERROR;
  }

  g.interp = 0;
  if( g.pScript ){
    Tcl_DecrRefCount(g.pScript);
    g.pScript = 0;
  }

  zScript = Tcl_GetString(objv[1]);
  if( zScript[0] ){
    g.interp = interp;
    g.pScript = Tcl_DuplicateObj(objv[1]);
    Tcl_IncrRefCount(g.pScript);
  }

  return TCL_OK;
}

const char *bcvtest_init(Tcl_Interp *interp){
  struct Command {
    const char *zName;
    Tcl_ObjCmdProc *xCmd;
  } aCmd[] = {
    { "breakpoint", test_breakpoint },
    { "sqlite3_bcv_sas_callback", test_bcv_sc },
    { "sqlite3_bcv_attach", test_bcv_attach },
    { "sqlite3_bcv_detach", test_bcv_detach },
    { "sqlite3_bcv_open", test_bcv_open },
    { "sqlite3_bcv_fcntl", test_bcv_fcntl },
    { "sqlite3_bcv_register", test_bcv_register },
    { "bcv_oom_control", test_bcv_oom_control },
    { "bcv_socket_fault_control", test_bcv_socket_fault_control },
    { "sqlite3_bcvfs_create", test_bcvfs_create },
    { "sqlite3_bcvfs_register_vtab", test_bcvfs_register_vtab },
    { "sqlite3_extended_errcode", test_extended_errcode },
    { "sqlite3_status", test_status },

    { "bcv_install_vfs_wrapper", test_bcv_install_vfs_wrapper },
    { "bcv_ioerr_control", test_bcv_ioerr_control },
    { "vfs_delete", test_vfs_delete },

    { "bcv_curl_config_cb", test_bcv_curl_config_cb },
  };
  int i;
  sqlite3_mem_methods mem;

  // sqlite3_config(SQLITE_CONFIG_LOG, log_to_stdout, (void*)0);
  (void)log_to_stdout;

  /* Install wrappers around malloc() and realloc(). */
  memset(&g, 0, sizeof(g));
  sqlite3_config(SQLITE_CONFIG_GETMALLOC, &g.mem);
  memcpy(&mem, &g.mem, sizeof(mem));
  mem.xMalloc = bcvTestMalloc;
  mem.xRealloc = bcvTestRealloc;
  sqlite3_config(SQLITE_CONFIG_MALLOC, &mem);
  sqlite3_test_control(SQLITE_TESTCTRL_BENIGN_MALLOC_HOOKS,
      bcvTestBenignStart, bcvTestBenignEnd
  );

  /* Install wrappers around send() and recv() */
  sqlite3_bcv_test_socket_api(bcvTestRecv, bcvTestSend, &g.xRecv, &g.xSend); 

  /* Install Tcl commands */
  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xCmd, 0, 0);
  }

  return 0;
}

