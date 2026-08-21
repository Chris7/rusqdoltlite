# Upgrade model

The files in `doltlite/` are pristine upstream artifacts. RusqDoltLite changes
belong in [`patches/`](patches/README.md) and are applied to a build-directory
copy of the amalgamation. This separation is intentional: `upgrade_git.sh` may
replace the upstream files without erasing or hiding local behavior.

Use `upgrade_git.sh` to clone the exact upstream Git release ref configured by
`DOLTLITE_GIT_REF` in that script, build its amalgamation, vendor the matching
remote sources, regenerate bindings, check the local patch series, and run the
same bundled test suites.

Some releases, including `v0.11.52`, keep `doltlite_remotesrv.c` in the native
library but omit it from the published amalgamation. Both upgraders vendor that
pristine sidecar. For Cargo builds with the `remote` feature, `build.rs` adds an
amalgamation-compatible copy only under `OUT_DIR` and compiles the matching
server-side TLS supplement; checked-in upstream sources remain unchanged.

# Checks
* new [error code(s)](https://sqlite.org/rescode.html)
  => Update [libsqlite3-sys/src/error.rs](https://github.com/rusqlite/rusqlite/blob/006c8b77e7d235a3072237f006ebabd66b937911/libsqlite3-sys/src/error.rs#L127)
     And [code_to_str](https://github.com/rusqlite/rusqlite/blob/006c8b77e7d235a3072237f006ebabd66b937911/libsqlite3-sys/src/error.rs#L195)
* new [SQLITE_OPEN_*](https://www.sqlite.org/c3ref/c_open_autoproxy.html)
 => Update [struct OpenFlags](https://github.com/rusqlite/rusqlite/blob/19d08871799500d64336f413dc329cc964149f10/src/lib.rs#L999)
* new [SQLITE_LIMIT_*](https://sqlite.org/c3ref/c_limit_attached.html)
 => Update [enum Limit](https://github.com/rusqlite/rusqlite/blob/66ace52c4a24a811b405ffd9e9010163352a6186/libsqlite3-sys/src/lib.rs#L27)
* new [SQLITE_DBCONFIG_*](https://sqlite.org/c3ref/c_dbconfig_defensive.html)
 => Update [enum DbConfig](https://github.com/rusqlite/rusqlite/blob/7056e656ac92330a3d78f5ac456dea1e56f6bfee/src/config.rs#L15)
* new [Authorizer Action Codes](https://sqlite.org/c3ref/c_alter_table.html)
 => Update [enum AuthAction](https://github.com/rusqlite/rusqlite/blob/2ddbebad9763ab8054e55ef509672b7537ba7cf5/src/hooks.rs#L63)
* new [SQLITE_STMTSTATUS_*](https://www.sqlite.org/c3ref/c_stmtstatus_counter.html)
 => Update [enum StatementStatus](https://github.com/rusqlite/rusqlite/blob/ce90b519bb9946bf1cbab77479bb92d0fbc467c0/src/statement.rs#L937)
* new [SQLITE_INDEX_CONSTRAINT_*](https://sqlite.org/c3ref/c_index_constraint_eq.html)
 => Update [enum IndexConstraintOp](https://github.com/rusqlite/rusqlite/blob/5d42ba7c29a35dbb8eeb047e84ae0739cb152754/src/vtab/mod.rs#L267)
* new [function flag(s)](https://sqlite.org/c3ref/c_deterministic.html)
 => Update [struct FunctionFlags](https://github.com/rusqlite/rusqlite/blob/0312937d6a75b45d7e603fa8c6b083bc7774270b/src/functions.rs#L317)
