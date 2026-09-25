# Local DoltLite patches

Files under `../doltlite/` are vendored, unmodified upstream artifacts. Do not
make rusqdoltlite changes there: `upgrade_git.sh` replaces them on every
upstream refresh.

The bundled build copies `doltlite.c` into Cargo's `OUT_DIR`, applies the
numbered standard Git patches with `git apply`, and compiles that generated
copy. This keeps the upstream source replaceable while making local behavior
explicit and reviewable. Git is therefore required when compiling the bundled
DoltLite source.

To update DoltLite:

1. Set `DOLTLITE_GIT_REF` in `../upgrade_git.sh` to the upstream release tag.
2. Run `../upgrade_git.sh`. It builds the pristine amalgamation from a fresh
   clone of that exact ref, vendors matching remote sidecars, regenerates
   bindings, and runs the validation workflow.
3. If a patch no longer applies, check whether upstream incorporated that
   behavior. Remove the obsolete hunk or refresh only that patch; never edit
   `../doltlite/doltlite.c`.
4. Commit the upstream artifact update separately from any patch adjustment
   when possible.

`git apply` deliberately fails when the standard patch context no longer
matches. A failed build is an upgrade-review signal, not permission to silently
skip a local change.

Each patch owns one independently removable behavior. The numbered core
patches apply to `doltlite.c`; remote-server sidecar patches apply to the
staged `doltlite_remotesrv.c` and `doltlite_remotesrv.h` files before the
server is appended to the amalgamation. Currently, `0001` and `0002` add
`dolt_remote('set-url', ...)`, `0003` adds the CBS upload lock hooks, and
`0004-remote-server-vfs.patch` lets each native remote server select an
optional named SQLite VFS while preserving the process default when omitted.

Keep new behavior isolated the same way so an upstreamed fix can be removed
without rebasing unrelated changes.

`0003-support-blockcachevfs-upload-lock.patch` exports the DoltLite graph-lock
hooks used by the CBS upload integration. The upload-side patch holds that
lock across checkpointing and manifest installation so an upload observes a
stable block set; a database without a DoltLite store reports `SQLITE_NOTFOUND`
and retains CBS's stock behavior.

The blockcache VFS patch series is applied separately to the staged CBS
sources by `libdoltlite-sys/build.rs`, after the vendored VFS files have been
copied to `OUT_DIR`. `0013-bounded-staged-cache.patch` gives
`cachefile.bcv` a block-aligned hard payload-slot limit and reuses those slots
instead of growing the file or allocating one local payload per evicted block.
On restart, persisted clean-slot mappings are discarded before the cache file
is reconciled: a clean-only tail may be truncated for a smaller limit, while
a retained dirty or unpublished high-water slot makes that reduction fail
rather than silently discarding local data. There is no general online cache
compaction.
At the cache watermark, complete immutable block objects may be PUT to the
provider and recorded in staged metadata, but those objects remain
unpublished until the existing explicit `sqlite3_bcvfs_upload` operation
successfully installs a conditional manifest. Database `xSync` forwards to the
cachefile's `xSync` and syncs local payload bytes only; it neither stages data
nor publishes a manifest. Dirty bytes written before `xSync` are outside the
committed local-durability guarantee if power is lost. The patch also
invalidates persisted clean-cache slot mappings at startup, forcing published
blocks to be fetched again rather than treating a clean slot as durable.

The patch records the logical container/database/block generation and object
ID, retains staged mappings across slot reuse and restart, and keeps them for
retry when a block PUT fails or manifest publication is lost to a CAS conflict
or an ambiguous response. Rewrites and truncates tombstone superseded staged
generations in `staged_gc`; their remote object IDs remain tracked for delayed
garbage collection instead of being deleted immediately. The next eligible
manifest publication adds those IDs to the manifest's existing delayed-GC
list, while a failed publication leaves the tombstones available for retry. A
pending-publication marker and sequence counter allow startup to reconcile a
remote manifest with local staged generations. The publication path only
attempts cleanup after a successful conditional
manifest operation, while retained mappings remain available if cleanup is
interrupted. Metadata initialization uses `synchronous=FULL` for this bounded
path because `synchronous=OFF` cannot provide the ordering needed before a
slot is reused. The implementation's crash and power-loss behavior is still
defined by the corresponding fault-injection tests; this patch does not claim
secure erasure or a bound on
all local disk bytes. Staging metadata and retryable remote objects can grow
independently of the bounded block-payload file.
