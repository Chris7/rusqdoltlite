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

Each patch owns one independently removable behavior. Currently,
`0001-support-remote-set-url.patch` adds `dolt_remote('set-url', ...)`.

Keep new behavior isolated the same way so an upstreamed fix can be removed
without rebasing unrelated changes.
