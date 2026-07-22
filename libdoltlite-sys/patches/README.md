# Local DoltLite patches

Files under `../doltlite/` are vendored, unmodified upstream artifacts. Do not
make rusqdoltlite changes there: `upgrade.sh` replaces them on every upstream
refresh.

The bundled build copies `doltlite.c` into Cargo's `OUT_DIR`, applies the
numbered standard Git patches with `git apply`, and compiles that generated
copy. This keeps the upstream source replaceable while making local behavior
explicit and reviewable. Git is therefore required when compiling the bundled
DoltLite source.

To update DoltLite:

1. Change `DOLTLITE_VERSION` in `../upgrade.sh`.
2. Run `../upgrade.sh`. It downloads pristine release artifacts and matching
   remote sidecars.
   To build from Git instead, run `../upgrade_git.sh`; it builds the pristine
   amalgamation from a fresh clone of the `DOLTLITE_GIT_REF` configured in that
   script and then performs the same vendoring and validation workflow. The ref
   may be a branch or tag.
3. If a patch no longer applies, check whether upstream incorporated that
   behavior. Remove the obsolete hunk or refresh only that patch; never edit
   `../doltlite/doltlite.c`.
4. Commit the upstream artifact update separately from any patch adjustment
   when possible.

`git apply` deliberately fails when the standard patch context no longer
matches. A failed build is an upgrade-review signal, not permission to silently
skip a local change.

`0001-rusqdoltlite-behavior-fixes.patch` contains the DoltLite-internal parts
of point-in-time table discovery and remote correctness. Its header maps the
individual behaviors to the patch so an upstreamed fix can be removed without
reconstructing the change from the amalgamation.
