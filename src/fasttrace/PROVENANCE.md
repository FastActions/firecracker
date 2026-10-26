# Provenance

Vendored copy of the `fasttrace` crate from the private Blacksmith monorepo.

- Source repository: `FastActions/fa`
- Source path: `agent/fasttrace`
- Source commit: `be66bdfcf8c600bdf875f7015b7e6d5dcb937aeb` (branch
  `devin/1792721429-fasttrace-perfetto-spans`, PR #6822)

The crate is copied verbatim apart from build plumbing that does not apply to
this workspace: `BUILD.bazel`, `Cargo.lock`, `.gitignore`, the Criterion bench
(`benches/`, the `criterion` dev-dependency and `[[bench]]` table) and the
crate-local `[profile.release]` override are dropped. On top of that commit it
carries the multi-dump merge (`fasttrace-dump a.ftrc b.ftrc -o out.json`,
`fasttrace::write_chrome_json_merged`) from `FastActions/fa` PR #6829. The
sources are formatted with this repository's `rustfmt.toml`. The crate is a path
dependency of `vmm` behind the off-by-default `fasttrace` feature; a cargo `git`
dependency is not used because the dev container has no credentials for the
private repository.

To refresh, copy `agent/fasttrace` from `FastActions/fa` over this directory,
reapply the removals above and update the commit hash here.
