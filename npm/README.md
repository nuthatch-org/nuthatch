# nuthatch-indexer

Runs [nuthatch](https://github.com/nuthatch-org/nuthatch), the self-hosted EVM indexer, from npm:

```sh
npx -y nuthatch-indexer init 0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48 --alias usdc
npx -y nuthatch-indexer dev --backfill 300
claude mcp add nuthatch -- npx -y nuthatch-indexer mcp --url http://127.0.0.1:8288
```

The package holds no binary and runs nothing when it is installed. On first run it downloads the
release tarball for this package's version and platform from the project's GitHub release, checks
its SHA-256 against the release's `.sha256` file and against the digests bundled into the package
when it was published, and caches the binary. A mismatch is refused, and nothing is cached. Later
runs start the cached binary directly. Every line it prints goes to stderr, so `nuthatch mcp` keeps
stdout to itself.

Releases are built for macOS (Apple Silicon and Intel) and Linux (x86_64 glibc and musl, aarch64
glibc). Elsewhere, build from source.

- `NUTHATCH_NPM_CACHE`: where binaries are cached. Defaults to `~/Library/Caches/nuthatch/npm` on
  macOS and `$XDG_CACHE_HOME/nuthatch/npm` (or `~/.cache/nuthatch/npm`) on Linux.
- `NUTHATCH_NPM_RELEASE_URL`: a mirror of `https://github.com/nuthatch-org/nuthatch/releases/download`.

To check a binary's provenance as well as its checksum, see
[docs/install.md](https://github.com/nuthatch-org/nuthatch/blob/main/docs/install.md).

## Publishing

The package version is the release version: `scripts/version-check.sh` and a unit test hold
`package.json` and `server.json` to `Cargo.toml`, so a release PR bumps all three. On a `v*` tag,
`release.yml`'s `npm` job writes `checksums.json` from the release's own `.sha256` files and runs
`npm publish --provenance`, once the repository variable `NPM_PUBLISH` is `true`.

The first publish is by hand, because npm needs the package to exist before it can trust a
workflow to publish it:

1. On npmjs.com, signed in as a maintainer with two-factor authentication on, check that the name
   `nuthatch-indexer` is still free.
2. From a checkout of the release tag, after the release is published:

   ```sh
   cd npm
   gh release download v<version> --repo nuthatch-org/nuthatch --pattern 'nuthatch-*.tar.gz.sha256' -D sums
   node scripts/checksums.js sums > checksums.json
   npm publish --access public
   ```

3. On the package's settings page on npmjs.com, add a trusted publisher: GitHub Actions,
   organisation `nuthatch-org`, repository `nuthatch`, workflow `release.yml`.
4. In the repository settings, add the Actions variable `NPM_PUBLISH` with the value `true`.
5. Run the `mcp registry` workflow, which can now verify the npm package `server.json` names.

From then on a tag publishes it. Until step 4 the `npm` job is skipped and a release is unaffected.
