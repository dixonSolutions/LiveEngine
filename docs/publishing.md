# Publishing

npm package: `@dixonsolutions/liveengine`. Cargo crate/binary: `liveengine` / `liveengine-hub`. Both manifests use the same version. The release workflow verifies that a `v<version>` tag matches both manifests.

## Current registry state

- `0.2.2` is published to npm and crates.io; `0.2.1` was the trusted-publishing release and `0.2.0` the bootstrap release.
- npm and crates.io trusted publishers are configured for `dixonSolutions/LiveEngine` and `.github/workflows/release.yml`. The npm connection is validated and releases include provenance.
- Tag releases publish future versions through OIDC. The workflow checks both registries first and skips a version that is already present, so a partially completed release can be retried.

The instructions below document how the trust relationships were established and how to rebuild them if the registries ever lose the configuration.

## npm trusted publishing

The GitHub workflow is `.github/workflows/release.yml`. Configure its trusted publisher on npm with:

- Provider: GitHub Actions
- Owner: `dixonSolutions`
- Repository: `LiveEngine`
- Workflow filename: `release.yml`
- Environment: leave empty (the workflow does not select an environment)
- Permission: publish

npm's configuration is registry-side. Committing `id-token: write` alone does not create a trust relationship. A package must already exist before `npm trust` can configure it; the first publication uses an interactive maintainer login. Never put a passkey or an npm session token into GitHub secrets.

After the package exists, use a current npm CLI (11.15+ for `npm trust`):

```sh
npm login --auth-type=web
npm trust github @dixonsolutions/liveengine \
  --repo=dixonSolutions/LiveEngine --file=release.yml --allow-publish
npm trust list @dixonsolutions/liveengine
```

The browser flow may require touching the account's passkey. The workflow installs the latest npm, requests GitHub OIDC, and publishes with provenance. Newly created npm trust configurations require their first successful OIDC publication within two days; configure trust when ready to release.

References: [npm trust](https://docs.npmjs.com/cli/v11/commands/npm-trust/) and [trusted publishing](https://docs.npmjs.com/trusted-publishers/).

## Cargo trusted publishing

Configure the crate's crates.io publisher for the same owner/repository and `release.yml`. The Cargo job uses [`rust-lang/crates-io-auth-action`](https://github.com/rust-lang/crates-io-auth-action) to obtain a short-lived token. A new crate requires an initial authenticated publication before its publisher can be configured.

```sh
cargo login
cargo publish --locked
```

Keep local credentials out of the repository. If the crate name is already owned by another publisher, choose an available name and update Cargo instructions before publishing.

## Release verification

```sh
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
npm ci
npm test
npm run build:hub
npm pack --dry-run
cargo package --locked
```

Create and push the matching tag after the registry-side publishers are ready. Linux, macOS and Windows builders produce the addon and host binaries. The npm release contains all supported binaries, so installation uses the local prebuilt artifact. Platforms outside the build matrix compile from the included Rust source.

A workflow dispatch builds downloadable package artifacts by default. Enable its `publish` input to publish the current manifest version; do not enable publication for an already published version. Registry publication is immutable. The release workflow does not automatically overwrite or unpublish any package.
