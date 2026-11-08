# CI and Releases

GraphArrow uses the version in `pyproject.toml` as the application version.
The initial version is `0.1.0`; the REST/OpenAPI version is read from installed
package metadata. ANA protocol and on-disk catalog/snapshot versions are
separate contracts.

## Continuous integration

The CI workflow runs on pull requests, pushes to `main`, and manual dispatch.
Feature-branch pushes do not start a second run in addition to their pull request.

| Check | Pull requests | Main and release tags |
| --- | --- | --- |
| Lint, formatting and typechecking | Separate jobs, once each | Separate jobs, once each |
| Local unit and integration tests | Python 3.11 and 3.15 | Python 3.11, 3.12, 3.13, 3.14 and 3.15 |
| Wheel/source build | Once | Once |
| Installed-wheel smoke | Python 3.11 and 3.15 | All five Python versions |
| ANA and Cypher reference comparisons | One Python 3.12 job | One Python 3.12 job |
| Docker build and smoke | Linux amd64 | Linux amd64 and arm64 |

Real-cloud tests and benchmarks are excluded from ordinary CI. Local integration
checks include disk storage, simulated S3, interface comparisons, authentication,
session isolation and transaction behavior. Reference services are disposable;
the reference server and Neo4j driver are test dependencies only.

The `CI required` job succeeds only when every required job succeeds. It provides
a stable check name for the maintainer's branch rules. Those rules are configured
in GitHub separately from the workflow files. A release reuses the same CI
workflow with the complete matrix, including both container architectures.

The wheel is built from the source distribution, checked for private-file
exclusions and installed outside the checkout. Each runtime image installs that
wheel and hash-locked runtime requirements, without development dependencies.
Docker smoke checks cover authenticated REST, two isolated ANA sessions sharing
the short handle `h1`, Bolt authentication/query execution and persistence after
container restart. The runtime interpreter is Python 3.12 for both architectures.

## Preparing a release

1. Update `project.version` in `pyproject.toml` when preparing a new version.
   The first release already uses `0.1.0`.
2. Run `uv lock`, review the changes, and merge the release commit into `main`.
3. Check that CI succeeds for that commit.
4. Create and push the matching tag. For the first release:

   ```sh
   git tag -a v0.1.0 -m "GraphArrow 0.1.0"
   git push origin v0.1.0
   ```

**Pushing a `v*` tag starts publication** after all release checks pass. A normal
push to `main` or a pull request does not publish anything. Only create a tag
when the commit is intended for release.

The release workflow requires the tagged commit to belong to `main`, and the
tag must exactly match the project and lockfile versions. Supported version
forms are `X.Y.Z`, `X.Y.ZaN`, `X.Y.ZbN` and `X.Y.ZrcN`, with a leading `v` only
in Git tags. Prereleases are marked as prereleases on GitHub and do not update
the Docker `latest` alias.

## Published artifacts

For `v0.1.0`, the workflow creates a GitHub Release containing:

- `grapharrow-0.1.0-py3-none-any.whl`;
- `grapharrow-0.1.0.tar.gz`;
- `runtime-requirements.txt`, the exported runtime lock used by Docker;
- `SHA256SUMS`, covering those artifacts.

Install the wheel downloaded from the release with:

```sh
python -m pip install ./grapharrow-0.1.0-py3-none-any.whl
```

The wheel's dependencies use the requirement ranges in its metadata; installing
it does not automatically enforce the repository lockfile.

The corresponding container image is:

```text
ghcr.io/paoloalbano/grapharrow:0.1.0
```

It selects `linux/amd64` or `linux/arm64` automatically. Architecture-specific
tags are `0.1.0-amd64` and `0.1.0-arm64`; the newest stable release also updates `latest`.
See [Server Startup and Authentication](server-configuration.md#docker) for
credentials, ports, persistent data and running Bolt from the image.

PyPI publication is not configured. GitHub Release attachments and GHCR are the
initial distribution channels. GHCR packages are initially private by default;
the owner must configure package visibility separately so public users can pull
the image without authentication. See the
[GitHub Container registry guide](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

## Publication integrity and failures

The release job downloads the tested wheel/source artifacts and saved container
images from the same workflow run. It verifies artifact checksums, package/tag
identity, remote tag identity and image version/revision/architecture labels.
It does not rebuild the package or the images during publication.

Publishing uses the workflow's `GITHUB_TOKEN`, with write permissions restricted
to the publication job. No personal access token or PyPI credential is required.

Release workflows are serialized to keep publication and the `latest` alias
consistent across tags. Versioned release assets and image tags are not
automatically replaced. A
failure after creating the release leaves a GitHub draft; it is only published
after both architecture tags and their combined image are available. GitHub and
GHCR publication are not a single atomic transaction: inspect any partial draft
or container uploads before attempting recovery. The workflow refuses existing
release versions or image tags rather than silently overwriting them. Use a new
version for changed artifacts, and never move a published tag.

This process validates distribution and the implemented subset. It does not
establish complete openCypher conformance; review the
[known Cypher issues](known-cypher-issues.md) when evaluating a release.
