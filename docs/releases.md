# CI and Releases

GraphArrow uses the version in `pyproject.toml` as the application version.
Use `grapharrow.__version__` in Python to read the installed version. REST
reports the same value in OpenAPI. ANA protocol and on-disk catalog/snapshot
versions are separate contracts.

## Continuous integration

The CI workflow runs on pull requests, pushes to `main`, and manual dispatch.
The separate coverage workflow runs the local test suite on Python 3.12 for
every branch push and uploads the report to Codecov using `CODECOV_TOKEN`.
The README displays the Codecov result for `main`.

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

Run `make version` to show the current project version. The release tooling
requires Python 3.11 or newer and a version of uv with the `uv version` command.
Prepare the bump on the branch you will merge into `main`.

| Command | Example result |
| --- | --- |
| `make version BUMP=major` | `0.1.0` → `1.0.0` |
| `make version BUMP=minor` | `0.1.0` → `0.2.0` |
| `make version BUMP=patch` | `0.1.0` → `0.1.1` |

The bump updates `pyproject.toml` and `uv.lock` together. Review and commit those files, then
merge the commit into `main`. After CI passes on the release commit, check out
`main` and bring it up to date before creating the annotated tag.

```sh
git switch main
git pull --ff-only
make version BUMP=tag
```

The command checks that both metadata files are clean and agree on the
version. It prints the explicit push command for the tag it created.

**The repository owner starts a release by pushing the `v*` tag.** The
repository's GitHub tag ruleset restricts creation of those tags to the
owner. The release workflow also checks the owner's GitHub identity before
validating the release. A normal push to `main` or a pull request runs CI
without publishing artifacts.

The release workflow requires the tagged commit to belong to `main`, and the
tag must exactly match the project and lockfile versions. Manual releases
use `X.Y.Z`, with a leading `v` in Git tags.

## Nightly builds

The Nightly is scheduled once a week, on Mondays at 23:55 in `Europe/Rome`.
The release workflow checks the selected commit on `main` and skips
the build if a published stable or Nightly release already contains it.
Otherwise it runs the full release checks and publishes the tested artifacts
as a GitHub prerelease.

A Nightly build uses a tag such as `nightly-<full-commit-sha>` and a
development version such as `0.1.1.dev42`. The version uses the pending
version in `pyproject.toml` if it is ahead of the latest published stable
version, or the next patch of the latest stable version. The suffix is the
release workflow run number. The build checkout updates the package metadata
and lockfile for the wheel and source archive. The commit on `main` retains
its original version. GHCR publishes the versioned development image and its
architecture tags. The stable release and Docker `latest` alias are unaffected.

The workflow creates a Nightly tag only after validation succeeds. If
publication fails after creating the tag, inspect the tag, any draft and
the versioned GHCR images before retrying. A partial publication is never
silently replaced.

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
