# CI and Releases

Stable releases derive their version from the Git tag. Builds directly from a
development checkout use the version in `pyproject.toml`. Use
`grapharrow.__version__` in Python to read the installed version. REST reports
the same value in OpenAPI. ANA protocol and on-disk catalog/snapshot versions
are separate contracts.

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
| Docker build, smoke tests and vulnerability scan | Linux amd64 | Linux amd64 and arm64 |
| Helm profile lint and packaged-chart integration | Native on a PVC and Delta on Azurite | Native on a PVC and Delta on Azurite |

Real-cloud tests and benchmarks are excluded from ordinary CI. Local integration
checks include disk storage, simulated S3, interface comparisons, authentication,
session isolation and transaction behavior. Reference services are disposable;
the reference server and Neo4j driver are test dependencies only.

The `CI required` job succeeds only when every required job succeeds. It provides
a stable check name for the maintainer's branch rules. Those rules are configured
in GitHub separately from the workflow files. A release reuses the same CI
workflow with the complete matrix, including both container architectures.

The wheel is built from the source distribution, checked for private-file
exclusions and installed outside the checkout. The Python 3.15 runtime image
installs that wheel and hash-locked runtime requirements. Docker smoke checks
cover the interpreter, authenticated REST, two isolated ANA sessions using
the short handle `h1`, Bolt authentication and queries, and persistence after
container restart with a read-only root filesystem. Trivy scans system
packages and Python dependencies in the final images. CI retains the reports
and fails if a high or critical finding has a fix available. Each report bundle
includes an SPDX inventory and the BuildKit provenance record.

CI installs the packaged Helm chart on kind using the tested runtime image.
It runs the chart's `helm test` hook and checks Native persistence, read-only
Native replicas on shared object storage, authenticated per-pod metrics,
and Delta replica behavior. The Delta check also exercises the
[TLS edge examples](kubernetes.md#https-for-rest-and-ana), including ANA
session affinity across proxy replicas and certificate verification for
REST and Bolt. It changes the StatefulSet scale subresource in autoscaling
mode and checks that the gateway serves the new pods and preserves existing
ANA sessions.

To refresh the pinned base, resolve the current multi-platform digest for
`python:3.15-slim-trixie` with `docker buildx imagetools inspect`, then update
the digest in the [Dockerfile](../Dockerfile) and rerun CI. The Dockerfile
applies Debian package updates before scanning. Update runtime Python
dependencies through `uv.lock` and rerun CI.

## Preparing a release

Run `make version` to show the checkout version. After CI passes on the commit
you want to release, update your local `main`.

| Command | Example result |
| --- | --- |
| `make version BUMP=major` | `0.1.0` → `1.0.0` |
| `make version BUMP=minor` | `0.1.0` → `0.2.0` |
| `make version BUMP=patch` | `0.1.0` → `0.1.1` |
| `make version BUMP=tag VERSION=0.2.0` | Explicit tag `v0.2.0` |

The release commands fetch `main` and tags from `origin` and require local
`main` to match the fetched revision. The bump commands calculate the next
version from the highest reachable stable tag. Each command creates an
annotated tag and pushes it to `origin`, starting the release workflow.

Add `DRY_RUN=1` to any release command to run the same checks and preview the
tag and commit. The preview fetches from `origin` and stops before creating
or pushing a tag.

```sh
git switch main
git pull --ff-only origin main
make version BUMP=minor DRY_RUN=1
make version BUMP=minor
```

If the push fails, the command keeps the local tag and prints how to retry
the push for that tag.

**The repository owner starts a release by pushing the `v*` tag.** The
repository's GitHub tag ruleset restricts creation of those tags to the
owner. The release workflow also checks the owner's GitHub identity before
validating the release. A normal push to `main` or a pull request runs CI
without publishing artifacts.

The release workflow requires the tagged commit to belong to `main` and the
tag to use the form `vX.Y.Z`. It applies that version to `pyproject.toml` and
`uv.lock` in the build checkout. The wheel, source archive and container
use that version. The release requires no version-bump commit.

## Nightly builds

The Nightly is scheduled once a week, on Mondays at 23:55 in `Europe/Rome`.
The release workflow checks the selected commit on `main` and skips
the build if a published stable or Nightly release already contains it.
Otherwise it runs the full release checks and publishes the tested artifacts
as a GitHub prerelease.

A Nightly build uses a tag such as `nightly-<full-commit-sha>` and a
development version such as `0.1.1.dev42`. The version uses the checkout
version in `pyproject.toml` if it is ahead of the latest published stable
version, or the next patch of the latest stable version. The suffix is the
release workflow run number. The build checkout updates the package metadata
and lockfile for the wheel and source archive. The commit on `main` retains
its original version. GHCR publishes a versioned development image and its
architecture tags. The Docker `latest` tag tracks the latest stable release.

The workflow creates a Nightly tag only after validation succeeds. If
publication fails after creating the tag, inspect the tag, any draft and
the versioned GHCR images before retrying. A partial publication is never
silently replaced.

## Published artifacts

For a release version `X.Y.Z`, the workflow creates a GitHub Release containing:

- `grapharrow-X.Y.Z-py3-none-any.whl`;
- `grapharrow-X.Y.Z.tar.gz`;
- `runtime-requirements.txt`, the exported runtime lock used by Docker, including the `cloud` and `delta` extras;
- `SHA256SUMS`, covering those artifacts;
- a Helm chart package and `SHA256SUMS-helm`;
- SPDX SBOMs, BuildKit provenance records and vulnerability reports for both architectures.

Install the wheel downloaded from the release with:

```sh
python -m pip install ./grapharrow-0.1.0-py3-none-any.whl
```

The wheel's dependencies use the requirement ranges in its metadata; installing
it does not automatically enforce the repository lockfile.

The corresponding container image uses the release version as its tag.

```text
ghcr.io/paoloalbano/grapharrow:X.Y.Z
```

The image selects `linux/amd64` or `linux/arm64` automatically. Append `-amd64` or
`-arm64` to select an architecture explicitly. The newest stable release
updates `latest`. The chart has its own version in `Chart.yaml` and is
packaged with its application version set to `X.Y.Z`. See
[Deploying GraphArrow on Kubernetes](kubernetes.md).
See [Server Startup and Authentication](server-configuration.md#docker) for
credentials, ports, persistent data and running Bolt from the image.

PyPI publication is not configured. GitHub Release attachments and GHCR are the
initial distribution channels. GHCR packages are initially private by default;
the owner must configure package visibility separately so public users can pull
the image without authentication. See the
[GitHub Container registry guide](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

## Publication integrity and failures

The release job downloads the tested wheel/source artifacts, Helm package,
image reports and saved container images from the same workflow run. It
verifies package checksums, package/tag identity, remote tag identity and
the identity of each image, including its architecture and runtime user.
It does not rebuild the package or the images during publication.

Publishing uses the workflow's `GITHUB_TOKEN`, with write permissions restricted
to the publication job. No personal access token or PyPI credential is required.

Release workflows are serialized to keep publication and the `latest` alias
consistent across tags. If the registry reports `unknown blob` during an
image push, the job retries the same tested image up to twice. Versioned
release assets and image tags are not
automatically replaced. A
failure after creating the release leaves a GitHub draft; it is only published
after both architecture tags and the combined image are available. GitHub and
GHCR publication are not a single atomic transaction: inspect any partial draft
or container uploads before attempting recovery. The workflow refuses existing
release versions or image tags rather than silently overwriting them. Use a new
version for changed artifacts, and never move a published tag.

This process validates distribution and the implemented subset. It does not
establish complete openCypher conformance; review the
[known Cypher issues](known-cypher-issues.md) when evaluating a release.
