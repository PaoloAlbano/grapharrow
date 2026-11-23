# Release builds consume the verified wheel; local builds create it from source.
ARG PYTHON_BASE=python:3.15-slim-trixie@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928
ARG DISTRIBUTION_SOURCE=wheel-builder

FROM ${PYTHON_BASE} AS wheel-builder
ARG STORAGE_EXTRA="cloud delta"
WORKDIR /build
RUN pip install --no-cache-dir uv==0.8.12
COPY pyproject.toml uv.lock README.md LICENSE THIRD_PARTY_NOTICES.md ./
COPY src ./src
RUN uv build --wheel --no-sources --out-dir /dist \
    && set -- \
    && for extra in $STORAGE_EXTRA; do set -- "$@" --extra "$extra"; done \
    && uv export --locked --no-dev --no-emit-project "$@" \
       --output-file /dist/runtime-requirements.txt

FROM scratch AS prebuilt
COPY dist/*.whl dist/runtime-requirements.txt /dist/

FROM ${DISTRIBUTION_SOURCE} AS distribution

FROM ${PYTHON_BASE} AS dependencies
RUN --mount=type=bind,from=distribution,source=/dist,target=/dist \
    python -m venv --without-pip /app/.venv \
    && python -m pip --python /app/.venv install --no-cache-dir --only-binary=:all: \
       --require-hashes -r /dist/runtime-requirements.txt \
    && python -m pip --python /app/.venv install --no-cache-dir --no-deps /dist/*.whl \
    && install -d -m 0755 -o 65532 -g 65532 /data

FROM ${PYTHON_BASE} AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    GRAPHARROW_DATABASE_PATH=/data/grapharrow \
    PATH="/app/.venv/bin:$PATH"
WORKDIR /app
RUN --mount=type=tmpfs,target=/var/cache/apt/archives \
    apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip uninstall --yes pip \
    && rm -rf /usr/local/lib/python*/ensurepip
COPY --from=dependencies /app/.venv /app/.venv
COPY --from=dependencies --chown=65532:65532 /data /data
USER 65532:65532
EXPOSE 8000 7687
CMD ["grapharrow-server", "--host", "0.0.0.0", "--port", "8000"]
