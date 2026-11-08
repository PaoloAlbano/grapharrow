# Release builds consume the verified wheel; local builds create it from source.
ARG DISTRIBUTION_SOURCE=wheel-builder

FROM python:3.12-slim AS wheel-builder
WORKDIR /build
RUN pip install --no-cache-dir uv==0.8.12
COPY pyproject.toml uv.lock README.md LICENSE THIRD_PARTY_NOTICES.md ./
COPY src ./src
RUN uv build --wheel --no-sources --out-dir /dist \
    && uv export --locked --no-dev --no-emit-project --output-file /dist/runtime-requirements.txt

FROM scratch AS prebuilt
COPY dist/*.whl dist/runtime-requirements.txt /dist/

FROM ${DISTRIBUTION_SOURCE} AS distribution

FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    GRAPHARROW_DATABASE_PATH=/data/grapharrow \
    PATH="/app/.venv/bin:$PATH"
WORKDIR /app
COPY --from=distribution /dist /tmp/distribution
RUN python -m venv /app/.venv \
    && pip install --no-cache-dir --only-binary=:all: --require-hashes \
       -r /tmp/distribution/runtime-requirements.txt \
    && pip install --no-cache-dir --no-deps /tmp/distribution/*.whl \
    && rm -rf /tmp/distribution
EXPOSE 8000
CMD ["grapharrow-server", "--host", "0.0.0.0", "--port", "8000"]
