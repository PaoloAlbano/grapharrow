# Deploying GraphArrow on Kubernetes

The [GraphArrow chart](../charts/grapharrow/) runs HTTP (REST and ANA) and Bolt
as separate containers in each GraphArrow pod. Use Kubernetes 1.27 or newer
and Helm 3. Release chart packages default to the runtime image for the
GraphArrow version shipped with that package. Set `image.tag` to select a
version or `image.digest` to pin a particular build.

| Deployment | Storage | Endpoint |
| --- | --- | --- |
| Native on a PVC | One pod, one PVC shared by HTTP and Bolt | Service points directly to the pod |
| Native on object storage | One writer pod and separate read-only pods sharing an `s3://` or `az://` URI | Separate writer and reader Services |
| Delta | One or more pods sharing an `s3://` or `az://` database URI | Service points to HAProxy gateways |

## Quickstart on a single PVC

Use `kubectl` with the target cluster, Helm 3, Python 3 and a default
StorageClass. Copy the URL of the `grapharrow-*.tgz` chart from a
[GraphArrow release](https://github.com/PaoloAlbano/grapharrow/releases)
that includes the chart. The package supplies the corresponding GraphArrow
image version.

```sh
CHART_URL='https://github.com/PaoloAlbano/grapharrow/releases/download/<release-tag>/<chart-file>.tgz'
CHART=./grapharrow-chart.tgz
curl --fail --location "$CHART_URL" --output "$CHART"

kubectl create namespace grapharrow
python3 - <<'PYTHON' | kubectl --namespace grapharrow create -f -
import json
from secrets import token_urlsafe

print(json.dumps({
    "apiVersion": "v1",
    "kind": "Secret",
    "metadata": {"name": "grapharrow-auth"},
    "type": "Opaque",
    "stringData": {
        "rest-token": token_urlsafe(32),
        "bolt-token": token_urlsafe(32),
        "clients.json": json.dumps({
            "clients": [{"id": "quickstart", "api_key": token_urlsafe(32), "profile": "readonly"}],
        }),
    },
}))
PYTHON

helm upgrade --install grapharrow "$CHART" \
  --namespace grapharrow --wait --timeout 5m \
  --set auth.existingSecret=grapharrow-auth
helm test grapharrow --namespace grapharrow --logs
```

The test pod uses the existing authentication Secret and connects through
the release Service. It runs `RETURN 1 AS ok` via REST and Bolt,
then verifies an authenticated ANA session and tool result. It closes the
ANA session after checking the result. For client connections, see
[Connect and expose the Service](#connect-and-expose-the-service).

If installation times out, check the pod and PVC status with
`kubectl --namespace grapharrow get pods,pvc`. Describe a pending pod or PVC
to see missing Secret keys or storage provisioning errors. For startup
errors on a running pod, check its `http` and `bolt` container logs.

### Use an existing Secret or PVC

For upgrades, reuse the namespace and Secret. An existing Secret must
contain `rest-token`, `bolt-token` and `clients.json`. Set its name in
`auth.existingSecret`. To rotate credentials, update the Secret and restart
the GraphArrow StatefulSet. The restart creates new ANA session stores and
reloads REST and Bolt credentials.

Set `storage.pvc.storageClass` if the cluster has no suitable default, or
`storage.pvc.existingClaim` to use a claim in the same namespace. The PVC
mounts at `/data` and the database lives at `/data/grapharrow`. GraphArrow
runs as UID/GID 65532 and the pod requests `fsGroup: 65532`. An existing
dataset must be writable by that identity.

The chart retains a dynamically created PVC when you uninstall. To reinstall
against it, set `storage.pvc.existingClaim` to the retained claim
(`grapharrow-data` in this example). The storage lock serializes commits from
HTTP and Bolt while readers may run concurrently. The chart requires one
GraphArrow replica for a writable Native release.

For a Native values file with initial CPU and memory requests, unpack the
chart and use `profiles/native.yaml`. A source checkout has the same profiles
under `charts/grapharrow/profiles/`.

## Native writer and read replicas

Use two releases on the same Native database URI. Create `grapharrow-auth`
as in the quickstart. Create `grapharrow-writer-storage` with credentials
for reading, publishing and cleaning up data in the database prefix. Create
`grapharrow-reader-storage` with credentials for reading and listing the
same data. On Azure, a scoped SAS with read and list permissions is one
option. The S3 and Azure environment variables are described under
[Deploying on shared object storage](#multiple-replicas-on-s3-or-azure).

Using the profiles from the chart archive, install the writer first so it
can publish an initial snapshot. Then install the readers.

```sh
mkdir -p chart
tar -xzf "$CHART" -C chart
DATABASE_URI='s3://my-bucket/graphs/native'

helm upgrade --install grapharrow-writer "$CHART" \
  --namespace grapharrow --wait --timeout 5m \
  -f chart/grapharrow/profiles/native-writer.yaml \
  --set-string storage.uri="$DATABASE_URI"

helm upgrade --install grapharrow-readers "$CHART" \
  --namespace grapharrow --wait --timeout 5m \
  -f chart/grapharrow/profiles/native-readers.yaml \
  --set-string storage.uri="$DATABASE_URI"

helm test grapharrow-readers --namespace grapharrow --logs
```

Send mutations and maintenance to the writer Service. The writer's HTTP
and Bolt containers share a lock for commit serialization. Deploy one
writer release per database. Send reads to the reader Service, which routes
ANA sessions to their owning pod. Readers return HTTP `403` with
`READ_ONLY` for REST writes, and Bolt reports
`Neo.ClientError.General.ForbiddenOnReadOnlyDatabase`. The Neo4j Python driver
surfaces this as `WriteServiceUnavailable`. Use the writer for transactions
that mix reads and writes.

New reads see the published snapshot selected when they begin. Existing
ANA sessions and open Bolt transactions retain their selected snapshot.
While the writer is unavailable, readers can serve published snapshots.
The profiles leave writer replacement to the StatefulSet and keep the role
fixed. Scale the reader release with `replicaCount` or enable its HPA with
`--set autoscaling.enabled=true`. The conditions in
[Autoscale workers](#autoscale-workers) apply to Delta and Native readers.

## Multiple replicas on S3 or Azure

Provision a bucket or container and choose a dedicated database prefix.
Use a new Helm release for Delta. Create or reuse `grapharrow-auth` in the
target namespace and configure provider credentials as a separate Secret.
The chart passes the storage Secret as environment variables to HTTP and Bolt.
See [Delta Lake setup](storage-backends.md#delta-lake-setup) for provider settings.

Extract the profiles from the downloaded package.

```sh
mkdir -p chart
tar -xzf "$CHART" -C chart
```

### Delta on S3

For access-key authentication, create a local `s3.env` with
`GRAPHARROW_S3_REGION`, `GRAPHARROW_S3_ACCESS_KEY` and
`GRAPHARROW_S3_SECRET_KEY`. Include `GRAPHARROW_S3_SESSION_TOKEN` when using
temporary session credentials. Keep the file private.

```sh
kubectl --namespace grapharrow create secret generic grapharrow-s3-storage \
  --from-env-file=s3.env
helm upgrade --install grapharrow-s3 "$CHART" \
  --namespace grapharrow --wait --timeout 5m \
  --values chart/grapharrow/profiles/delta-s3.yaml \
  --set-string storage.uri=s3://my-bucket/graphs/production
helm test grapharrow-s3 --namespace grapharrow --logs
```

### Delta on Azure Blob Storage

For account-key authentication, create a local `azure.env` with
`GRAPHARROW_AZURE_ACCOUNT_NAME` and `GRAPHARROW_AZURE_ACCOUNT_KEY`.
Keep the file private.

```sh
kubectl --namespace grapharrow create secret generic grapharrow-azure-storage \
  --from-env-file=azure.env
helm upgrade --install grapharrow-azure "$CHART" \
  --namespace grapharrow --wait --timeout 5m \
  --values chart/grapharrow/profiles/delta-azure.yaml \
  --set-string storage.uri=az://my-container/graphs/production
helm test grapharrow-azure --namespace grapharrow --logs
```

The Delta profiles request two GraphArrow replicas and two gateways. Tune
`resources.http`, `resources.bolt` and `gateway.resources` for the workload.
The chart prefers spreading GraphArrow replicas across nodes and balances
each gateway revision across eligible nodes. It installs PodDisruptionBudgets
when the replica count allows it. Schedule across distinct workers and
failure domains for availability.

The gateway balances REST and new ANA sessions across healthy pods. For later
ANA calls, send the full `X-ANA-Session` value returned at session creation.
The gateway uses the pod-name prefix to find the owning HTTP process. Handle
state lives in memory on that process. If the pod is removed, restarted, or
unavailable, open a new session and recreate its handles. Failed in-flight
requests may return a transport or gateway error. The gateway does not replay
writes. Bolt uses its own TCP pool, so each connection remains with its
selected pod for the lifetime of the socket.

Concurrent Delta writes can return a write conflict. For REST Cypher writes,
[reuse the same idempotency key when retrying](transactions.md#retrying-rest-cypher-writes).

To scale, set `replicaCount` in Helm values and run `helm upgrade`. The chart
updates the set of per-pod Services and rolls the gateway configuration.
Clients may need to reopen sessions during a rollout. A Helm rollback restores
the deployment configuration.

### Autoscale workers

Enable the Horizontal Pod Autoscaler by adding these settings to either
Delta profile or to the Native readers profile. These profiles supply the
HTTP and Bolt CPU requests needed to calculate utilization. The cluster
must publish the `metrics.k8s.io` API,
usually through [Metrics Server](https://kubernetes-sigs.github.io/metrics-server/).

```yaml
autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 6
  targetCPUUtilizationPercentage: 70
```

Apply the values at installation or with `helm upgrade`. Save the same
storage configuration for each upgrade. For example, to enable autoscaling
on a new S3 release using the profile above, add `--set autoscaling.enabled=true`
to its installation command.

The HPA sets the replica count from the average CPU usage relative to the
GraphArrow pods' combined HTTP and Bolt CPU requests. The chart provisions
per-pod Services and gateway routes up to `maxReplicas`. HAProxy health
checks add each new pod to the pool once it is ready. Existing ANA sessions
continue on their owning pod, and new sessions can use the new pods.

For an existing release, schedule the first upgrade that enables the HPA
during a maintenance window. Kubernetes can briefly default the workload
to one pod when Helm transfers replica control to the HPA. Set
`autoscaling.maxReplicas` at least as high as the existing replica count.

Check the HPA and pod placement with these commands.

```sh
kubectl -n grapharrow get hpa
kubectl -n grapharrow get pods -o wide
```

Scale-up is automatic. To reduce capacity during a quiet period, use
the manual drain below. With a node autoscaler on the cluster, pods awaiting
capacity can trigger provisioning of additional worker nodes, including an
eligible spot node pool.

### Drain a worker before scaling down

For Delta and Native readers, run the script from the chart archive with
Python 3 and `kubectl`. Use an account that can read the
release's workloads, create and delete a Kubernetes Lease, execute a
command in each gateway pod, and scale the StatefulSet. Target the desired
cluster through your current kubeconfig or pass `--context`.

```sh
mkdir -p chart
tar -xzf "$CHART" -C chart
python3 chart/grapharrow/files/scale_down.py grapharrow-readers \
  --namespace grapharrow --grace-seconds 60
```

The command removes one pod, starting with the highest ordinal. It sets
that pod's HTTP and Bolt weights to zero on each gateway via a local Unix
socket. New traffic goes to other pods. Existing ANA sessions still use
their per-pod routes and existing Bolt connections can finish during the
grace period, including transactions that write on Delta. At the deadline,
the command requests one fewer replica. Workflows tied to the removed pod
must open a new ANA session and recreate their handles. Bolt clients must
reconnect. An open Bolt transaction that never sent `COMMIT` loses its
pending changes. A REST client that missed a write response can
[retry with the original idempotency key](transactions.md#retrying-rest-cypher-writes)
through a surviving replica. The command restores the gateway weights after
removal so the ordinal can take traffic again when it returns.

Run the drain during a quiet period, above the HPA minimum when autoscaling
is enabled. If the gateway pods change during the drain, the command
cancels the scale-down and restores the weights on surviving gateways.
The `<release>-drain` Lease serializes commands against the same release.
If the process is forcibly killed, the Lease and runtime weights can remain
in place. After confirming the drain has stopped, restore the affected
server weights and delete the Lease before retrying. With chart defaults,
the original weight is `1` in both pools.
For a fixed replica count, update `replicaCount` in your saved Helm values
so a later upgrade keeps the new size.

## HTTP request metrics

The chart enables `GET /v1/metrics` for Native and Delta. Scrape each
GraphArrow pod's HTTP port to collect the
[HTTP and ANA metrics](server-configuration.md#http-and-ana-metrics).
Set `requestMetrics.enabled=false` to disable the endpoint.

For Prometheus Operator, create a separate token Secret and point the release
at it. With `requestMetrics.existingSecret` configured, this token authorizes
the metrics endpoint. The REST token authorizes the usual graph endpoints.
Without a metrics Secret, the endpoint uses the REST token.

```sh
openssl rand -hex 32 | tr -d '\n' | kubectl -n grapharrow create secret generic \
  grapharrow-metrics --from-file=metrics-token=/dev/stdin

helm upgrade grapharrow "$CHART" --namespace grapharrow --wait --reuse-values \
  --set requestMetrics.existingSecret=grapharrow-metrics
kubectl -n grapharrow apply -f examples/kubernetes/prometheus/podmonitor.yaml
```

The [PodMonitor example](../examples/kubernetes/prometheus/podmonitor.yaml)
discovers GraphArrow pods directly. Match the release names in its selector
and set the labels required by your Prometheus instance's
`podMonitorSelector`. Apply the same metrics Secret setting to both
releases when running a Native writer and readers.

Start with these PromQL queries and the
[p95 example](server-configuration.md#http-and-ana-metrics).

| View | Query |
| --- | --- |
| Requests per second | `sum by (interface) (rate(grapharrow_http_operations_total[5m]))` |
| Operations executing | `sum by (interface) (grapharrow_http_operations_executing)` |
| Errors per second | `sum by (interface, status) (rate(grapharrow_http_operations_total{status=~"[45]..\|aborted"}[5m]))` |
| REST write conflicts per second | `sum(rate(grapharrow_rest_write_conflicts_total[5m]))` |

The HPA uses CPU utilization from the cluster Metrics API.

## Back up and restore a database

Take a cold backup during a maintenance window. Stop incoming work, close ANA
sessions, and scale the source GraphArrow StatefulSet to zero. Wait for its pods
to terminate and pause other writers and storage maintenance jobs for the same
database. For managed Delta, check that admission has no active pins or pending
cleanup before copying. See [managed Delta retention](storage-backends.md#applying-managed-delta-retention).

- **Native with a PVC.** Mount the source PVC read-only in a helper pod and archive the
  complete `grapharrow` directory. Extract it into an empty compatible PVC,
  preserving the database directory and write access for UID/GID 65532. Start
  a release with that PVC in `storage.pvc.existingClaim`.
- **Native with object storage.** Copy the complete database prefix, including
  the snapshot head and all referenced objects, and verify checksums.
  Start the writer against the fresh prefix before starting its readers.
- **Delta.** Copy the complete database prefix to a backup, including the
  Delta log, checkpoints, Parquet files and the snapshot admission object when
  managed retention is enabled. Restore the same relative paths to a fresh
  prefix and verify the copied bytes with checksums. Start a release pointing
  `storage.uri` to that prefix and supply its provider credentials.

Run `grapharrow verify PATH --verify-checksums` with the restored backend's
settings and confirm the expected snapshot ID. Compare a representative
query across the interfaces you use before directing traffic to the restore.
Clients open new ANA sessions on the restored deployment.

## Connect and expose the Service

```sh
kubectl -n grapharrow port-forward service/grapharrow 8000:8000 7687:7687
```

Use the [HTTP and Bolt client examples](server-configuration.md) on those local
ports.

### HTTPS for REST and ANA

The [TLS edge example](../examples/kubernetes/tls-edge/https/) uses two
HAProxy replicas to terminate TLS and forwards requests to the Helm Service.
It passes `Authorization` and `X-ANA-Session` through, so the same ANA
session can make requests through either proxy. With Delta, the chart's
gateway routes those requests to the GraphArrow pod that owns the session.

Use the `grapharrow` Helm release in the same namespace, with authentication
configured as in the quickstart. From the repository root, create the TLS
Secret with your certificate chain and private key. The certificate must
cover the DNS names clients use to connect.

```sh
kubectl -n grapharrow create secret tls grapharrow-edge-tls \
  --cert=fullchain.pem --key=privkey.pem
kubectl -n grapharrow apply -k examples/kubernetes/tls-edge/https
kubectl -n grapharrow rollout status deployment/grapharrow-tls-edge
```

The Service starts as `ClusterIP`. On a cluster with LoadBalancer support,
expose port 443, then point your DNS name at the assigned IP or hostname.
Choose any provider-specific annotations or source restrictions for your
network before exposing the Service.

```sh
kubectl -n grapharrow patch service grapharrow-https --type=merge \
  -p '{"spec":{"type":"LoadBalancer"}}'
kubectl -n grapharrow get service grapharrow-https
curl --fail https://graph.example.com/health
```

If you already use an HTTPS ingress, you can instead send REST and ANA to
the Helm Service on port 8000. Preserve both headers and send every session
request through that Service. Restrict direct access to the application
pods and their per-pod Services.

### Optional Bolt over TLS

Apply the [Bolt TLS overlay](../examples/kubernetes/tls-edge/bolt-tls/) in
the same namespace to add a TCP listener and a separate Service alongside
HTTPS. Enable Bolt in the chart and include the Bolt DNS name in the same
certificate.

```sh
kubectl -n grapharrow apply -k examples/kubernetes/tls-edge/bolt-tls
kubectl -n grapharrow rollout status deployment/grapharrow-tls-edge
kubectl -n grapharrow patch service grapharrow-bolt-tls --type=merge \
  -p '{"spec":{"type":"LoadBalancer"}}'
kubectl -n grapharrow get service grapharrow-bolt-tls
```

Point the Bolt DNS name at its Service address and connect with
`bolt+s://bolt.example.com:7687`, a trusted certificate and the Bolt token
configured in the Helm release. If you use an existing load balancer in
front of these TLS Services, configure TCP passthrough on both ports. TLS
terminates at HAProxy and the hop from HAProxy to GraphArrow uses the
cluster network in plaintext.

After updating the TLS Secret, run
`kubectl -n grapharrow rollout restart deployment/grapharrow-tls-edge`
to load the updated certificate.

For validation and packaging details, see [CI and Releases](releases.md).
