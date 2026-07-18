# JanusGraph YDB Storage Backend

A [JanusGraph](https://janusgraph.org) storage backend that keeps graph data in
[YDB](https://ydb.tech) — a distributed, strongly consistent NewSQL database.
The result is a horizontally scalable, transactional TinkerPop/Gremlin graph.

The backend implements JanusGraph's **ordered key-value SPI**
(`OrderedKeyValueStoreManager`), the same integration path used by the
BerkeleyDB and FoundationDB backends. YDB is a natural fit for this SPI: it is
an ordered (range-sharded by primary key), transactional, distributed store —
architecturally the closest open-source relative of FoundationDB.

## Compatibility

| Component | Version |
|---|---|
| JanusGraph | 1.1.0 |
| YDB Java SDK | 2.4.7 |
| YDB server | any version supported by SDK 2.4.x (tested with `ydbplatform/local-ydb` 26.x) |
| Java | 8+ (module is built with target 1.8) |

## Quick start

Run a local YDB:

```bash
docker run -d --rm --name ydb-local -h localhost \
  -p 2136:2136 -p 8765:8765 \
  -e GRPC_PORT=2136 -e MON_PORT=8765 -e YDB_USE_IN_MEMORY_PDISKS=true \
  ydbplatform/local-ydb:latest
```

Put the `janusgraph-ydb` jar and its dependencies on the JanusGraph classpath
(e.g. into `lib/` or `ext/` of a JanusGraph distribution) and configure:

```properties
gremlin.graph=org.janusgraph.core.JanusGraphFactory
storage.backend=org.janusgraph.diskstorage.ydb.YdbStoreManager
storage.ydb.endpoint=grpc://localhost:2136
storage.ydb.database=/local
storage.ydb.directory=janusgraph
# activates batched slice reads (recommended)
query.batch.enabled=true
```

> **Why the full class name?** JanusGraph resolves `storage.backend` against a
> hard-coded enum of built-in backends and otherwise treats the value as a fully
> qualified class name. There is no registration mechanism (ServiceLoader or
> similar) that would let an external module claim a shorthand like `ydb`, so
> the FQCN is the only way to wire in any third-party backend — the
> FoundationDB backend is configured the same way.

Then use JanusGraph as usual:

```java
JanusGraph graph = JanusGraphFactory.open("janusgraph-ydb.properties");
GraphTraversalSource g = graph.traversal();
```

## Configuration reference (`storage.ydb.*`)

| Option | Default | Description |
|---|---|---|
| `storage.ydb.endpoint` | `grpc://localhost:2136` | YDB endpoint; use `grpcs://` for TLS |
| `storage.ydb.database` | `/local` | YDB database path |
| `storage.ydb.directory` | `janusgraph` | Directory inside the database holding all store tables; falls back to `graph.graphname` |
| `storage.ydb.read-tx-mode` | `snapshot_ro` | YDB transaction mode for reads: `snapshot_ro`, `stale_ro`, `online_ro` (lock-free pooled reads, buffered writes) or `serializable_rw` (one interactive transaction) |
| `storage.ydb.auth-mode` | `anonymous` | `anonymous`, `token`, `static` or `environ` (standard `YDB_*` env vars) |
| `storage.ydb.auth-token` | — | Access token for `auth-mode=token` |
| `storage.ydb.auth-username` / `auth-password` | — | Credentials for `auth-mode=static` |
| `storage.ydb.session-pool-max` | `50` | Max YDB sessions; bounds the number of concurrently open JanusGraph transactions |
| `storage.ydb.session-acquire-timeout` | `10 s` | Wait for a free session before failing (retryably) |
| `storage.ydb.scan-page-size` | `10000` | Max rows per round trip during range scans |
| `storage.ydb.slice-batch-size` | `10` | Max ranges combined into one multi-statement query by batched slice reads |
| `storage.ydb.auto-partitioning` | `true` | Enable auto-partitioning by size and load for store tables |
| `storage.ydb.presplit-partitions` | `0` | Pre-split new store tables into N shards at uniform key boundaries (aligned with JanusGraph's virtual partitions); avoids the single-hot-shard warm-up phase under ingest |

## Data model

Each JanusGraph store (`edgestore`, `graphindex`, `janusgraph_ids`,
`system_properties`, `txlog`, `systemlog`) becomes one YDB row table under
`<database>/<directory>/`:

```sql
CREATE TABLE <store> (
    key   String NOT NULL,   -- raw bytes
    value String,            -- raw bytes
    PRIMARY KEY (key)
);
```

YDB orders `String` primary keys by unsigned lexicographic byte comparison
(`memcmp`, shorter prefix first) — exactly the order JanusGraph requires from an
ordered key-value store; a dedicated test (`YdbKeyOrderTest`) guards this
property. Tables are created idempotently on startup (`CREATE TABLE IF NOT
EXISTS`); existing tables are never touched.

## Transactions and consistency

The backend has two shapes, selected by `storage.ydb.read-tx-mode`.

**Read-only read modes** (`snapshot_ro`, default; also `stale_ro` / `online_ro`
for lower-latency, less-fresh reads):

* Reads execute as independent **lock-free** read-only queries through the
  session pool: no session pinning, no read locks, no transaction aborts for
  long traversals, fully parallel batched slice reads.
* Writes are buffered client-side; a transaction always observes its own writes
  (reads merge the buffer). On commit the whole buffer is applied atomically in
  one `SERIALIZABLE_RW` batch driven through YDB's retry machinery.
* There is **no locking at all, by design** (last-write-wins): concurrent
  writers to the same element race, and unique-index constraints are not
  enforced under concurrent insertion of the same key. This is the intended
  trade-off for read-heavy workloads such as agent memory; use
  `serializable_rw` when you need enforced invariants.
* Visibility is read-committed-style: each read sees the latest committed data
  (`snapshot_ro` — a consistent per-query snapshot).

**`serializable_rw`**: one interactive serializable YDB transaction per
JanusGraph transaction, pinned to a pooled session. Reads take optimistic
locks; conflicting commits fail with `ABORTED` (surfaced as retryable
`TemporaryBackendException`). The transaction keeps one consistent snapshot for
its whole lifetime. See `YdbReadModeSemanticsTest` for both semantics.

**Common to both modes**:

* Retryable YDB statuses map to `TemporaryBackendException`, which JanusGraph's
  own machinery retries (e.g. id-block allocation — safe in both modes: it
  relies on consistent reads, not locking).
* Mutations of one flush are applied as a **single multi-statement query**
  (`DELETE … ON SELECT FROM AS_TABLE($keys)` + `UPSERT … SELECT FROM
  AS_TABLE($rows)` per store) — one round trip regardless of batch size.
* Batched slice reads (`getSlices`, activated by `query.batch.enabled=true`)
  combine up to `slice-batch-size` range reads into one multi-statement query;
  in the read-only read modes the batches additionally execute **in parallel**.
* With `storage.batch-loading=true` writes bypass transactions entirely:
  additions stream through YDB's **BulkUpsert** API, deletions run as
  auto-committed batches — the fastest ingestion path (not atomic, not
  rollbackable, as usual for batch loading).

## Limitations

* `storage.backend=ydb` shorthand is impossible (see above) — use the FQCN.
* No TTL support (`cellTTL`/`storeTTL` are disabled); JanusGraph TTL features
  are unavailable with this backend.
* In the read-only read modes there is no locking: concurrent writes to the
  same element are last-write-wins and unique indexes are not enforced against
  concurrent duplicate insertion. Switch to `read-tx-mode=serializable_rw` for
  enforced invariants.
* In `serializable_rw` mode every open transaction pins one pooled session
  (size `storage.ydb.session-pool-max` accordingly) and a single transaction
  must not be used from multiple threads concurrently.
* Very large single transactions are bounded by YDB's transaction limits;
  prefer `storage.batch-loading=true` (BulkUpsert) for bulk ingestion.

## Development and testing

A [devcontainer](.devcontainer/) ships a JDK 11 + Maven toolchain and a
`ydbplatform/local-ydb` sidecar — nothing needs to be installed on the host:

```bash
docker compose -f .devcontainer/docker-compose.yml up -d
docker compose -f .devcontainer/docker-compose.yml exec dev mvn test
```

Integration tests resolve YDB as follows:

1. If `YDB_ENDPOINT` and `YDB_DATABASE` env vars are set (the devcontainer sets
   them to the sidecar), that instance is used directly.
2. Otherwise a `ydbplatform/local-ydb` container is started via Testcontainers.

The suite includes the standard JanusGraph TCK: `KeyValueStoreTest` (both read
modes), `KeyColumnValueStoreTest` (fixed- and variable-length key packing),
`MultiWriteKeyColumnValueStoreTest`, `IDAuthorityTest`, `KCVSLogTest`,
`JanusGraphTest`, plus a byte-order guard test, a read-mode semantics test
(`YdbReadModeSemanticsTest`), a batch-loading (BulkUpsert) test and a
10k-vertex traversal smoke test. The TCK runs against the default
`snapshot_ro` read mode.

Three TCK methods are disabled, each with an in-code justification:

* `testConcurrentGetSliceAndMutate` — the TCK helper races rollback/replace of
  a shared transaction handle across 64 threads while writing through it,
  which is inherently racy for any backend with stateful transactions;
  BerkeleyJE and FoundationDB disable it too. The read-only variant
  (`testConcurrentGetSlice`) stays enabled: pooled reads are stateless.
* `testConsistencyEnforcement` / `testConcurrentConsistencyEnforcement` —
  assert the exception type of JanusGraph's built-in locker, which this
  backend never installs: it is deliberately lock-free in the read-only read
  modes and relies on YDB's own conflict detection in `serializable_rw`;
  BerkeleyJE and FoundationDB disable the same pair.
