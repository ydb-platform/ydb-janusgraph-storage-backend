// Copyright 2026 YDB Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.ydb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.AuthRpcProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.auth.EnvironAuthProvider;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.impl.auth.GrpcAuthRpc;
import tech.ydb.query.QueryClient;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryStream;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.EntryType;
import tech.ydb.scheme.description.ListDirectoryResult;
import tech.ydb.table.TableClient;
import tech.ydb.table.query.Params;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;

import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.AUTH_MODE;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.AUTH_PASSWORD;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.AUTH_TOKEN;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.AUTH_USERNAME;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.AUTO_PARTITIONING;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.DATABASE;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.DIRECTORY;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.ENDPOINT;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.PRESPLIT_PARTITIONS;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.READ_TX_MODE;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.SCAN_PAGE_SIZE;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.SESSION_ACQUIRE_TIMEOUT;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.SESSION_POOL_MAX;
import static org.janusgraph.diskstorage.ydb.YdbConfigOptions.SLICE_BATCH_SIZE;

/**
 * JanusGraph {@link OrderedKeyValueStoreManager} backed by YDB. Every JanusGraph
 * store becomes one YDB row table {@code (key String NOT NULL, value String,
 * PRIMARY KEY(key))} under a common directory inside the configured database.
 *
 * <p>Configure with {@code storage.backend=org.janusgraph.diskstorage.ydb.YdbStoreManager}
 * (JanusGraph offers no shorthand registration mechanism for external backends).
 */
@PreInitializeConfigOptions
public class YdbStoreManager extends AbstractStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(YdbStoreManager.class);

    private static final StructType DELETE_ROW_TYPE = StructType.of("key", PrimitiveType.Bytes);
    private static final StructType UPSERT_ROW_TYPE =
        StructType.of("key", PrimitiveType.Bytes, "value", PrimitiveType.Bytes);

    /** Row cap of one statement batch inside a commit or bulk load. */
    private static final int WRITE_CHUNK_ROWS = 50_000;

    private final Map<String, YdbKeyValueStore> stores = new ConcurrentHashMap<>();

    private final GrpcTransport transport;
    private final QueryClient queryClient;
    private final SchemeClient schemeClient;
    private final SessionRetryContext retryCtx;
    private final StoreFeatures features;

    // lazily created, only used for BulkUpsert under storage.batch-loading
    private volatile TableClient tableClient;
    private volatile tech.ydb.table.SessionRetryContext tableRetryCtx;

    private final String rootPath;
    private final TxMode readTxMode;
    private final Duration sessionAcquireTimeout;
    private final int scanPageSize;
    private final int sliceBatchSize;
    private final boolean autoPartitioning;
    private final int presplitPartitions;

    public YdbStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        String databasePath = normalizeDatabase(configuration.get(DATABASE));
        this.rootPath = databasePath + "/" + determineRootDirectoryName(configuration);
        this.readTxMode = parseReadTxMode(configuration.get(READ_TX_MODE));
        this.sessionAcquireTimeout = configuration.get(SESSION_ACQUIRE_TIMEOUT);
        this.scanPageSize = configuration.get(SCAN_PAGE_SIZE);
        this.sliceBatchSize = configuration.get(SLICE_BATCH_SIZE);
        this.autoPartitioning = configuration.get(AUTO_PARTITIONING);
        this.presplitPartitions = configuration.get(PRESPLIT_PARTITIONS);
        if (presplitPartitions > 256) {
            throw new PermanentBackendException("storage.ydb.presplit-partitions must be at most 256");
        }

        String endpoint = configuration.get(ENDPOINT);
        GrpcTransport newTransport;
        try {
            newTransport = GrpcTransport.forConnectionString(buildConnectionString(endpoint, databasePath))
                .withAuthProvider(buildAuthProvider(configuration))
                .build();
        } catch (RuntimeException e) {
            throw new PermanentBackendException("Could not connect to YDB at " + endpoint + databasePath, e);
        }
        this.transport = newTransport;
        try {
            this.queryClient = QueryClient.newClient(transport)
                .sessionPoolMaxSize(configuration.get(SESSION_POOL_MAX))
                .build();
            this.schemeClient = SchemeClient.newClient(transport).build();
            this.retryCtx = SessionRetryContext.create(queryClient).idempotent(true).build();
        } catch (RuntimeException e) {
            transport.close();
            throw new PermanentBackendException("Could not initialize YDB clients", e);
        }

        // locking(true) is deliberate in every read mode: YDB either enforces conflicts
        // itself (serializable_rw) or the backend runs lock-free by design (read-only
        // read modes, last-write-wins) — JanusGraph's own locker is never installed.
        this.features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .keyOrdered(true)
            .transactional(transactional)
            .distributed(true)
            .multiQuery(true)
            .batchMutation(true)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .locking(true)
            .optimisticLocking(true)
            .supportsInterruption(false)
            .build();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getName() {
        return rootPath;
    }

    QueryClient getQueryClient() {
        return queryClient;
    }

    Duration getSessionAcquireTimeout() {
        return sessionAcquireTimeout;
    }

    @Override
    public YdbKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        YdbKeyValueStore store = stores.get(name);
        if (store != null) {
            return store;
        }
        synchronized (this) {
            store = stores.get(name);
            if (store == null) {
                ensureRootDirectory();
                createTableIfNotExists(name);
                store = new YdbKeyValueStore(name, tablePath(name), this, scanPageSize, sliceBatchSize);
                stores.put(name, store);
                log.debug("Opened YDB-backed store {} at {}", name, tablePath(name));
            }
            return store;
        }
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new YdbTx(this, readTxMode, config);
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        YdbTx tx = YdbTx.of(txh);
        if (tx.isInteractive()) {
            mutateManyInTx(mutations, tx);
        } else if (batchLoading) {
            applyBulkImmediate(mutations);
        } else {
            bufferMutations(mutations, tx);
        }
    }

    /** serializable_rw: one multi-statement query inside the interactive transaction. */
    private void mutateManyInTx(Map<String, KVMutation> mutations, YdbTx tx) throws BackendException {
        StringBuilder declares = new StringBuilder();
        StringBuilder statements = new StringBuilder();
        Params params = Params.create();
        int storeIndex = 0;
        for (Map.Entry<String, KVMutation> entry : mutations.entrySet()) {
            KVMutation mutation = entry.getValue();
            if (mutation == null || mutation.isEmpty()) {
                continue;
            }
            openDatabase(entry.getKey());
            String table = "`" + tablePath(entry.getKey()) + "`";
            mutation.consolidate();
            if (mutation.hasDeletions()) {
                String param = "$del" + storeIndex;
                declares.append("DECLARE ").append(param).append(" AS ")
                    .append(ListType.of(DELETE_ROW_TYPE)).append(";\n");
                statements.append("DELETE FROM ").append(table)
                    .append(" ON SELECT * FROM AS_TABLE(").append(param).append(");\n");
                params.put(param, deletionRows(mutation.getDeletions()));
            }
            if (mutation.hasAdditions()) {
                String param = "$add" + storeIndex;
                declares.append("DECLARE ").append(param).append(" AS ")
                    .append(ListType.of(UPSERT_ROW_TYPE)).append(";\n");
                statements.append("UPSERT INTO ").append(table)
                    .append(" SELECT * FROM AS_TABLE(").append(param).append(");\n");
                params.put(param, additionRows(mutation.getAdditions()));
            }
            storeIndex++;
        }
        if (statements.length() > 0) {
            tx.executeInTx(declares.append(statements).toString(), params, true);
        }
    }

    /** Read-only read modes: mutations accumulate in the transaction's buffer. */
    private void bufferMutations(Map<String, KVMutation> mutations, YdbTx tx) throws BackendException {
        for (Map.Entry<String, KVMutation> entry : mutations.entrySet()) {
            KVMutation mutation = entry.getValue();
            if (mutation == null || mutation.isEmpty()) {
                continue;
            }
            openDatabase(entry.getKey());
            String path = tablePath(entry.getKey());
            mutation.consolidate();
            if (mutation.hasDeletions()) {
                for (StaticBuffer deletion : mutation.getDeletions()) {
                    tx.bufferDelete(path, deletion.as(StaticBuffer.ARRAY_FACTORY));
                }
            }
            if (mutation.hasAdditions()) {
                for (KeyValueEntry addition : mutation.getAdditions()) {
                    tx.bufferInsert(path,
                        addition.getKey().as(StaticBuffer.ARRAY_FACTORY),
                        addition.getValue().as(StaticBuffer.ARRAY_FACTORY));
                }
            }
        }
    }

    // ------------------------------------------------------------ read helpers

    /** One retryable read-only query through the session pool (buffered modes). */
    QueryReader readOutsideTx(String yql, Params params) throws BackendException {
        return awaitRead(readOutsideTxAsync(yql, params));
    }

    CompletableFuture<Result<QueryReader>> readOutsideTxAsync(String yql, Params params) {
        return retryCtx.supplyResult(session -> QueryReader.readFrom(session.createQuery(yql, readTxMode, params)));
    }

    QueryReader awaitRead(CompletableFuture<Result<QueryReader>> future) throws BackendException {
        try {
            Result<QueryReader> result = future.join();
            if (!result.isSuccess()) {
                throw YdbExceptions.fromStatus(result.getStatus(), "read");
            }
            return result.getValue();
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "read");
        }
    }

    // ------------------------------------------------------------ commit paths

    /**
     * Applies the buffered writes of one transaction atomically: all chunks execute
     * inside a single SERIALIZABLE_RW transaction driven through the retry context
     * (UPSERT/DELETE batches are idempotent, so retries are safe).
     */
    void applyBuffered(Map<String, YdbTx.StoreBuffer> buffers) throws BackendException {
        List<WriteChunk> chunks = buildChunks(buffers);
        if (chunks.isEmpty()) {
            return;
        }
        try {
            Status status = retryCtx.supplyStatus(session -> executeChunksInTx(session, chunks)).join();
            if (!status.isSuccess()) {
                throw YdbExceptions.fromStatus(status, "transaction commit");
            }
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "transaction commit");
        }
    }

    private CompletableFuture<Status> executeChunksInTx(QuerySession session, List<WriteChunk> chunks) {
        QueryTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
        CompletableFuture<Status> chain = CompletableFuture.completedFuture(Status.SUCCESS);
        for (int i = 0; i < chunks.size(); i++) {
            WriteChunk chunk = chunks.get(i);
            boolean last = i == chunks.size() - 1;
            chain = chain.thenCompose(previous -> {
                if (!previous.isSuccess()) {
                    return CompletableFuture.completedFuture(previous);
                }
                QueryStream query = last
                    ? transaction.createQueryWithCommit(chunk.yql, chunk.params)
                    : transaction.createQuery(chunk.yql, chunk.params);
                return query.execute().thenApply(Result::getStatus);
            });
        }
        return chain;
    }

    private static final class WriteChunk {
        final String yql;
        final Params params;

        WriteChunk(String yql, Params params) {
            this.yql = yql;
            this.params = params;
        }
    }

    private List<WriteChunk> buildChunks(Map<String, YdbTx.StoreBuffer> buffers) {
        List<WriteChunk> chunks = new ArrayList<>();
        StringBuilder declares = new StringBuilder();
        StringBuilder statements = new StringBuilder();
        Params params = Params.create();
        int paramIndex = 0;
        int rows = 0;

        for (Map.Entry<String, YdbTx.StoreBuffer> entry : buffers.entrySet()) {
            String table = "`" + entry.getKey() + "`";
            YdbTx.StoreBuffer buffer = entry.getValue();

            for (List<byte[]> part : Lists.partition(new ArrayList<>(buffer.deletions), WRITE_CHUNK_ROWS)) {
                if (rows > 0 && rows + part.size() > WRITE_CHUNK_ROWS) {
                    chunks.add(new WriteChunk(declares.append(statements).toString(), params));
                    declares = new StringBuilder();
                    statements = new StringBuilder();
                    params = Params.create();
                    rows = 0;
                }
                String param = "$del" + paramIndex++;
                declares.append("DECLARE ").append(param).append(" AS ")
                    .append(ListType.of(DELETE_ROW_TYPE)).append(";\n");
                statements.append("DELETE FROM ").append(table)
                    .append(" ON SELECT * FROM AS_TABLE(").append(param).append(");\n");
                params.put(param, ListType.of(DELETE_ROW_TYPE).newValue(part.stream()
                    .map(key -> DELETE_ROW_TYPE.newValue("key", PrimitiveValue.newBytes(key)))
                    .collect(Collectors.toList())));
                rows += part.size();
            }

            List<Map.Entry<byte[], byte[]>> additions = new ArrayList<>(buffer.additions.entrySet());
            for (List<Map.Entry<byte[], byte[]>> part : Lists.partition(additions, WRITE_CHUNK_ROWS)) {
                if (rows > 0 && rows + part.size() > WRITE_CHUNK_ROWS) {
                    chunks.add(new WriteChunk(declares.append(statements).toString(), params));
                    declares = new StringBuilder();
                    statements = new StringBuilder();
                    params = Params.create();
                    rows = 0;
                }
                String param = "$add" + paramIndex++;
                declares.append("DECLARE ").append(param).append(" AS ")
                    .append(ListType.of(UPSERT_ROW_TYPE)).append(";\n");
                statements.append("UPSERT INTO ").append(table)
                    .append(" SELECT * FROM AS_TABLE(").append(param).append(");\n");
                params.put(param, ListType.of(UPSERT_ROW_TYPE).newValue(part.stream()
                    .map(e -> UPSERT_ROW_TYPE.newValue(
                        "key", PrimitiveValue.newBytes(e.getKey()),
                        "value", PrimitiveValue.newBytes(e.getValue())))
                    .collect(Collectors.toList())));
                rows += part.size();
            }
        }
        if (statements.length() > 0) {
            chunks.add(new WriteChunk(declares.append(statements).toString(), params));
        }
        return chunks;
    }

    /**
     * storage.batch-loading: writes bypass transactions — additions stream through
     * the BulkUpsert API, deletions run as auto-committed batches. Fastest path;
     * not atomic and not rollbackable, as usual for batch loading.
     */
    private void applyBulkImmediate(Map<String, KVMutation> mutations) throws BackendException {
        for (Map.Entry<String, KVMutation> entry : mutations.entrySet()) {
            KVMutation mutation = entry.getValue();
            if (mutation == null || mutation.isEmpty()) {
                continue;
            }
            openDatabase(entry.getKey());
            String path = tablePath(entry.getKey());
            mutation.consolidate();
            if (mutation.hasDeletions()) {
                for (List<StaticBuffer> part : Lists.partition(mutation.getDeletions(), WRITE_CHUNK_ROWS)) {
                    String yql = "DECLARE $del AS " + ListType.of(DELETE_ROW_TYPE) + "; "
                        + "DELETE FROM `" + path + "` ON SELECT * FROM AS_TABLE($del);";
                    executeAutoCommit(yql, Params.of("$del", deletionRows(part)), "bulk delete");
                }
            }
            if (mutation.hasAdditions()) {
                for (List<KeyValueEntry> part : Lists.partition(mutation.getAdditions(), WRITE_CHUNK_ROWS)) {
                    ListValue rows = additionRows(part);
                    try {
                        Status status = tableRetry()
                            .supplyStatus(session -> session.executeBulkUpsert(path, rows, new BulkUpsertSettings()))
                            .join();
                        if (!status.isSuccess()) {
                            throw YdbExceptions.fromStatus(status, "bulk upsert");
                        }
                    } catch (RuntimeException e) {
                        throw YdbExceptions.fromThrowable(e, "bulk upsert");
                    }
                }
            }
        }
    }

    /** One auto-committed write query through the session pool. */
    private void executeAutoCommit(String yql, Params params, String operation) throws BackendException {
        try {
            Result<QueryInfo> result = retryCtx
                .supplyResult(session -> session.createQuery(yql, TxMode.SERIALIZABLE_RW, params).execute())
                .join();
            if (!result.isSuccess()) {
                throw YdbExceptions.fromStatus(result.getStatus(), operation);
            }
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, operation);
        }
    }

    private synchronized tech.ydb.table.SessionRetryContext tableRetry() {
        if (tableRetryCtx == null) {
            tableClient = TableClient.newClient(transport).sessionPoolSize(0, 8).build();
            tableRetryCtx = tech.ydb.table.SessionRetryContext.create(tableClient).idempotent(true).build();
        }
        return tableRetryCtx;
    }

    private static ListValue deletionRows(List<StaticBuffer> deletions) {
        return ListType.of(DELETE_ROW_TYPE).newValue(deletions.stream()
            .map(key -> DELETE_ROW_TYPE.newValue("key", bytesValue(key)))
            .collect(Collectors.toList()));
    }

    private static ListValue additionRows(List<KeyValueEntry> additions) {
        return ListType.of(UPSERT_ROW_TYPE).newValue(additions.stream()
            .map(e -> UPSERT_ROW_TYPE.newValue(
                "key", bytesValue(e.getKey()),
                "value", bytesValue(e.getValue())))
            .collect(Collectors.toList()));
    }

    private static PrimitiveValue bytesValue(StaticBuffer buffer) {
        return PrimitiveValue.newBytes(buffer.as(StaticBuffer.ARRAY_FACTORY));
    }

    // ------------------------------------------------------------ admin

    @Override
    public synchronized void clearStorage() throws BackendException {
        for (YdbKeyValueStore store : stores.values().toArray(new YdbKeyValueStore[0])) {
            store.close();
        }
        stores.clear();
        try {
            Result<ListDirectoryResult> listing = schemeClient.listDirectory(rootPath).join();
            if (!listing.isSuccess()) {
                if (listing.getStatus().getCode() == StatusCode.SCHEME_ERROR) {
                    return; // nothing to clear
                }
                throw YdbExceptions.fromStatus(listing.getStatus(), "list directory " + rootPath);
            }
            for (tech.ydb.scheme.description.Entry child : listing.getValue().getEntryChildren()) {
                if (child.getType() == EntryType.TABLE) {
                    executeDdl("DROP TABLE `" + rootPath + "/" + child.getName() + "`;",
                        "drop table " + child.getName());
                }
            }
            Status removed = schemeClient.removeDirectory(rootPath).join();
            if (!removed.isSuccess()) {
                log.warn("Could not remove YDB directory {}: {}", rootPath, removed);
            }
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "clear storage");
        }
        log.info("Cleared YDB storage at {}", rootPath);
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            Result<ListDirectoryResult> listing = schemeClient.listDirectory(rootPath).join();
            if (!listing.isSuccess()) {
                if (listing.getStatus().getCode() == StatusCode.SCHEME_ERROR) {
                    return false;
                }
                throw YdbExceptions.fromStatus(listing.getStatus(), "list directory " + rootPath);
            }
            return listing.getValue().getEntryChildren().stream()
                .anyMatch(child -> child.getType() == EntryType.TABLE);
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "exists check");
        }
    }

    @Override
    public void close() throws BackendException {
        try {
            stores.clear();
            if (tableClient != null) {
                tableClient.close();
            }
            schemeClient.close();
            queryClient.close();
            transport.close();
        } catch (Exception e) {
            throw new PermanentBackendException("Could not close YDB storage manager", e);
        }
        log.info("YdbStoreManager closed");
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    void removeDatabase(YdbKeyValueStore store) {
        stores.remove(store.getName());
    }

    private String tablePath(String storeName) {
        return rootPath + "/" + storeName;
    }

    private void ensureRootDirectory() throws BackendException {
        try {
            Status status = schemeClient.makeDirectories(rootPath).join();
            if (!status.isSuccess()) {
                throw YdbExceptions.fromStatus(status, "create directory " + rootPath);
            }
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "create directory " + rootPath);
        }
    }

    private void createTableIfNotExists(String storeName) throws BackendException {
        List<String> with = new ArrayList<>();
        if (autoPartitioning) {
            with.add("AUTO_PARTITIONING_BY_SIZE = ENABLED");
            with.add("AUTO_PARTITIONING_BY_LOAD = ENABLED");
        }
        if (presplitPartitions > 1) {
            with.add("AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " + presplitPartitions);
            with.add("PARTITION_AT_KEYS = (" + presplitBoundaries(presplitPartitions) + ")");
        }
        String ddl = "CREATE TABLE IF NOT EXISTS `" + tablePath(storeName) + "` ("
            + "key String NOT NULL, value String, PRIMARY KEY (key))"
            + (with.isEmpty() ? "" : " WITH (" + String.join(", ", with) + ")")
            + ";";
        executeDdl(ddl, "create table for store '" + storeName + "'");
    }

    /** N-1 uniform single-byte boundaries over the key space, as YQL string literals. */
    private static String presplitBoundaries(int partitions) {
        StringBuilder boundaries = new StringBuilder();
        for (int i = 1; i < partitions; i++) {
            if (boundaries.length() > 0) {
                boundaries.append(", ");
            }
            boundaries.append(String.format("'\\x%02x'", (int) (i * 256L / partitions)));
        }
        return boundaries.toString();
    }

    private void executeDdl(String ddl, String operation) throws BackendException {
        try {
            Result<QueryInfo> result = retryCtx
                .supplyResult(session -> session.createQuery(ddl, TxMode.NONE).execute())
                .join();
            if (!result.isSuccess()) {
                throw YdbExceptions.fromStatus(result.getStatus(), operation);
            }
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, operation);
        }
    }

    private static TxMode parseReadTxMode(String value) throws BackendException {
        switch (value.trim().toLowerCase()) {
            case "snapshot_ro":
                return TxMode.SNAPSHOT_RO;
            case "stale_ro":
                return TxMode.STALE_RO;
            case "online_ro":
                return TxMode.ONLINE_RO;
            case "serializable_rw":
                return TxMode.SERIALIZABLE_RW;
            default:
                throw new PermanentBackendException("Unrecognized storage.ydb.read-tx-mode: " + value
                    + " (expected snapshot_ro, stale_ro, online_ro or serializable_rw)");
        }
    }

    private static String determineRootDirectoryName(Configuration config) {
        if (!config.has(DIRECTORY) && config.has(GraphDatabaseConfiguration.GRAPH_NAME)) {
            return config.get(GraphDatabaseConfiguration.GRAPH_NAME);
        }
        return config.get(DIRECTORY);
    }

    private static String normalizeDatabase(String database) {
        String normalized = database.endsWith("/") ? database.substring(0, database.length() - 1) : database;
        return normalized.startsWith("/") ? normalized : "/" + normalized;
    }

    private static String buildConnectionString(String endpoint, String databasePath) {
        String base = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        return base + databasePath;
    }

    private static AuthRpcProvider<? super GrpcAuthRpc> buildAuthProvider(Configuration configuration)
            throws BackendException {
        String mode = configuration.get(AUTH_MODE).trim().toLowerCase();
        switch (mode) {
            case "anonymous":
                return NopAuthProvider.INSTANCE;
            case "token":
                String token = configuration.get(AUTH_TOKEN);
                Preconditions.checkArgument(!token.isEmpty(),
                    "storage.ydb.auth-token must be set when auth-mode=token");
                return new TokenAuthProvider(token);
            case "static":
                return new StaticCredentials(configuration.get(AUTH_USERNAME), configuration.get(AUTH_PASSWORD));
            case "environ":
                return new EnvironAuthProvider();
            default:
                throw new PermanentBackendException("Unrecognized storage.ydb.auth-mode: " + mode);
        }
    }
}
