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

package org.janusgraph.diskstorage.ydb.index;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexFeatures;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexProvider;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.ydb.YdbExceptions;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.And;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.query.condition.Not;
import org.janusgraph.graphdb.query.condition.Or;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.tinkerpop.optimize.step.Aggregation;
import org.janusgraph.graphdb.types.ParameterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.NopAuthProvider;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.scheme.SchemeClient;
import tech.ydb.scheme.description.EntryType;
import tech.ydb.scheme.description.ListDirectoryResult;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.AlterTableSettings;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * JanusGraph mixed-index backend (IndexProvider) storing index documents in YDB
 * row tables — one table per index store, one column per indexed field, plus a
 * {@code vector_kmeans_tree} index per vector field for approximate kNN.
 *
 * <p>Supported capabilities: exact/range predicates ({@link Cmp}) on scalar
 * fields and nearest-neighbour search on {@code float[]} fields through the
 * raw-query path ({@code graph.indexQuery(name, YdbVectors.nearest(key, vec))}).
 * Full-text and geo predicates are not supported (YDB has neither today).
 *
 * <p>kNN queries run through the vector index once it has been built with
 * {@link #buildVectorIndex(String, String)}; before that they fall back to an
 * exact scan, which returns identical results at O(n) cost.
 *
 * <p>Configure with {@code index.[X].backend=org.janusgraph.diskstorage.ydb.index.YdbIndexProvider}.
 */
public class YdbIndexProvider implements IndexProvider {

    private static final Logger log = LoggerFactory.getLogger(YdbIndexProvider.class);

    private static final String DOC_ID = "doc_id";
    private static final String INDEX_PREFIX = "knn_";
    private static final String TMP_INDEX_SUFFIX = "_tmp";

    /** Flush a write batch once its serialized parameters approach this size (YDB caps them at 50 MB). */
    private static final long WRITE_BATCH_BYTES = 16L * 1024 * 1024;
    /** Whole-document deletes are id-only, so bound them by count rather than bytes. */
    private static final int DELETE_CHUNK_ROWS = 50_000;

    private static final IndexFeatures FEATURES = new IndexFeatures.Builder()
        .setDefaultStringMapping(Mapping.STRING)
        .supportedStringMappings(Mapping.STRING)
        .supportsCardinality(Cardinality.SINGLE)
        .build();

    /** Distance strategy of vector fields; must be consistent between index build and search. */
    enum Distance {
        COSINE("Knn::CosineDistance", "ASC", "distance=cosine") {
            double score(double raw) {
                return 1.0 - raw;
            }
        },
        EUCLIDEAN("Knn::EuclideanDistance", "ASC", "distance=euclidean") {
            double score(double raw) {
                return -raw;
            }
        },
        MANHATTAN("Knn::ManhattanDistance", "ASC", "distance=manhattan") {
            double score(double raw) {
                return -raw;
            }
        },
        INNER_PRODUCT("Knn::InnerProductSimilarity", "DESC", "similarity=inner_product") {
            double score(double raw) {
                return raw;
            }
        };

        final String function;
        final String order;
        final String withClause;

        Distance(String function, String order, String withClause) {
            this.function = function;
            this.order = order;
            this.withClause = withClause;
        }

        abstract double score(double raw);

        static Distance parse(String value) {
            switch (value.trim().toLowerCase()) {
                case "cosine": return COSINE;
                case "euclidean": return EUCLIDEAN;
                case "manhattan": return MANHATTAN;
                case "inner_product": return INNER_PRODUCT;
                default: throw new IllegalArgumentException("Unknown vector distance: " + value);
            }
        }
    }

    private final GrpcTransport transport;
    private final QueryClient queryClient;
    private final SessionRetryContext retryCtx;
    private final SchemeClient schemeClient;
    private final TableClient tableClient;
    private final tech.ydb.table.SessionRetryContext tableRetryCtx;

    private final String rootPath;
    private final Distance defaultDistance;
    private final int defaultDimension;
    private final boolean useVectorIndex;
    private final int searchTopSize;
    private final int indexLevels;
    private final int indexClusters;
    private final int maxResultSetSize;
    private final long negativeCacheTtlNanos;

    private final Set<String> ensuredTables = ConcurrentHashMap.newKeySet();
    private final Map<String, Set<String>> knownColumns = new ConcurrentHashMap<>();
    // key = store + "/" + column; positive results are permanent (an index only disappears via
    // clearStore/clearStorage, which invalidate it), negatives carry a timestamp and are re-probed
    private final Map<String, IndexProbe> vectorIndexPresent = new ConcurrentHashMap<>();
    // per-field vector distance, recorded at register() so buildVectorIndex builds with the same
    // metric that planKnn queries with (mismatched metric is a hard INTERNAL_ERROR in YDB)
    private final Map<String, Distance> fieldDistance = new ConcurrentHashMap<>();

    private static final class IndexProbe {
        final boolean present;
        final long checkedAtNanos;

        IndexProbe(boolean present, long checkedAtNanos) {
            this.present = present;
            this.checkedAtNanos = checkedAtNanos;
        }
    }

    public YdbIndexProvider(Configuration config) throws BackendException {
        String database = normalize(config.get(YdbIndexConfigOptions.DATABASE));
        this.rootPath = database + "/" + config.get(YdbIndexConfigOptions.DIRECTORY);
        this.defaultDistance = Distance.parse(config.get(YdbIndexConfigOptions.VECTOR_DISTANCE));
        this.defaultDimension = config.get(YdbIndexConfigOptions.VECTOR_DIMENSION);
        this.useVectorIndex = config.get(YdbIndexConfigOptions.USE_VECTOR_INDEX);
        this.searchTopSize = config.get(YdbIndexConfigOptions.SEARCH_TOP_SIZE);
        this.indexLevels = config.get(YdbIndexConfigOptions.VECTOR_INDEX_LEVELS);
        this.indexClusters = config.get(YdbIndexConfigOptions.VECTOR_INDEX_CLUSTERS);
        this.maxResultSetSize = config.get(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE);
        this.negativeCacheTtlNanos = config.get(YdbIndexConfigOptions.VECTOR_INDEX_RECHECK_MS) * 1_000_000L;

        String endpoint = config.get(YdbIndexConfigOptions.ENDPOINT);
        GrpcTransport newTransport;
        try {
            String base = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
            newTransport = GrpcTransport.forConnectionString(base + database)
                .withAuthProvider(NopAuthProvider.INSTANCE)
                .build();
        } catch (RuntimeException e) {
            throw new PermanentBackendException("Could not connect to YDB index backend at " + endpoint, e);
        }
        this.transport = newTransport;
        try {
            this.queryClient = QueryClient.newClient(transport)
                .sessionPoolMaxSize(config.get(YdbIndexConfigOptions.SESSION_POOL_MAX))
                .build();
            this.retryCtx = SessionRetryContext.create(queryClient).idempotent(true).build();
            this.schemeClient = SchemeClient.newClient(transport).build();
            this.tableClient = TableClient.newClient(transport).sessionPoolSize(0, 8).build();
            this.tableRetryCtx = tech.ydb.table.SessionRetryContext.create(tableClient).idempotent(true).build();
        } catch (RuntimeException e) {
            transport.close();
            throw new PermanentBackendException("Could not initialize YDB index clients", e);
        }
    }

    // ------------------------------------------------------------------- SPI

    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx)
            throws BackendException {
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (information.getCardinality() != Cardinality.SINGLE) {
            throw new IllegalArgumentException("Only SINGLE cardinality is supported, got: "
                + information.getCardinality());
        }
        if (dataType == String.class) {
            if (mapping != Mapping.DEFAULT && mapping != Mapping.STRING) {
                throw new IllegalArgumentException("Unsupported string mapping: " + mapping);
            }
        } else if (mapping != Mapping.DEFAULT) {
            throw new IllegalArgumentException("Specified illegal mapping [" + mapping + "] for data type ["
                + dataType + "]");
        }
        if (dataType == float[].class && vectorDimension(information) <= 0) {
            throw new IllegalArgumentException("Vector key '" + key + "' needs a dimension: set the custom "
                + "index parameter 'dimension' or index.[X].ydb.vector-dimension");
        }
        yqlType(dataType); // throws IllegalArgumentException for unsupported types
        ensureTable(store);
        ensureColumn(store, columnName(key), yqlType(dataType));
        if (dataType == float[].class) {
            // remember the per-key distance so buildVectorIndex builds with the same metric
            // planKnn later queries with (a mismatch is a hard INTERNAL_ERROR in YDB)
            fieldDistance.put(store + "/" + columnName(key), distanceOf(information));
        }
    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations,
                       KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        for (Map.Entry<String, Map<String, IndexMutation>> storeEntry : mutations.entrySet()) {
            String store = storeEntry.getKey();
            KeyInformation.StoreRetriever infos = information.get(store);
            ensureTable(store);
            ColumnBatch batch = new ColumnBatch();
            for (Map.Entry<String, IndexMutation> docEntry : storeEntry.getValue().entrySet()) {
                String docId = docEntry.getKey();
                IndexMutation mutation = docEntry.getValue();
                if (mutation.isDeleted()) {
                    batch.delete(docId);
                    continue;
                }
                // deletions first so a same-field addition overrides the field-clear
                if (mutation.hasDeletions()) {
                    for (IndexEntry deletion : mutation.getDeletions()) {
                        batch.clearField(store, infos, docId, deletion.field);
                    }
                }
                if (mutation.hasAdditions()) {
                    for (IndexEntry addition : mutation.getAdditions()) {
                        batch.setField(store, infos, docId, addition.field, addition.value);
                    }
                }
            }
            writeBatch(store, batch);
        }
    }

    @Override
    public void restore(Map<String, Map<String, List<IndexEntry>>> documents,
                        KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        for (Map.Entry<String, Map<String, List<IndexEntry>>> storeEntry : documents.entrySet()) {
            String store = storeEntry.getKey();
            KeyInformation.StoreRetriever infos = information.get(store);
            ensureTable(store);
            ColumnBatch batch = new ColumnBatch();
            for (Map.Entry<String, List<IndexEntry>> docEntry : storeEntry.getValue().entrySet()) {
                String docId = docEntry.getKey();
                // restore fully replaces the document: delete the row, then re-add the new fields
                batch.delete(docId);
                List<IndexEntry> entries = docEntry.getValue();
                if (entries != null && !entries.isEmpty()) {
                    for (IndexEntry entry : entries) {
                        batch.setField(store, infos, docId, entry.field, entry.value);
                    }
                }
            }
            writeBatch(store, batch);
        }
    }

    /**
     * One store's pending index writes, grouped by column so the emitted query text is
     * O(number of indexed fields), not O(number of documents): each column becomes one
     * {@code UPSERT ... SELECT * FROM AS_TABLE($rows)} that touches only doc_id and that
     * column, and whole-document deletes become one {@code DELETE ... WHERE doc_id IN $ids}.
     */
    private final class ColumnBatch {
        final List<String> deletes = new ArrayList<>();
        // column -> item type (the non-optional element type of the column)
        final Map<String, Type> columnType = new LinkedHashMap<>();
        // column -> (docId -> optional value; empty optional clears the field)
        final Map<String, LinkedHashMap<String, Value<?>>> columnRows = new LinkedHashMap<>();

        void delete(String docId) {
            deletes.add(docId);
        }

        void clearField(String store, KeyInformation.StoreRetriever infos, String docId, String field)
                throws BackendException {
            KeyInformation info = keyInfo(infos, field);
            String column = ensureColumn(store, columnName(field), yqlType(info.getDataType()));
            Type type = typeOf(info.getDataType());
            columnType.put(column, type);
            rows(column).put(docId, OptionalType.of(type).emptyValue());
        }

        void setField(String store, KeyInformation.StoreRetriever infos, String docId, String field, Object value)
                throws BackendException {
            KeyInformation info = keyInfo(infos, field);
            String column = ensureColumn(store, columnName(field), yqlType(info.getDataType()));
            columnType.put(column, typeOf(info.getDataType()));
            rows(column).put(docId, toValue(info, field, value).makeOptional());
        }

        private LinkedHashMap<String, Value<?>> rows(String column) {
            return columnRows.computeIfAbsent(column, c -> new LinkedHashMap<>());
        }

        boolean isEmpty() {
            return deletes.isEmpty() && columnRows.isEmpty();
        }
    }

    private void writeBatch(String store, ColumnBatch batch) throws BackendException {
        if (batch.isEmpty()) {
            return;
        }
        String table = "`" + tablePath(store) + "`";
        StatementBuffer buffer = new StatementBuffer();

        for (int i = 0; i < batch.deletes.size(); i += DELETE_CHUNK_ROWS) {
            List<String> part = batch.deletes.subList(i, Math.min(i + DELETE_CHUNK_ROWS, batch.deletes.size()));
            String param = buffer.nextParam();
            buffer.declares.append("DECLARE ").append(param).append(" AS List<Utf8>;\n");
            buffer.statements.append("DELETE FROM ").append(table).append(" WHERE ").append(DOC_ID)
                .append(" IN ").append(param).append(";\n");
            buffer.params.put(param, ListType.of(PrimitiveType.Text).newValue(part.stream()
                .map(PrimitiveValue::newText).collect(Collectors.toList())));
            buffer.bytes += part.size() * 24L;
            buffer.flushIfLarge();
        }

        for (Map.Entry<String, LinkedHashMap<String, Value<?>>> col : batch.columnRows.entrySet()) {
            String column = col.getKey();
            StructType rowType = StructType.of(DOC_ID, PrimitiveType.Text,
                column, OptionalType.of(batch.columnType.get(column)));
            List<Value<?>> rows = new ArrayList<>();
            long rowBytes = 0;
            for (Map.Entry<String, Value<?>> row : col.getValue().entrySet()) {
                rows.add(rowType.newValue(DOC_ID, PrimitiveValue.newText(row.getKey()), column, row.getValue()));
                rowBytes += estimateBytes(row.getValue()) + row.getKey().length() + 16L;
                if (rowBytes >= WRITE_BATCH_BYTES) {
                    emitUpsert(buffer, table, rowType, rows, rowBytes);
                    rows = new ArrayList<>();
                    rowBytes = 0;
                }
            }
            if (!rows.isEmpty()) {
                emitUpsert(buffer, table, rowType, rows, rowBytes);
            }
        }
        buffer.flush();
    }

    private void emitUpsert(StatementBuffer buffer, String table, StructType rowType,
                            List<Value<?>> rows, long rowBytes) throws BackendException {
        String param = buffer.nextParam();
        buffer.declares.append("DECLARE ").append(param).append(" AS ")
            .append(ListType.of(rowType)).append(";\n");
        buffer.statements.append("UPSERT INTO ").append(table)
            .append(" SELECT * FROM AS_TABLE(").append(param).append(");\n");
        buffer.params.put(param, ListType.of(rowType).newValue(rows));
        buffer.bytes += rowBytes;
        buffer.flushIfLarge();
    }

    /** Accumulates statements + params and flushes them as one query once they get large. */
    private final class StatementBuffer {
        StringBuilder declares = new StringBuilder();
        StringBuilder statements = new StringBuilder();
        Params params = Params.create();
        long bytes = 0;
        private int seq = 0;

        String nextParam() {
            return "$p" + seq++;
        }

        void flushIfLarge() throws BackendException {
            if (bytes >= WRITE_BATCH_BYTES) {
                flush();
            }
        }

        void flush() throws BackendException {
            if (statements.length() == 0) {
                return;
            }
            executeWrite(declares.append(statements).toString(), params);
            declares = new StringBuilder();
            statements = new StringBuilder();
            params = Params.create();
            bytes = 0;
        }
    }

    private static long estimateBytes(Value<?> value) {
        if (value instanceof tech.ydb.table.values.OptionalValue) {
            tech.ydb.table.values.OptionalValue opt = (tech.ydb.table.values.OptionalValue) value;
            if (!opt.isPresent()) {
                return 1;
            }
            return estimateBytes(opt.get());
        }
        if (value instanceof PrimitiveValue) {
            PrimitiveValue pv = (PrimitiveValue) value;
            try {
                return pv.getBytesUnsafe().length;
            } catch (RuntimeException notBytes) {
                return 8;
            }
        }
        return 8;
    }

    @Override
    public Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx)
            throws BackendException {
        String store = query.getStore();
        StringBuilder where = new StringBuilder();
        Params params = Params.create();
        compileCondition(query.getCondition(), information.get(store), where, params, new int[]{0});

        StringBuilder yql = new StringBuilder(declaresOf(params))
            .append("SELECT ").append(DOC_ID).append(" FROM `").append(tablePath(store)).append('`');
        if (where.length() > 0) {
            yql.append(" WHERE ").append(where);
        }
        yql.append(" ORDER BY ");
        for (IndexQuery.OrderEntry order : query.getOrder()) {
            yql.append(columnName(order.getKey()))
                .append(order.getOrder() == org.janusgraph.graphdb.internal.Order.DESC ? " DESC" : " ASC")
                .append(", ");
        }
        yql.append(DOC_ID);
        if (query.hasLimit()) {
            yql.append(" LIMIT ").append(query.getLimit());
        }

        ResultSetReader rs = readOrNull(yql.toString(), params);
        List<String> ids = new ArrayList<>();
        while (rs != null && rs.next()) {
            ids.add(rs.getColumn(DOC_ID).getText());
        }
        return ids.stream();
    }

    @Override
    public Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever information,
                                                 BaseTransaction tx) throws BackendException {
        KnnPlan plan = planKnn(query, information);
        ResultSetReader rs = readOrNull(plan.yql, plan.params);
        List<RawQuery.Result<String>> results = new ArrayList<>();
        int skipped = 0;
        while (rs != null && rs.next()) {
            if (!rs.getColumn("score").isOptionalItemPresent()) {
                continue; // dimension/format mismatch produces NULL distances
            }
            if (skipped < query.getOffset()) {
                skipped++;
                continue;
            }
            results.add(new RawQuery.Result<>(rs.getColumn(DOC_ID).getText(),
                plan.distance.score(rs.getColumn("score").getFloat())));
        }
        return results.stream();
    }

    @Override
    public Long totals(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx)
            throws BackendException {
        KnnPlan plan = planKnn(query, information);
        String yql = "SELECT COUNT(*) AS c FROM `" + tablePath(plan.store) + "` WHERE "
            + plan.column + " IS NOT NULL;";
        ResultSetReader rs = readOrNull(yql, Params.empty());
        long total = 0;
        if (rs != null && rs.next()) {
            total = rs.getColumn("c").getUint64();
        }
        long adjusted = Math.max(0, total - query.getOffset());
        if (query.hasLimit()) {
            adjusted = Math.min(adjusted, query.getLimit());
        }
        return adjusted;
    }

    @Override
    public Number queryAggregation(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx,
                                   Aggregation aggregation) throws BackendException {
        if (!"COUNT".equals(aggregation.getType().name())) {
            throw new UnsupportedOperationException("Aggregation not supported: " + aggregation.getType());
        }
        StringBuilder where = new StringBuilder();
        Params params = Params.create();
        compileCondition(query.getCondition(), information.get(query.getStore()), where, params, new int[]{0});
        String yql = declaresOf(params) + "SELECT COUNT(*) AS c FROM `" + tablePath(query.getStore()) + "`"
            + (where.length() > 0 ? " WHERE " + where : "") + ";";
        ResultSetReader rs = readOrNull(yql, params);
        return rs != null && rs.next() ? rs.getColumn("c").getUint64() : 0L;
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate predicate) {
        Class<?> dataType = information.getDataType();
        if (information.getCardinality() != Cardinality.SINGLE || dataType == float[].class) {
            return false; // kNN flows through raw queries, not predicates
        }
        if (dataType == Boolean.class || dataType == UUID.class) {
            return predicate == Cmp.EQUAL || predicate == Cmp.NOT_EQUAL;
        }
        if (dataType == String.class) {
            Mapping mapping = Mapping.getMapping(information);
            return (mapping == Mapping.DEFAULT || mapping == Mapping.STRING) && predicate instanceof Cmp;
        }
        if (Number.class.isAssignableFrom(dataType) || dataType == Date.class || dataType == Instant.class) {
            return predicate instanceof Cmp;
        }
        return false;
    }

    @Override
    public boolean supports(KeyInformation information) {
        if (information.getCardinality() != Cardinality.SINGLE) {
            return false;
        }
        Class<?> dataType = information.getDataType();
        Mapping mapping = Mapping.getMapping(information);
        if (dataType == String.class) {
            return mapping == Mapping.DEFAULT || mapping == Mapping.STRING;
        }
        if (mapping != Mapping.DEFAULT) {
            return false;
        }
        return dataType == float[].class || dataType == Boolean.class || dataType == UUID.class
            || dataType == Date.class || dataType == Instant.class
            || Number.class.isAssignableFrom(dataType);
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        IndexProvider.checkKeyValidity(key);
        return key.replace(' ', IndexProvider.REPLACEMENT_CHAR);
    }

    @Override
    public IndexFeatures getFeatures() {
        return FEATURES;
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) {
        // writes are applied synchronously in mutate/restore; nothing to commit here
        return new YdbIndexTx(config);
    }

    @Override
    public void close() throws BackendException {
        try {
            tableClient.close();
            schemeClient.close();
            queryClient.close();
            transport.close();
        } catch (Exception e) {
            throw new PermanentBackendException("Could not close YDB index provider", e);
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            Result<ListDirectoryResult> listing = schemeClient.listDirectory(rootPath).join();
            if (!listing.isSuccess()) {
                if (listing.getStatus().getCode() == StatusCode.SCHEME_ERROR) {
                    return;
                }
                throw YdbExceptions.fromStatus(listing.getStatus(), "list index directory");
            }
            for (tech.ydb.scheme.description.Entry child : listing.getValue().getEntryChildren()) {
                if (child.getType() == EntryType.TABLE) {
                    // IF EXISTS so a retried drop (idempotent retry after a lost response) is a no-op
                    executeDdl("DROP TABLE IF EXISTS `" + rootPath + "/" + child.getName() + "`;");
                }
            }
            Status removed = schemeClient.removeDirectory(rootPath).join();
            if (!removed.isSuccess()) {
                log.warn("Could not remove YDB index directory {}: {}", rootPath, removed);
            }
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "clear index storage");
        } finally {
            ensuredTables.clear();
            knownColumns.clear();
            vectorIndexPresent.clear();
            fieldDistance.clear();
        }
    }

    @Override
    public void clearStore(String storeName) throws BackendException {
        executeDdl("DROP TABLE IF EXISTS `" + tablePath(storeName) + "`;");
        ensuredTables.remove(storeName);
        knownColumns.remove(storeName);
        // drop cached per-column state for this store so a rebuilt store is re-probed, not
        // served from a stale "index present" entry that would make kNN silently return empty
        String prefix = storeName + "/";
        vectorIndexPresent.keySet().removeIf(key -> key.startsWith(prefix));
        fieldDistance.keySet().removeIf(key -> key.startsWith(prefix));
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            Result<ListDirectoryResult> listing = schemeClient.listDirectory(rootPath).join();
            if (!listing.isSuccess()) {
                if (listing.getStatus().getCode() == StatusCode.SCHEME_ERROR) {
                    return false;
                }
                throw YdbExceptions.fromStatus(listing.getStatus(), "list index directory");
            }
            return listing.getValue().getEntryChildren().stream()
                .anyMatch(child -> child.getType() == EntryType.TABLE);
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "index exists check");
        }
    }

    // -------------------------------------------------------- vector index ops

    /**
     * Builds (or rebuilds) the {@code vector_kmeans_tree} index for a vector field with the
     * default distance strategy (or the per-key {@code distance} recorded at register time):
     * a new index is built online under a temporary name and atomically swapped in.
     * Until this has been called at least once, kNN queries use exact scans.
     * Recommended after initial data load and periodically when the data
     * distribution drifts (YDB never recalculates centroids on updates).
     */
    public void buildVectorIndex(String store, String field) throws BackendException {
        String column = columnName(field);
        buildVectorIndex(store, field, fieldDistance.getOrDefault(store + "/" + column, defaultDistance));
    }

    /**
     * Builds the vector index with an explicit distance strategy. The metric MUST match the
     * one used by kNN queries for this field; a mismatch is a hard INTERNAL_ERROR in YDB.
     */
    public void buildVectorIndex(String store, String field, Distance distance) throws BackendException {
        String column = columnName(field);
        int dimension = defaultDimension > 0 ? defaultDimension : probeDimension(store, column);
        String indexName = INDEX_PREFIX + column;
        String tmpName = indexName + TMP_INDEX_SUFFIX;
        // drop any leftover temp index from an interrupted earlier build so this is re-runnable
        // (YDB's ADD INDEX cannot overwrite an existing index, and has no DROP INDEX IF EXISTS)
        if (tableIndexExists(store, tmpName)) {
            executeDdl("ALTER TABLE `" + tablePath(store) + "` DROP INDEX " + tmpName + ";");
        }
        executeDdl("ALTER TABLE `" + tablePath(store) + "` ADD INDEX " + tmpName
            + " GLOBAL USING vector_kmeans_tree ON (" + column + ") COVER (" + column + ")"
            + " WITH (" + distance.withClause + ", vector_type=\"float\", vector_dimension=" + dimension
            + ", levels=" + indexLevels + ", clusters=" + indexClusters + ");");
        try {
            Status status = tableRetryCtx.supplyStatus(session -> session.alterTable(tablePath(store),
                new AlterTableSettings().addRenameIndex(tmpName, indexName, true))).join();
            if (!status.isSuccess()) {
                throw YdbExceptions.fromStatus(status, "swap vector index");
            }
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "swap vector index");
        }
        vectorIndexPresent.put(store + "/" + column, new IndexProbe(true, System.nanoTime()));
        log.info("Built vector index {} on {} with {} (dimension {})",
            indexName, tablePath(store), distance.withClause, dimension);
    }

    private int probeDimension(String store, String column) throws BackendException {
        ResultSetReader rs = readOrNull("SELECT Len(" + column + ") AS l FROM `" + tablePath(store)
            + "` WHERE " + column + " IS NOT NULL LIMIT 1;", Params.empty());
        if (rs != null && rs.next()) {
            long len = rs.getColumn("l").getUint32();
            if (len > 1 && (len - 1) % Float.BYTES == 0) {
                return (int) ((len - 1) / Float.BYTES);
            }
        }
        throw new PermanentBackendException("Cannot determine vector dimension for " + store + "." + column
            + ": no vectors stored yet and no configured vector-dimension");
    }

    /**
     * Whether a vector index exists for the field. A positive result is cached permanently
     * (an index only disappears via clearStore/clearStorage, which invalidate the entry); a
     * negative result is re-probed after {@link #NEGATIVE_CACHE_TTL_NANOS} so an index built
     * elsewhere is eventually adopted; a probe error is never cached (so it never pins the
     * field to the O(n) exact-scan fallback until a JVM restart).
     */
    private boolean vectorIndexExists(String store, String column) {
        String cacheKey = store + "/" + column;
        IndexProbe cached = vectorIndexPresent.get(cacheKey);
        if (cached != null && (cached.present || System.nanoTime() - cached.checkedAtNanos < negativeCacheTtlNanos)) {
            return cached.present;
        }
        Boolean present = describeIndexPresence(store, INDEX_PREFIX + column);
        if (present == null) {
            return false; // transient/scheme error — do not cache, re-probe next time
        }
        vectorIndexPresent.put(cacheKey, new IndexProbe(present, System.nanoTime()));
        return present;
    }

    /** Whether {@code indexName} exists on the store table; false if the probe fails (not cached). */
    private boolean tableIndexExists(String store, String indexName) {
        return Boolean.TRUE.equals(describeIndexPresence(store, indexName));
    }

    /** Returns TRUE/FALSE if the index presence could be determined, or null on a probe error. */
    private Boolean describeIndexPresence(String store, String indexName) {
        try {
            Result<TableDescription> description = tableRetryCtx
                .supplyResult(session -> session.describeTable(tablePath(store))).join();
            if (!description.isSuccess()) {
                return null;
            }
            return description.getValue().getIndexes().stream()
                .map(TableIndex::getName)
                .anyMatch(name -> name.equals(indexName));
        } catch (RuntimeException e) {
            log.debug("Index probe failed for {}.{}, treating as absent this time", store, indexName, e);
            return null;
        }
    }

    // ---------------------------------------------------------------- helpers

    private static final class KnnPlan {
        final String store;
        final String column;
        final Distance distance;
        final String yql;
        final Params params;

        KnnPlan(String store, String column, Distance distance, String yql, Params params) {
            this.store = store;
            this.column = column;
            this.distance = distance;
            this.yql = yql;
            this.params = params;
        }
    }

    private KnnPlan planKnn(RawQuery query, KeyInformation.IndexRetriever information) throws BackendException {
        YdbVectors.ParsedKnn knn = YdbVectors.parse(query.getQuery());
        if (knn == null) {
            throw new PermanentBackendException("Unsupported raw query syntax: '" + query.getQuery()
                + "' (expected <field>:knn:<base64-vector>, see YdbVectors.nearest)");
        }
        String store = query.getStore();
        String column = columnName(knn.field);
        Distance distance = distanceOf(information.get(store, knn.field));
        int fetch = (int) Math.min((long) Integer.MAX_VALUE,
            (long) query.getOffset() + (query.hasLimit() ? query.getLimit() : maxResultSetSize));
        Params params = Params.of("$q", PrimitiveValue.newBytes(knn.vector));
        String yql;
        if (useVectorIndex && vectorIndexExists(store, column)) {
            String fn = distance.function + "(" + column + ", $q)";
            yql = "PRAGMA ydb.KMeansTreeSearchTopSize = \"" + searchTopSize + "\";\n"
                + "DECLARE $q AS String;\n"
                + "SELECT " + DOC_ID + ", " + fn + " AS score"
                + " FROM `" + tablePath(store) + "` VIEW " + INDEX_PREFIX + column
                + " ORDER BY " + fn + " " + distance.order  // the Knn call must appear literally
                + " LIMIT " + fetch + ";";
        } else {
            yql = "DECLARE $q AS String;\n"
                + "SELECT " + DOC_ID + ", " + distance.function + "(" + column + ", $q) AS score"
                + " FROM `" + tablePath(store) + "`"
                + " WHERE " + column + " IS NOT NULL"
                + " ORDER BY score " + distance.order
                + " LIMIT " + fetch + ";";
        }
        return new KnnPlan(store, column, distance, yql, params);
    }

    private Distance distanceOf(KeyInformation information) {
        if (information != null) {
            for (Parameter parameter : ParameterType.getCustomParameters(information.getParameters())) {
                if ("distance".equals(parameter.key())) {
                    return Distance.parse(String.valueOf(parameter.value()));
                }
            }
        }
        return defaultDistance;
    }

    private int vectorDimension(KeyInformation information) {
        for (Parameter parameter : ParameterType.getCustomParameters(information.getParameters())) {
            if ("dimension".equals(parameter.key())) {
                return ((Number) parameter.value()).intValue();
            }
        }
        return defaultDimension;
    }

    private void compileCondition(Condition<?> condition, KeyInformation.StoreRetriever infos,
                                  StringBuilder out, Params params, int[] counter) {
        if (condition instanceof PredicateCondition) {
            PredicateCondition<?, ?> atom = (PredicateCondition<?, ?>) condition;
            String field = atom.getKey().toString();
            KeyInformation info = infos.get(field);
            if (info == null) {
                throw new IllegalArgumentException("Unknown field: " + field);
            }
            if (!(atom.getPredicate() instanceof Cmp)) {
                throw new IllegalArgumentException("Predicate not supported: " + atom.getPredicate());
            }
            String column = columnName(field);
            Object value = atom.getValue();
            Cmp cmp = (Cmp) atom.getPredicate();
            if (value == null) {
                if (cmp == Cmp.EQUAL) {
                    out.append(column).append(" IS NULL");
                } else if (cmp == Cmp.NOT_EQUAL) {
                    out.append(column).append(" IS NOT NULL");
                } else {
                    throw new IllegalArgumentException("Null value with predicate: " + cmp);
                }
                return;
            }
            String param = "$c" + counter[0]++;
            params.put(param, toValue(info, field, value));
            switch (cmp) {
                case EQUAL: out.append(column).append(" = ").append(param); break;
                // inclusive of missing fields: JanusGraph re-filters supersets in memory
                case NOT_EQUAL: out.append('(').append(column).append(" IS NULL OR ")
                    .append(column).append(" != ").append(param).append(')'); break;
                case LESS_THAN: out.append(column).append(" < ").append(param); break;
                case LESS_THAN_EQUAL: out.append(column).append(" <= ").append(param); break;
                case GREATER_THAN: out.append(column).append(" > ").append(param); break;
                case GREATER_THAN_EQUAL: out.append(column).append(" >= ").append(param); break;
                default: throw new IllegalArgumentException("Predicate not supported: " + cmp);
            }
        } else if (condition instanceof Not) {
            out.append("NOT (");
            compileCondition(((Not<?>) condition).getChild(), infos, out, params, counter);
            out.append(')');
        } else if (condition instanceof And || condition instanceof Or) {
            String joiner = condition instanceof And ? " AND " : " OR ";
            out.append('(');
            boolean first = true;
            for (Condition<?> child : condition.getChildren()) {
                if (!first) {
                    out.append(joiner);
                }
                compileCondition(child, infos, out, params, counter);
                first = false;
            }
            out.append(')');
        } else {
            throw new IllegalArgumentException("Condition not supported: " + condition);
        }
    }

    private KeyInformation keyInfo(KeyInformation.StoreRetriever infos, String field) {
        KeyInformation info = infos.get(field);
        if (info == null) {
            throw new IllegalArgumentException("Unknown field: " + field);
        }
        return info;
    }

    private String tablePath(String store) {
        return rootPath + "/" + store;
    }

    private void ensureTable(String store) throws BackendException {
        if (ensuredTables.contains(store)) {
            return;
        }
        synchronized (this) {
            if (ensuredTables.contains(store)) {
                return;
            }
            try {
                Status status = schemeClient.makeDirectories(rootPath).join();
                if (!status.isSuccess()) {
                    throw YdbExceptions.fromStatus(status, "create index directory");
                }
            } catch (RuntimeException e) {
                throw YdbExceptions.fromThrowable(e, "create index directory");
            }
            executeDdl("CREATE TABLE IF NOT EXISTS `" + tablePath(store) + "` ("
                + DOC_ID + " Utf8 NOT NULL, PRIMARY KEY (" + DOC_ID + "));");
            ensuredTables.add(store);
        }
    }

    private String ensureColumn(String store, String column, String yqlType) throws BackendException {
        Set<String> columns = knownColumns.get(store);
        if (columns != null && columns.contains(column)) {
            return column;
        }
        synchronized (this) {
            columns = knownColumns.computeIfAbsent(store, s -> ConcurrentHashMap.newKeySet());
            if (columns.contains(column)) {
                return column;
            }
            try {
                Result<TableDescription> description = tableRetryCtx
                    .supplyResult(session -> session.describeTable(tablePath(store))).join();
                if (description.isSuccess()) {
                    description.getValue().getColumns()
                        .forEach(c -> knownColumns.get(store).add(c.getName()));
                }
            } catch (RuntimeException e) {
                throw YdbExceptions.fromThrowable(e, "describe index table");
            }
            if (!columns.contains(column)) {
                executeDdl("ALTER TABLE `" + tablePath(store) + "` ADD COLUMN " + column + " " + yqlType + ";");
                columns.add(column);
            }
            return column;
        }
    }

    /** Maps a field name to a safe YDB column name (deterministic, no metadata needed). */
    static String columnName(String field) {
        StringBuilder sb = new StringBuilder("f_");
        boolean clean = true;
        for (int i = 0; i < field.length(); i++) {
            char c = field.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
                sb.append(c);
            } else {
                clean = false;
                sb.append('_');
            }
        }
        if (!clean) {
            sb.append('x').append(Integer.toHexString(field.hashCode()));
        }
        return sb.toString();
    }

    private static String yqlType(Class<?> dataType) {
        if (dataType == String.class || dataType == UUID.class || dataType == Character.class) {
            return "Utf8";
        }
        if (dataType == Boolean.class) {
            return "Bool";
        }
        if (dataType == Float.class || dataType == Double.class) {
            return "Double";
        }
        if (Number.class.isAssignableFrom(dataType)) {
            return "Int64";
        }
        if (dataType == Date.class || dataType == Instant.class) {
            return "Int64"; // epoch microseconds
        }
        if (dataType == float[].class) {
            return "String"; // serialized FloatVector
        }
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    private static Type typeOf(Class<?> dataType) {
        switch (yqlType(dataType)) {
            case "Utf8": return PrimitiveType.Text;
            case "Bool": return PrimitiveType.Bool;
            case "Double": return PrimitiveType.Double;
            case "Int64": return PrimitiveType.Int64;
            default: return PrimitiveType.Bytes;
        }
    }

    private Value<?> toValue(KeyInformation info, String field, Object value) {
        Class<?> dataType = info.getDataType();
        if (dataType == float[].class) {
            float[] vector = (float[]) value;
            int expected = vectorDimension(info);
            if (expected > 0 && vector.length != expected) {
                throw new IllegalArgumentException("Vector dimension mismatch for field '" + field
                    + "': expected " + expected + ", got " + vector.length);
            }
            return PrimitiveValue.newBytes(YdbVectors.toBinaryString(vector));
        }
        if (dataType == String.class) {
            return PrimitiveValue.newText((String) value);
        }
        if (dataType == UUID.class || dataType == Character.class) {
            return PrimitiveValue.newText(value.toString());
        }
        if (dataType == Boolean.class) {
            return PrimitiveValue.newBool((Boolean) value);
        }
        if (dataType == Float.class || dataType == Double.class) {
            return PrimitiveValue.newDouble(((Number) value).doubleValue());
        }
        if (dataType == Date.class) {
            return PrimitiveValue.newInt64(((Date) value).getTime() * 1000L);
        }
        if (dataType == Instant.class) {
            Instant instant = (Instant) value;
            return PrimitiveValue.newInt64(instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L);
        }
        if (Number.class.isAssignableFrom(dataType)) {
            return PrimitiveValue.newInt64(((Number) value).longValue());
        }
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

    private static String declaresOf(Params params) {
        StringBuilder declares = new StringBuilder();
        params.values().forEach((name, value) ->
            declares.append("DECLARE ").append(name).append(" AS ").append(value.getType()).append(";\n"));
        return declares.toString();
    }

    /** Runs a read query; returns null when the store table does not exist yet. */
    private ResultSetReader readOrNull(String yql, Params params) throws BackendException {
        try {
            Result<QueryReader> result = retryCtx.supplyResult(session ->
                QueryReader.readFrom(session.createQuery(yql, TxMode.SNAPSHOT_RO, params))).join();
            if (!result.isSuccess()) {
                if (result.getStatus().getCode() == StatusCode.SCHEME_ERROR) {
                    return null;
                }
                throw YdbExceptions.fromStatus(result.getStatus(), "index query");
            }
            return result.getValue().getResultSet(0);
        } catch (RuntimeException e) {
            BackendException mapped = YdbExceptions.fromThrowable(e, "index query");
            if (mapped.getCause() instanceof tech.ydb.core.UnexpectedResultException
                && ((tech.ydb.core.UnexpectedResultException) mapped.getCause()).getStatus().getCode()
                    == StatusCode.SCHEME_ERROR) {
                return null;
            }
            throw mapped;
        }
    }

    private void executeWrite(String yql, Params params) throws BackendException {
        try {
            Result<tech.ydb.query.result.QueryInfo> result = retryCtx.supplyResult(session ->
                session.createQuery(yql, TxMode.SERIALIZABLE_RW, params).execute()).join();
            if (!result.isSuccess()) {
                throw YdbExceptions.fromStatus(result.getStatus(), "index mutation");
            }
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "index mutation");
        }
    }

    private void executeDdl(String yql) throws BackendException {
        try {
            Result<tech.ydb.query.result.QueryInfo> result = retryCtx.supplyResult(session ->
                session.createQuery(yql, TxMode.NONE).execute()).join();
            if (!result.isSuccess()) {
                throw YdbExceptions.fromStatus(result.getStatus(), "index ddl");
            }
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "index ddl");
        }
    }

    private static String normalize(String database) {
        String normalized = database.endsWith("/") ? database.substring(0, database.length() - 1) : database;
        return normalized.startsWith("/") ? normalized : "/" + normalized;
    }

    private static final class YdbIndexTx implements BaseTransactionConfigurable {
        private final BaseTransactionConfig config;

        private YdbIndexTx(BaseTransactionConfig config) {
            this.config = config;
        }

        @Override
        public BaseTransactionConfig getConfiguration() {
            return config;
        }

        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }
    }
}
