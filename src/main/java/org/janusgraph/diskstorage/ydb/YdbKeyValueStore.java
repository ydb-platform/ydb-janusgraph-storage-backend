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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.Result;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

/**
 * One JanusGraph store backed by one YDB row table with schema
 * {@code (key String NOT NULL, value String, PRIMARY KEY(key))}. YDB orders the
 * primary key by unsigned lexicographic byte comparison, which is exactly the
 * order JanusGraph requires from an ordered key-value store.
 *
 * <p>All range reads run through a merging scan that overlays the transaction's
 * buffered writes onto the rows fetched from YDB, so a transaction always
 * observes its own writes; in {@code serializable_rw} mode the buffer is empty
 * and the scan degenerates to a plain paginated range read.
 */
public class YdbKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(YdbKeyValueStore.class);

    private static final String KEY_COLUMN = "key";
    private static final String VALUE_COLUMN = "value";

    private final String name;
    private final String tablePath;
    private final YdbStoreManager manager;
    private final int scanPageSize;
    private final int sliceBatchSize;

    private final String getQuery;
    private final String insertQuery;
    private final String deleteQuery;
    private final String firstPageQuery;
    private final String nextPageQuery;

    private volatile boolean isOpen;

    YdbKeyValueStore(String name, String tablePath, YdbStoreManager manager, int scanPageSize, int sliceBatchSize) {
        this.name = name;
        this.tablePath = tablePath;
        this.manager = manager;
        this.scanPageSize = scanPageSize;
        this.sliceBatchSize = sliceBatchSize;
        this.isOpen = true;

        String table = "`" + tablePath + "`";
        this.getQuery = "DECLARE $key AS String; "
            + "SELECT " + VALUE_COLUMN + " FROM " + table + " WHERE " + KEY_COLUMN + " = $key;";
        this.insertQuery = "DECLARE $key AS String; DECLARE $value AS String; "
            + "UPSERT INTO " + table + " (" + KEY_COLUMN + ", " + VALUE_COLUMN + ") VALUES ($key, $value);";
        this.deleteQuery = "DECLARE $key AS String; "
            + "DELETE FROM " + table + " WHERE " + KEY_COLUMN + " = $key;";
        this.firstPageQuery = rangeQuery(table, ">=");
        this.nextPageQuery = rangeQuery(table, ">");
    }

    private static String rangeQuery(String table, String startOp) {
        return "DECLARE $start AS String; DECLARE $end AS String; DECLARE $limit AS Uint64; "
            + "SELECT " + KEY_COLUMN + ", " + VALUE_COLUMN + " FROM " + table
            + " WHERE " + KEY_COLUMN + " " + startOp + " $start AND " + KEY_COLUMN + " < $end"
            + " ORDER BY " + KEY_COLUMN + " LIMIT $limit;";
    }

    String getTablePath() {
        return tablePath;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        YdbTx tx = YdbTx.of(txh);
        byte[] rawKey = key.as(StaticBuffer.ARRAY_FACTORY);
        YdbTx.BufferLookup buffered = tx.bufferedGet(tablePath, rawKey);
        if (buffered.hit) {
            return buffered.value == null ? null : StaticArrayBuffer.of(buffered.value);
        }
        QueryReader reader = tx.executeRead(getQuery, Params.of("$key", PrimitiveValue.newBytes(rawKey)));
        ResultSetReader rs = reader.getResultSet(0);
        if (!rs.next()) {
            return null;
        }
        return StaticArrayBuffer.of(rs.getColumn(VALUE_COLUMN).getBytes());
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key, txh) != null;
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer ttl) throws BackendException {
        // TTL is not supported: cellTTL/storeTTL are disabled in the store features
        YdbTx tx = YdbTx.of(txh);
        if (tx.isInteractive()) {
            tx.executeInTx(insertQuery,
                Params.of("$key", bytesValue(key), "$value", bytesValue(value)), true);
        } else {
            tx.bufferInsert(tablePath, key.as(StaticBuffer.ARRAY_FACTORY), value.as(StaticBuffer.ARRAY_FACTORY));
        }
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        YdbTx tx = YdbTx.of(txh);
        if (tx.isInteractive()) {
            tx.executeInTx(deleteQuery, Params.of("$key", bytesValue(key)), true);
        } else {
            tx.bufferDelete(tablePath, key.as(StaticBuffer.ARRAY_FACTORY));
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        // Deliberately a no-op. In serializable_rw mode YDB's serializable transactions
        // detect conflicting commits; in the read-only read modes there is no locking at
        // all by design (last-write-wins), same as FoundationDB's read_committed modes.
        if (txh == null) {
            log.warn("Attempt to acquire lock with null transaction");
        }
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        YdbTx tx = YdbTx.of(txh);
        List<KeyValueEntry> entries = new ArrayList<>();
        mergeScan(tx, query, entries, null, 0);
        return new ListRecordIterator(entries);
    }

    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh)
            throws BackendException {
        YdbTx tx = YdbTx.of(txh);
        Map<KVQuery, RecordIterator<KeyValueEntry>> result = new HashMap<>(queries.size());
        List<List<KVQuery>> batches = new ArrayList<>();
        for (int from = 0; from < queries.size(); from += sliceBatchSize) {
            batches.add(queries.subList(from, Math.min(from + sliceBatchSize, queries.size())));
        }
        if (tx.isInteractive()) {
            // one session — batches execute sequentially, each as a single round trip
            for (List<KVQuery> batch : batches) {
                QueryReader reader = tx.executeInTx(batchQueryText(batch.size()), batchQueryParams(batch), false);
                collectBatchResults(tx, batch, reader, result);
            }
        } else {
            // pooled read-only queries — all batches fly in parallel
            List<CompletableFuture<Result<QueryReader>>> futures = new ArrayList<>(batches.size());
            for (List<KVQuery> batch : batches) {
                futures.add(manager.readOutsideTxAsync(batchQueryText(batch.size()), batchQueryParams(batch)));
            }
            for (int i = 0; i < batches.size(); i++) {
                QueryReader reader = manager.awaitRead(futures.get(i));
                collectBatchResults(tx, batches.get(i), reader, result);
            }
        }
        return result;
    }

    /** Multi-statement text of one slice batch: one SELECT (own result set) per range. */
    private String batchQueryText(int size) {
        StringBuilder declares = new StringBuilder();
        StringBuilder statements = new StringBuilder();
        for (int i = 0; i < size; i++) {
            declares.append("DECLARE $start").append(i).append(" AS String; ")
                .append("DECLARE $end").append(i).append(" AS String; ")
                .append("DECLARE $limit").append(i).append(" AS Uint64;\n");
            statements.append("SELECT ").append(KEY_COLUMN).append(", ").append(VALUE_COLUMN)
                .append(" FROM `").append(tablePath).append('`')
                .append(" WHERE ").append(KEY_COLUMN).append(" >= $start").append(i)
                .append(" AND ").append(KEY_COLUMN).append(" < $end").append(i)
                .append(" ORDER BY ").append(KEY_COLUMN).append(" LIMIT $limit").append(i).append(";\n");
        }
        return declares.append(statements).toString();
    }

    private Params batchQueryParams(List<KVQuery> batch) {
        Params params = Params.create();
        for (int i = 0; i < batch.size(); i++) {
            KVQuery query = batch.get(i);
            params.put("$start" + i, bytesValue(query.getStart()))
                .put("$end" + i, bytesValue(query.getEnd()))
                .put("$limit" + i, PrimitiveValue.newUint64(pageLimit(query.getLimit())));
        }
        return params;
    }

    private void collectBatchResults(YdbTx tx, List<KVQuery> batch, QueryReader reader,
                                     Map<KVQuery, RecordIterator<KeyValueEntry>> result) throws BackendException {
        for (int i = 0; i < batch.size(); i++) {
            KVQuery query = batch.get(i);
            List<KeyValueEntry> entries = new ArrayList<>();
            mergeScan(tx, query, entries, reader.getResultSet(i), pageLimit(query.getLimit()));
            result.put(query, new ListRecordIterator(entries));
        }
    }

    /**
     * Scans [start, end) of the given query, merging the transaction's buffered
     * writes (empty in serializable_rw mode) with rows fetched from YDB in pages
     * of at most scan-page-size rows, feeding merged keys through the selector
     * until its limit is reached or the range is exhausted. An already-fetched
     * first page (from a slice batch) can be supplied via {@code preloaded}.
     */
    private void mergeScan(YdbTx tx, KVQuery query, List<KeyValueEntry> sink,
                           ResultSetReader preloaded, long preloadedLimit) throws BackendException {
        KeySelector selector = query.getKeySelector();
        byte[] end = query.getEnd().as(StaticBuffer.ARRAY_FACTORY);
        byte[] cursor = query.getStart().as(StaticBuffer.ARRAY_FACTORY);
        boolean inclusive = true;
        long pageLimit = pageLimit(query.getLimit());

        YdbTx.BufferView buffer = tx.bufferView(tablePath, cursor, end);
        Iterator<Map.Entry<byte[], byte[]>> additions = buffer.additions.entrySet().iterator();
        Map.Entry<byte[], byte[]> nextAddition = additions.hasNext() ? additions.next() : null;

        ResultSetReader rs = preloaded;
        long currentLimit = preloaded != null ? preloadedLimit : 0;
        while (true) {
            if (rs == null) {
                rs = readPage(tx, cursor, inclusive, end, pageLimit);
                currentLimit = pageLimit;
            }
            int rows = 0;
            while (!selector.reachedLimit() && rs.next()) {
                rows++;
                byte[] dbKey = rs.getColumn(KEY_COLUMN).getBytes();
                cursor = dbKey;
                inclusive = false;
                while (nextAddition != null && YdbTx.KEY_ORDER.compare(nextAddition.getKey(), dbKey) < 0
                        && !selector.reachedLimit()) {
                    offer(selector, sink, nextAddition.getKey(), nextAddition.getValue());
                    nextAddition = additions.hasNext() ? additions.next() : null;
                }
                if (selector.reachedLimit()) {
                    break;
                }
                if (nextAddition != null && YdbTx.KEY_ORDER.compare(nextAddition.getKey(), dbKey) == 0) {
                    // a buffered write overrides the stored row
                    offer(selector, sink, nextAddition.getKey(), nextAddition.getValue());
                    nextAddition = additions.hasNext() ? additions.next() : null;
                } else if (!buffer.deletions.contains(dbKey)) {
                    offer(selector, sink, dbKey, rs.getColumn(VALUE_COLUMN).getBytes());
                }
            }
            if (selector.reachedLimit()) {
                return;
            }
            if (rows < currentLimit) {
                break; // range exhausted in YDB
            }
            rs = null;
        }
        // remaining buffered additions beyond the last stored row
        while (nextAddition != null && !selector.reachedLimit()) {
            offer(selector, sink, nextAddition.getKey(), nextAddition.getValue());
            nextAddition = additions.hasNext() ? additions.next() : null;
        }
    }

    private ResultSetReader readPage(YdbTx tx, byte[] start, boolean inclusive, byte[] end, long limit)
            throws BackendException {
        Params params = Params.of(
            "$start", PrimitiveValue.newBytes(start),
            "$end", PrimitiveValue.newBytes(end),
            "$limit", PrimitiveValue.newUint64(limit));
        QueryReader reader = tx.executeRead(inclusive ? firstPageQuery : nextPageQuery, params);
        return reader.getResultSet(0);
    }

    private static void offer(KeySelector selector, List<KeyValueEntry> sink, byte[] key, byte[] value) {
        StaticBuffer keyBuffer = StaticArrayBuffer.of(key);
        if (selector.include(keyBuffer)) {
            sink.add(new KeyValueEntry(keyBuffer, StaticArrayBuffer.of(value)));
        }
    }

    private long pageLimit(int queryLimit) {
        return Math.min((long) scanPageSize, (long) queryLimit);
    }

    private static PrimitiveValue bytesValue(StaticBuffer buffer) {
        return PrimitiveValue.newBytes(buffer.as(StaticBuffer.ARRAY_FACTORY));
    }

    @Override
    public synchronized void close() throws BackendException {
        if (isOpen) {
            manager.removeDatabase(this);
        }
        isOpen = false;
    }

    private static final class ListRecordIterator implements RecordIterator<KeyValueEntry> {

        private final Iterator<KeyValueEntry> iterator;

        private ListRecordIterator(List<KeyValueEntry> entries) {
            this.iterator = entries.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public KeyValueEntry next() {
            if (!iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void close() throws IOException {
            // results are fully materialized; nothing to release
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
