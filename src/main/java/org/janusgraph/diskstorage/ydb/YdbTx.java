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

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Result;
import tech.ydb.query.QuerySession;
import tech.ydb.query.QueryTransaction;
import tech.ydb.query.result.QueryInfo;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.table.query.Params;

/**
 * JanusGraph transaction over YDB, in one of two shapes depending on
 * {@code storage.ydb.read-tx-mode}:
 *
 * <ul>
 * <li>{@code snapshot_ro} / {@code stale_ro} / {@code online_ro} (buffered mode,
 * default): reads execute as independent lock-free read-only queries through the
 * session pool; writes are buffered client-side and committed atomically in a
 * single {@code SERIALIZABLE_RW} batch (or via BulkUpsert under batch loading).
 * Reads merge the write buffer so the transaction observes its own writes.</li>
 *
 * <li>{@code serializable_rw} (interactive mode): reads and writes share one
 * interactive serializable YDB transaction pinned to a pooled session; conflicts
 * are detected by YDB at commit.</li>
 * </ul>
 */
public class YdbTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(YdbTx.class);

    static final Comparator<byte[]> KEY_ORDER = UnsignedBytes.lexicographicalComparator();

    /** Client-side write buffer of one store (table) in buffered mode. */
    static final class StoreBuffer {
        final NavigableMap<byte[], byte[]> additions = new TreeMap<>(KEY_ORDER);
        final NavigableSet<byte[]> deletions = new TreeSet<>(KEY_ORDER);

        boolean isEmpty() {
            return additions.isEmpty() && deletions.isEmpty();
        }
    }

    /** Immutable snapshot of the buffered writes overlapping one key range. */
    static final class BufferView {
        static final BufferView EMPTY =
            new BufferView(new TreeMap<>(KEY_ORDER), new TreeSet<>(KEY_ORDER));

        final NavigableMap<byte[], byte[]> additions;
        final NavigableSet<byte[]> deletions;

        private BufferView(NavigableMap<byte[], byte[]> additions, NavigableSet<byte[]> deletions) {
            this.additions = additions;
            this.deletions = deletions;
        }
    }

    /** Result of a point lookup in the write buffer. */
    static final class BufferLookup {
        static final BufferLookup MISS = new BufferLookup(false, null);
        static final BufferLookup DELETED = new BufferLookup(true, null);

        final boolean hit;
        final byte[] value; // null when the key is buffered as deleted

        private BufferLookup(boolean hit, byte[] value) {
            this.hit = hit;
            this.value = value;
        }
    }

    private final YdbStoreManager manager;
    private final TxMode readMode;
    private final boolean interactive;

    // interactive (serializable_rw) state
    private QuerySession session;
    private QueryTransaction tx;
    private boolean hasWrites;
    private boolean broken;

    // buffered (read-only read modes) state, keyed by table path
    private final Map<String, StoreBuffer> buffers = new LinkedHashMap<>();

    private boolean closed;

    public YdbTx(YdbStoreManager manager, TxMode readMode, BaseTransactionConfig config) {
        super(config);
        this.manager = manager;
        this.readMode = readMode;
        this.interactive = readMode == TxMode.SERIALIZABLE_RW;
    }

    static YdbTx of(StoreTransaction txh) {
        Preconditions.checkArgument(txh instanceof YdbTx, "Unexpected transaction type: %s", txh);
        return (YdbTx) txh;
    }

    boolean isInteractive() {
        return interactive;
    }

    // ---------------------------------------------------------------- reads

    /**
     * Executes a read query: inside the interactive transaction in
     * {@code serializable_rw} mode, or as an independent retryable read-only
     * query through the session pool otherwise. Pooled reads are stateless, so
     * they remain valid even after the transaction is closed (the write buffer
     * is empty then and the read simply observes the latest committed data).
     */
    QueryReader executeRead(String yql, Params params) throws BackendException {
        if (interactive) {
            return executeInTx(yql, params, false);
        }
        return manager.readOutsideTx(yql, params);
    }

    /** Point lookup in the write buffer; always a miss in interactive mode. */
    synchronized BufferLookup bufferedGet(String tablePath, byte[] key) {
        StoreBuffer buffer = buffers.get(tablePath);
        if (buffer == null) {
            return BufferLookup.MISS;
        }
        byte[] value = buffer.additions.get(key);
        if (value != null) {
            return new BufferLookup(true, value);
        }
        return buffer.deletions.contains(key) ? BufferLookup.DELETED : BufferLookup.MISS;
    }

    /** Snapshot of buffered writes within [start, end) for merging into a scan. */
    synchronized BufferView bufferView(String tablePath, byte[] start, byte[] end) {
        StoreBuffer buffer = buffers.get(tablePath);
        if (buffer == null || buffer.isEmpty()) {
            return BufferView.EMPTY;
        }
        NavigableMap<byte[], byte[]> additions = new TreeMap<>(KEY_ORDER);
        additions.putAll(buffer.additions.subMap(start, true, end, false));
        NavigableSet<byte[]> deletions = new TreeSet<>(KEY_ORDER);
        deletions.addAll(buffer.deletions.subSet(start, true, end, false));
        return new BufferView(additions, deletions);
    }

    // ---------------------------------------------------------------- writes

    synchronized void bufferInsert(String tablePath, byte[] key, byte[] value) throws BackendException {
        checkOpen();
        StoreBuffer buffer = buffers.computeIfAbsent(tablePath, p -> new StoreBuffer());
        buffer.deletions.remove(key);
        buffer.additions.put(key, value);
    }

    synchronized void bufferDelete(String tablePath, byte[] key) throws BackendException {
        checkOpen();
        StoreBuffer buffer = buffers.computeIfAbsent(tablePath, p -> new StoreBuffer());
        buffer.additions.remove(key);
        buffer.deletions.add(key);
    }

    /** Executes a query inside the interactive transaction ({@code serializable_rw} mode only). */
    synchronized QueryReader executeInTx(String yql, Params params, boolean isWrite) throws BackendException {
        Preconditions.checkState(interactive, "executeInTx is only valid in serializable_rw mode");
        ensureActive();
        try {
            Result<QueryReader> result = QueryReader.readFrom(tx.createQuery(yql, params)).join();
            if (!result.isSuccess()) {
                invalidate();
                throw YdbExceptions.fromStatus(result.getStatus(), "query");
            }
            if (isWrite) {
                hasWrites = true;
            }
            return result.getValue();
        } catch (RuntimeException e) {
            invalidate();
            throw YdbExceptions.fromThrowable(e, "query");
        }
    }

    // ---------------------------------------------------------------- lifecycle

    @Override
    public synchronized void commit() throws BackendException {
        if (closed) {
            return;
        }
        if (interactive) {
            commitInteractive();
            return;
        }
        closed = true;
        try {
            if (!buffers.isEmpty()) {
                manager.applyBuffered(buffers);
            }
        } finally {
            buffers.clear();
        }
    }

    @Override
    public synchronized void rollback() throws BackendException {
        if (closed) {
            return;
        }
        if (!interactive) {
            buffers.clear();
            closed = true;
            return;
        }
        try {
            if (!broken && tx != null && tx.isActive()) {
                tx.rollback().join();
            }
        } catch (RuntimeException e) {
            log.warn("YDB transaction rollback failed", e);
        } finally {
            releaseSession();
            closed = true;
        }
    }

    private void commitInteractive() throws BackendException {
        if (broken) {
            closed = true;
            throw new TemporaryBackendException(
                "YDB transaction was invalidated by a previous failure and cannot be committed");
        }
        try {
            if (tx != null && tx.isActive()) {
                if (hasWrites) {
                    Result<QueryInfo> result = tx.commit().join();
                    if (!result.isSuccess()) {
                        throw YdbExceptions.fromStatus(result.getStatus(), "commit");
                    }
                } else {
                    // Nothing to persist; rolling back releases the read locks without
                    // the risk of a commit-time conflict.
                    tx.rollback().join();
                }
            }
        } catch (RuntimeException e) {
            throw YdbExceptions.fromThrowable(e, "commit");
        } finally {
            releaseSession();
            closed = true;
        }
    }

    private void checkOpen() throws BackendException {
        if (closed) {
            throw new PermanentBackendException("YDB transaction has already been closed");
        }
    }

    private void ensureActive() throws BackendException {
        checkOpen();
        if (broken) {
            throw new TemporaryBackendException(
                "YDB transaction was invalidated by a previous failure; retry the enclosing transaction");
        }
        if (tx == null) {
            try {
                Result<QuerySession> result =
                    manager.getQueryClient().createSession(manager.getSessionAcquireTimeout()).join();
                if (!result.isSuccess()) {
                    throw YdbExceptions.fromStatus(result.getStatus(), "session acquisition");
                }
                session = result.getValue();
                tx = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
            } catch (RuntimeException e) {
                throw YdbExceptions.fromThrowable(e, "session acquisition");
            }
        }
    }

    // On failure the server-side transaction is gone (the SDK drops the tx id);
    // release the session and refuse all further operations.
    private void invalidate() {
        broken = true;
        releaseSession();
    }

    private void releaseSession() {
        if (session != null) {
            session.close();
            session = null;
            tx = null;
        }
    }
}
