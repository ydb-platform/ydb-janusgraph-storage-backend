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

import java.util.Arrays;

import org.janusgraph.YdbStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Regression for finding #4: a transaction whose total value payload exceeds the per-chunk
 * threshold (16 MB) must be split into several parameter batches so it stays under YDB's 50 MB
 * per-query parameter limit — a row-count-only split would send one oversized query that YDB
 * rejects, making the transaction impossible to commit. Here ~20 MB of values cross the byte
 * boundary and must all round-trip.
 */
public class YdbLargeValueChunkingTest {

    private static final int VALUE_SIZE = 1024 * 1024;   // 1 MB per value
    private static final int COUNT = 20;                 // ~20 MB total -> more than one 16 MB chunk

    private YdbStoreManager manager;

    @BeforeEach
    public void setUp() throws BackendException {
        manager = new YdbStoreManager(YdbStorageSetup.getYdbConfiguration("janusgraph-large-value-test"));
        manager.clearStorage();
    }

    @AfterEach
    public void tearDown() throws BackendException {
        if (manager != null) {
            manager.clearStorage();
            manager.close();
        }
    }

    private static StaticBuffer key(int i) {
        return StaticArrayBuffer.of(new byte[]{
            (byte) (i >>> 24), (byte) (i >>> 16), (byte) (i >>> 8), (byte) i});
    }

    private static byte[] value(int i) {
        byte[] v = new byte[VALUE_SIZE];
        Arrays.fill(v, (byte) i);
        return v;
    }

    @Test
    public void largeTransactionSpanningMultipleChunksCommits() throws BackendException {
        YdbKeyValueStore store = manager.openDatabase("bigvalues");

        StoreTransaction tx = manager.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MICRO));
        for (int i = 0; i < COUNT; i++) {
            store.insert(key(i), StaticArrayBuffer.of(value(i)), tx, null);
        }
        tx.commit(); // must not throw: buildChunks splits the ~20 MB payload into <=16 MB chunks

        StoreTransaction readTx = manager.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MICRO));
        try {
            for (int i = 0; i < COUNT; i++) {
                StaticBuffer read = store.get(key(i), readTx);
                assertNotNull(read, "value " + i + " must be persisted across chunk boundaries");
                assertArrayEquals(value(i), read.as(StaticBuffer.ARRAY_FACTORY), "value " + i + " must round-trip");
            }
        } finally {
            readTx.commit();
        }
        store.close();
    }
}
