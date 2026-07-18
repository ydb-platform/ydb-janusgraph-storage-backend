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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.janusgraph.YdbStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Verifies that YDB's primary-key ordering of the {@code String} (bytes) column is
 * identical to JanusGraph's {@link StaticBuffer} order: unsigned lexicographic
 * byte comparison with shorter prefixes first. This property is the foundation of
 * the whole ordered-KV mapping and must never regress.
 */
public class YdbKeyOrderTest {

    private YdbStoreManager manager;

    @BeforeEach
    public void setUp() throws BackendException {
        manager = new YdbStoreManager(YdbStorageSetup.getYdbConfiguration("janusgraph-order-test"));
        manager.clearStorage();
    }

    @AfterEach
    public void tearDown() throws BackendException {
        if (manager != null) {
            manager.clearStorage();
            manager.close();
        }
    }

    @Test
    public void ydbOrdersKeysExactlyLikeStaticBuffers() throws BackendException {
        // covers sign-bit boundaries (0x7f vs 0x80), 0x00/0xff extremes and
        // shorter-prefix-first ties
        byte[][] keys = {
            {0x00},
            {0x00, 0x00},
            {0x00, 0x01},
            {0x00, (byte) 0xff},
            {0x01},
            {0x42},
            {0x7f},
            {0x7f, (byte) 0xff},
            {(byte) 0x80},
            {(byte) 0x80, 0x00},
            {(byte) 0x81},
            {(byte) 0xfe, (byte) 0xff},
            {(byte) 0xff},
            {(byte) 0xff, 0x00},
            {(byte) 0xff, (byte) 0xff},
        };

        YdbKeyValueStore store = manager.openDatabase("ordertest");
        StoreTransaction tx = manager.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MICRO));
        // insert in shuffled order so that YDB, not insertion order, determines the result
        List<byte[]> shuffled = new ArrayList<>(Arrays.asList(keys));
        java.util.Collections.shuffle(shuffled, new java.util.Random(42));
        for (byte[] key : shuffled) {
            store.insert(StaticArrayBuffer.of(key), StaticArrayBuffer.of(key), tx, null);
        }
        tx.commit();

        List<StaticBuffer> expected = new ArrayList<>();
        for (byte[] key : keys) {
            expected.add(StaticArrayBuffer.of(key));
        }
        expected.sort(StaticBuffer::compareTo);

        StoreTransaction readTx = manager.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MICRO));
        try {
            KVQuery all = new KVQuery(
                StaticArrayBuffer.of(new byte[0]),
                StaticArrayBuffer.of(new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}));
            RecordIterator<KeyValueEntry> it = store.getSlice(all, readTx);
            List<StaticBuffer> actual = new ArrayList<>();
            while (it.hasNext()) {
                KeyValueEntry entry = it.next();
                actual.add(entry.getKey());
                assertArrayEquals(entry.getKey().as(StaticBuffer.ARRAY_FACTORY),
                    entry.getValue().as(StaticBuffer.ARRAY_FACTORY), "value must round-trip unchanged");
            }
            assertEquals(expected, actual, "YDB range scan order must match StaticBuffer order");

            // point reads see every key
            for (byte[] key : keys) {
                StaticBuffer value = store.get(StaticArrayBuffer.of(key), readTx);
                assertFalse(value == null, "missing key " + Arrays.toString(key));
            }
        } finally {
            readTx.commit();
        }
        store.close();
    }
}
