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

package org.janusgraph.graphdb.ydb;

import org.janusgraph.YdbStorageSetup;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.ydb.YdbStoreManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Documents the visibility semantics of the two read modes:
 *
 * <ul>
 * <li>{@code snapshot_ro} (default): every read is an independent snapshot, so an
 * open JanusGraph transaction observes commits made after it started —
 * read-committed-style visibility with lock-free reads.</li>
 * <li>{@code serializable_rw}: the whole transaction shares one YDB transaction
 * and keeps a consistent view; concurrent commits stay invisible until the next
 * transaction.</li>
 * </ul>
 */
public class YdbReadModeSemanticsTest {

    private static final String DIR = "janusgraph-readmode-test";

    private JanusGraph graph;

    @AfterEach
    public void tearDown() throws BackendException {
        if (graph != null) {
            graph.close();
        }
        clearStorage();
    }

    private void clearStorage() throws BackendException {
        YdbStoreManager manager = new YdbStoreManager(YdbStorageSetup.getYdbConfiguration(DIR));
        manager.clearStorage();
        manager.close();
    }

    private void open(ModifiableConfiguration config) throws BackendException {
        clearStorage();
        graph = JanusGraphFactory.open(config.getConfiguration());
    }

    /** Adds one vertex, opens a reading transaction, commits two more vertices concurrently. */
    private long[] runScenario() {
        graph.addVertex();
        graph.tx().commit();

        long before = graph.traversal().V().count().next(); // fixes the thread-local tx view

        JanusGraphTransaction other = graph.newTransaction();
        other.addVertex();
        other.addVertex();
        other.commit();

        long during = graph.traversal().V().count().next(); // same open thread-local tx

        graph.tx().rollback();
        long after = graph.traversal().V().count().next();  // fresh tx

        return new long[]{before, during, after};
    }

    @Test
    public void snapshotReadsSeeConcurrentCommits() throws BackendException {
        open(YdbStorageSetup.getYdbConfiguration(DIR));
        long[] counts = runScenario();
        assertEquals(1, counts[0]);
        assertEquals(3, counts[1], "snapshot_ro reads are per-query snapshots and see concurrent commits");
        assertEquals(3, counts[2], "everything is durable");
    }

    @Test
    public void serializableModeKeepsConsistentView() throws BackendException {
        open(YdbStorageSetup.getYdbSerializableConfiguration(DIR));
        long[] counts = runScenario();
        assertEquals(1, counts[0]);
        assertEquals(1, counts[1], "serializable_rw keeps the transaction's consistent snapshot");
        assertEquals(3, counts[2], "everything is durable and visible to the next transaction");
    }
}
