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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.YdbStorageSetup;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.ydb.YdbStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Exercises the batch-loading path: with storage.batch-loading=true writes bypass
 * transactions and stream through YDB's BulkUpsert API. Verifies the loaded data
 * with a separate normally-configured graph instance.
 */
public class YdbBulkLoadTest {

    private static final String DIR = "janusgraph-bulkload-test";
    private static final int VERTICES = 2_000;
    private static final int BATCH = 500;

    @BeforeEach
    @AfterEach
    public void clearStorage() throws BackendException {
        YdbStoreManager manager = new YdbStoreManager(YdbStorageSetup.getYdbConfiguration(DIR));
        manager.clearStorage();
        manager.close();
    }

    @Test
    public void bulkLoadedDataIsReadableByRegularGraph() {
        JanusGraph loader = JanusGraphFactory.open(YdbStorageSetup.getYdbConfiguration(DIR)
            .set(GraphDatabaseConfiguration.STORAGE_BATCH, true)
            .getConfiguration());
        try {
            // batch loading disables automatic schema creation — define it explicitly
            org.janusgraph.core.schema.JanusGraphManagement mgmt = loader.openManagement();
            mgmt.makePropertyKey("uid").dataType(Integer.class).make();
            mgmt.makeEdgeLabel("next").make();
            mgmt.commit();
            for (int from = 0; from < VERTICES; from += BATCH) {
                JanusGraphTransaction tx = loader.newTransaction();
                Vertex previous = null;
                for (int i = from; i < from + BATCH; i++) {
                    Vertex v = tx.addVertex("uid", i);
                    if (previous != null) {
                        previous.addEdge("next", v);
                    }
                    previous = v;
                }
                tx.commit();
            }
        } finally {
            loader.close();
        }

        JanusGraph reader = JanusGraphFactory.open(YdbStorageSetup.getYdbConfiguration(DIR).getConfiguration());
        try {
            assertEquals(VERTICES, reader.traversal().V().count().next());
            assertEquals(VERTICES - (VERTICES / BATCH), reader.traversal().E().count().next());
        } finally {
            reader.close();
        }
    }
}
