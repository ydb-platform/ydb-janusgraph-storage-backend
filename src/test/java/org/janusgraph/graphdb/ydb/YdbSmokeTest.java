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

import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.YdbStorageSetup;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.ydb.YdbStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end smoke test: load ~10k vertices connected by deterministic "knows"
 * edges, then verify 2- and 3-hop Gremlin traversals against exact path counts
 * computed independently in plain Java. Runs with query batching enabled so the
 * traversals exercise the batched getSlices (multiQuery) path.
 */
public class YdbSmokeTest {

    private static final Logger log = LoggerFactory.getLogger(YdbSmokeTest.class);

    private static final int VERTICES = 10_000;
    private static final int BATCH = 1_000;
    private static final String DIR = "janusgraph-smoke-test";

    private JanusGraph graph;

    @BeforeEach
    public void setUp() throws BackendException {
        clearStorage();
        ModifiableConfiguration config = YdbStorageSetup.getYdbConfiguration(DIR)
            .set(GraphDatabaseConfiguration.USE_MULTIQUERY, true);
        graph = JanusGraphFactory.open(config.getConfiguration());
    }

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

    /** Deterministic edge targets: i --knows--> (2i+1) mod N and (3i+7) mod N. */
    private static int[] targets(int i) {
        return new int[]{(2 * i + 1) % VERTICES, (3 * i + 7) % VERTICES};
    }

    @Test
    public void tenThousandVerticesAndMultiHopTraversal() {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("uid").dataType(Integer.class).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.buildIndex("byUid", Vertex.class)
            .addKey(mgmt.getPropertyKey("uid")).unique()
            .buildCompositeIndex();
        mgmt.commit();

        long start = System.currentTimeMillis();
        long[] vertexIds = new long[VERTICES];
        for (int from = 0; from < VERTICES; from += BATCH) {
            JanusGraphTransaction tx = graph.newTransaction();
            for (int i = from; i < from + BATCH; i++) {
                JanusGraphVertex v = tx.addVertex("uid", i);
                vertexIds[i] = (Long) v.id();
            }
            tx.commit();
        }
        log.info("Loaded {} vertices in {} ms", VERTICES, System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        for (int from = 0; from < VERTICES; from += BATCH) {
            JanusGraphTransaction tx = graph.newTransaction();
            for (int i = from; i < from + BATCH; i++) {
                JanusGraphVertex source = (JanusGraphVertex) tx.traversal().V(vertexIds[i]).next();
                for (int target : targets(i)) {
                    source.addEdge("knows", tx.traversal().V(vertexIds[target]).next());
                }
            }
            tx.commit();
        }
        log.info("Loaded {} edges in {} ms", VERTICES * 2, System.currentTimeMillis() - start);

        // exact expected path counts via multiset BFS over the same deterministic structure
        int root = 1;
        Map<Integer, Long> frontier = new HashMap<>();
        frontier.put(root, 1L);
        long[] expectedPaths = new long[4];
        expectedPaths[0] = 1;
        for (int hop = 1; hop <= 3; hop++) {
            Map<Integer, Long> next = new HashMap<>();
            for (Map.Entry<Integer, Long> e : frontier.entrySet()) {
                for (int target : targets(e.getKey())) {
                    next.merge(target, e.getValue(), Long::sum);
                }
            }
            frontier = next;
            expectedPaths[hop] = frontier.values().stream().mapToLong(Long::longValue).sum();
        }

        GraphTraversalSource g = graph.traversal();
        start = System.currentTimeMillis();
        long twoHop = g.V().has("uid", root).out("knows").out("knows").count().next();
        long twoHopMs = System.currentTimeMillis() - start;
        start = System.currentTimeMillis();
        long threeHop = g.V().has("uid", root).out("knows").out("knows").out("knows").count().next();
        long threeHopMs = System.currentTimeMillis() - start;
        log.info("2-hop: {} paths in {} ms; 3-hop: {} paths in {} ms", twoHop, twoHopMs, threeHop, threeHopMs);

        assertEquals(expectedPaths[2], twoHop, "2-hop path count");
        assertEquals(expectedPaths[3], threeHop, "3-hop path count");

        long total = g.V().count().next();
        assertEquals(VERTICES, total, "vertex count");
    }
}
