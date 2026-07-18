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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.YdbStorageSetup;
import org.janusgraph.YdbTestEnv;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.ydb.YdbStoreManager;
import org.janusgraph.diskstorage.ydb.index.YdbIndexConfigOptions;
import org.janusgraph.diskstorage.ydb.index.YdbVectors;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.types.ParameterType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end mixed-index test: schema with a float[] embedding key, writes through
 * graph commits, kNN through graph.indexQuery, scalar lookups routed to the index,
 * and REINDEX driving the provider's restore path.
 */
public class YdbMixedIndexGraphTest {

    private static final String DIR = "janusgraph-mixed-test";
    private static final String INDEX_DIR = "janusgraph-mixed-test-index";
    private static final String BACKEND_NAME = "vec";
    private static final String INDEX_NAME = "memoryVectors";

    private JanusGraph graph;

    @BeforeEach
    public void setUp() throws BackendException {
        clearStorage();
        ModifiableConfiguration config = YdbStorageSetup.getYdbConfiguration(DIR);
        config.set(GraphDatabaseConfiguration.INDEX_BACKEND,
            "org.janusgraph.diskstorage.ydb.index.YdbIndexProvider", BACKEND_NAME);
        config.set(YdbIndexConfigOptions.ENDPOINT, YdbTestEnv.endpoint(), BACKEND_NAME);
        config.set(YdbIndexConfigOptions.DATABASE, YdbTestEnv.database(), BACKEND_NAME);
        config.set(YdbIndexConfigOptions.DIRECTORY, INDEX_DIR, BACKEND_NAME);
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
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(YdbIndexConfigOptions.ENDPOINT, YdbTestEnv.endpoint(), BACKEND_NAME);
        config.set(YdbIndexConfigOptions.DATABASE, YdbTestEnv.database(), BACKEND_NAME);
        config.set(YdbIndexConfigOptions.DIRECTORY, INDEX_DIR, BACKEND_NAME);
        org.janusgraph.diskstorage.ydb.index.YdbIndexProvider provider =
            new org.janusgraph.diskstorage.ydb.index.YdbIndexProvider(config.restrictTo(BACKEND_NAME));
        provider.clearStorage();
        provider.close();
    }

    private static final float[][] VECTORS = {
        {1f, 0f, 0f, 0f},
        {0.9f, 0.1f, 0f, 0f},
        {0f, 1f, 0f, 0f},
        {0f, 0f, 1f, 0f},
        {0.5f, 0.5f, 0f, 0f},
    };

    private void defineSchemaAndLoad() {
        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey embedding = mgmt.makePropertyKey("embedding").dataType(float[].class).make();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.buildIndex(INDEX_NAME, Vertex.class)
            .addKey(embedding, Parameter.of(ParameterType.customParameterName("dimension"), 4))
            .addKey(name, Mapping.STRING.asParameter())
            .buildMixedIndex(BACKEND_NAME);
        mgmt.commit();

        for (int i = 0; i < VECTORS.length; i++) {
            JanusGraphVertex v = graph.addVertex("name", "memory" + i);
            v.property("embedding", VECTORS[i]);
        }
        graph.tx().commit();
    }

    private List<String> knnNames(float[] target, int k) {
        return graph.indexQuery(INDEX_NAME, YdbVectors.nearest("embedding", target))
            .limit(k)
            .vertexStream()
            .map(r -> (String) r.getElement().value("name"))
            .collect(Collectors.toList());
    }

    @Test
    public void knnAndScalarLookupsThroughGraph() throws Exception {
        defineSchemaAndLoad();

        // kNN via graph.indexQuery: nearest to ~[1,0,0,0]
        assertEquals(List.of("memory0", "memory1", "memory4"), knnNames(new float[]{1f, 0.05f, 0f, 0f}, 3));

        // scores are exposed and descending
        List<Double> scores = graph.indexQuery(INDEX_NAME, YdbVectors.nearest("embedding", new float[]{1f, 0f, 0f, 0f}))
            .limit(3)
            .vertexStream()
            .map(JanusGraphIndexQuery.Result::getScore)
            .collect(Collectors.toList());
        assertTrue(scores.get(0) >= scores.get(1) && scores.get(1) >= scores.get(2));
        assertTrue(scores.get(0) > 0.99, "exact match must have ~1.0 cosine similarity");

        // scalar lookup routed through the mixed index (no composite index exists)
        assertEquals(1, graph.traversal().V().has("name", "memory3").count().next());

        // updates flow through: memory3 moves from a far vector into second place
        // (memory0 stays first — it matches the target exactly, cosine 1.0)
        graph.traversal().V().has("name", "memory3")
            .property("embedding", new float[]{1f, 0.01f, 0f, 0f}).iterate();
        graph.tx().commit();
        assertEquals(List.of("memory0", "memory3"), knnNames(new float[]{1f, 0f, 0f, 0f}, 2));

        // deletion removes the document from the index
        graph.traversal().V().has("name", "memory3").drop().iterate();
        graph.tx().commit();
        List<String> afterDrop = knnNames(new float[]{1f, 0f, 0f, 0f}, 5);
        assertTrue(!afterDrop.contains("memory3"));
        assertEquals(4, afterDrop.size());
    }

    @Test
    public void reindexRebuildsTheIndex() throws Exception {
        defineSchemaAndLoad();

        ManagementSystem.awaitGraphIndexStatus(graph, INDEX_NAME).call();
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.updateIndex(mgmt.getGraphIndex(INDEX_NAME), SchemaAction.REINDEX).get();
        mgmt.commit();

        assertEquals(List.of("memory0", "memory1", "memory4"), knnNames(new float[]{1f, 0.05f, 0f, 0f}, 3));
        assertEquals(1, graph.traversal().V().has("name", "memory2").count().next());
    }
}
