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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.janusgraph.YdbTestEnv;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.IndexQuery;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.query.condition.PredicateCondition;
import org.janusgraph.graphdb.types.ParameterType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Direct SPI test of the YDB mixed-index backend: register/mutate/restore,
 * scalar predicates, kNN through the exact-scan path and through the
 * vector_kmeans_tree path after buildVectorIndex.
 */
public class YdbIndexProviderVectorTest {

    private static final String INDEX_NAME = "vec";
    private static final String STORE = "vertex";
    private static final String EMBEDDING = "embedding";
    private static final String NAME = "name";
    private static final String WEIGHT = "weight";

    private YdbIndexProvider index;
    private Map<String, KeyInformation> keys;
    private KeyInformation.IndexRetriever retriever;
    private BaseTransaction tx;

    private static Configuration indexConfig() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(YdbIndexConfigOptions.ENDPOINT, YdbTestEnv.endpoint(), INDEX_NAME);
        config.set(YdbIndexConfigOptions.DATABASE, YdbTestEnv.database(), INDEX_NAME);
        config.set(YdbIndexConfigOptions.DIRECTORY, "janusgraph-index-spi-test", INDEX_NAME);
        config.set(YdbIndexConfigOptions.VECTOR_INDEX_LEVELS, 1, INDEX_NAME);
        config.set(YdbIndexConfigOptions.VECTOR_INDEX_CLUSTERS, 2, INDEX_NAME);
        return config.restrictTo(INDEX_NAME);
    }

    @BeforeEach
    public void setUp() throws BackendException {
        index = new YdbIndexProvider(indexConfig());
        index.clearStorage();

        keys = new HashMap<>();
        keys.put(EMBEDDING, new StandardKeyInformation(float[].class, Cardinality.SINGLE,
            Parameter.of(ParameterType.customParameterName("dimension"), 4)));
        keys.put(NAME, new StandardKeyInformation(String.class, Cardinality.SINGLE,
            Mapping.STRING.asParameter()));
        keys.put(WEIGHT, new StandardKeyInformation(Double.class, Cardinality.SINGLE));
        retriever = retriever(keys);

        tx = index.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MILLI));
        for (Map.Entry<String, KeyInformation> key : keys.entrySet()) {
            index.register(STORE, key.getKey(), key.getValue(), tx);
        }
    }

    @AfterEach
    public void tearDown() throws BackendException {
        if (index != null) {
            index.clearStorage();
            index.close();
        }
    }

    private static KeyInformation.IndexRetriever retriever(Map<String, KeyInformation> keys) {
        KeyInformation.StoreRetriever store = keys::get;
        return new KeyInformation.IndexRetriever() {
            @Override
            public KeyInformation get(String storeName, String key) {
                return keys.get(key);
            }

            @Override
            public KeyInformation.StoreRetriever get(String storeName) {
                return store;
            }

            @Override
            public void invalidate(String storeName) {
            }
        };
    }

    private void loadDocuments() throws BackendException {
        Map<String, Map<String, IndexMutation>> mutations = new HashMap<>();
        Map<String, IndexMutation> docs = new HashMap<>();
        float[][] vectors = {
            {1f, 0f, 0f, 0f},
            {0.9f, 0.1f, 0f, 0f},
            {0f, 1f, 0f, 0f},
            {0f, 0f, 1f, 0f},
            {0.5f, 0.5f, 0f, 0f},
        };
        for (int i = 0; i < vectors.length; i++) {
            docs.put("doc" + i, new IndexMutation(keys::get,
                ImmutableList.of(
                    new IndexEntry(EMBEDDING, vectors[i]),
                    new IndexEntry(NAME, "name" + i),
                    new IndexEntry(WEIGHT, (double) i)),
                ImmutableList.of(), true, false));
        }
        mutations.put(STORE, docs);
        index.mutate(mutations, retriever, tx);
    }

    private List<String> knn(float[] target, int k) throws BackendException {
        RawQuery query = new RawQuery(STORE,
            EMBEDDING + YdbVectors.KNN_MARKER
                + java.util.Base64.getEncoder().encodeToString(YdbVectors.toBinaryString(target)),
            new Parameter[0]);
        query.setLimit(k);
        List<RawQuery.Result<String>> results = index.query(query, retriever, tx).collect(Collectors.toList());
        // scores must be monotonically non-increasing (higher = closer)
        for (int i = 1; i < results.size(); i++) {
            assertTrue(results.get(i - 1).getScore() >= results.get(i).getScore(),
                "scores must be sorted descending");
        }
        return results.stream().map(RawQuery.Result::getResult).collect(Collectors.toList());
    }

    @Test
    public void scalarPredicatesAndKnnExactAndIndexed() throws BackendException {
        loadDocuments();

        // scalar equality through IndexQuery
        List<String> byName = index.query(new IndexQuery(STORE,
            PredicateCondition.of(NAME, Cmp.EQUAL, "name3")), retriever, tx).collect(Collectors.toList());
        assertEquals(ImmutableList.of("doc3"), byName);

        // range predicate
        List<String> heavy = index.query(new IndexQuery(STORE,
            PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 3.0)), retriever, tx)
            .collect(Collectors.toList());
        assertEquals(2, heavy.size());
        assertTrue(heavy.contains("doc3") && heavy.contains("doc4"));

        // kNN, exact-scan path (no vector index yet)
        assertEquals(ImmutableList.of("doc0", "doc1", "doc4"), knn(new float[]{1f, 0.05f, 0f, 0f}, 3));

        // totals counts documents that carry the vector field
        RawQuery totalsQuery = new RawQuery(STORE, EMBEDDING + YdbVectors.KNN_MARKER
            + java.util.Base64.getEncoder().encodeToString(YdbVectors.toBinaryString(new float[]{1, 0, 0, 0})),
            new Parameter[0]);
        assertEquals(5L, index.totals(totalsQuery, retriever, tx));

        // kNN through the vector index — same results
        index.buildVectorIndex(STORE, EMBEDDING);
        assertEquals(ImmutableList.of("doc0", "doc1", "doc4"), knn(new float[]{1f, 0.05f, 0f, 0f}, 3));

        // the index is mutable: a new document is immediately visible
        Map<String, Map<String, IndexMutation>> add = new HashMap<>();
        add.put(STORE, new HashMap<>());
        add.get(STORE).put("doc9", new IndexMutation(keys::get,
            ImmutableList.of(new IndexEntry(EMBEDDING, new float[]{1f, 0.04f, 0f, 0f})),
            ImmutableList.of(), true, false));
        index.mutate(add, retriever, tx);
        assertEquals("doc9", knn(new float[]{1f, 0.05f, 0f, 0f}, 1).get(0));
    }

    @Test
    public void mutateDeletesAndRestore() throws BackendException {
        loadDocuments();

        // field deletion
        Map<String, Map<String, IndexMutation>> mutations = new HashMap<>();
        mutations.put(STORE, new HashMap<>());
        mutations.get(STORE).put("doc0", new IndexMutation(keys::get,
            ImmutableList.of(), ImmutableList.of(new IndexEntry(NAME, "name0")), false, false));
        // whole-document deletion
        mutations.get(STORE).put("doc4", new IndexMutation(keys::get, false, true));
        index.mutate(mutations, retriever, tx);

        assertEquals(0, index.query(new IndexQuery(STORE,
            PredicateCondition.of(NAME, Cmp.EQUAL, "name0")), retriever, tx).count());
        assertEquals(0, index.query(new IndexQuery(STORE,
            PredicateCondition.of(NAME, Cmp.EQUAL, "name4")), retriever, tx).count());
        // doc0 still exists (only the field was removed)
        assertEquals(1, index.query(new IndexQuery(STORE,
            PredicateCondition.of(WEIGHT, Cmp.EQUAL, 0.0)), retriever, tx).count());

        // restore fully replaces a document and deletes empty ones
        Map<String, Map<String, List<IndexEntry>>> documents = new HashMap<>();
        documents.put(STORE, new HashMap<>());
        documents.get(STORE).put("doc1", ImmutableList.of(new IndexEntry(NAME, "restored")));
        documents.get(STORE).put("doc2", ImmutableList.of());
        index.restore(documents, retriever, tx);

        assertEquals(ImmutableList.of("doc1"), index.query(new IndexQuery(STORE,
            PredicateCondition.of(NAME, Cmp.EQUAL, "restored")), retriever, tx).collect(Collectors.toList()));
        assertEquals(0, index.query(new IndexQuery(STORE,
            PredicateCondition.of(NAME, Cmp.EQUAL, "name1")), retriever, tx).count());
        assertEquals(0, index.query(new IndexQuery(STORE,
            PredicateCondition.of(NAME, Cmp.EQUAL, "name2")), retriever, tx).count());
        // doc1's weight was NOT restored — the document was fully replaced
        assertEquals(0, index.query(new IndexQuery(STORE,
            PredicateCondition.of(WEIGHT, Cmp.EQUAL, 1.0)), retriever, tx).count());
    }

    /**
     * Regression for finding #1: one mutate() call carrying hundreds of documents must not
     * exceed YDB's 10 KB query-text limit. The per-column AS_TABLE batching keeps the query
     * text constant-size regardless of document count, whereas the old per-document statements
     * blew the limit at ~70 documents.
     */
    @Test
    public void manyDocumentsInOneMutateCall() throws BackendException {
        int n = 500;
        Map<String, IndexMutation> docs = new HashMap<>();
        for (int i = 0; i < n; i++) {
            docs.put("doc" + i, new IndexMutation(keys::get,
                ImmutableList.of(new IndexEntry(NAME, "name" + i), new IndexEntry(WEIGHT, (double) i)),
                ImmutableList.of(), true, false));
        }
        Map<String, Map<String, IndexMutation>> mutations = new HashMap<>();
        mutations.put(STORE, docs);
        index.mutate(mutations, retriever, tx);

        // every document is present and individually addressable
        assertEquals(ImmutableList.of("doc123"), index.query(new IndexQuery(STORE,
            PredicateCondition.of(NAME, Cmp.EQUAL, "name123")), retriever, tx).collect(Collectors.toList()));
        assertEquals(n, index.query(new IndexQuery(STORE,
            PredicateCondition.of(WEIGHT, Cmp.GREATER_THAN_EQUAL, 0.0)), retriever, tx).count());
    }

    /**
     * Regression for finding #2: buildVectorIndex must use the field's per-key distance, not the
     * backend default. A cosine-built index queried with euclidean is a hard INTERNAL_ERROR.
     */
    @Test
    public void perKeyDistanceIsHonoredByBuild() throws BackendException {
        String dir = "janusgraph-index-euclid-test";
        YdbIndexProvider euclid = new YdbIndexProvider(providerConfig(dir, 30000));
        euclid.clearStorage();
        Map<String, KeyInformation> ekeys = new HashMap<>();
        ekeys.put(EMBEDDING, new StandardKeyInformation(float[].class, Cardinality.SINGLE,
            Parameter.of(ParameterType.customParameterName("dimension"), 4),
            Parameter.of(ParameterType.customParameterName("distance"), "euclidean")));
        KeyInformation.IndexRetriever er = retriever(ekeys);
        BaseTransaction etx = euclid.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MILLI));
        try {
            euclid.register(STORE, EMBEDDING, ekeys.get(EMBEDDING), etx);
            float[][] vectors = {{1f, 0f, 0f, 0f}, {0.9f, 0.1f, 0f, 0f}, {0f, 1f, 0f, 0f},
                {0f, 0f, 1f, 0f}, {0.5f, 0.5f, 0f, 0f}};
            Map<String, IndexMutation> docs = new HashMap<>();
            for (int i = 0; i < vectors.length; i++) {
                docs.put("doc" + i, new IndexMutation(ekeys::get,
                    ImmutableList.of(new IndexEntry(EMBEDDING, vectors[i])), ImmutableList.of(), true, false));
            }
            Map<String, Map<String, IndexMutation>> m = new HashMap<>();
            m.put(STORE, docs);
            euclid.mutate(m, er, etx);

            // build with the per-key euclidean distance, then query — must not INTERNAL_ERROR
            euclid.buildVectorIndex(STORE, EMBEDDING);
            RawQuery q = new RawQuery(STORE, YdbVectors.nearest(EMBEDDING, new float[]{1f, 0.05f, 0f, 0f})
                .replaceFirst("^v\\.", ""), new Parameter[0]).setLimit(1);
            List<String> nearest = euclid.query(q, er, etx).map(RawQuery.Result::getResult)
                .collect(Collectors.toList());
            assertEquals("doc0", nearest.get(0));
        } finally {
            euclid.clearStorage();
            euclid.close();
        }
    }

    /**
     * Regression for finding #3: clearStore must invalidate the vectorIndexPresent cache; otherwise
     * a rebuilt store queries a VIEW of a no-longer-existing index and kNN silently returns empty.
     */
    @Test
    public void clearStoreInvalidatesVectorIndexCache() throws BackendException {
        loadDocuments();
        index.buildVectorIndex(STORE, EMBEDDING);           // caches "index present" = TRUE
        assertEquals(3, knn(new float[]{1f, 0.05f, 0f, 0f}, 3).size());

        index.clearStore(STORE);                            // drops table + must clear the cache
        // re-register and repopulate the store WITHOUT a vector index
        for (Map.Entry<String, KeyInformation> key : keys.entrySet()) {
            index.register(STORE, key.getKey(), key.getValue(), tx);
        }
        loadDocuments();

        // kNN must fall back to exact scan and return results, not a stale-VIEW empty set
        assertEquals(ImmutableList.of("doc0", "doc1", "doc4"), knn(new float[]{1f, 0.05f, 0f, 0f}, 3));
    }

    /**
     * Regression for finding #6: buildVectorIndex must be re-runnable even if a previous build left
     * a temporary index behind. The DROP-tmp-before-ADD makes rebuilds idempotent.
     */
    @Test
    public void buildVectorIndexIsRerunnable() throws BackendException {
        loadDocuments();
        index.buildVectorIndex(STORE, EMBEDDING);
        // a second build (the periodic-rebuild path) must not fail with "tmp index exists"
        index.buildVectorIndex(STORE, EMBEDDING);
        assertEquals(ImmutableList.of("doc0", "doc1", "doc4"), knn(new float[]{1f, 0.05f, 0f, 0f}, 3));
    }

    /**
     * Regression for finding #7: a separate provider instance that probed "no index" before the
     * index was built must adopt it after the negative-cache window elapses (here 0 ms = every
     * query), instead of being pinned to the exact-scan fallback forever.
     */
    @Test
    public void negativeIndexCacheIsReprobed() throws BackendException {
        String dir = "janusgraph-index-reprobe-test";
        YdbIndexProvider serving = new YdbIndexProvider(providerConfig(dir, 0));   // re-probe every query
        YdbIndexProvider admin = new YdbIndexProvider(providerConfig(dir, 30000));
        serving.clearStorage();
        BaseTransaction stx = serving.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MILLI));
        BaseTransaction atx = admin.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MILLI));
        try {
            for (YdbIndexProvider p : new YdbIndexProvider[]{serving, admin}) {
                BaseTransaction t = p == serving ? stx : atx;
                p.register(STORE, EMBEDDING, keys.get(EMBEDDING), t);
            }
            float[][] vectors = {{1f, 0f, 0f, 0f}, {0.9f, 0.1f, 0f, 0f}, {0f, 1f, 0f, 0f},
                {0f, 0f, 1f, 0f}, {0.5f, 0.5f, 0f, 0f}};
            Map<String, IndexMutation> docs = new HashMap<>();
            for (int i = 0; i < vectors.length; i++) {
                docs.put("doc" + i, new IndexMutation(keys::get,
                    ImmutableList.of(new IndexEntry(EMBEDDING, vectors[i])), ImmutableList.of(), true, false));
            }
            Map<String, Map<String, IndexMutation>> m = new HashMap<>();
            m.put(STORE, docs);
            serving.mutate(m, retriever, stx);

            // serving probes "no index yet" (exact scan) — under the bug this FALSE would be pinned
            RawQuery q = new RawQuery(STORE, YdbVectors.nearest(EMBEDDING, new float[]{1f, 0.05f, 0f, 0f})
                .replaceFirst("^v\\.", ""), new Parameter[0]).setLimit(3);
            assertEquals(3, serving.query(q, retriever, stx).count());

            // admin instance builds the index
            admin.buildVectorIndex(STORE, EMBEDDING);

            // serving re-probes (ttl 0) and now serves via the index — still correct results
            assertEquals(3, serving.query(q, retriever, stx).count());
        } finally {
            serving.clearStorage();
            serving.close();
            admin.close();
        }
    }

    private static Configuration providerConfig(String directory, int recheckMs) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(YdbIndexConfigOptions.ENDPOINT, YdbTestEnv.endpoint(), INDEX_NAME);
        config.set(YdbIndexConfigOptions.DATABASE, YdbTestEnv.database(), INDEX_NAME);
        config.set(YdbIndexConfigOptions.DIRECTORY, directory, INDEX_NAME);
        config.set(YdbIndexConfigOptions.VECTOR_INDEX_LEVELS, 1, INDEX_NAME);
        config.set(YdbIndexConfigOptions.VECTOR_INDEX_CLUSTERS, 2, INDEX_NAME);
        config.set(YdbIndexConfigOptions.VECTOR_INDEX_RECHECK_MS, recheckMs, INDEX_NAME);
        return config.restrictTo(INDEX_NAME);
    }
}
