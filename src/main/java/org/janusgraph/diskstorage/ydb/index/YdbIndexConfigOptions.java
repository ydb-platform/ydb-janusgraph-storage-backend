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

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

/**
 * Configuration of the YDB mixed-index backend ({@code index.[X].ydb.*}).
 */
@PreInitializeConfigOptions
public interface YdbIndexConfigOptions {

    ConfigNamespace YDB_INDEX_NS = new ConfigNamespace(
        GraphDatabaseConfiguration.INDEX_NS,
        "ydb",
        "YDB index backend options");

    ConfigOption<String> ENDPOINT = new ConfigOption<>(
        YDB_INDEX_NS,
        "endpoint",
        "YDB endpoint to connect to, in the form grpc://host:port (or grpcs:// for TLS).",
        ConfigOption.Type.LOCAL,
        "grpc://localhost:2136");

    ConfigOption<String> DATABASE = new ConfigOption<>(
        YDB_INDEX_NS,
        "database",
        "Path of the YDB database, e.g. /local.",
        ConfigOption.Type.LOCAL,
        "/local");

    ConfigOption<String> DIRECTORY = new ConfigOption<>(
        YDB_INDEX_NS,
        "directory",
        "Directory inside the YDB database that holds the index tables of this index backend.",
        ConfigOption.Type.LOCAL,
        "janusgraph-index");

    ConfigOption<Integer> SESSION_POOL_MAX = new ConfigOption<>(
        YDB_INDEX_NS,
        "session-pool-max",
        "Maximum size of the YDB query session pool of the index backend.",
        ConfigOption.Type.LOCAL,
        50,
        ConfigOption.positiveInt());

    ConfigOption<String> VECTOR_DISTANCE = new ConfigOption<>(
        YDB_INDEX_NS,
        "vector-distance",
        "Default distance strategy for vector (float[]) keys: cosine, euclidean, manhattan " +
            "or inner_product. Can be overridden per key with the custom index parameter 'distance'.",
        ConfigOption.Type.LOCAL,
        "cosine");

    ConfigOption<Integer> VECTOR_DIMENSION = new ConfigOption<>(
        YDB_INDEX_NS,
        "vector-dimension",
        "Default dimension of vector (float[]) keys; 0 means it must be provided per key via the " +
            "custom index parameter 'dimension'. Used for validation and for building the vector index.",
        ConfigOption.Type.LOCAL,
        0,
        ConfigOption.nonnegativeInt());

    ConfigOption<Boolean> USE_VECTOR_INDEX = new ConfigOption<>(
        YDB_INDEX_NS,
        "use-vector-index",
        "Serve kNN queries through the vector_kmeans_tree index when one has been built for the " +
            "field (see YdbIndexProvider#buildVectorIndex); otherwise fall back to an exact scan, " +
            "which is always correct but O(n).",
        ConfigOption.Type.LOCAL,
        true);

    ConfigOption<Integer> VECTOR_INDEX_RECHECK_MS = new ConfigOption<>(
        YDB_INDEX_NS,
        "vector-index-recheck-ms",
        "How long (milliseconds) a negative 'no vector index yet' probe is cached before the " +
            "server is re-checked. Keeps kNN from issuing a describeTable per query while an index " +
            "has not been built, yet lets an index built elsewhere be adopted within this window. " +
            "0 re-probes every query.",
        ConfigOption.Type.LOCAL,
        30000,
        ConfigOption.nonnegativeInt());

    ConfigOption<Integer> SEARCH_TOP_SIZE = new ConfigOption<>(
        YDB_INDEX_NS,
        "search-top-size",
        "PRAGMA ydb.KMeansTreeSearchTopSize used for vector-index searches: how many nearest " +
            "clusters are scanned per tree level (recall knob).",
        ConfigOption.Type.LOCAL,
        3,
        ConfigOption.positiveInt());

    ConfigOption<Integer> VECTOR_INDEX_LEVELS = new ConfigOption<>(
        YDB_INDEX_NS,
        "vector-index-levels",
        "levels parameter of vector_kmeans_tree indexes built by buildVectorIndex.",
        ConfigOption.Type.LOCAL,
        2,
        ConfigOption.positiveInt());

    ConfigOption<Integer> VECTOR_INDEX_CLUSTERS = new ConfigOption<>(
        YDB_INDEX_NS,
        "vector-index-clusters",
        "clusters parameter of vector_kmeans_tree indexes built by buildVectorIndex.",
        ConfigOption.Type.LOCAL,
        128,
        ConfigOption.positiveInt());
}
