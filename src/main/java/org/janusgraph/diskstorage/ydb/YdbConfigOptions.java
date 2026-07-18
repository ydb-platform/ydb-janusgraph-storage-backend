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

import java.time.Duration;

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

/**
 * Configuration options for the YDB storage backend ({@code storage.ydb.*}).
 */
@PreInitializeConfigOptions
public interface YdbConfigOptions {

    ConfigNamespace YDB_NS = new ConfigNamespace(
        GraphDatabaseConfiguration.STORAGE_NS,
        "ydb",
        "YDB storage backend options");

    ConfigOption<String> ENDPOINT = new ConfigOption<>(
        YDB_NS,
        "endpoint",
        "YDB endpoint to connect to, in the form grpc://host:port (or grpcs://host:port for TLS).",
        ConfigOption.Type.LOCAL,
        "grpc://localhost:2136");

    ConfigOption<String> DATABASE = new ConfigOption<>(
        YDB_NS,
        "database",
        "Path of the YDB database, e.g. /local.",
        ConfigOption.Type.LOCAL,
        "/local");

    ConfigOption<String> DIRECTORY = new ConfigOption<>(
        YDB_NS,
        "directory",
        "Name of the directory inside the YDB database that holds all JanusGraph store tables. " +
            "It is created if it does not exist. When not set explicitly, graph.graphname is used as a fallback.",
        ConfigOption.Type.LOCAL,
        "janusgraph");

    ConfigOption<String> READ_TX_MODE = new ConfigOption<>(
        YDB_NS,
        "read-tx-mode",
        "YDB transaction mode used for reads. 'snapshot_ro' (default): reads run as independent " +
            "lock-free snapshot reads through the session pool; writes are buffered and committed " +
            "atomically in one serializable_rw batch. 'stale_ro'/'online_ro': same architecture " +
            "with weaker read freshness and lower latency. 'serializable_rw': reads and writes " +
            "share one interactive serializable YDB transaction with commit-time conflict detection.",
        ConfigOption.Type.LOCAL,
        "snapshot_ro");

    ConfigOption<String> AUTH_MODE = new ConfigOption<>(
        YDB_NS,
        "auth-mode",
        "Authentication mode: 'anonymous', 'token' (storage.ydb.auth-token), " +
            "'static' (storage.ydb.auth-username/auth-password) or 'environ' " +
            "(credentials resolved from the standard YDB_* environment variables).",
        ConfigOption.Type.LOCAL,
        "anonymous");

    ConfigOption<String> AUTH_TOKEN = new ConfigOption<>(
        YDB_NS,
        "auth-token",
        "Access token used when auth-mode=token.",
        ConfigOption.Type.LOCAL,
        "");

    ConfigOption<String> AUTH_USERNAME = new ConfigOption<>(
        YDB_NS,
        "auth-username",
        "User name used when auth-mode=static.",
        ConfigOption.Type.LOCAL,
        "");

    ConfigOption<String> AUTH_PASSWORD = new ConfigOption<>(
        YDB_NS,
        "auth-password",
        "Password used when auth-mode=static.",
        ConfigOption.Type.LOCAL,
        "");

    ConfigOption<Integer> SESSION_POOL_MAX = new ConfigOption<>(
        YDB_NS,
        "session-pool-max",
        "Maximum size of the YDB query session pool. Every open JanusGraph transaction pins one " +
            "session from its first storage operation until commit or rollback, so this bounds the " +
            "number of concurrently active transactions.",
        ConfigOption.Type.LOCAL,
        50,
        ConfigOption.positiveInt());

    ConfigOption<Duration> SESSION_ACQUIRE_TIMEOUT = new ConfigOption<>(
        YDB_NS,
        "session-acquire-timeout",
        "How long a transaction waits for a free session from the pool before failing.",
        ConfigOption.Type.LOCAL,
        Duration.ofSeconds(10));

    ConfigOption<Integer> SCAN_PAGE_SIZE = new ConfigOption<>(
        YDB_NS,
        "scan-page-size",
        "Maximum number of rows fetched from YDB in one round trip while scanning a key range. " +
            "Larger slices are read with successive pages inside the same transaction.",
        ConfigOption.Type.LOCAL,
        10000,
        ConfigOption.positiveInt());

    ConfigOption<Integer> SLICE_BATCH_SIZE = new ConfigOption<>(
        YDB_NS,
        "slice-batch-size",
        "Maximum number of range queries combined into a single multi-statement YDB query " +
            "when JanusGraph issues batched slice reads (multiQuery).",
        ConfigOption.Type.LOCAL,
        10,
        ConfigOption.positiveInt());

    ConfigOption<Boolean> AUTO_PARTITIONING = new ConfigOption<>(
        YDB_NS,
        "auto-partitioning",
        "Enable automatic partitioning by size and by load for the created store tables.",
        ConfigOption.Type.LOCAL,
        true);
}
