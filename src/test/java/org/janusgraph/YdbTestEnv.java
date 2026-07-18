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

package org.janusgraph;

import java.io.IOException;
import java.net.ServerSocket;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Resolves the YDB instance used by integration tests. When the YDB_ENDPOINT and
 * YDB_DATABASE environment variables are set (e.g. by the devcontainer, which runs
 * a local-ydb sidecar), that instance is used directly. Otherwise a
 * ydbplatform/local-ydb container is started once per JVM via Testcontainers.
 */
public final class YdbTestEnv {

    private static final String DEFAULT_IMAGE = "ydbplatform/local-ydb:latest";

    private static String endpoint;
    private static String database;

    private YdbTestEnv() {
    }

    public static synchronized String endpoint() {
        init();
        return endpoint;
    }

    public static synchronized String database() {
        init();
        return database;
    }

    private static void init() {
        if (endpoint != null) {
            return;
        }
        String envEndpoint = System.getenv("YDB_ENDPOINT");
        String envDatabase = System.getenv("YDB_DATABASE");
        if (envEndpoint != null && !envEndpoint.isEmpty() && envDatabase != null && !envDatabase.isEmpty()) {
            endpoint = envEndpoint.contains("://") ? envEndpoint : "grpc://" + envEndpoint;
            database = envDatabase;
            return;
        }
        startContainer();
    }

    @SuppressWarnings({"deprecation", "resource"})
    private static void startContainer() {
        // local-ydb reports its own hostname/port during discovery, so host and
        // container ports must be equal (same trick as the YDB SDK test helper)
        int port = findFreePort();
        String image = System.getenv().getOrDefault("YDB_DOCKER_IMAGE", DEFAULT_IMAGE);
        FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>(image)
            .withFixedExposedPort(port, port)
            .withEnv("YDB_USE_IN_MEMORY_PDISKS", "true")
            .withEnv("GRPC_PORT", String.valueOf(port))
            .waitingFor(Wait.forHealthcheck());
        container.start();
        Runtime.getRuntime().addShutdownHook(new Thread(container::stop));
        endpoint = "grpc://" + container.getHost() + ":" + port;
        database = "/local";
    }

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Could not allocate a free port for the YDB container", e);
        }
    }
}
