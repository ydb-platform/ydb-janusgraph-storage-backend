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
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.JanusGraphTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class YdbGraphTest extends JanusGraphTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return YdbStorageSetup.getYdbConfiguration("janusgraph-test-graph").getConfiguration();
    }

    // Same workaround as BerkeleyJE: for KV backends the storage is cleared by
    // dropping the whole store directory, which requires drop-on-clear=true.
    @Override
    @Test
    public void testClearStorage() throws Exception {
        tearDown();
        config.set(ConfigElement.getPath(GraphDatabaseConfiguration.DROP_ON_CLEAR), true);
        Backend backend = getBackend(config, false);
        assertTrue(backend.getStoreManager().exists(), "graph should exist before clearing storage");
        clearGraph(config);
        backend.close();
        backend = getBackend(config, false);
        assertFalse(backend.getStoreManager().exists(), "graph should not exist after clearing storage");
        backend.close();
    }

    // The backend runs deliberately lock-free in the read-only read modes (and relies
    // on YDB's own conflict detection in serializable_rw); JanusGraph's built-in
    // ConsistentKeyLocker is never installed, so these tests, which assert its
    // specific PermanentLockingException, do not apply. BerkeleyJE and FoundationDB
    // disable the same pair.
    @Override
    @Test
    @Disabled("No JanusGraph locker by design: lock-free in read-only read modes, YDB-native conflicts in serializable_rw")
    public void testConsistencyEnforcement() {
    }

    @Override
    @Test
    @Disabled("No JanusGraph locker by design: lock-free in read-only read modes, YDB-native conflicts in serializable_rw")
    public void testConcurrentConsistencyEnforcement() {
    }
}
