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

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.ydb.YdbConfigOptions;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

public class YdbStorageSetup extends StorageSetup {

    public static ModifiableConfiguration getYdbConfiguration(String graphName) {
        return GraphDatabaseConfiguration.buildGraphConfiguration()
            .set(GraphDatabaseConfiguration.STORAGE_BACKEND, "org.janusgraph.diskstorage.ydb.YdbStoreManager")
            .set(YdbConfigOptions.ENDPOINT, YdbTestEnv.endpoint())
            .set(YdbConfigOptions.DATABASE, YdbTestEnv.database())
            .set(YdbConfigOptions.DIRECTORY, graphName)
            .set(GraphDatabaseConfiguration.DROP_ON_CLEAR, false);
    }

    public static ModifiableConfiguration getYdbConfiguration() {
        return getYdbConfiguration("janusgraph-test-ydb");
    }

    /** Same configuration but with reads inside one interactive serializable transaction. */
    public static ModifiableConfiguration getYdbSerializableConfiguration(String graphName) {
        return getYdbConfiguration(graphName)
            .set(YdbConfigOptions.READ_TX_MODE, "serializable_rw");
    }

    public static WriteConfiguration getYdbGraphConfiguration() {
        return getYdbConfiguration().getConfiguration();
    }
}
