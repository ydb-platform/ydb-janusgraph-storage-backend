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

import org.janusgraph.YdbStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.jupiter.api.Test;

/**
 * KCV TCK over the YDB store with variable-length key packing — the packing
 * JanusGraph uses for graphindex, system_properties and the log stores.
 */
public class YdbVariableLengthKCVSTest extends KeyColumnValueStoreTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        YdbStoreManager sm = new YdbStoreManager(YdbStorageSetup.getYdbConfiguration("janusgraph-test-kcvs-var"));
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    // See YdbFixedLengthKCVSTest: the TCK helper races rollback/replace of a shared
    // transaction handle while writing through it; only the mutating variant is
    // disabled, stateless pooled reads keep testConcurrentGetSlice enabled.
    @Test @Override
    public void testConcurrentGetSliceAndMutate() {
    }
}
