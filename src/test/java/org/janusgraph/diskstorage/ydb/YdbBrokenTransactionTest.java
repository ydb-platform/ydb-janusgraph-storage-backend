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
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tech.ydb.table.query.Params;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Regression for finding #5: once an interactive (serializable_rw) transaction has failed a
 * statement, its server-side transaction is gone. Further operations must fail with a
 * {@link PermanentBackendException} so JanusGraph's BackendOperation stops retrying the dead
 * transaction (a TemporaryBackendException would make it retry uselessly for the whole
 * write-time budget).
 */
public class YdbBrokenTransactionTest {

    private YdbStoreManager manager;

    @BeforeEach
    public void setUp() throws BackendException {
        manager = new YdbStoreManager(
            YdbStorageSetup.getYdbSerializableConfiguration("janusgraph-broken-tx-test"));
        manager.clearStorage();
    }

    @AfterEach
    public void tearDown() throws BackendException {
        if (manager != null) {
            manager.clearStorage();
            manager.close();
        }
    }

    @Test
    public void failedInteractiveStatementBecomesPermanent() throws BackendException {
        YdbTx tx = (YdbTx) manager.beginTransaction(
            StandardBaseTransactionConfig.of(TimestampProviders.MICRO));

        // a query against a non-existent table fails and kills the server-side transaction
        assertThrows(PermanentBackendException.class, () ->
            tx.executeInTx("SELECT 1 FROM `/local/janusgraph-broken-tx-test/no_such_table`;",
                Params.empty(), false));

        // every subsequent operation on the now-broken transaction must also fail permanently,
        // not temporarily — otherwise BackendOperation would retry the dead transaction
        assertThrows(PermanentBackendException.class, () ->
            tx.executeInTx("SELECT 1;", Params.empty(), false));
        assertThrows(PermanentBackendException.class, tx::commit);
    }
}
