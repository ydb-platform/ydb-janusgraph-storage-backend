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

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;

/**
 * Maps YDB operation outcomes to the JanusGraph {@link BackendException} hierarchy.
 * Statuses that YDB itself considers retryable become {@link TemporaryBackendException}
 * so that JanusGraph's {@code BackendOperation} machinery can retry them.
 */
final class YdbExceptions {

    private YdbExceptions() {
    }

    static BackendException fromStatus(Status status, String operation) {
        String message = "YDB " + operation + " failed: " + status;
        return isTemporary(status.getCode())
            ? new TemporaryBackendException(message)
            : new PermanentBackendException(message);
    }

    static BackendException fromThrowable(Throwable t, String operation) {
        Throwable cause = t;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof BackendException) {
            return (BackendException) cause;
        }
        if (cause instanceof UnexpectedResultException) {
            UnexpectedResultException ure = (UnexpectedResultException) cause;
            String message = "YDB " + operation + " failed: " + ure.getStatus();
            return isTemporary(ure.getStatus().getCode())
                ? new TemporaryBackendException(message, ure)
                : new PermanentBackendException(message, ure);
        }
        return new PermanentBackendException("YDB " + operation + " failed", cause);
    }

    private static boolean isTemporary(StatusCode code) {
        switch (code) {
            case TIMEOUT:
            case CANCELLED:
            case SESSION_EXPIRED:
            case CLIENT_DEADLINE_EXPIRED: // e.g. session-pool acquire timeout under load
                return true;
            default:
                // includes ABORTED, UNAVAILABLE, OVERLOADED, BAD_SESSION, SESSION_BUSY,
                // UNDETERMINED, TRANSPORT_UNAVAILABLE and other client-side retryables
                return code.isRetryable(true);
        }
    }
}
