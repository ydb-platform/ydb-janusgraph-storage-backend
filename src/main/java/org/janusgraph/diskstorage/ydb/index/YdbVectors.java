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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;

/**
 * Vector helpers: YDB binary serialization of float vectors (little-endian floats
 * plus the trailing 0x01 "Float" format byte, as produced by
 * {@code Knn::ToBinaryStringFloat}) and the raw-query syntax of this index backend.
 *
 * <p>The kNN raw-query form accepted by {@link YdbIndexProvider} is
 * {@code <field>:knn:<base64 of the serialized vector>}; build it with
 * {@link #nearest(String, float[])} and pass to
 * {@code graph.indexQuery(indexName, query).limit(k).vertexStream()}.
 */
public final class YdbVectors {

    static final byte FLOAT_FORMAT_BYTE = 0x01;
    static final String KNN_MARKER = ":knn:";

    private YdbVectors() {
    }

    /** Serializes a float vector into YDB's binary FloatVector representation. */
    public static byte[] toBinaryString(float[] vector) {
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Float.BYTES + 1)
            .order(ByteOrder.LITTLE_ENDIAN);
        for (float value : vector) {
            buffer.putFloat(value);
        }
        buffer.put(FLOAT_FORMAT_BYTE);
        return buffer.array();
    }

    /** Dimension of a serialized vector, or -1 if the bytes are not a float vector. */
    public static int dimensionOf(byte[] serialized) {
        if (serialized.length < 1 || serialized[serialized.length - 1] != FLOAT_FORMAT_BYTE
            || (serialized.length - 1) % Float.BYTES != 0) {
            return -1;
        }
        return (serialized.length - 1) / Float.BYTES;
    }

    /**
     * Builds the nearest-neighbour raw query for {@code graph.indexQuery(...)}.
     * The key must be referenced through the element prefix so that JanusGraph
     * substitutes the mapped field name, hence the {@code v.} form.
     */
    public static String nearest(String propertyKey, float[] vector) {
        return "v.\"" + propertyKey + "\"" + KNN_MARKER
            + Base64.getEncoder().encodeToString(toBinaryString(vector));
    }

    /** Parses {@code <field>:knn:<base64>}; returns null when the syntax does not match. */
    static ParsedKnn parse(String rawQuery) {
        int marker = rawQuery.indexOf(KNN_MARKER);
        if (marker <= 0) {
            return null;
        }
        String field = rawQuery.substring(0, marker);
        if (field.startsWith("\"") && field.endsWith("\"") && field.length() > 1) {
            field = field.substring(1, field.length() - 1);
        }
        try {
            byte[] vector = Base64.getDecoder().decode(rawQuery.substring(marker + KNN_MARKER.length()).trim());
            return new ParsedKnn(field, vector);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    static final class ParsedKnn {
        final String field;
        final byte[] vector;

        private ParsedKnn(String field, byte[] vector) {
            this.field = field;
            this.vector = vector;
        }
    }
}
