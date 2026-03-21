/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.FloatComparator;

import static java.lang.Math.min;

@Internal
public class FloatPrimitiveArrayComparator
        extends PrimitiveArrayComparator<float[], FloatComparator> {
    public FloatPrimitiveArrayComparator(boolean ascending) {
        super(ascending, new FloatComparator(ascending));
    }

    @Override
    public int hash(float[] record) {
        int result = 0;
        final float[] r = record;
        int i = 0;
        int len = r.length;

        // Process 4 elements per iteration to reduce loop overhead and bounds checks.
        int limit = len - 3;
        while (i < limit) {
            result += Float.floatToIntBits(r[i]);
            result += Float.floatToIntBits(r[i + 1]);
            result += Float.floatToIntBits(r[i + 2]);
            result += Float.floatToIntBits(r[i + 3]);
            i += 4;
        }
        // Handle remaining elements.
        while (i < len) {
            result += Float.floatToIntBits(r[i++]);
        }
        return result;
    }

    @Override
    public int compare(float[] first, float[] second) {
        for (int x = 0; x < min(first.length, second.length); x++) {
            int cmp = Float.compare(first[x], second[x]);
            if (cmp != 0) {
                return ascending ? cmp : -cmp;
            }
        }
        int cmp = first.length - second.length;
        return ascending ? cmp : -cmp;
    }

    @Override
    public TypeComparator<float[]> duplicate() {
        FloatPrimitiveArrayComparator dupe = new FloatPrimitiveArrayComparator(this.ascending);
        dupe.setReference(this.reference);
        return dupe;
    }
}
