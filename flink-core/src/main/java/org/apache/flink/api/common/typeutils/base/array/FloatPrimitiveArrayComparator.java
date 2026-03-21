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
        for (float field : record) {
            result += Float.floatToIntBits(field);
        }
        return result;
    }

    @Override
    public int compare(float[] first, float[] second) {
        int firstLen = first.length;
        int secondLen = second.length;
        int limit = firstLen < secondLen ? firstLen : secondLen;
        boolean asc = ascending;

        for (int x = 0; x < limit; x++) {
            float a = first[x];
            float b = second[x];

            int cmp;
            if (a < b) {
                cmp = -1;
            } else if (a > b) {
                cmp = 1;
            } else {
                // Handle 0.0 vs -0.0 and NaN semantics exactly as Float.compare
                int ai = Float.floatToIntBits(a);
                int bi = Float.floatToIntBits(b);
                if (ai == bi) {
                    continue;
                }
                cmp = ai < bi ? -1 : 1;
            }

            return asc ? cmp : -cmp;
        }

        int cmp = firstLen - secondLen;
        return asc ? cmp : -cmp;
    }

    @Override
    public TypeComparator<float[]> duplicate() {
        FloatPrimitiveArrayComparator dupe = new FloatPrimitiveArrayComparator(this.ascending);
        dupe.setReference(this.reference);
        return dupe;
    }
}
