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
import org.apache.flink.api.common.typeutils.base.DoubleComparator;

import static java.lang.Math.min;

@Internal
public class DoublePrimitiveArrayComparator
        extends PrimitiveArrayComparator<double[], DoubleComparator> {
    public DoublePrimitiveArrayComparator(boolean ascending) {
        super(ascending, new DoubleComparator(ascending));
    }

    @Override
    public int hash(double[] record) {
        int result = 0;
        for (double field : record) {
            long bits = Double.doubleToLongBits(field);
            result += (int) (bits ^ (bits >>> 32));
        }
        return result;
    }

    @Override
    public int compare(double[] first, double[] second) {
        int len1 = first.length;
        int len2 = second.length;
        int lim = len1 < len2 ? len1 : len2;
        int sign = ascending ? 1 : -1;

        int i = 0;
        // Unroll loop in chunks of 4 for lower loop overhead in hot paths
        int bound = lim - 3;
        while (i < bound) {
            int cmp = Double.compare(first[i], second[i]);
            if (cmp != 0) {
                return cmp * sign;
            }
            cmp = Double.compare(first[i + 1], second[i + 1]);
            if (cmp != 0) {
                return cmp * sign;
            }
            cmp = Double.compare(first[i + 2], second[i + 2]);
            if (cmp != 0) {
                return cmp * sign;
            }
            cmp = Double.compare(first[i + 3], second[i + 3]);
            if (cmp != 0) {
                return cmp * sign;
            }
            i += 4;
        }
        // Remainder
        while (i < lim) {
            int cmp = Double.compare(first[i], second[i]);
            if (cmp != 0) {
                return cmp * sign;
            }
            i++;
        }
        int cmp = len1 - len2;
        return ascending ? cmp : -cmp;
    }

    @Override
    public TypeComparator<double[]> duplicate() {
        DoublePrimitiveArrayComparator dupe = new DoublePrimitiveArrayComparator(this.ascending);
        dupe.setReference(this.reference);
        return dupe;
    }
}
