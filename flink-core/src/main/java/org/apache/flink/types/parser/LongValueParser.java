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

package org.apache.flink.types.parser;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.LongValue;

/** Parses a decimal text field into a LongValue. Only characters '1' to '0' and '-' are allowed. */
@PublicEvolving
public class LongValueParser extends FieldParser<LongValue> {

    private LongValue result;

    @Override
    public int parseField(
            byte[] bytes, int startPos, int limit, byte[] delimiter, LongValue reusable) {

        if (startPos == limit) {
            setErrorState(ParseErrorState.EMPTY_COLUMN);
            return -1;
        }

        long val = 0;
        boolean neg = false;

        final int delimLimit = limit - delimiter.length + 1;

        this.result = reusable;

        // Cache locals for performance
        final byte[] b = bytes;
        final byte[] del = delimiter;
        final int delLen = del.length;
        final int firstDelByte = delLen > 0 ? del[0] : 0;

        if (b[startPos] == '-') {
            neg = true;
            startPos++;

            // check for empty field with only the sign
            if (startPos == limit
                    || (startPos < delimLimit && matchesDelimiterAt(b, startPos, del, delLen, firstDelByte))) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_ORPHAN_SIGN);
                return -1;
            }
        }

        int i = startPos;
        while (i < limit) {
            if (i < delimLimit && matchesDelimiterAt(b, i, del, delLen, firstDelByte)) {
                if (i == startPos) {
                    setErrorState(ParseErrorState.EMPTY_COLUMN);
                    return -1;
                }
                reusable.setValue(neg ? -val : val);
                return i + delLen;
            }

            byte ch = b[i];
            if (ch < 48 || ch > 57) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
                return -1;
            }
            val = val * 10 + (ch - 48);

            // check for overflow / underflow
            if (val < 0) {
                // this is an overflow/underflow, unless we hit exactly the Long.MIN_VALUE
                if (neg && val == Long.MIN_VALUE) {
                    reusable.setValue(Long.MIN_VALUE);

                    if (i + 1 >= limit) {
                        return limit;
                    } else if (i + 1 < delimLimit && matchesDelimiterAt(b, i + 1, del, delLen, firstDelByte)) {
                        return i + 1 + delLen;
                    } else {
                        setErrorState(ParseErrorState.NUMERIC_VALUE_OVERFLOW_UNDERFLOW);
                        return -1;
                    }
                } else {
                    setErrorState(ParseErrorState.NUMERIC_VALUE_OVERFLOW_UNDERFLOW);
                    return -1;
                }
            }

            i++;
        }

        reusable.setValue(neg ? -val : val);
        return limit;
    }

    @Override
    public LongValue createValue() {
        return new LongValue();
    }

    @Override
    public LongValue getLastResult() {
        return this.result;
    }

    /**
     * Fast delimiter match at position pos.
     * Expects caller to have checked pos < (limit - delLen + 1) semantics.
     */
    private static boolean matchesDelimiterAt(byte[] bytes, int pos, byte[] del, int delLen, int firstDelByte) {
        if (delLen == 1) {
            return bytes[pos] == firstDelByte;
        }
        for (int j = 0; j < delLen; j++) {
            if (bytes[pos + j] != del[j]) {
                return false;
            }
        }
        return true;
    }

}
