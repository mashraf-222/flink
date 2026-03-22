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
import org.apache.flink.configuration.ConfigConstants;

import java.sql.Timestamp;

/** Parses a text field into a {@link Timestamp}. */
@PublicEvolving
public class SqlTimestampParser extends FieldParser<Timestamp> {

    private static final Timestamp TIMESTAMP_INSTANCE = new Timestamp(0L);

    private Timestamp result;

    @Override
    public int parseField(
            byte[] bytes, int startPos, int limit, byte[] delimiter, Timestamp reusable) {
        final int endPos = nextStringEndPos(bytes, startPos, limit, delimiter);
        if (endPos < 0) {
            return -1;
        }

        if (endPos > startPos
                && (Character.isWhitespace(bytes[startPos])
                        || Character.isWhitespace(bytes[(endPos - 1)]))) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
            return -1;
        }

        // Fast path: parse numeric fields directly from the byte array without creating a String.
        int idx = startPos;
        try {
            // year: digits until '-'
            if (idx >= endPos) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            int year = 0;
            int digitCount = 0;
            while (idx < endPos) {
                int ch = bytes[idx];
                if (ch == '-') {
                    break;
                }
                int d = ch - '0';
                if (d < 0 || d > 9) {
                    setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                    return -1;
                }
                year = year * 10 + d;
                digitCount++;
                idx++;
            }
            if (digitCount == 0 || idx >= endPos || bytes[idx] != '-') {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            idx++; // skip '-'

            // month: digits until '-'
            if (idx >= endPos) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            int month = 0;
            digitCount = 0;
            while (idx < endPos) {
                int ch = bytes[idx];
                if (ch == '-') {
                    break;
                }
                int d = ch - '0';
                if (d < 0 || d > 9) {
                    setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                    return -1;
                }
                month = month * 10 + d;
                digitCount++;
                idx++;
            }
            if (digitCount == 0 || idx >= endPos || bytes[idx] != '-') {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            idx++; // skip '-'

            // day: digits until ' ' (space)
            if (idx >= endPos) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            int day = 0;
            digitCount = 0;
            while (idx < endPos) {
                int ch = bytes[idx];
                if (ch == ' ') {
                    break;
                }
                int d = ch - '0';
                if (d < 0 || d > 9) {
                    setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                    return -1;
                }
                day = day * 10 + d;
                digitCount++;
                idx++;
            }
            if (digitCount == 0 || idx >= endPos || bytes[idx] != ' ') {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            idx++; // skip space

            // hour: digits until ':'
            if (idx >= endPos) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            int hour = 0;
            digitCount = 0;
            while (idx < endPos) {
                int ch = bytes[idx];
                if (ch == ':') {
                    break;
                }
                int d = ch - '0';
                if (d < 0 || d > 9) {
                    setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                    return -1;
                }
                hour = hour * 10 + d;
                digitCount++;
                idx++;
            }
            if (digitCount == 0 || idx >= endPos || bytes[idx] != ':') {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            idx++; // skip ':'

            // minute: digits until ':'
            if (idx >= endPos) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            int minute = 0;
            digitCount = 0;
            while (idx < endPos) {
                int ch = bytes[idx];
                if (ch == ':') {
                    break;
                }
                int d = ch - '0';
                if (d < 0 || d > 9) {
                    setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                    return -1;
                }
                minute = minute * 10 + d;
                digitCount++;
                idx++;
            }
            if (digitCount == 0 || idx >= endPos || bytes[idx] != ':') {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            idx++; // skip ':'

            // second: digits until '.' or end
            if (idx >= endPos) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            int second = 0;
            digitCount = 0;
            while (idx < endPos) {
                int ch = bytes[idx];
                if (ch == '.' ) {
                    break;
                }
                int d = ch - '0';
                if (d < 0 || d > 9) {
                    setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                    return -1;
                }
                second = second * 10 + d;
                digitCount++;
                idx++;
            }
            if (digitCount == 0) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }

            int nanos = 0;
            if (idx < endPos && bytes[idx] == '.') {
                idx++; // skip '.'
                int nanoDigits = 0;
                // Read up to 9 digits for nanoseconds; if more, we consume them but only use first 9 (padding/truncating behavior)
                while (idx < endPos) {
                    int ch = bytes[idx];
                    int d = ch - '0';
                    if (d < 0 || d > 9) {
                        setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                        return -1;
                    }
                    if (nanoDigits < 9) {
                        nanos = nanos * 10 + d;
                    } else {
                        // just consume extra fractional digits without expanding nanos beyond 9 digits
                    }
                    nanoDigits++;
                    idx++;
                }
                if (nanoDigits == 0) {
                    setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                    return -1;
                }
                // If less than 9 digits, scale
                if (nanoDigits < 9) {
                    for (int i = nanoDigits; i < 9; i++) {
                        nanos *= 10;
                    }
                } else if (nanoDigits > 9) {
                    // If more than 9 digits, the above loop already truncated to first 9 digits.
                    // No further action needed.
                }
            } else if (idx != endPos) {
                // Unexpected trailing characters
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }

            // Build LocalDateTime and then a Timestamp to preserve the same semantics as Timestamp.valueOf(String).
            try {
                java.time.LocalDateTime ldt = java.time.LocalDateTime.of(
                        year, month, day, hour, minute, second, nanos);
                // Convert LocalDateTime to Timestamp
                this.result = Timestamp.valueOf(ldt);
                return (endPos == limit) ? limit : endPos + delimiter.length;
            } catch (RuntimeException e) {
                // Covers java.time.DateTimeException and other runtime errors that indicate invalid date/time
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
        } catch (IndexOutOfBoundsException | ArithmeticException e) {
            // Defensive: if any unexpected index/overflow occurs, treat as format error
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }
    }

    @Override
    public Timestamp createValue() {
        return TIMESTAMP_INSTANCE;
    }

    @Override
    public Timestamp getLastResult() {
        return this.result;
    }

    /**
     * Static utility to parse a field of type Timestamp from a byte sequence that represents text
     * characters (such as when read from a file stream).
     *
     * @param bytes The bytes containing the text data that should be parsed.
     * @param startPos The offset to start the parsing.
     * @param length The length of the byte sequence (counting from the offset).
     * @return The parsed value.
     * @throws IllegalArgumentException Thrown when the value cannot be parsed because the text
     *     represents not a correct number.
     */
    public static final Timestamp parseField(byte[] bytes, int startPos, int length) {
        return parseField(bytes, startPos, length, (char) 0xffff);
    }

    /**
     * Static utility to parse a field of type Timestamp from a byte sequence that represents text
     * characters (such as when read from a file stream).
     *
     * @param bytes The bytes containing the text data that should be parsed.
     * @param startPos The offset to start the parsing.
     * @param length The length of the byte sequence (counting from the offset).
     * @param delimiter The delimiter that terminates the field.
     * @return The parsed value.
     * @throws IllegalArgumentException Thrown when the value cannot be parsed because the text
     *     represents not a correct number.
     */
    public static final Timestamp parseField(
            byte[] bytes, int startPos, int length, char delimiter) {
        final int limitedLen = nextStringLength(bytes, startPos, length, delimiter);

        if (limitedLen > 0
                && (Character.isWhitespace(bytes[startPos])
                        || Character.isWhitespace(bytes[startPos + limitedLen - 1]))) {
            throw new NumberFormatException(
                    "There is leading or trailing whitespace in the numeric field.");
        }

        final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
        return Timestamp.valueOf(str);
    }
}
