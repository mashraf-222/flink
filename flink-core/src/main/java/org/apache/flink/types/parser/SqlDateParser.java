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

import java.sql.Date;

/** Parses a text field into a {@link java.sql.Date}. */
@PublicEvolving
public class SqlDateParser extends FieldParser<Date> {

    private static final Date DATE_INSTANCE = new Date(0L);

    private Date result;

    @Override
    public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, Date reusable) {
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

        String str =
                new String(bytes, startPos, endPos - startPos, ConfigConstants.DEFAULT_CHARSET);
        try {
            this.result = Date.valueOf(str);
            return (endPos == limit) ? limit : endPos + delimiter.length;
        } catch (IllegalArgumentException e) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }
    }

    @Override
    public Date createValue() {
        return DATE_INSTANCE;
    }

    @Override
    public Date getLastResult() {
        return this.result;
    }

    /**
     * Static utility to parse a field of type Date from a byte sequence that represents text
     * characters (such as when read from a file stream).
     *
     * @param bytes The bytes containing the text data that should be parsed.
     * @param startPos The offset to start the parsing.
     * @param length The length of the byte sequence (counting from the offset).
     * @return The parsed value.
     * @throws IllegalArgumentException Thrown when the value cannot be parsed because the text
     *     represents not a correct number.
     */
    public static final Date parseField(byte[] bytes, int startPos, int length) {
        // Fast-path for the common ISO date format "YYYY-MM-DD" (10 chars).
        // This path uses only byte arithmetic and integer math to compute epoch millis
        // and reuses DATE_INSTANCE to avoid allocations.
        if (length == 10) {
            final int p = startPos;
            // positions: 0-3 year digits, 4 '-', 5-6 month digits, 7 '-', 8-9 day digits
            // Ensure separators are '-' quickly.
            if (bytes[p + 4] == '-' && bytes[p + 7] == '-') {
                final int b0 = bytes[p]   - 48;
                final int b1 = bytes[p+1] - 48;
                final int b2 = bytes[p+2] - 48;
                final int b3 = bytes[p+3] - 48;
                final int b5 = bytes[p+5] - 48;
                final int b6 = bytes[p+6] - 48;
                final int b8 = bytes[p+8] - 48;
                final int b9 = bytes[p+9] - 48;

                // Quick digit validation (0-9)
                if ((b0 | b1 | b2 | b3 | b5 | b6 | b8 | b9) >= 0 && (b0 <= 9 && b1 <= 9 && b2 <= 9 && b3 <= 9 && b5 <= 9 && b6 <= 9 && b8 <= 9 && b9 <= 9)) {
                    final int year = b0 * 1000 + b1 * 100 + b2 * 10 + b3;
                    final int month = b5 * 10 + b6;
                    final int day = b8 * 10 + b9;

                    // Basic validation of month/day ranges. On invalid values, preserve behavior by throwing.
                    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
                        // Validate day against month lengths including leap year for February.
                        final boolean feb = (month == 2);
                        if (feb) {
                            final boolean leap = ((year % 4 == 0) && (year % 100 != 0)) || (year % 400 == 0);
                            if (day > (leap ? 29 : 28)) {
                                throw new IllegalArgumentException("Could not parse date");
                            }
                        } else if ((month == 4 || month == 6 || month == 9 || month == 11) && day > 30) {
                            throw new IllegalArgumentException("Could not parse date");
                        }

                        // Compute epochDay using integer arithmetic similar to java.time.LocalDate.toEpochDay()
                        long y = year;
                        long m = month;
                        long d = day;
                        if (m <= 2) {
                            y -= 1;
                            m += 12;
                        }
                        long era = y / 400;
                        long yoe = y - era * 400;                       // [0, 399]
                        long doy = (153 * (m - 3) + 2) / 5 + d - 1;     // [0, 365]
                        long doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
                        long epochDay = era * 146097 + doe - 719468;

                        long epochMilli = epochDay * 86_400_000L;

                        // Reuse static instance to match original allocation/instance behavior.
                        synchronized (DATE_INSTANCE) {
                            DATE_INSTANCE.setTime(epochMilli);
                            return DATE_INSTANCE;
                        }
                    } else {
                        throw new IllegalArgumentException("Could not parse date");
                    }
                }
            }
        }

        // Fallback to the original general parser for all other formats to preserve behavior.
        return parseField(bytes, startPos, length, (char) 0xffff);
    }

    /**
     * Static utility to parse a field of type Date from a byte sequence that represents text
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
    public static final Date parseField(byte[] bytes, int startPos, int length, char delimiter) {
        final int limitedLen = nextStringLength(bytes, startPos, length, delimiter);

        if (limitedLen > 0
                && (Character.isWhitespace(bytes[startPos])
                        || Character.isWhitespace(bytes[startPos + limitedLen - 1]))) {
            throw new NumberFormatException(
                    "There is leading or trailing whitespace in the numeric field.");
        }

        final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
        return Date.valueOf(str);
    }
}
