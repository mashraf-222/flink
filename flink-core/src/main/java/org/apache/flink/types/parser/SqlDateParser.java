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

        // Fast path: parse yyyy-mm-dd directly from bytes to avoid allocating a String.
        final int len = endPos - startPos;
        if (len == 0) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }

        int i = startPos;
        final int end = endPos;

        // Parse year (allow optional leading '-')
        boolean negativeYear = false;
        if (bytes[i] == '-') {
            negativeYear = true;
            i++;
            if (i == end) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
        }

        int year = 0;
        int yearStart = i;
        while (i < end) {
            int b = bytes[i];
            if (b < '0' || b > '9') {
                break;
            }
            year = year * 10 + (b - '0');
            i++;
        }
        if (i == yearStart) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }
        if (negativeYear) {
            year = -year;
        }

        // Expect '-'
        if (i >= end || bytes[i] != '-') {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }
        i++; // skip '-'

        // Parse month
        int month = 0;
        int monthStart = i;
        while (i < end) {
            int b = bytes[i];
            if (b < '0' || b > '9') {
                break;
            }
            month = month * 10 + (b - '0');
            i++;
        }
        if (i == monthStart) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }

        // Expect '-'
        if (i >= end || bytes[i] != '-') {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }
        i++; // skip '-'

        // Parse day
        int day = 0;
        int dayStart = i;
        while (i < end) {
            int b = bytes[i];
            if (b < '0' || b > '9') {
                setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
                return -1;
            }
            day = day * 10 + (b - '0');
            i++;
        }
        if (i == dayStart) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }

        // Ensure we've consumed exactly the field
        if (i != end) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }

        try {
            // Validate and build the date. Use fully-qualified LocalDate to avoid adding imports.
            this.result = Date.valueOf(java.time.LocalDate.of(year, month, day));
            return (endPos == limit) ? limit : endPos + delimiter.length;
        } catch (RuntimeException e) {
            // Matches original behavior which catches IllegalArgumentException from Date.valueOf(str)
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
