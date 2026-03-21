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

        // Empty field: preserve original behavior (Date.valueOf("") throws IllegalArgumentException)
        if (limitedLen == 0) {
            return Date.valueOf("");
        }

        final int end = startPos + limitedLen;

        // Find the two separators '-' while accounting for optional sign on the year.
        int scan = startPos;
        if (bytes[scan] == '+' || bytes[scan] == '-') {
            scan++; // skip leading sign of year
            if (scan >= end) {
                // malformed, let fall through to error
                throw new IllegalArgumentException("Invalid date format");
            }
        }

        int firstSep = -1;
        for (int i = scan; i < end; i++) {
            if (bytes[i] == '-') {
                firstSep = i;
                break;
            }
        }
        if (firstSep == -1) {
            throw new IllegalArgumentException("Invalid date format");
        }

        int secondSep = -1;
        for (int i = firstSep + 1; i < end; i++) {
            if (bytes[i] == '-') {
                secondSep = i;
                break;
            }
        }
        if (secondSep == -1) {
            throw new IllegalArgumentException("Invalid date format");
        }

        // Segments: year = [startPos, firstSep), month = (firstSep, secondSep), day = (secondSep, end)
        int yearLen = firstSep - startPos;
        int monthLen = secondSep - firstSep - 1;
        int dayLen = end - secondSep - 1;

        if (yearLen <= 0 || monthLen <= 0 || dayLen <= 0) {
            throw new IllegalArgumentException("Invalid date format");
        }

        final int year = parseIntFromBytes(bytes, startPos, yearLen, true);
        final int month = parseIntFromBytes(bytes, firstSep + 1, monthLen, false);
        final int day = parseIntFromBytes(bytes, secondSep + 1, dayLen, false);

        // Basic range checks for month/day to avoid unnecessary exceptions from LocalDate/of.
        if (month < 1 || month > 12) {
            throw new IllegalArgumentException("Invalid month value: " + month);
        }
        if (day < 1) {
            throw new IllegalArgumentException("Invalid day value: " + day);
        }
        int maxDay = maxDayOfMonth(year, month);
        if (day > maxDay) {
            throw new IllegalArgumentException("Invalid day value: " + day);
        }

        try {
            java.time.LocalDate ld = java.time.LocalDate.of(year, month, day);
            return Date.valueOf(ld);
        } catch (java.time.DateTimeException dte) {
            // Preserve exception type similar to Date.valueOf(String)
            throw new IllegalArgumentException(dte);
        }
    }

    private static int parseIntFromBytes(byte[] bytes, int offset, int len, boolean allowSign) {
        if (len <= 0) {
            throw new IllegalArgumentException("Invalid integer format");
        }
        int i = offset;
        int end = offset + len;
        int sign = 1;
        int result = 0;

        byte b = bytes[i];
        if (allowSign && (b == '+' || b == '-')) {
            if (b == '-') {
                sign = -1;
            }
            i++;
            if (i >= end) {
                throw new IllegalArgumentException("Invalid integer format");
            }
        }

        for (; i < end; i++) {
            b = bytes[i];
            int digit = b - '0';
            if (digit < 0 || digit > 9) {
                throw new IllegalArgumentException("Invalid integer format");
            }
            result = result * 10 + digit;
        }
        return result * sign;
    }

    private static int maxDayOfMonth(int year, int month) {
        switch (month) {
            case 1:
            case 3:
            case 5:
            case 7:
            case 8:
            case 10:
            case 12:
                return 31;
            case 4:
            case 6:
            case 9:
            case 11:
                return 30;
            case 2:
                return isLeapYear(year) ? 29 : 28;
            default:
                throw new IllegalArgumentException("Invalid month value: " + month);
        }
    }

    private static boolean isLeapYear(int year) {
        // Proleptic Gregorian rule (same as java.time.LocalDate)
        return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
    }

}
