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

/** Parses a text field into a Double. */
@PublicEvolving
public class DoubleParser extends FieldParser<Double> {

    private static final Double DOUBLE_INSTANCE = Double.valueOf(0.0);

    private double result;

    @Override
    public int parseField(
            byte[] bytes, int startPos, int limit, byte[] delimiter, Double reusable) {
        final int endPos = nextStringEndPos(bytes, startPos, limit, delimiter);
        if (endPos < 0) {
            return -1;
        }

        final int len = endPos - startPos;

        // Fast-path: empty field -> same outcome as Double.parseDouble("") which throws NFE
        if (len == 0) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }

        // Only check ASCII whitespace characters; this preserves the original behavior
        // for single-byte characters and avoids widening/Character API overhead.
        if (isAsciiWhitespace(bytes[startPos]) || isAsciiWhitespace(bytes[endPos - 1])) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
            return -1;
        }

        String str =
                new String(bytes, startPos, len, ConfigConstants.DEFAULT_CHARSET);
        try {
            this.result = Double.parseDouble(str);
            return (endPos == limit) ? limit : endPos + delimiter.length;
        } catch (NumberFormatException e) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }
    }

    @Override
    public Double createValue() {
        return DOUBLE_INSTANCE;
    }

    @Override
    public Double getLastResult() {
        return Double.valueOf(this.result);
    }

    /**
     * Static utility to parse a field of type double from a byte sequence that represents text
     * characters (such as when read from a file stream).
     *
     * @param bytes The bytes containing the text data that should be parsed.
     * @param startPos The offset to start the parsing.
     * @param length The length of the byte sequence (counting from the offset).
     * @return The parsed value.
     * @throws IllegalArgumentException Thrown when the value cannot be parsed because the text
     *     represents not a correct number.
     */
    public static final double parseField(byte[] bytes, int startPos, int length) {
        return parseField(bytes, startPos, length, (char) 0xffff);
    }

    /**
     * Static utility to parse a field of type double from a byte sequence that represents text
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
    public static final double parseField(byte[] bytes, int startPos, int length, char delimiter) {
        final int limitedLen = nextStringLength(bytes, startPos, length, delimiter);

        if (limitedLen > 0
                && (Character.isWhitespace(bytes[startPos])
                        || Character.isWhitespace(bytes[startPos + limitedLen - 1]))) {
            throw new NumberFormatException(
                    "There is leading or trailing whitespace in the numeric field.");
        }

        final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
        return Double.parseDouble(str);
    }

    private static boolean isAsciiWhitespace(byte b) {
        // whitespace characters covered: space(32), tab(9), linefeed(10), vertical tab(11),
        // form feed(12), carriage return(13)
        return b == 0x20 || b == 0x09 || b == 0x0A || b == 0x0B || b == 0x0C || b == 0x0D;
    }

}
