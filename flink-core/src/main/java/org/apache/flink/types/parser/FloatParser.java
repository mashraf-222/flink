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

/** Parses a text field into a {@link Float}. */
@PublicEvolving
public class FloatParser extends FieldParser<Float> {

    private float result;

    @Override
    public int parseField(byte[] bytes, int startPos, int limit, byte[] delimiter, Float reusable) {
        final int endPos = nextStringEndPos(bytes, startPos, limit, delimiter);
        if (endPos < 0) {
            return -1;
        }

        if (endPos > startPos
                && (Character.isWhitespace(bytes[startPos])
                        || Character.isWhitespace(bytes[endPos - 1]))) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
            return -1;
        }

        String str =
                new String(bytes, startPos, endPos - startPos, ConfigConstants.DEFAULT_CHARSET);
        try {
            this.result = Float.parseFloat(str);
            return (endPos == limit) ? limit : endPos + delimiter.length;
        } catch (NumberFormatException e) {
            setErrorState(ParseErrorState.NUMERIC_VALUE_FORMAT_ERROR);
            return -1;
        }
    }

    @Override
    public Float createValue() {
        return Float.MIN_VALUE;
    }

    @Override
    public Float getLastResult() {
        return Float.valueOf(this.result);
    }

    /**
     * Static utility to parse a field of type float from a byte sequence that represents text
     * characters (such as when read from a file stream).
     *
     * @param bytes The bytes containing the text data that should be parsed.
     * @param startPos The offset to start the parsing.
     * @param length The length of the byte sequence (counting from the offset).
     * @return The parsed value.
     * @throws IllegalArgumentException Thrown when the value cannot be parsed because the text
     *     represents not a correct number.
     */
    public static final float parseField(byte[] bytes, int startPos, int length) {
        return parseField(bytes, startPos, length, (char) 0xffff);
    }

    /**
     * Static utility to parse a field of type float from a byte sequence that represents text
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
    public static final float parseField(byte[] bytes, int startPos, int length, char delimiter) {
        final int limitedLen = nextStringLength(bytes, startPos, length, delimiter);

        if (limitedLen > 0
                && (Character.isWhitespace(bytes[startPos])
                        || Character.isWhitespace(bytes[startPos + limitedLen - 1]))) {
            throw new NumberFormatException(
                    "There is leading or trailing whitespace in the numeric field.");
        }

        // Fast-path: only handle common ASCII decimal formats:
        //   [+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?
        // Only attempt fast parse when all bytes involved are single-byte ASCII (>= 0).
        // For any non-ASCII or non-conforming input, fall back to the original behavior.
        if (limitedLen == 0) {
            // preserve original behavior for empty fields (will throw NumberFormatException)
            final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
            return Float.parseFloat(str);
        }

        final int end = startPos + limitedLen;
        int idx = startPos;
        // Quick ASCII check for the first byte
        byte b = bytes[idx];
        if (b < 0) {
            // non-ASCII -> fallback
            final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
            return Float.parseFloat(str);
        }

        // optional sign
        boolean negative = false;
        if (b == '+' || b == '-') {
            negative = (b == '-');
            idx++;
            if (idx >= end) {
                final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
                return Float.parseFloat(str);
            }
        }

        // Parse integer part digits
        double intFracAccum = 0.0; // accumulate digits (both integer and fractional as integer)
        int digits = 0;
        boolean sawDigit = false;

        while (idx < end) {
            b = bytes[idx];
            if (b < '0' || b > '9') {
                break;
            }
            sawDigit = true;
            intFracAccum = intFracAccum * 10.0 + (b - '0');
            digits++;
            idx++;
        }

        int fracDigits = 0;
        // fractional part
        if (idx < end && bytes[idx] == '.') {
            idx++;
            if (idx >= end) {
                // trailing dot without digits -> fallback to preserve original parsing behavior
                final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
                return Float.parseFloat(str);
            }
            while (idx < end) {
                b = bytes[idx];
                if (b < '0' || b > '9') {
                    break;
                }
                sawDigit = true;
                intFracAccum = intFracAccum * 10.0 + (b - '0');
                fracDigits++;
                idx++;
            }
        }

        if (!sawDigit) {
            // no digits at all -> fallback
            final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
            return Float.parseFloat(str);
        }

        // exponent part
        int exp = 0;
        if (idx < end && (bytes[idx] == 'e' || bytes[idx] == 'E')) {
            idx++;
            if (idx >= end) {
                // malformed exponent -> fallback
                final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
                return Float.parseFloat(str);
            }
            boolean expNeg = false;
            b = bytes[idx];
            if (b == '+' || b == '-') {
                expNeg = (b == '-');
                idx++;
                if (idx >= end) {
                    final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
                    return Float.parseFloat(str);
                }
            }
            int expVal = 0;
            int expDigits = 0;
            while (idx < end) {
                b = bytes[idx];
                if (b < '0' || b > '9') {
                    // non-digit in exponent -> fallback
                    final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
                    return Float.parseFloat(str);
                }
                expVal = expVal * 10 + (b - '0');
                idx++;
                expDigits++;
                // avoid potential int overflow in pathological exponent strings:
                if (expVal > 100000000) { // very large exponent, clamp and break to keep performance
                    // will be handled correctly by Math.pow later (may produce inf/0)
                    break;
                }
            }
            if (expDigits == 0) {
                final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
                return Float.parseFloat(str);
            }
            exp = expNeg ? -expVal : expVal;
        }

        // if there are remaining characters (non-digit/allowed), fallback
        if (idx != end) {
            final String str = new String(bytes, startPos, limitedLen, ConfigConstants.DEFAULT_CHARSET);
            return Float.parseFloat(str);
        }

        // All checked: compute the floating value.
        // intFracAccum holds all digits as integer; fracDigits counts digits after decimal point.
        int adjustedExp = exp - fracDigits;

        // Use Math.pow for exponent scaling. Using double for intermediate to keep precision before cast.
        double value = intFracAccum;
        if (adjustedExp != 0) {
            value = value * Math.pow(10.0, adjustedExp);
        }
        if (negative) {
            value = -value;
        }
        // Cast to float (matches Float.parseFloat behavior broadly for decimal numbers).
        return (float) value;
    }
}
