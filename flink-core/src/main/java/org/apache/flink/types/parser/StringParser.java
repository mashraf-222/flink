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

/**
 * Converts a variable length field of a byte array into a {@link String}. The byte contents between
 * delimiters is interpreted as an ASCII string. The string may be quoted in double quotes. For
 * quoted strings, whitespaces (space and tab) leading and trailing before and after the quotes are
 * removed.
 */
@PublicEvolving
public class StringParser extends FieldParser<String> {

    private boolean quotedStringParsing = false;
    private byte quoteCharacter;
    private static final byte BACKSLASH = 92;

    private String result;

    public void enableQuotedStringParsing(byte quoteCharacter) {
        this.quotedStringParsing = true;
        this.quoteCharacter = quoteCharacter;
    }

    @Override
    public int parseField(
            byte[] bytes, int startPos, int limit, byte[] delimiter, String reusable) {

        if (startPos == limit) {
            setErrorState(ParseErrorState.EMPTY_COLUMN);
            this.result = "";
            return limit;
        }

        int i = startPos;

        final int delimLen = delimiter.length;
        final int delimLimit = limit - delimLen + 1;
        final java.nio.charset.Charset cs = getCharset();

        // Local copies for faster access in hot loops
        final boolean qsp = this.quotedStringParsing;
        final byte qchar = this.quoteCharacter;

        if (qsp && bytes[i] == qchar) {
            // quoted string parsing enabled and first character is a quote
            i++;

            // search for ending quote character, continue when it is escaped
            while (i < limit && (bytes[i] != qchar || bytes[i - 1] == BACKSLASH)) {
                i++;
            }

            if (i == limit) {
                setErrorState(ParseErrorState.UNTERMINATED_QUOTED_STRING);
                return -1;
            } else {
                i++;
                // check for proper termination
                if (i == limit) {
                    // either by end of line
                    this.result = new String(bytes, startPos + 1, i - startPos - 2, cs);
                    return limit;
                } else {
                    // If delimiter length is 1, do a fast single-byte compare instead of calling delimiterNext
                    if (delimLen == 1) {
                        byte d = delimiter[0];
                        if (i < delimLimit && bytes[i] == d) {
                            this.result = new String(bytes, startPos + 1, i - startPos - 2, cs);
                            return i + delimLen;
                        } else {
                            setErrorState(ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING);
                            return -1;
                        }
                    } else {
                        if (i < delimLimit && delimiterNext(bytes, i, delimiter)) {
                            this.result = new String(bytes, startPos + 1, i - startPos - 2, cs);
                            return i + delimLen;
                        } else {
                            // no proper termination
                            setErrorState(ParseErrorState.UNQUOTED_CHARS_AFTER_QUOTED_STRING);
                            return -1;
                        }
                    }
                }
            }
        } else {

            // look for delimiter
            if (delimLen == 1) {
                byte d = delimiter[0];
                while (i < delimLimit && bytes[i] != d) {
                    i++;
                }
            } else {
                while (i < delimLimit && !delimiterNext(bytes, i, delimiter)) {
                    i++;
                }
            }

            if (i >= delimLimit) {
                this.result = new String(bytes, startPos, limit - startPos, cs);
                return limit;
            } else {
                // delimiter found.
                if (i == startPos) {
                    setErrorState(ParseErrorState.EMPTY_COLUMN); // mark empty column
                }
                this.result = new String(bytes, startPos, i - startPos, cs);
                return i + delimLen;
            }
        }
    }

    @Override
    public String createValue() {
        return "";
    }

    @Override
    public String getLastResult() {
        return this.result;
    }
}
