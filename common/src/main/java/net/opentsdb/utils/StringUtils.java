// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Helper class for working with strings.
 * @since 3.0
 */
public class StringUtils {

  /** A random number generator. NOT SECURE! */
  private final static Random GENERATOR = new Random(System.currentTimeMillis());
  
  /** A simple upper and lower case basic ASCII character set. */
  private final static int[] ASCII_CHARS;
  static {
    ASCII_CHARS = new int[52];
    int c = 65;
    for (int i = 0; i < ASCII_CHARS.length; i++) {
      ASCII_CHARS[i] = c++;
      // skip carrots, braces and such.
      if (c == 90) {
        c = 97;
      }
    }
  }
  
  /**
   * Utility to generate a random string of upper and lower case basic ASCII
   * characters.
   * @param length A length for the returned string, must be greater than zero. 
   * @return A string of length characters.
   * @throws IllegalArgumentException if the string is less than 1.
   */
  public static String getRandomString(final int length) {
    if (length < 1) {
      throw new IllegalArgumentException("Length must be greater than zero.");
    }
    
    final StringBuilder buffer = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      buffer.append(
          (char) ASCII_CHARS[GENERATOR.nextInt(ASCII_CHARS.length - 1)]);
    }
    return buffer.toString();
  }

  /**
   * Optimized version of {@code String#split} that doesn't use regexps.
   * This function works in O(5n) where n is the length of the string to
   * split.
   * @param s The string to split.
   * @param c The separator to use to split the string.
   * @return A non-null, non-empty array.
   * @since 1.0
   */
  public static String[] splitString(final String s, final char c) {
    final char[] chars = s.toCharArray();
    int num_substrings = 1;
    for (final char x : chars) {
      if (x == c) {
        num_substrings++;
      }
    }
    final String[] result = new String[num_substrings];
    final int len = chars.length;
    int start = 0;  // starting index in chars of the current substring.
    int pos = 0;    // current index in chars.
    int i = 0;      // number of the current substring.
    for (; pos < len; pos++) {
      if (chars[pos] == c) {
        result[i++] = new String(chars, start, pos - start);
        start = pos + 1;
      }
    }
    result[i] = new String(chars, start, pos - start);
    return result;
  }

  /**
   * Same as {@code StringUtils#splitString} but preserves anything in
   * brackets as defined by open-close pairs of \{\}, \[\] and \(\).
   * Preserves bracket ordering and nesting.
   * @param s The string to split.
   * @param c The separator to use to split the string.
   * @return A non-null, non-empty array.
   * @throws IllegalArgumentException if separator is a supported bracket character,
   *  bracket open/close mismatch or too many brackets are nested (max limit = 10)
   * @since 3.0
   */
  public static String[] splitStringWithBrackets(final String s, final char c) {
    if (c == '{' || c == '}' || c == '(' || c == ')' || c == '[' || c == ']') {
      throw new IllegalArgumentException("separator is a bracket character");
    }
    final char[] chars = s.toCharArray();
    int num_substrings = 1;
    final char[] bracketStack = new char[10]; // max depth of 10
    int stackHeadPos = -1;
    for (final char x : chars) {
      if (x == '{' || x == '(' || x == '[') {
        if (stackHeadPos == bracketStack.length - 1) {
          throw new IllegalArgumentException("more than 10 nested brackets");
        }
        bracketStack[++stackHeadPos] = x;
      } else if (x == '}' || x == ')' || x == ']') {
          if (stackHeadPos < 0) {
            throw new IllegalArgumentException("too many brackets closed w/o open");
          }
          if ((x == '}' && bracketStack[stackHeadPos] != '{') ||
              (x == ')' && bracketStack[stackHeadPos] != '(') ||
              (x == ']' && bracketStack[stackHeadPos] != '[') ) {
            throw new IllegalArgumentException("bracket open/close mismatch");
          }
          stackHeadPos--;
      } else if (x == c) {
          if (stackHeadPos == -1) num_substrings++;
      }
    }
    final String[] result = new String[num_substrings];
    final int len = chars.length;
    int start = 0;  // starting index in chars of the current substring.
    int pos = 0;    // current index in chars.
    int i = 0;      // number of the current substring.
    int brackets = 0; // within bracket context or not
    for (; pos < len; pos++) {
      final char x = chars[pos];
      if (x == '{' || x == '(' || x == '[') {
        brackets++;
      } else if (x == '}' || x == ')' || x == ']') {
        brackets--;
      } else if (x == c) {
        if (brackets == 0) {
          result[i++] = new String(chars, start, pos - start);
          start = pos + 1;
        }
      }

    }
    result[i] = new String(chars, start, pos - start);
    return result;
  }

  /**
   * Determines the number of bytes needed to encode the UTF-16 Java string to
   * UTF-8 for serialization.
   * @param string The string to encode.
   * @return The length of the encoded value in bytes. 0 if the string was null
   * or empty.
   */
  public static int stringToUTF8BytesLength(final String string) {
    if (string == null) {
      return 0;
    }

    int len = 0;
    for (int i = 0; i < string.length(); i++) {
      final int c = string.charAt(i);
      if (c < 0x80) {
        len++;
      } else if (c > 0x07FF) {
        len += 3;
      } else {
        len += 2;
      }
    }
    return len;
  }

  /**
   * Encodes the UTF-16 Java string to UTF-8 in the byte array at the given
   * offset.
   * @param string The string to encode.
   * @param buffer The non-null buffer sized properly. See
   * {@link #stringToUTF8Bytes(String, byte[], int)}
   * @param offset The offset in which to start encoding.
   * @return The length of bytes encoded. 0 if the string was null or empty.
   */
  public static int stringToUTF8Bytes(final String string,
                                      final byte[] buffer,
                                      int offset) {
    if (string == null) {
      return 0;
    }

    int start = offset;
    for (int i = 0; i < string.length(); i++) {
      final int c = string.charAt(i);
      if (c < 0x80) {
        buffer[offset++] = (byte) c;
      } else if (c > 0x07FF) {
        buffer[offset++] = (byte) (0xE0 | (c >> 12 & 0x0F));
        buffer[offset++] = (byte) (0x80 | (c >> 6 & 0x3F));
        buffer[offset++] = (byte) (0x80 | (c & 0x3F));
      } else {
        buffer[offset++] = (byte) (0xC0 | (c >> 6));
        buffer[offset++] = (byte) (0x80 | (c & 0x3F));
      }
    }
    return offset - start;
  }

  /**
   * Encodes the UTF-16 Java string to UTF-8 in the byte array at the given
   * offset.
   * @param string The string to encode.
   * @param stream The non-null and opened stream. If sizing is required, see
   * {@link #stringToUTF8Bytes(String, byte[], int)}
   * @return The length of bytes encoded. 0 if the string was null or empty.
   * @throws IOException if the stream throws during writing.
   */
  public static int stringToUTF8Bytes(final String string,
                                      final OutputStream stream) throws IOException {
    if (string == null) {
      return 0;
    }

    int len = 0;
    for (int i = 0; i < string.length(); i++) {
      final int c = string.charAt(i);
      if (c < 0x80) {
        stream.write((byte) c);
        ++len;
      } else if (c > 0x07FF) {
        stream.write((byte) (0xE0 | (c >> 12 & 0x0F)));
        stream.write((byte) (0x80 | (c >> 6 & 0x3F)));
        stream.write((byte) (0x80 | (c & 0x3F)));
        len += 3;
      } else {
        stream.write((byte) (0xC0 | (c >> 6)));
        stream.write((byte) (0x80 | (c & 0x3F)));
        len += 2;
      }
    }
    return len;
  }
}

