// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;

/**
 * Represents a single numeric data point.
 * <p>
 * Implementations of this interface aren't expected to be synchronized.
 */
public interface NumericType extends TimeSeriesDataType {

  /** The data type reference to pass around. */
  public static final TypeToken<NumericType> TYPE = TypeToken.of(NumericType.class);
  
  @Override
  default TypeToken<? extends TimeSeriesDataType> type() {
    return TYPE;
  }
  
  /**
   * Tells whether or not the this data point is a value of integer type.
   * @return {@code true} if the value is of integer type (call {@link #longValue()}), 
   * {@code false} if it's of double point type (call {@link #doubleValue()}.
   */
  public boolean isInteger();

  /**
   * Returns the value of the this data point as a {@code long}.
   * @return The value as a long if {@link #isInteger()} was true.
   * @throws ClassCastException if the {@code isInteger() == false}.
   */
  public long longValue();

  /**
   * Returns the value of the this data point as a {@code double}.
   * @return The value as a double if {@link #isInteger()} was false.
   * @throws ClassCastException if the {@code isInteger() == true}.
   */
  public double doubleValue();

  /**
   * Returns the value of the this data point as a {@code double}, even if
   * it's a {@code long}.
   * @return When {@code isInteger() == false}, this method returns the same
   * thing as {@link #doubleValue}.  Otherwise, it returns the same thing as
   * {@link #longValue}'s return value casted to a {@code double}.
   */
  public double toDouble();
  
  /**
   * Returns true if the given string can fit into a float.
   * @param value The String holding the float value.
   * @return true if the value can fit into a float, false otherwise.
   * @throws NumberFormatException if the value is not numeric.
   */
  public static boolean fitsInFloat(final String value) {
    // TODO - probably still a better way to do this and we could save a lot
    // of space by dropping useless precision, but for now this should help. 
    final double d = Double.parseDouble(value);
    return ((float) d) == d;
  }
  
  /**
   * Returns true if the given double can fit into a float.
   * @param value The double to evaluate
   * @return true if the value can fit into a float, false otherwise.
   * @throws NumberFormatException if the value is not numeric.
   */
  public static boolean fitsInFloat(final double value) {
    // TODO - probably still a better way to do this and we could save a lot
    // of space by dropping useless precision, but for now this should help. 
    return ((float) value) == value;
  }

  /**
   * Returns true if the given string looks like an integer.
   * <p>
   * This function doesn't do any checking on the string other than looking
   * for some characters that are generally found in floating point values
   * such as '.' or 'e'.
   * @param value The value to validate.
   * @return True if it appears to be an integer, false if not.
   * @since 1.1
   */
  public static boolean looksLikeInteger(final String value) {
    final int n = value.length();
    for (int i = 0; i < n; i++) {
      final char c = value.charAt(i);
      if (c == '.' || c == 'e' || c == 'E') {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Parses an integer value as a long from the given character sequence.
   * <p>
   * This is equivalent to {@link Long#parseLong(String)} except it's up to
   * 100% faster on {@link String} and always works in O(1) space even with
   * {@link StringBuilder} buffers (where it's 2x to 5x faster).
   * @param s The character sequence containing the integer value to parse.
   * @return The value parsed.
   * @throws NumberFormatException if the value is malformed or overflows.
   * @since 1.0
   */
  public static long parseLong(final CharSequence s) {
    final int n = s.length();  // Will NPE if necessary.
    if (n == 0) {
      throw new NumberFormatException("Empty string");
    }
    char c = s.charAt(0);  // Current character.
    int i = 1;  // index in `s'.
    if (c < '0' && (c == '+' || c == '-')) {  // Only 1 test in common case.
      if (n == 1) {
        throw new NumberFormatException("Just a sign, no value: " + s);
      } else if (n > 20) {  // "+9223372036854775807" or "-9223372036854775808"
          throw new NumberFormatException("Value too long: " + s);
      }
      c = s.charAt(1);
      i = 2;  // Skip over the sign.
    } else if (n > 19) {  // "9223372036854775807"
      throw new NumberFormatException("Value too long: " + s);
    }
    long v = 0;  // The result (negated to easily handle MIN_VALUE).
    do {
      if ('0' <= c && c <= '9') {
        v -= c - '0';
      } else {
        throw new NumberFormatException("Invalid character '" + c
                                        + "' in " + s);
      }
      if (i == n) {
        break;
      }
      v *= 10;
      c = s.charAt(i++);
    } while (true);
    if (v > 0) {
      throw new NumberFormatException("Overflow in " + s);
    } else if (s.charAt(0) == '-') {
      return v;  // Value is already negative, return unchanged.
    } else if (v == Long.MIN_VALUE) {
      throw new NumberFormatException("Overflow in " + s);
    } else {
      return -v;  // Positive value, need to fix the sign.
    }
  }

}
