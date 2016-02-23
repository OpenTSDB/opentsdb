// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.query.expression;

import java.util.NoSuchElementException;

/**
 * Parses a Graphite style expression.
 * Please use {@link #isEOF()} before any method call. Otherwise the methods
 * will throw a NoSuchElementException. 
 * @since 2.3
 */
public class ExpressionReader {
  /** The character array to parse */
  protected final char[] chars;

  /** The current index in the character array */
  private int mark = 0;

  /**
   * Default ctor 
   * @param chars The characters to parse
   */
  public ExpressionReader(final char[] chars) {
    if (chars == null) {
      throw new IllegalArgumentException("Character set cannot be null");
    }
    this.chars = chars;
  }

  /** @return the current index */
  public int getMark() {
    return mark;
  }

  /** @return the current character without advancing the index */
  public char peek() {
    if (isEOF()) {
      throw new NoSuchElementException("Index " + mark + " is out of bounds " 
          + chars.length);
    }
    return chars[mark];
  }

  /** @return the current character and advances the index */
  public char next() {
    if (isEOF()) {
      throw new NoSuchElementException("Index " + mark + " is out of bounds " 
          + chars.length);
    }
    return chars[mark++];
  }

  /** @param the number of characters to skip */
  public void skip(final int num) {
    if (num < 0) {
      throw new UnsupportedOperationException("Skipping backwards is not allowed");
    }
    mark += num;
  }

  /**
   * Checks to see if the next character matches the parameter
   * @param c The character to check for
   * @return True if they match, false if not
   */
  public boolean isNextChar(final char c) {
    return peek() == c;
  }

  /** @return true if the given sequence appears next in the array. */
  public boolean isNextSeq(final CharSequence seq) {
    if (seq == null) {
      throw new IllegalArgumentException("Comparative sequence cannot be null");
    }
    for (int i = 0; i < seq.length(); i++) {
      if (mark + i >= chars.length) {
        return false;
      }
      if (chars[mark + i] != seq.charAt(i)) {
        return false;
      }
    }

    return true;
  }

  /** @return the name of the function */
  public String readFuncName() {
    // in case we get something like "  function(foo)" consume a bit
    skipWhitespaces();
    StringBuilder builder = new StringBuilder();
    while (peek() != '(' && !Character.isWhitespace(peek())) {
      builder.append(next());
    }
    skipWhitespaces(); // increment over whitespace after
    return builder.toString();
  }

  /** @return Whether or not the index is at the end of the character array */
  public boolean isEOF() {
    return mark >= chars.length;
  }

  /** Increments the mark over white spaces */
  public void skipWhitespaces() {
    for (int i = mark; i < chars.length; i++) {
      if (Character.isWhitespace(chars[i])) {
        mark++;
      } else {
        break;
      }
    }
  }

  /** @return the next parameter from the expression
   * TODO - may need some work */
  public String readNextParameter() {    
    final StringBuilder builder = new StringBuilder();
    int num_nested = 0;
    while (!isEOF() && !Character.isWhitespace(peek())) {
      final char ch = peek();
      if (ch == '(') {
        num_nested++;
      } else if (ch == ')')  {
        num_nested--;
      }
      
      if (num_nested < 0) {
        break;
      }
      if (num_nested <= 0 && isNextSeq(",,")) {
        break;
      }
      builder.append(next());
    }
    return builder.toString();
  }

  @Override
  public String toString() {
    // make a copy
    return new String(chars);
  }

}
