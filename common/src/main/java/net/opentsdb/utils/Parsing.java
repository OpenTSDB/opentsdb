/*
 * Copyright (c) 1994, 2013, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 * 
 * Copyright (C) 2020  The OpenTSDB Authors.
 */
package net.opentsdb.utils;

// WARNING - watch out here in later JVMs.
import sun.misc.DoubleConsts;
import sun.misc.FDBigInteger;

/**
 * This class is used to efficiently parse long and double values from ASCII
 * encoded strings already stored in a buffer. The built-ins require that a
 * string be constructed with the characters so we're trying to save some 
 * object creation here by passing the underlying buffer and parsing at the
 * proper offsets. Also, the built-ins throw exceptions if the value is in the
 * wrong format. Instead these implementations will return a boolean and stash
 * the output in an array ala c-style. We also re-use the digit buffer in a
 * thread local collection to avoid additional allocations in a high-throughput
 * app.
 * <p>
 * The code is modified from the OpenJDK {@link Long#parseLong(String)} and
 * {@link Double#parseDouble(String)} methods. There is still a dependency on
 * sun.misc.FDBigInteger which creates an object but we'll have to live with it.
 * 
 * @since 3.0
 */
public class Parsing {

  private static final byte[] INFINITY = new byte[] { 
      'I', 'N', 'F', 'I', 'N', 'I', 'T', 'Y' };
  
  /** Bits and bobs from FloatingDecimal that we need. */
  private static final int MAX_DECIMAL_DIGITS = 15;
  private static final double[] SMALL_10_POW = {
      1.0e0,
      1.0e1, 1.0e2, 1.0e3, 1.0e4, 1.0e5,
      1.0e6, 1.0e7, 1.0e8, 1.0e9, 1.0e10,
      1.0e11, 1.0e12, 1.0e13, 1.0e14, 1.0e15,
      1.0e16, 1.0e17, 1.0e18, 1.0e19, 1.0e20,
      1.0e21, 1.0e22
  };
  private static final int MAX_SMALL_TEN = SMALL_10_POW.length-1;
  private static final double[] BIG_10_POW = { 1e16, 1e32, 1e64, 1e128, 1e256 };
  private static final double[] TINY_10_POW = {1e-16, 1e-32, 1e-64, 1e-128, 1e-256 };
  private static final int MAX_NDIGITS = 1100;
  private static final int EXP_SHIFT = DoubleConsts.SIGNIFICAND_WIDTH - 1;
  private static final long FRACT_HOB = (1L << EXP_SHIFT);
  private static final int MAX_DECIMAL_EXPONENT = 308;
  private static final int MIN_DECIMAL_EXPONENT = -324;
  
  /**
   * Thread local buffer for writing digits, initially allocated with 32 chars.
   */
  private static final ThreadLocal<char[]> PARSING_DIGIT_BUFFER = 
      new ThreadLocal<char[]>() {
        @Override
        protected char[] initialValue() {
          return new char[32];
        }
      };
  
  private Parsing() {
    // Don't instantiate me!
  }
  
  /**
   * Attempts to parse a signed integer value from a byte array wherein the
   * integer is encoded as ASCII numerals starting at the offset and finishing
   * at the end indices in the buffer. The method returns true if parsing was
   * successful (instead of throwing an exception) and if so, index 0 of the 
   * output array will contain the parsed value. (back to the c days).
   * <p>
   * Note that this is party cribbed from the {@link Long#parseLong(String)} 
   * method of OpenJDK.
   * 
   * @param buffer The non-null buffer to index into.
   * @param offset The starting index of the value to parse.
   * @param end The index (exclusive) at which to stop parsing.
   * @param output A non-null array with at least one slot to update with the
   * output.
   * @return True if parsing was successful, false if not.
   */
  public static boolean parseLong(final byte[] buffer, 
                                  final int offset, 
                                  final int end, 
                                  final long[] output) {
    boolean is_negative = false;
    if (end - offset > 0) {
      int i = offset;
      final char first_char = (char) buffer[i];
      long limit = -Long.MAX_VALUE;
      if (first_char < '0') {
        if (first_char == '-') {
          is_negative = true;
          limit = Long.MIN_VALUE;
        } else if (first_char != '+') {
          return false;
        }

        if (end - offset == 1) {
          return false;
        }
        i++;
      }
      long min = limit / 10;
      long result = 0;
      int cur_digit;
      while (i < end) {
        cur_digit = Character.digit(buffer[i++], 10);
        if (cur_digit < 0) {
          return false;
        }
        if (result < min) {
          return false;
        }
        result *= 10;
        if (result < limit + cur_digit) {
          return false;
        }
        result -= cur_digit;
      }
      output[0] = is_negative ? result : - result;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Attempts to parse a signed floating point value from a byte array wherein 
   * the number is encoded as ASCII numerals starting at the offset and 
   * finishing at the end indices in the buffer. The method returns true if 
   * parsing was successful (instead of throwing an exception) and if so, index 
   * 0 of the output array will contain the parsed value. (back to the c days).
   * <p>
   * Note that this will parse NaN and INFINITY but will return early if we have
   * a string in the buffer like `NaNMoreJunk`.
   * <p>
   * Note that this is party cribbed from the {@link Double#parseDouble(String)} 
   * method of OpenJDK and subsequently FloatingDecimal.
   * 
   * @param buffer The non-null buffer to index into.
   * @param offset The starting index of the value to parse.
   * @param end The index (exclusive) at which to stop parsing.
   * @param output A non-null array with at least one slot to update with the
   * output.
   * @return True if parsing was successful, false if not.
   */
  public static boolean parseDouble(final byte[] buffer,
                                    final int offset,
                                    final int end,
                                    final double[] output) {
    if (end - offset < 1) {
      return false;
    }
    
    boolean is_negative = false;
    boolean signed = false;
    int decimal_exponent;
    char c;
    int i = offset;
    
    switch (buffer[i]){
    case '-':
      is_negative = true;
    case '+':
      i++;
      signed = true;
    }
    
    c = (char) buffer[i];
    if (c == 'N') { // Check for NaN
      if ((end - i) == 3 && buffer[i + 1] == 'a' && buffer[i + 2] == 'N') {
        // already NaN
        output[0] = Double.NaN;
        return true;
      }
      // invalid then if it wasn't strictly `NaN`.
      return false;
    } else if(c == 'I') { // Check for Infinity strings
      if ((end - i) == INFINITY.length) {
        for (int x = i + 1; x < end; x++) {
          if (buffer[x] != INFINITY[x - offset]) {
            // NOT infinity
            return false;
          }
        }
        output[0] = is_negative ? Double.NEGATIVE_INFINITY :
          Double.POSITIVE_INFINITY;
        return true;
      }
      // Nope, not infinity.
      return false;
    }

    int num_digits = 0;
    boolean decimal_seen = false;
    int decimal_index = 0;
    int leading_zeros = 0;
    // leading zeros.
    while (i < end) {
      c = (char) buffer[i];
      if (c == '0') {
        leading_zeros++;
      } else if (c == '.') {
        if (decimal_seen) {
         return false;
        }
        decimal_index = i;
        if (signed) {
          decimal_index -= 1;
        }
        decimal_seen = true;
      } else {
        break;
      }
      i++;
    }
    
    // pull out the digits into the char array.
    int trailing_zeros= 0;
    char[] digits = PARSING_DIGIT_BUFFER.get();
    if (digits.length < (end - offset)) {
      digits = new char[digits.length * 2];
      PARSING_DIGIT_BUFFER.set(digits);
    }
    while (i < end) {
      c = (char) buffer[i];
      if (c >= '1' && c <= '9') {
        digits[num_digits++] = c;
        trailing_zeros = 0;
      } else if (c == '0') {
        digits[num_digits++] = c;
        trailing_zeros++;
      } else if (c == '.') {
        if (decimal_seen) {
          return false;
        }
        decimal_index = i;
        if (signed) {
          decimal_index -= 1;
        }
        decimal_seen = true;
      } else {
        break;
      }
      i++;
    }
    
    num_digits -= trailing_zeros;
    if (num_digits == 0 && leading_zeros == 0){
      return false;
    }
    
    if (decimal_seen) {
      decimal_exponent = decimal_index - offset - leading_zeros;
    } else {
      decimal_exponent = num_digits + trailing_zeros;
    }
    
    // exponent search
    if ((i < end) && (((c = (char) buffer[i]) =='e') || (c == 'E'))){
      int exponent_sign = 1;
      int exponent = 0;
      boolean overflow = false;
      switch( buffer[++i] ){
      case '-':
        exponent_sign = -1;
      case '+':
        i++;
      }
      int exponent_idx = i - offset;
      
      while (i < end){
        if (exponent >= Integer.MAX_VALUE / 10) {
          overflow = true;
        }
        c = (char) buffer[i++];
        if (c >= '0' && c <= '9') { 
          exponent = exponent * 10 + ((int) c - (int) '0');
        } else {
          i--;
          break;
        }
      }
      
      int limit = 324 + num_digits + trailing_zeros;
      if (overflow || (exponent > limit)){
        decimal_exponent = exponent_sign * limit;
      } else {
        decimal_exponent = decimal_exponent + exponent_sign * exponent;
      }
      
      if (i == exponent_idx) {
        return false;
      }
    }
    
    if (i < end &&
        ((i != end - 1) ||
        (buffer[i] != 'f' &&
         buffer[i] != 'F' &&
         buffer[i] != 'd' &&
         buffer[i] != 'D'))) {
      return false;
    }
    
    if (num_digits == 0) {
      output[0] = is_negative ? Double.doubleToRawLongBits(-0D) : 
        Double.doubleToRawLongBits(0D);
      return true;
    }
    
    int min_digits = Math.min(num_digits, MAX_DECIMAL_DIGITS + 1);
    
    int integer_value = (int) digits[0] - (int) '0';
    int integer_digits = Math.min(min_digits, 9);
    i = 1;
    for (; i < integer_digits; i++) {
      integer_value = integer_value * 10 + (int) digits[i] - (int) '0';
    }
    
    long long_value = (long) integer_value;
    i = integer_digits;
    for (; i < min_digits; i++) {
      long_value = long_value * 10L + (long) ((int) digits[i] - (int) '0');
    }
    double decimal_value = (double) long_value;
    int exp = decimal_exponent - min_digits;

    if (num_digits <= MAX_DECIMAL_DIGITS) {
      if (exp == 0 || decimal_value == 0.0) {
        output[0] = (is_negative) ? -decimal_value : decimal_value;
        return true;
      } else if (exp >= 0) {
        if (exp <= MAX_SMALL_TEN) {
          double real_value = decimal_value * SMALL_10_POW[exp];
          output[0] = (is_negative) ? -real_value : real_value;
          return true;
        }
        int slop = MAX_DECIMAL_DIGITS - min_digits;
        if (exp <= MAX_SMALL_TEN + slop) {
          decimal_value *= SMALL_10_POW[slop];
          double real_value = decimal_value * SMALL_10_POW[exp - slop];
          output[0] = (is_negative) ? -real_value : real_value;
          return true;
        }
      } else {
        if (exp >= -MAX_SMALL_TEN) {
          double rValue = decimal_value / SMALL_10_POW[-exp];
          output[0] = (is_negative) ? -rValue : rValue;
          return true;
        }
      }
    }
    
    if (exp > 0) {
      if (decimal_exponent > MAX_DECIMAL_EXPONENT + 1) {
        output[0] = (is_negative) ? Double.NEGATIVE_INFINITY : 
          Double.POSITIVE_INFINITY;
        return true;
      }
      if ((exp & 15) != 0) {
        decimal_value *= SMALL_10_POW[exp & 15];
      }
      if ((exp >>= 4) != 0) {
        int j;
        for (j = 0; exp > 1; j++, exp >>= 1) {
          if ((exp & 1) != 0) {
            decimal_value *= BIG_10_POW[j];
          }
        }
        
        double t = decimal_value * BIG_10_POW[j];
        if (Double.isInfinite(t)) {
          t = decimal_value / 2.0;
          t *= BIG_10_POW[j];
          if (Double.isInfinite(t)) {
            output[0] = (is_negative) ? Double.NEGATIVE_INFINITY : 
              Double.POSITIVE_INFINITY;
            return true;
          }
          t = Double.MAX_VALUE;
        }
        decimal_value = t;
      }
    } else if (exp < 0) {
      exp = -exp;
      if (decimal_exponent < MIN_DECIMAL_EXPONENT - 1) {
        output[0] =  (is_negative) ? -0.0 : 0.0;
        return true;
      }
      if ((exp & 15) != 0) {
        decimal_value /= SMALL_10_POW[exp & 15];
      }
      if ((exp >>= 4) != 0) {
        int j;
        for (j = 0; exp > 1; j++, exp >>= 1) {
          if ((exp & 1) != 0) {
            decimal_value *= TINY_10_POW[j];
          }
        }
        
        double t = decimal_value * TINY_10_POW[j];
        if (t == 0.0) {
          t = decimal_value * 2.0;
          t *= TINY_10_POW[j];
          if (t == 0.0) {
            output[0] = (is_negative) ? -0.0 : 0.0;
            return true;
          }
          t = Double.MIN_VALUE;
        }
        decimal_value = t;
      }
    }
    
    if (num_digits > MAX_NDIGITS) {
      num_digits = MAX_NDIGITS + 1;
      digits[MAX_NDIGITS] = '1';
    }
    
    // TODO - will this still be in latter JDKs? If not... ew.
    // Likewise, wish we could re-use a single instance of this.
    FDBigInteger big_i = new FDBigInteger(long_value, digits, min_digits, num_digits);
    exp = decimal_exponent - num_digits;
    
    long ieee_bits = Double.doubleToRawLongBits(decimal_value);
    final int B5 = Math.max(0, -exp);
    final int D5 = Math.max(0, exp);
    big_i = big_i.multByPow52(D5, 0);
    big_i.makeImmutable();
    FDBigInteger bigD = null;
    int prevD2 = 0;
    
    while (true) {
      int binexp = (int) (ieee_bits >>> EXP_SHIFT);
      long bigBbits = ieee_bits & DoubleConsts.SIGNIF_BIT_MASK;
      if (binexp > 0) {
        bigBbits |= FRACT_HOB;
      } else {
        assert bigBbits != 0L : bigBbits;
        int leadingZeros = Long.numberOfLeadingZeros(bigBbits);
        int shift = leadingZeros - (63 - EXP_SHIFT);
        bigBbits <<= shift;
        binexp = 1 - shift;
      }
      binexp -= DoubleConsts.EXP_BIAS;
      int lowOrderZeros = Long.numberOfTrailingZeros(bigBbits);
      bigBbits >>>= lowOrderZeros;
      final int bigIntExp = binexp - EXP_SHIFT + lowOrderZeros;
      final int bigIntNBits = EXP_SHIFT + 1 - lowOrderZeros;
      
      int B2 = B5;
      int D2 = D5;
      int Ulp2;
      if (bigIntExp >= 0) {
        B2 += bigIntExp;
      } else {
        D2 -= bigIntExp;
      }
      Ulp2 = B2;
      
      int hulpbias;
      if (binexp <= -DoubleConsts.EXP_BIAS) {
        hulpbias = binexp + lowOrderZeros + DoubleConsts.EXP_BIAS;
      } else {
        hulpbias = 1 + lowOrderZeros;
      }
      B2 += hulpbias;
      D2 += hulpbias;
      int common2 = Math.min(B2, Math.min(D2, Ulp2));
      B2 -= common2;
      D2 -= common2;
      Ulp2 -= common2;
      FDBigInteger bigB = FDBigInteger.valueOfMulPow52(bigBbits, B5, B2);
      if (bigD == null || prevD2 != D2) {
          bigD = big_i.leftShift(D2);
          prevD2 = D2;
      }
      
      FDBigInteger diff;
      int cmpResult;
      boolean overvalue;
      if ((cmpResult = bigB.cmp(bigD)) > 0) {
        overvalue = true;
        diff = bigB.leftInplaceSub(bigD);
        if ((bigIntNBits == 1) && (bigIntExp > -DoubleConsts.EXP_BIAS + 1)) {
          Ulp2 -= 1;
          if (Ulp2 < 0) {
            Ulp2 = 0;
            diff = diff.leftShift(1);
          }
        }
      } else if (cmpResult < 0) {
        overvalue = false;
        diff = bigD.rightInplaceSub(bigB);
      } else {
        break;
      }
      cmpResult = diff.cmpPow52(B5, Ulp2);
      if ((cmpResult) < 0) {
        break;
      } else if (cmpResult == 0) {
        if ((ieee_bits & 1) != 0) {
          ieee_bits += overvalue ? -1 : 1;
        }
        break;
      } else {
        ieee_bits += overvalue ? -1 : 1;
        if (ieee_bits == 0 || ieee_bits == DoubleConsts.EXP_BIT_MASK) {
          break;
        }
        continue;
      }
    }
    if (is_negative) {
      ieee_bits |= DoubleConsts.SIGN_BIT_MASK;
    }
    output[0] = Double.longBitsToDouble(ieee_bits);
    return true;
  }
}