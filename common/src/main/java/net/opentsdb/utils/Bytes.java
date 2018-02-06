// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import javax.xml.bind.DatatypeConverter;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

/**
 * Helper functions to manipulate byte arrays.
 * 
 * @since 3.0
 */
public final class Bytes {

  private Bytes() {  // Can't instantiate.
  }

  /**
   * Create a max byte array with the specified max byte count
   * @param num_bytes the length of returned byte array
   * @return the created max byte array
   */
  public static byte[] createMaxByteArray(int num_bytes) {
    byte[] max_byte_arr = new byte[num_bytes];
    Arrays.fill(max_byte_arr, (byte) 0xff);
    return max_byte_arr;
  }

  // ------------------------------ //
  // Byte array conversion utilies. //
  // ------------------------------ //

  /**
   * Reads a big-endian 2-byte short from the begining of the given array.
   * @param b The array to read from.
   * @return A short integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static short getShort(final byte[] b) {
    return getShort(b, 0);
  }

  /**
   * Reads a big-endian 2-byte short from an offset in the given array.
   * @param b The array to read from.
   * @param offset The offset in the array to start reading from.
   * @return A short integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static short getShort(final byte[] b, final int offset) {
    return (short) (b[offset] << 8 | b[offset + 1] & 0xFF);
  }

  /**
   * Reads a big-endian 2-byte unsigned short from the begining of the
   * given array.
   * @param b The array to read from.
   * @return A positive short integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static int getUnsignedShort(final byte[] b) {
    return getUnsignedShort(b, 0);
  }

  /**
   * Reads a big-endian 2-byte unsigned short from an offset in the
   * given array.
   * @param b The array to read from.
   * @param offset The offset in the array to start reading from.
   * @return A positive short integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static int getUnsignedShort(final byte[] b, final int offset) {
    return getShort(b, offset) & 0x0000FFFF;
  }

  /**
   * Writes a big-endian 2-byte short at the begining of the given array.
   * @param b The array to write to.
   * @param n A short integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static void setShort(final byte[] b, final short n) {
    setShort(b, n, 0);
  }

  /**
   * Writes a big-endian 2-byte short at an offset in the given array.
   * @param b The array to write to.
   * @param n A short integer.
   * @param offset The offset in the array to start writing at.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static void setShort(final byte[] b, final short n,
                              final int offset) {
    b[offset + 0] = (byte) (n >>> 8);
    b[offset + 1] = (byte) (n >>> 0);
  }

  /**
   * Creates a new byte array containing a big-endian 2-byte short integer.
   * @param n A short integer.
   * @return A new byte array containing the given value.
   */
  public static byte[] fromShort(final short n) {
    final byte[] b = new byte[2];
    setShort(b, n);
    return b;
  }

  /**
   * Reads a big-endian 4-byte integer from the begining of the given array.
   * @param b The array to read from.
   * @return An integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static int getInt(final byte[] b) {
    return getInt(b, 0);
  }

  /**
   * Reads a big-endian 4-byte integer from an offset in the given array.
   * @param b The array to read from.
   * @param offset The offset in the array to start reading from.
   * @return An integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static int getInt(final byte[] b, final int offset) {
    return (b[offset + 0] & 0xFF) << 24
         | (b[offset + 1] & 0xFF) << 16
         | (b[offset + 2] & 0xFF) << 8
         | (b[offset + 3] & 0xFF) << 0;
  }

  /**
   * Reads a big-endian 4-byte unsigned integer from the begining of the
   * given array.
   * @param b The array to read from.
   * @return A positive integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static long getUnsignedInt(final byte[] b) {
    return getUnsignedInt(b, 0);
  }

  /**
   * Reads a big-endian 4-byte unsigned integer from an offset in the
   * given array.
   * @param b The array to read from.
   * @param offset The offset in the array to start reading from.
   * @return A positive integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static long getUnsignedInt(final byte[] b, final int offset) {
    return getInt(b, offset) & 0x00000000FFFFFFFFL;
  }

  /**
   * Writes a big-endian 4-byte int at the begining of the given array.
   * @param b The array to write to.
   * @param n An integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static void setInt(final byte[] b, final int n) {
    setInt(b, n, 0);
  }

  /**
   * Writes a big-endian 4-byte int at an offset in the given array.
   * @param b The array to write to.
   * @param n An integer.
   * @param offset The offset in the array to start writing at.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static void setInt(final byte[] b, final int n, final int offset) {
    b[offset + 0] = (byte) (n >>> 24);
    b[offset + 1] = (byte) (n >>> 16);
    b[offset + 2] = (byte) (n >>>  8);
    b[offset + 3] = (byte) (n >>>  0);
  }

  /**
   * Creates a new byte array containing a big-endian 4-byte integer.
   * @param n An integer.
   * @return A new byte array containing the given value.
   */
  public static byte[] fromInt(final int n) {
    final byte[] b = new byte[4];
    setInt(b, n);
    return b;
  }

  /**
   * Reads a big-endian 8-byte long from the begining of the given array.
   * @param b The array to read from.
   * @return A long integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static long getLong(final byte[] b) {
    return getLong(b, 0);
  }

  /**
   * Reads a big-endian 8-byte long from an offset in the given array.
   * @param b The array to read from.
   * @param offset The offset in the array to start reading from.
   * @return A long integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static long getLong(final byte[] b, final int offset) {
    return (b[offset + 0] & 0xFFL) << 56
         | (b[offset + 1] & 0xFFL) << 48
         | (b[offset + 2] & 0xFFL) << 40
         | (b[offset + 3] & 0xFFL) << 32
         | (b[offset + 4] & 0xFFL) << 24
         | (b[offset + 5] & 0xFFL) << 16
         | (b[offset + 6] & 0xFFL) << 8
         | (b[offset + 7] & 0xFFL) << 0;
  }

  /**
   * Writes a big-endian 8-byte long at the begining of the given array.
   * @param b The array to write to.
   * @param n A long integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static void setLong(final byte[] b, final long n) {
    setLong(b, n, 0);
  }

  /**
   * Writes a big-endian 8-byte long at an offset in the given array.
   * @param b The array to write to.
   * @param n A long integer.
   * @param offset The offset in the array to start writing at.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static void setLong(final byte[] b, final long n, final int offset) {
    b[offset + 0] = (byte) (n >>> 56);
    b[offset + 1] = (byte) (n >>> 48);
    b[offset + 2] = (byte) (n >>> 40);
    b[offset + 3] = (byte) (n >>> 32);
    b[offset + 4] = (byte) (n >>> 24);
    b[offset + 5] = (byte) (n >>> 16);
    b[offset + 6] = (byte) (n >>>  8);
    b[offset + 7] = (byte) (n >>>  0);
  }

  /**
   * Creates a new byte array containing a big-endian 8-byte long integer.
   * @param n A long integer.
   * @return A new byte array containing the given value.
   */
  public static byte[] fromLong(final long n) {
    final byte[] b = new byte[8];
    setLong(b, n);
    return b;
  }
  
  /**
   * Determines if the byte array is in the given list.
   * @param list A non-null list to search through.
   * @param needle A non-null needle to search for.
   * @return True if found, false if not.
   */
  public static boolean contains(final List<byte[]> list, final byte[] needle) {
    if (list == null || needle == null) {
      throw new IllegalArgumentException("Neither list nor needle may be null.");
    }
    for (final byte[] stack : list) {
      if (memcmp(stack, needle) == 0) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Converts the given byte array to a hex encoded string using 
   * {@link DatatypeConverter#printHexBinary(byte[])}.
   * @param array A non-null array. May be empty, returning an empty string.
   * @return A non-null string that may be empty.
   * @throws IllegalArgumentException if the array was null.
   */
  public static String byteArrayToString(final byte[] array) {
    if (array == null) {
      throw new IllegalArgumentException("Array cannot be null.");
    }
    if (array.length == 0) {
      return "";
    }
    return DatatypeConverter.printHexBinary(array);
  }
  
  /**
   * Converts the given string into a byte array using 
   * {@link DatatypeConverter#parseHexBinary(String)}.
   * @param hex A non-null string. If empty returns a zero-length byte array.
   * @return A non-null byte array.
   * @throws IllegalArgumentException if the hex string was null or if the 
   * string length was not a multiple of 2.
   */
  public static byte[] stringToByteArray(final String hex) {
    if (hex == null) {
      throw new IllegalArgumentException("Hex value cannot be null.");
    }
    if (hex.isEmpty()) {
      return new byte[0];
    }
    if (hex.length() % 2 != 0) {
      throw new IllegalArgumentException("Hex length must be a multiple of 2.");
    }
    return DatatypeConverter.parseHexBinary(hex);
  }
  
  // ---------------------------- //
  // Pretty-printing byte arrays. //
  // ---------------------------- //

  private static final byte[] HEX = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'
  };

  /**
   * Pretty-prints a byte array into a human-readable output buffer.
   * @param outbuf The buffer where to write the output.
   * @param array The (possibly {@code null}) array to pretty-print.
   */
  public static void pretty(final StringBuilder outbuf, final byte[] array) {
    if (array == null) {
      outbuf.append("null");
      return;
    }
    int ascii = 0;
    final int start_length = outbuf.length();
    final int n = array.length;
    outbuf.ensureCapacity(start_length + 1 + n + 1);
    outbuf.append('"');
    for (int i = 0; i < n; i++) {
      final byte b = array[i];
      if (' ' <= b && b <= '~') {
        ascii++;
        outbuf.append((char) b);
      } else if (b == '\n') {
        outbuf.append('\\').append('n');
      } else if (b == '\t') {
        outbuf.append('\\').append('t');
      } else {
        outbuf.append("\\x")
          .append((char) HEX[(b >>> 4) & 0x0F])
          .append((char) HEX[b & 0x0F]);
      }
    }
    if (ascii < n / 2) {
      outbuf.setLength(start_length);
      outbuf.append(Arrays.toString(array));
    } else {
      outbuf.append('"');
    }
  }

  /**
   * Pretty-prints an array of byte arrays into a human-readable output buffer.
   * @param outbuf The buffer where to write the output.
   * @param arrays The (possibly {@code null}) array of arrays to pretty-print.
   * @since 1.3
   */
  public static void pretty(final StringBuilder outbuf, final byte[][] arrays) {
    if (arrays == null) {
      outbuf.append("null");
      return;
    } else {  // Do some right-sizing.
      int size = 2;
      for (int i = 0; i < arrays.length; i++) {
        size += 2 + 2 + arrays[i].length;
      }
      outbuf.ensureCapacity(outbuf.length() + size);
    }
    outbuf.append('[');
    for (int i = 0; i < arrays.length; i++) {
      Bytes.pretty(outbuf, arrays[i]);
      outbuf.append(", ");
    }
    outbuf.setLength(outbuf.length() - 2);  // Remove the last ", "
    outbuf.append(']');
  }

  /**
   * Pretty-prints a byte array into a human-readable string.
   * @param array The (possibly {@code null}) array to pretty-print.
   * @return The array in a pretty-printed string.
   */
  public static String pretty(final byte[] array) {
    if (array == null) {
      return "null";
    }
    final StringBuilder buf = new StringBuilder(1 + array.length + 1);
    pretty(buf, array);
    return buf.toString();
  }

  // This doesn't really belong here but it doesn't belong anywhere else
  // either, so let's put it close to the other pretty-printing functions.
  /**
   * Pretty-prints a {@code long} into a fixed-width hexadecimal number.
   * @param v The long value to convert to a hex encoded string.
   * @return A string of the form {@code 0x0123456789ABCDEF}.
   */
  public static String hex(long v) {
    final byte[] buf = new byte[2 + 16];
    buf[0] = '0';
    buf[1] = 'x';
    int i = 2 + 16;
    do {
      buf[--i] = HEX[(int) v & 0x0F];
      v >>>= 4;
    } while (v != 0);
    for (/**/; i > 1; i--) {
      buf[i] = '0';
    }
    return new String(buf);
  }
  
  // ---------------------- //
  // Comparing byte arrays. //
  // ---------------------- //
  // Don't ask me why this isn't in java.util.Arrays.

  /**
   * A singleton {@link Comparator} for non-{@code null} byte arrays.
   * @see #memcmp
   */
  public static final MemCmp MEMCMP = new MemCmp();

  /** {@link Comparator} for non-{@code null} byte arrays.  */
  private final static class MemCmp implements Comparator<byte[]> {

    private MemCmp() {  // Can't instantiate outside of this class.
    }

    @Override
    public int compare(final byte[] a, final byte[] b) {
      return memcmp(a, b);
    }

  }

  /**
   * {@code memcmp} in Java, hooray.
   * @param a First non-{@code null} byte array to compare.
   * @param b Second non-{@code null} byte array to compare.
   * @return 0 if the two arrays are identical, otherwise the difference
   * between the first two different bytes, otherwise the different between
   * their lengths.
   */
  public static int memcmp(final byte[] a, final byte[] b) {
    final int length = Math.min(a.length, b.length);
    if (a == b) {  // Do this after accessing a.length and b.length
      return 0;    // in order to NPE if either a or b is null.
    }
    for (int i = 0; i < length; i++) {
      if (a[i] != b[i]) {
        return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
      }
    }
    return a.length - b.length;
  }

  /**
   * {@code memcmp(3)} with a given offset and length.
   * @param a First non-{@code null} byte array to compare.
   * @param b Second non-{@code null} byte array to compare.
   * @param offset The offset at which to start comparing both arrays.
   * @param length The number of bytes to compare.
   * @return 0 if the two arrays are identical, otherwise the difference
   * between the first two different bytes (treated as unsigned), otherwise
   * the different between their lengths.
   * @throws IndexOutOfBoundsException if either array isn't large enough.
   */
  public static int memcmp(final byte[] a, final byte[] b,
                           final int offset, int length) {
    if (a == b && a != null) {
      return 0;
    }
    length += offset;
    for (int i = offset; i < length; i++) {
      if (a[i] != b[i]) {
        return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
      }
    }
    return 0;
  }

  /**
   * De-duplicates two byte arrays.
   * <p>
   * If two byte arrays have the same contents but are different, this
   * function helps to re-use the old one and discard the new copy.
   * @param old The existing byte array.
   * @param neww The new byte array we're trying to de-duplicate.
   * @return {@code old} if {@code neww} is a different array with the same
   * contents, otherwise {@code neww}.
   */
  public static byte[] deDup(final byte[] old, final byte[] neww) {
    return memcmp(old, neww) == 0 ? old : neww;
  }

  /**
   * Tests whether two byte arrays have the same contents.
   * @param a First non-{@code null} byte array to compare.
   * @param b Second non-{@code null} byte array to compare.
   * @return {@code true} if the two arrays are identical,
   * {@code false} otherwise.
   */
  public static boolean equals(final byte[] a, final byte[] b) {
    return memcmp(a, b) == 0;
  }
  
  /**
   * Evaluates two lists of byte arrays for equality. Either list may be 
   * null.
   * @param a A list of byte arrays, possibly null.
   * @param b A list of byte arrays, possibly null.
   * @return True if the lists contain the same byte array contents in the same
   * order. False if not.
   */
  public static boolean equals(final List<byte[]> a, final List<byte[]> b) {
    if (a == null && b == null) {
      return true;
    }
    if (a == null && b != null || a != null && b == null) {
      return false;
    }
    if (a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); i++) {
      if (memcmpMaybeNull(a.get(i), b.get(i)) != 0) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Evaluates two byte maps of byte arrays for equality. Either map may be null.
   * @param a A byte map of byte arrays, possibly null.
   * @param b A byte map of byte arrays, possibly null.
   * @return True if the maps contain the same byte array contents, false if not.
   */
  public static boolean equals(final ByteMap<byte[]> a, final ByteMap<byte[]> b) {
    if (a == null && b == null) {
      return true;
    }
    if (a == null && b != null || a != null && b == null) {
      return false;
    }
    if (a.size() != b.size()) {
      return false;
    }
    for (final Entry<byte[], byte[]> entry : a.entrySet()) {
      final byte[] b_value = b.get(entry.getKey());
      if (b_value == null && entry.getValue() != null) {
        return false;
      }
      if (memcmpMaybeNull(entry.getValue(), b_value) != 0) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * {@code memcmp(3)} in Java for possibly {@code null} arrays, hooray.
   * @param a First possibly {@code null} byte array to compare.
   * @param b Second possibly {@code null} byte array to compare.
   * @return 0 if the two arrays are identical (or both are {@code null}),
   * otherwise the difference between the first two different bytes (treated
   * as unsigned), otherwise the different between their lengths (a {@code
   * null} byte array is considered shorter than an empty byte array).
   */
  public static int memcmpMaybeNull(final byte[] a, final byte[] b) {
    if (a == null) {
      if (b == null) {
        return 0;
      }
      return -1;
    } else if (b == null) {
      return 1;
    }
    return memcmp(a, b);
  }

  /** A convenient map keyed with a byte array.  */
  public static final class ByteMap<V> extends TreeMap<byte[], V>
    implements Iterable<Map.Entry<byte[], V>> {

    public ByteMap() {
      super(MEMCMP);
    }

    /** Returns an iterator that goes through all the entries in this map.  */
    public Iterator<Map.Entry<byte[], V>> iterator() {
      return super.entrySet().iterator();
    }

    /** {@code byte[]} friendly implementation.  */
    public String toString() {
      final int size = size();
      if (size == 0) {
        return "{}";
      }
      final StringBuilder buf = new StringBuilder(size << 4);
      buf.append('{');
      for (final Map.Entry<byte[], V> e : this) {
        Bytes.pretty(buf, e.getKey());
        buf.append('=');
        final V value = e.getValue();
        if (value instanceof byte[]) {
          Bytes.pretty(buf, (byte[]) value);
        } else {
          buf.append(value == this ? "(this map)" : value);
        }
        buf.append(", ");
      }
      buf.setLength(buf.length() - 2);  // Remove the extra ", ".
      buf.append('}');
      return buf.toString();
    }

    private static final long serialVersionUID = 1280744742;

  }

  /** A singleton {@link Funnel} used for hashing a list of byte arrays. */
  public static final ByteListFunnel BYTE_LIST_FUNNEL = new ByteListFunnel();
  
  /** {@link Funnel} used for hashing lists of byte arrays. */
  public static class ByteListFunnel implements Funnel<List<byte[]>> {
    private static final long serialVersionUID = 4102688353857386395L;
    private ByteListFunnel() { }
    @Override
    public void funnel(final List<byte[]> list, final PrimitiveSink sink) {
      if (list == null || list.isEmpty()) {
        return;
      }
      for (final byte[] entry : list) {
        sink.putBytes(entry);
      }
    }
  }
  
  /**
   * A singleton {@link Comparator} for ByteMap&lt;byte[]&gt; maps. Support nulls.
   */
  public static final ByteMapComparator BYTE_MAP_CMP = new ByteMapComparator();
  
  /** {@link Comparator} for ByteMap&lt;byte[]&gt;s .Support nulls.  */
  public static class ByteMapComparator implements Comparator<ByteMap<byte[]>> {
    private ByteMapComparator() { }
    @Override
    public int compare(final ByteMap<byte[]> a, final ByteMap<byte[]> b) {
      if (a == b || a == null && b == null) {
        return 0;
      }
      if (a == null && b != null) {
        return -1;
      }
      if (b == null && a != null) {
        return 1;
      }
      if (a.size() > b.size()) {
        return -1;
      }
      if (b.size() > a.size()) {
        return 1;
      }
      for (final Entry<byte[], byte[]> entry : a.entrySet()) {
        final byte[] b_value = b.get(entry.getKey());
        if (b_value == null && entry.getValue() != null) {
          return 1;
        }
        final int cmp = memcmpMaybeNull(entry.getValue(), b_value);
        if (cmp != 0) {
          return cmp;
        }
      }
      return 0;
    }
  }
  
  /** A singleton {@link Funnel} used for hashing a byte map of byte arrays. */
  public static final ByteMapFunnel BYTE_MAP_FUNNEL = new ByteMapFunnel();
  
  /** {@link Funnel} used for hashing byte maps of byte arrays. */
  public static class ByteMapFunnel implements Funnel<ByteMap<byte[]>> {
    private static final long serialVersionUID = -8868457139144886254L;
    private ByteMapFunnel() { }
    @Override
    public void funnel(final ByteMap<byte[]> map, final PrimitiveSink sink) {
      if (map == null || map.isEmpty()) {
        return;
      }
      for (final Entry<byte[], byte[]> entry : map.entrySet()) {
        sink.putBytes(entry.getKey())
            .putBytes(entry.getValue());
      }
    }
  }
  
  /**
   * Helper to print out a list of byte arrays with specific decoding or 
   * using the {@link #pretty(byte[])} method.
   * @param list A list that may be null, empty or full of byte arrays.
   * @param charset An optional character set. If provided, used to cast to a 
   * string. Otherwise uses {@link #pretty(byte[])}.
   * @return A non-null string.
   */
  public static String toString(final List<byte[]> list, final Charset charset) {
    final StringBuilder buf = new StringBuilder();
    if (list == null) {
      buf.append("null");
    } else if (list.isEmpty()) {
      buf.append("[]");
    } else {
      buf.append("[");
      for (int i = 0; i < list.size(); i++) {
        if (i > 0) {
          buf.append(", ");
        }
        if (charset != null) {
          buf.append(new String(list.get(i), charset));
        } else {
          buf.append(pretty(list.get(i)));
        }
      }
      buf.append("]");
    }
    return buf.toString();
  }
  
  /**
   * Helper to print out a {@link ByteMap} of byte arrays with specific decoding
   * for the key and values or using the {@link #pretty(byte[])} method.
   * @param map A map of values that may be null.
   * @param key_charset An optional character set for the keys. If provided, 
   * used to cast to a string. Otherwise uses {@link #pretty(byte[])}.
   * @param value_charset An optional character set for the values. If provided, 
   * used to cast to a string. Otherwise uses {@link #pretty(byte[])}.
   * @return A non-null string.
   */
  public static String toString(final ByteMap<byte[]> map, 
      final Charset key_charset, final Charset value_charset) {
    final StringBuilder buf = new StringBuilder();
    if (map == null) {
      buf.append("null");
    } else if (map.isEmpty()) {
      buf.append("{}");
    } else {
      int i = 0;
      for (final Entry<byte[], byte[]> entry : map.entrySet()) {
        if (i > 0) {
          buf.append(", ");
        }
        buf.append("{")
           .append(key_charset != null ? new String(entry.getKey(), key_charset) : pretty(entry.getKey()))
           .append("=")
           .append(value_charset != null ? new String(entry.getValue(), value_charset) : pretty(entry.getValue()))
           .append("}");
        i++;
      }
    }
    
    return buf.toString();
  }
  
  /** Wrapper around a byte array to override the {@link #equals(Object)} and
   * {@link #hashCode()} methods so it can be used as the key in a map for
   * instances where we can't use a {@link ByteMap}. 
   */
  public static class ByteArrayKey {
    /** The key. */
    final byte[] key;
    
    /**
     * Default Ctor.
     * @param key A non-null byte array. Empty is ok.
     * @throws IllegalArgumentException if the key was null.
     */
    public ByteArrayKey(final byte[] key) {
      if (key == null) {
        throw new IllegalArgumentException("Null keys not allowed.");
      }
      this.key = key;
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ByteArrayKey other = (ByteArrayKey) o;
      return memcmp(key, other.key) == 0;
    }
    
    @Override
    public int hashCode() {
      return Arrays.hashCode(key);
    }
    
    /** @return The key. */
    public byte[] key() {
      return key;
    }
  }
}
