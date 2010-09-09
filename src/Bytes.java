/*
 * Copyright (c) 2010  StumbleUpon, Inc.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

import java.lang.reflect.Field;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.util.CharsetUtil;

/**
 * Helper functions to manipulate byte arrays.
 */
public final class Bytes {

  private Bytes() {  // Can't instantiate.
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

  /** Transforms a string into an UTF-8 encoded byte array.  */
  public static byte[] UTF8(final String s) {
    return s.getBytes(CharsetUtil.UTF_8);
  }

  /** Transforms a string into an ISO-8859-1 encoded byte array.  */
  public static byte[] ISO88591(final String s) {
    return s.getBytes(CharsetUtil.ISO_8859_1);
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
   * @param outbuf The (possibly {@code null}) buffer where to write the output.
   * @param array The array to pretty-print.
   */
  public static void pretty(final StringBuilder outbuf, final byte[] array) {
    if (array == null) {
      outbuf.append("null");
      return;
    }
    int ascii = 0;
    final int start_length = outbuf.length();
    outbuf.ensureCapacity(start_length + 1 + array.length + 1);
    outbuf.append('"');
    for (final byte b : array) {
      if (' ' <= b && b <= '~') {
        ascii++;
        outbuf.append((char) b);
      } else if (0 < b && b < '!') {
        switch (b) {
          case '\n':
            outbuf.append('\\').append('n');
            break;
          case '\t':
            outbuf.append('\\').append('t');
            break;
          default:
            outbuf.append('^').append((char) ('@' + b));
            break;
        }
      } else {
        outbuf.append("\\x")
          .append((char) HEX[(b >>> 4) & 0x0F])
          .append((char) HEX[b & 0x0F]);
      }
    }
    if (ascii < array.length / 2) {
      outbuf.setLength(start_length);
      outbuf.append(Arrays.toString(array));
    } else {
      outbuf.append('"');
    }
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

  // TODO(tsuna): Remove this unnecessary complication once Netty's
  // ReplayingDecoderBuffer#array is fixed -- see NETTY-346.
  // Background: when using ReplayingDecoder (which makes it easy to deal with
  // unframed RPC responses), the ChannelBuffer we manipulate is in fact a
  // ReplayingDecoderBuffer, a package-private class that Netty uses.  This
  // class, for some reason, throws UnsupportedOperationException on its
  // array() method.  This method is unfortunately the only way to easily dump
  // the contents of a ChannelBuffer, which is useful for debugging or logging
  // unexpected buffers.  So until Netty is fixed, we have to cheat the type
  // system using reflection.
  private static final Class<?> ReplayingDecoderBuffer;
  private static final Field RDB_buffer;
  static {
    try {
      ReplayingDecoderBuffer = Class.forName("org.jboss.netty.handler.codec."
                                             + "replay.ReplayingDecoderBuffer");
      RDB_buffer = ReplayingDecoderBuffer.getDeclaredField("buffer");
      RDB_buffer.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }

  /**
   * Pretty-prints all the bytes of a buffer into a human-readable string.
   * @param buf The (possibly {@code null}) buffer to pretty-print.
   * @return The buffer in a pretty-printed string.
   */
  public static String pretty(final ChannelBuffer buf) {
    if (buf == null) {
      return "null";
    }
    byte[] array;
    if (buf.getClass() != ReplayingDecoderBuffer) {
      array = buf.array();
    } else {
      // TODO(tsuna): Remove this unnecessary complication once Netty's
      // ReplayingDecoderBuffer#array is fixed -- see NETTY-346.
      try {
        final ChannelBuffer wrapped_buf = (ChannelBuffer) RDB_buffer.get(buf);
        array = wrapped_buf.array();
      } catch (IllegalAccessException e) {
        throw new AssertionError("Should not happen: " + e);
      }
    }
    return pretty(array);
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
                           final int offset, final int length) {
    if (a == b && a != null) {
      return 0;
    }
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

}
