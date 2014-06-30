// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import net.opentsdb.uid.UniqueId;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Callback;

/**
 * <strong>This class is not part of the public API.</strong>
 * <p><pre>
 * ,____________________________,
 * | This class is reserved for |
 * | OpenTSDB's internal usage! |
 * `----------------------------'
 *       \                   / \  //\
 *        \    |\___/|      /   \//  \\
 *             /0  0  \__  /    //  | \ \
 *            /     /  \/_/    //   |  \  \
 *            @_^_@'/   \/_   //    |   \   \
 *            //_^_/     \/_ //     |    \    \
 *         ( //) |        \///      |     \     \
 *       ( / /) _|_ /   )  //       |      \     _\
 *     ( // /) '/,_ _ _/  ( ; -.    |    _ _\.-~        .-~~~^-.
 *   (( / / )) ,-{        _      `-.|.-~-.           .~         `.
 *  (( // / ))  '/\      /                 ~-. _ .-~      .-~^-.  \
 *  (( /// ))      `.   {            }                   /      \  \
 *   (( / ))     .----~-.\        \-'                 .~         \  `. \^-.
 *              ///.----../        \             _ -~             `.  ^-`  ^-_
 *                ///-._ _ _ _ _ _ _}^ - - - - ~                     ~-- ,.-~
 *                                                                   /.-~
 *              You've been warned by the dragon!
 * </pre><p>
 * This class is reserved for OpenTSDB's own internal usage only.  If you use
 * anything from this package outside of OpenTSDB, a dragon will spontaneously
 * appear and eat you.  You've been warned.
 * <p>
 * This class only exists because Java's packaging system is annoying as the
 * "package-private" accessibility level only applies to the current package
 * but not its sub-packages,  and because Java doesn't have fine-grained API
 * visibility mechanism such as that of Scala or C++.
 * <p>
 * This package provides access into internal methods for higher-level
 * packages, for the sake of reducing code duplication and (ab)use of
 * reflection.
 */
public final class Internal {

  /** @see Const#FLAG_BITS  */
  public static final short FLAG_BITS = Const.FLAG_BITS;

  /** @see Const#LENGTH_MASK  */
  public static final short LENGTH_MASK = Const.LENGTH_MASK;

  /** @see Const#FLAGS_MASK  */
  public static final short FLAGS_MASK = Const.FLAGS_MASK;

  private Internal() {
    // Can't instantiate.
  }

  /** @see TsdbQuery#getScanner */
  public static Scanner getScanner(final Query query) {
    return ((TsdbQuery) query).getScanner();
  }

  /** @see RowKey#metricName */
  public static String metricName(final TSDB tsdb, final byte[] id) {
    return RowKey.metricName(tsdb, id);
  }

  /** Extracts the timestamp from a row key.  */
  public static long baseTime(final TSDB tsdb, final byte[] row) {
    return Bytes.getUnsignedInt(row, tsdb.metrics.width());
  }

  /** @see Tags#getTags */
  public static Map<String, String> getTags(final TSDB tsdb, final byte[] row) {
    return Tags.getTags(tsdb, row);
  }

  /** @see RowSeq#extractIntegerValue */
  public static long extractIntegerValue(final byte[] values,
                                         final int value_idx,
                                         final byte flags) {
    return RowSeq.extractIntegerValue(values, value_idx, flags);
  }

  /** @see RowSeq#extractFloatingPointValue */
  public static double extractFloatingPointValue(final byte[] values,
                                                 final int value_idx,
                                                 final byte flags) {
    return RowSeq.extractFloatingPointValue(values, value_idx, flags);
  }

  /** @see TSDB#metrics_width() */
  public static short metricWidth(final TSDB tsdb) {
    return tsdb.metrics.width();
  }

  /**
   * Extracts a Cell from a single data point, fixing potential errors with
   * the qualifier flags
   * @param column The column to parse
   * @return A Cell if successful, null if the column did not contain a data
   * point (i.e. it was meta data) or failed to parse
   * @throws IllegalDataException  if the qualifier was not 2 bytes long or
   * it wasn't a millisecond qualifier
   * @since 2.0
   */
  public static Cell parseSingleValue(final KeyValue column) {
    if (column.qualifier().length == 2 || (column.qualifier().length == 4 && 
        inMilliseconds(column.qualifier()))) {
      final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
      row.add(column);
      final ArrayList<Cell> cells = extractDataPoints(row, 1);
      if (cells.isEmpty()) {
        return null;
      }
      return cells.get(0);
    }
    throw new IllegalDataException (
        "Qualifier does not appear to be a single data point: " + column);
  }
  
  /**
   * Extracts the data points from a single column.
   * While it's meant for use on a compacted column, you can pass any other type
   * of column and it will be returned. If the column represents a data point, 
   * a single cell will be returned. If the column contains an annotation or
   * other object, the result will be an empty array list. Compacted columns
   * will be split into individual data points.
   * <b>Note:</b> This method does not account for duplicate timestamps in
   * qualifiers.
   * @param column The column to parse
   * @return An array list of data point {@link Cell} objects. The list may be 
   * empty if the column did not contain a data point.
   * @throws IllegalDataException if one of the cells cannot be read because
   * it's corrupted or in a format we don't understand.
   * @since 2.0
   */
  public static ArrayList<Cell> extractDataPoints(final KeyValue column) {
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(column);
    return extractDataPoints(row, column.qualifier().length / 2);
  }
  
  /**
   * Breaks down all the values in a row into individual {@link Cell}s sorted on
   * the qualifier. Columns with non data-point data will be discarded.
   * <b>Note:</b> This method does not account for duplicate timestamps in
   * qualifiers.
   * @param row An array of data row columns to parse
   * @param estimated_nvalues Estimate of the number of values to compact.
   * Used to pre-allocate a collection of the right size, so it's better to
   * overshoot a bit to avoid re-allocations.
   * @return An array list of data point {@link Cell} objects. The list may be 
   * empty if the row did not contain a data point.
   * @throws IllegalDataException if one of the cells cannot be read because
   * it's corrupted or in a format we don't understand.
   * @since 2.0
   */
  public static ArrayList<Cell> extractDataPoints(final ArrayList<KeyValue> row,
      final int estimated_nvalues) {
    final ArrayList<Cell> cells = new ArrayList<Cell>(estimated_nvalues);
    for (final KeyValue kv : row) {
      final byte[] qual = kv.qualifier();
      final int len = qual.length;
      final byte[] val = kv.value();
      
      if (len % 2 != 0) {
        // skip a non data point column
        continue;
      } else if (len == 2) {  // Single-value cell.
        // Maybe we need to fix the flags in the qualifier.
        final byte[] actual_val = fixFloatingPointValue(qual[1], val);
        final byte q = fixQualifierFlags(qual[1], actual_val.length);
        final byte[] actual_qual;
        
        if (q != qual[1]) {  // We need to fix the qualifier.
          actual_qual = new byte[] { qual[0], q };  // So make a copy.
        } else {
          actual_qual = qual;  // Otherwise use the one we already have.
        }
        
        final Cell cell = new Cell(actual_qual, actual_val);
        cells.add(cell);
        continue;
      } else if (len == 4 && inMilliseconds(qual[0])) {
        // since ms support is new, there's nothing to fix
        final Cell cell = new Cell(qual, val);
        cells.add(cell);
        continue;
      }
      
      // Now break it down into Cells.
      int val_idx = 0;
      try {
        for (int i = 0; i < len; i += 2) {
          final byte[] q = extractQualifier(qual, i);
          final int vlen = getValueLengthFromQualifier(qual, i);
          if (inMilliseconds(qual[i])) {
            i += 2;
          }
          
          final byte[] v = new byte[vlen];
          System.arraycopy(val, val_idx, v, 0, vlen);
          val_idx += vlen;
          final Cell cell = new Cell(q, v);
          cells.add(cell);
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IllegalDataException("Corrupted value: couldn't break down"
            + " into individual values (consumed " + val_idx + " bytes, but was"
            + " expecting to consume " + (val.length - 1) + "): " + kv
            + ", cells so far: " + cells);
      }
      
      // Check we consumed all the bytes of the value.  Remember the last byte
      // is metadata, so it's normal that we didn't consume it.
      if (val_idx != val.length - 1) {
        throw new IllegalDataException("Corrupted value: couldn't break down"
          + " into individual values (consumed " + val_idx + " bytes, but was"
          + " expecting to consume " + (val.length - 1) + "): " + kv
          + ", cells so far: " + cells);
      }
    }
    
    Collections.sort(cells);
    return cells;
  }
  
  /**
   * Represents a single data point in a row. Compacted columns may not be
   * stored in a cell.
   * <p>
   * This is simply a glorified pair of (qualifier, value) that's comparable.
   * Only the qualifier is used to make comparisons.
   * @since 2.0
   */
  public static final class Cell implements Comparable<Cell> {
    /** Tombstone used as a helper during the complex compaction.  */
    public static final Cell SKIP = new Cell(null, null);

    final byte[] qualifier;
    final byte[] value;

    /**
     * Constructor that sets the cell
     * @param qualifier Qualifier to store
     * @param value Value to store
     */
    public Cell(final byte[] qualifier, final byte[] value) {
      this.qualifier = qualifier;
      this.value = value;
    }

    /** Compares the qualifiers of two cells */
    public int compareTo(final Cell other) {
      return compareQualifiers(qualifier, 0, other.qualifier, 0);
    }

    /** Determines if the cells are equal based on their qualifier */
    @Override
    public boolean equals(final Object o) {
      return o != null && o instanceof Cell && compareTo((Cell) o) == 0;
    }

    /** @return a hash code based on the qualifier bytes */
    @Override
    public int hashCode() {
      return Arrays.hashCode(qualifier);
    }

    /** Prints the raw data of the qualifier and value */
    @Override
    public String toString() {
      return "Cell(" + Arrays.toString(qualifier)
        + ", " + Arrays.toString(value) + ')';
    }
  
    /** @return the qualifier byte array */
    public byte[] qualifier() {
      return qualifier;
    }
    
    /** @return the value byte array */
    public byte[] value() {
      return value;
    }
    
    /**
     * Returns the value of the cell as a Number for passing to a StringBuffer
     * @return The numeric value of the cell
     * @throws IllegalDataException if the value is invalid
     */
    public Number parseValue() {
      if (isInteger()) {
        return extractIntegerValue(value, 0, 
            (byte)getFlagsFromQualifier(qualifier));
      } else {
        return extractFloatingPointValue(value, 0, 
            (byte)getFlagsFromQualifier(qualifier));
      }      
    }

    /**
     * Returns the Unix epoch timestamp in milliseconds
     * @param base_time Row key base time to add the offset to
     * @return Unix epoch timestamp in milliseconds
     */
    public long timestamp(final long base_time) {
      return getTimestampFromQualifier(qualifier, base_time);
    }
    
    /**
     * Returns the timestamp as stored in HBase for the cell, i.e. in seconds
     * or milliseconds
     * @param base_time Row key base time to add the offset to
     * @return Unix epoch timestamp
     */
    public long absoluteTimestamp(final long base_time) {
      final long timestamp = getTimestampFromQualifier(qualifier, base_time);
      if (inMilliseconds(qualifier)) {
        return timestamp;
      } else {
        return timestamp / 1000;
      }
    }
    
    /** @return Whether or not the value is an integer */
    public boolean isInteger() {
      return (Internal.getFlagsFromQualifier(qualifier) & 
          Const.FLAG_FLOAT) == 0x0;
    }
  }
  
  /**
   * Helper to sort a row with a mixture of millisecond and second data points.
   * In such a case, we convert all of the seconds into millisecond timestamps,
   * then perform the comparison.
   * <b>Note:</b> You must filter out all but the second, millisecond and
   * compacted rows
   * @since 2.0
   */
  public static final class KeyValueComparator implements Comparator<KeyValue> {
    
    /**
     * Compares the qualifiers from two key values
     * @param a The first kv
     * @param b The second kv
     * @return 0 if they have the same timestamp, -1 if a is less than b, 1 
     * otherwise.
     */
    public int compare(final KeyValue a, final KeyValue b) {
      return compareQualifiers(a.qualifier(), 0, b.qualifier(), 0);
    }
    
  }

  /**
   * Callback used to fetch only the last data point from a row returned as the
   * result of a GetRequest. Non data points will be parsed out and the
   * resulting time and value stored in an IncomingDataPoint if found. If no 
   * valid data was found, a null is returned.
   * @since 2.0
   */
  public static class GetLastDataPointCB implements Callback<IncomingDataPoint, 
    ArrayList<KeyValue>> {
    final TSDB tsdb;
    
    public GetLastDataPointCB(final TSDB tsdb) {
      this.tsdb = tsdb;
    }
    
    /**
     * Returns the last data point from a data row in the TSDB table.
     * @param row The row from HBase
     * @return null if no data was found, a data point if one was
     */
    public IncomingDataPoint call(final ArrayList<KeyValue> row) 
      throws Exception {
      if (row == null || row.size() < 1) {
        return null;
      }
      
      // check to see if the cells array is empty as it will flush out all
      // non-data points.
      final ArrayList<Cell> cells = extractDataPoints(row, row.size());
      if (cells.isEmpty()) {
        return null;
      }
      final Cell cell = cells.get(cells.size() - 1);
      final IncomingDataPoint dp = new IncomingDataPoint();
      final long base_time = baseTime(tsdb, row.get(0).key());
      dp.setTimestamp(getTimestampFromQualifier(cell.qualifier(), base_time));
      dp.setValue(cell.parseValue().toString());
      return dp;
    }
  }
  
  /**
   * Compares two data point byte arrays with offsets.
   * Can be used on:
   * <ul><li>Single data point columns</li>
   * <li>Compacted columns</li></ul>
   * <b>Warning:</b> Does not work on Annotation or other columns
   * @param a The first byte array to compare
   * @param offset_a An offset for a
   * @param b The second byte array
   * @param offset_b An offset for b
   * @return 0 if they have the same timestamp, -1 if a is less than b, 1 
   * otherwise.
   * @since 2.0
   */
  public static int compareQualifiers(final byte[] a, final int offset_a, 
      final byte[] b, final int offset_b) {
    final long left = Internal.getOffsetFromQualifier(a, offset_a);
    final long right = Internal.getOffsetFromQualifier(b, offset_b);
    if (left == right) {
      return 0;
    }
    return (left < right) ? -1 : 1;
  }
  
  /**
   * Fix the flags inside the last byte of a qualifier.
   * <p>
   * OpenTSDB used to not rely on the size recorded in the flags being
   * correct, and so for a long time it was setting the wrong size for
   * floating point values (pretending they were encoded on 8 bytes when
   * in fact they were on 4).  So overwrite these bits here to make sure
   * they're correct now, because once they're compacted it's going to
   * be quite hard to tell if the flags are right or wrong, and we need
   * them to be correct to easily decode the values.
   * @param flags The least significant byte of a qualifier.
   * @param val_len The number of bytes in the value of this qualifier.
   * @return The least significant byte of the qualifier with correct flags.
   */
  public static byte fixQualifierFlags(byte flags, final int val_len) {
    // Explanation:
    //   (1) Take the last byte of the qualifier.
    //   (2) Zero out all the flag bits but one.
    //       The one we keep is the type (floating point vs integer value).
    //   (3) Set the length properly based on the value we have.
    return (byte) ((flags & ~(Const.FLAGS_MASK >>> 1)) | (val_len - 1));
    //              ^^^^^   ^^^^^^^^^^^^^^^^^^^^^^^^^    ^^^^^^^^^^^^^
    //               (1)               (2)                    (3)
  }
  
  /**
   * Returns whether or not this is a floating value that needs to be fixed.
   * <p>
   * OpenTSDB used to encode all floating point values as `float' (4 bytes)
   * but actually store them on 8 bytes, with 4 leading 0 bytes, and flags
   * correctly stating the value was on 4 bytes.
   * (from CompactionQueue)
   * @param flags The least significant byte of a qualifier.
   * @param value The value that may need to be corrected.
   */
  public static boolean floatingPointValueToFix(final byte flags,
                                                 final byte[] value) {
    return (flags & Const.FLAG_FLOAT) != 0   // We need a floating point value.
      && (flags & Const.LENGTH_MASK) == 0x3  // That pretends to be on 4 bytes.
      && value.length == 8;                  // But is actually using 8 bytes.
  }

  /**
   * Returns a corrected value if this is a floating point value to fix.
   * <p>
   * OpenTSDB used to encode all floating point values as `float' (4 bytes)
   * but actually store them on 8 bytes, with 4 leading 0 bytes, and flags
   * correctly stating the value was on 4 bytes.
   * <p>
   * This function detects such values and returns a corrected value, without
   * the 4 leading zeros.  Otherwise it returns the value unchanged.
   * (from CompactionQueue)
   * @param flags The least significant byte of a qualifier.
   * @param value The value that may need to be corrected.
   * @throws IllegalDataException if the value is malformed.
   */
  public static byte[] fixFloatingPointValue(final byte flags,
                                              final byte[] value) {
    if (floatingPointValueToFix(flags, value)) {
      // The first 4 bytes should really be zeros.
      if (value[0] == 0 && value[1] == 0 && value[2] == 0 && value[3] == 0) {
        // Just keep the last 4 bytes.
        return new byte[] { value[4], value[5], value[6], value[7] };
      } else {  // Very unlikely.
        throw new IllegalDataException("Corrupted floating point value: "
          + Arrays.toString(value) + " flags=0x" + Integer.toHexString(flags)
          + " -- first 4 bytes are expected to be zeros.");
      }
    }
    return value;
  }
  
  /**
   * Determines if the qualifier is in milliseconds or not
   * @param qualifier The qualifier to parse
   * @param offset An offset from the start of the byte array
   * @return True if the qualifier is in milliseconds, false if not
   * @since 2.0
   */
  public static boolean inMilliseconds(final byte[] qualifier, 
      final byte offset) {
    return inMilliseconds(qualifier[offset]);
  }
  
  /**
   * Determines if the qualifier is in milliseconds or not
   * @param qualifier The qualifier to parse
   * @return True if the qualifier is in milliseconds, false if not
   * @since 2.0
   */
  public static boolean inMilliseconds(final byte[] qualifier) {
    return inMilliseconds(qualifier[0]);
  }
  
  /**
   * Determines if the qualifier is in milliseconds or not
   * @param qualifier The first byte of a qualifier
   * @return True if the qualifier is in milliseconds, false if not
   * @since 2.0
   */
  public static boolean inMilliseconds(final byte qualifier) {
    return (qualifier & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG;
  }
  
  /**
   * Returns the offset in milliseconds from the row base timestamp from a data
   * point qualifier
   * @param qualifier The qualifier to parse
   * @return The offset in milliseconds from the base time
   * @throws IllegalArgumentException if the qualifier is null or empty
   * @since 2.0
   */
  public static int getOffsetFromQualifier(final byte[] qualifier) {
    return getOffsetFromQualifier(qualifier, 0);
  }
  
  /**
   * Returns the offset in milliseconds from the row base timestamp from a data
   * point qualifier at the given offset (for compacted columns)
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return The offset in milliseconds from the base time
   * @throws IllegalDataException if the qualifier is null or the offset falls 
   * outside of the qualifier array
   * @since 2.0
   */
  public static int getOffsetFromQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);
    if ((qualifier[offset] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      return (int)(Bytes.getUnsignedInt(qualifier, offset) & 0x0FFFFFC0) 
        >>> Const.MS_FLAG_BITS;
    } else {
      final int seconds = (Bytes.getUnsignedShort(qualifier, offset) & 0xFFFF) 
        >>> Const.FLAG_BITS;
      return seconds * 1000;
    }
  }
  
  /**
   * Returns the length of the value, in bytes, parsed from the qualifier
   * @param qualifier The qualifier to parse
   * @return The length of the value in bytes, from 1 to 8.
   * @throws IllegalArgumentException if the qualifier is null or empty
   * @since 2.0
   */
  public static byte getValueLengthFromQualifier(final byte[] qualifier) {
    return getValueLengthFromQualifier(qualifier, 0);
  }
  
  /**
   * Returns the length of the value, in bytes, parsed from the qualifier
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return The length of the value in bytes, from 1 to 8.
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.0
   */
  public static byte getValueLengthFromQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);    
    short length;
    if ((qualifier[offset] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      length = (short) (qualifier[offset + 3] & Internal.LENGTH_MASK); 
    } else {
      length = (short) (qualifier[offset + 1] & Internal.LENGTH_MASK);
    }
    return (byte) (length + 1);
  }

  /**
   * Returns the length, in bytes, of the qualifier: 2 or 4 bytes
   * @param qualifier The qualifier to parse
   * @return The length of the qualifier in bytes
   * @throws IllegalArgumentException if the qualifier is null or empty
   * @since 2.0
   */
  public static short getQualifierLength(final byte[] qualifier) {
    return getQualifierLength(qualifier, 0);
  }
  
  /**
   * Returns the length, in bytes, of the qualifier: 2 or 4 bytes
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return The length of the qualifier in bytes
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.0
   */
  public static short getQualifierLength(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);    
    if ((qualifier[offset] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      if ((offset + 4) > qualifier.length) {
        throw new IllegalArgumentException(
            "Detected a millisecond flag but qualifier length is too short");
      }
      return 4;
    } else {
      if ((offset + 2) > qualifier.length) {
        throw new IllegalArgumentException("Qualifier length is too short");
      }
      return 2;
    }
  }
  
  /**
   * Returns the absolute timestamp of a data point qualifier in milliseconds
   * @param qualifier The qualifier to parse
   * @param base_time The base time, in seconds, from the row key
   * @return The absolute timestamp in milliseconds
   * @throws IllegalArgumentException if the qualifier is null or empty
   * @since 2.0
   */
  public static long getTimestampFromQualifier(final byte[] qualifier, 
      final long base_time) {
    return (base_time * 1000) + getOffsetFromQualifier(qualifier);
  }
  
  /**
   * Returns the absolute timestamp of a data point qualifier in milliseconds
   * @param qualifier The qualifier to parse
   * @param base_time The base time, in seconds, from the row key
   * @param offset An offset within the byte array
   * @return The absolute timestamp in milliseconds
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.0
   */
  public static long getTimestampFromQualifier(final byte[] qualifier, 
      final long base_time, final int offset) {
    return (base_time * 1000) + getOffsetFromQualifier(qualifier, offset);
  }

  /**
   * Parses the flag bits from the qualifier
   * @param qualifier The qualifier to parse
   * @return A short representing the last 4 bits of the qualifier
   * @throws IllegalArgumentException if the qualifier is null or empty
   * @since 2.0
   */
  public static short getFlagsFromQualifier(final byte[] qualifier) {
    return getFlagsFromQualifier(qualifier, 0);
  }
  
  /**
   * Parses the flag bits from the qualifier
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return A short representing the last 4 bits of the qualifier
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.0
   */
  public static short getFlagsFromQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);
    if ((qualifier[offset] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      return (short) (qualifier[offset + 3] & Internal.FLAGS_MASK); 
    } else {
      return (short) (qualifier[offset + 1] & Internal.FLAGS_MASK);
    }
  }

  /**
   * Extracts the 2 or 4 byte qualifier from a compacted byte array
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return A byte array with only the requested qualifier
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.0
   */
  public static byte[] extractQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);
    if ((qualifier[offset] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      return new byte[] { qualifier[offset], qualifier[offset + 1],
          qualifier[offset + 2], qualifier[offset + 3] };
    } else {
      return new byte[] { qualifier[offset], qualifier[offset + 1] };
    }
  }
  
  /**
   * Returns a 2 or 4 byte qualifier based on the timestamp and the flags. If
   * the timestamp is in seconds, this returns a 2 byte qualifier. If it's in
   * milliseconds, returns a 4 byte qualifier 
   * @param timestamp A Unix epoch timestamp in seconds or milliseconds
   * @param flags Flags to set on the qualifier (length &| float)
   * @return A 2 or 4 byte qualifier for storage in column or compacted column
   * @since 2.0
   */
  public static byte[] buildQualifier(final long timestamp, final short flags) {
    final long base_time;
    if ((timestamp & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      base_time = ((timestamp / 1000) - ((timestamp / 1000) 
          % Const.MAX_TIMESPAN));
      final int qual = (int) (((timestamp - (base_time * 1000) 
          << (Const.MS_FLAG_BITS)) | flags) | Const.MS_FLAG);
      return Bytes.fromInt(qual);
    } else {
      base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
      final short qual = (short) ((timestamp - base_time) << Const.FLAG_BITS
          | flags);
      return Bytes.fromShort(qual);
    }
  }

  /**
   * Checks the qualifier to verify that it has data and that the offset is
   * within bounds
   * @param qualifier The qualifier to validate
   * @param offset An optional offset
   * @throws IllegalDataException if the qualifier is null or the offset falls 
   * outside of the qualifier array
   * @since 2.0
   */
  private static void validateQualifier(final byte[] qualifier, 
      final int offset) {
    if (offset < 0 || offset >= qualifier.length - 1) {
      throw new IllegalDataException("Offset of [" + offset + 
          "] is out of bounds for the qualifier length of [" + 
          qualifier.length + "]");
    }
  }

  /**
   * Sets the server-side regexp filter on the scanner.
   * This will compile a list of the tagk/v pairs for the TSUIDs to prevent
   * storage from returning irrelevant rows.
   * @param scanner The scanner on which to add the filter.
   * @param tsuids The list of TSUIDs to filter on 
   * @since 2.1
   */
  public static void createAndSetTSUIDFilter(final Scanner scanner, 
      final List<String> tsuids) {
    Collections.sort(tsuids);
    
    // first, convert the tags to byte arrays and count up the total length
    // so we can allocate the string builder
    final short metric_width = TSDB.metrics_width();
    int tags_length = 0;
    final ArrayList<byte[]> uids = new ArrayList<byte[]>(tsuids.size());
    for (final String tsuid : tsuids) {
      final String tags = tsuid.substring(metric_width * 2);
      final byte[] tag_bytes = UniqueId.stringToUid(tags);
      tags_length += tag_bytes.length;
      uids.add(tag_bytes);
    }
    
    // Generate a regexp for our tags based on any metric and timestamp (since
    // those are handled by the row start/stop) and the list of TSUID tagk/v
    // pairs. The generated regex will look like: ^.{7}(tags|tags|tags)$
    // where each "tags" is similar to \\Q\000\000\001\000\000\002\\E
    final StringBuilder buf = new StringBuilder(
        13  // "(?s)^.{N}(" + ")$"
        + (tsuids.size() * 11) // "\\Q" + "\\E|"
        + tags_length); // total # of bytes in tsuids tagk/v pairs
    
    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
               + "^.{")
       // ... start by skipping the metric ID and timestamp.
       .append(TSDB.metrics_width() + Const.TIMESTAMP_BYTES)
       .append("}(");
    
    for (final byte[] tags : uids) {
       // quote the bytes
      buf.append("\\Q");
      UniqueId.addIdToRegexp(buf, tags);
      buf.append('|');
    }
    
    // Replace the pipe of the last iteration, close and set
    buf.setCharAt(buf.length() - 1, ')');
    buf.append("$");
    scanner.setKeyRegexp(buf.toString(), Charset.forName("ISO-8859-1"));
  }
}
