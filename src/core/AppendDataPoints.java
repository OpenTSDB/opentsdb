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
package net.opentsdb.core;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import net.opentsdb.core.Internal.Cell;
import net.opentsdb.utils.DateTime;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/**
 * A class that deals with serializing/deserializing appended data point columns.
 * In busy TSDB installs appends can save on storage at write time and network
 * bandwidth as TSD compactions are no longer necessary. Each data point is 
 * concatenated to a byte array in storage. At query time, the values are ordered
 * and de-duped. Optionally the column can be re-written when out of order or
 * duplicates are detected.
 * NOTE: This will increase CPU usage on your HBase servers as it has to perform
 * the atomic read-modify-write operation on the column. 
 * @since 2.2
 */
public class AppendDataPoints {
  private static final Logger LOG = LoggerFactory.getLogger(AppendDataPoints.class);
  
  /** The prefix ID of append columns */
  public static final byte APPEND_COLUMN_PREFIX = 0x05;
  
  /** The full column qualifier for append columns */
  public static final byte[] APPEND_COLUMN_QUALIFIER = new byte[] {
    APPEND_COLUMN_PREFIX, 0x00, 0x00};
  
  /** A threshold in seconds where we avoid writing repairs */
  public static final int REPAIR_THRESHOLD = 3600;
  
  /** Filled with the qualifiers in the compacted data points format after parsing */
  private byte[] qualifier;
  
  /** Filled with the values in the compacted data points format after parsing */
  private byte[] value;

  /** A deferred that is set if a repaired column was sent to storage */
  private Deferred<Object> repaired_deferred = null;
  
  /**
   * Default empty ctor
   */
  public AppendDataPoints() {
    
  }
  
  /**
   * Creates a new AppendDataPoints object from a qualifier and value. You can
   * then call {@link #getBytes()} to write to TSDB.
   * @param qualifier The qualifier with the time offset, type and length flags.
   * @param value The value to append
   * @throws IllegalArgumentException if the qualifier or value is null or empty
   */
  public AppendDataPoints(final byte[] qualifier, final byte[] value) {
    if (qualifier == null || qualifier.length < 1) {
      throw new IllegalArgumentException("Qualifier cannot be null or empty");
    }
    if (value == null || value.length < 1) {
      throw new IllegalArgumentException("Value cannot be null or empty");
    }
    this.qualifier = qualifier;
    this.value = value;
  }
  
  /**
   * Concatenates the qualifier and value for appending to a column in the 
   * backing data store.
   * @return A byte array to append to the value of a column.
   */
  public byte[] getBytes() {
    final byte[] bytes = new byte[qualifier.length + value.length];
    System.arraycopy(this.qualifier, 0, bytes, 0, qualifier.length);
    System.arraycopy(value, 0, bytes, qualifier.length, value.length);
    return bytes;
  }
  
  /**
   * Parses a column from storage, orders and drops newer duplicate data points.
   * The parsing will return both a Cell collection for debugging and add
   * the cells to concatenated qualifier and value arrays in the compacted data
   * point format so that the results can be merged with other non-append 
   * columns or rows.
   * <p>
   * WARNING: If the "tsd.core.repair_appends" config is set to true then this
   * method will issue puts against the database, overwriting the column with
   * sorted and de-duplicated data. It will only do this for rows that are at
   * least an hour old so as to avoid pounding current rows.
   * <p> 
   * TODO (CL) - allow for newer or older data points depending on a config.
   * @param tsdb The TSDB to which we belong
   * @param kv The key value t parse
   * @throws IllegalArgumentException if the given KV is not an append column
   * or we were unable to parse the value.
   */
  public final Collection<Cell> parseKeyValue(final TSDB tsdb, final KeyValue kv) {
    if (kv.qualifier().length != 3 || kv.qualifier()[0] != APPEND_COLUMN_PREFIX) {
      // it's really not an issue if the offset is not 0, maybe in the future
      // we'll support appends at different offsets.
      throw new IllegalArgumentException("Can not parse cell, it is not " +
        " an appended cell. It has a different qualifier " + 
        Bytes.pretty(kv.qualifier()) + ", row key " + Bytes.pretty(kv.key()));
    }
    final boolean repair = tsdb.getConfig().repair_appends();
    final long base_time;
    try {
      base_time = Internal.baseTime(tsdb, kv.key());
    } catch (ArrayIndexOutOfBoundsException oob) {
      throw new IllegalDataException("Corrupted value: invalid row key: " + kv, 
          oob);
    }

    int val_idx = 0;
    int val_length = 0;
    int qual_length = 0;
    int last_delta = -1;  // Time delta, extracted from the qualifier.
    
    final Map<Integer, Internal.Cell> deltas = new TreeMap<Integer, Cell>();
    boolean has_duplicates = false;
    boolean out_of_order = false;
    boolean needs_repair = false;
    
    try {
      while (val_idx < kv.value().length) {
        byte[] q = Internal.extractQualifier(kv.value(), val_idx);
        System.arraycopy(kv.value(), val_idx, q, 0, q.length);
        val_idx=val_idx + q.length;
        
        int vlen = Internal.getValueLengthFromQualifier(q, 0);
        byte[] v = new byte[vlen];
        System.arraycopy(kv.value(), val_idx, v, 0, vlen);
        val_idx += vlen;
        int delta = Internal.getOffsetFromQualifier(q);
    
        final Cell duplicate = deltas.get(delta);
        if (duplicate != null) {
          // This is a duplicate cell, skip it
          has_duplicates = true;
          qual_length -= duplicate.qualifier.length;
          val_length -= duplicate.value.length;
        }
  
        qual_length += q.length;
        val_length += vlen;
        final Cell cell = new Cell(q, v);
        deltas.put(delta, cell);
  
        if (!out_of_order) {
          // Data points needs to be sorted if we find at least one out of 
          // order data
          if (delta <= last_delta) {
            out_of_order = true;
          }
          last_delta = delta;
        }
      }
    } catch (ArrayIndexOutOfBoundsException oob) {
      throw new IllegalDataException("Corrupted value: couldn't break down"
          + " into individual values (consumed " + val_idx + " bytes, but was"
          + " expecting to consume " + (kv.value().length) + "): " + kv
          + ", cells so far: " + deltas.values(), oob);
    }
    
    if (has_duplicates || out_of_order) {
      if ((DateTime.currentTimeMillis() / 1000) - base_time > REPAIR_THRESHOLD) {
        needs_repair = true;
      }
    }

    // Check we consumed all the bytes of the value.
    if (val_idx != kv.value().length) {
      throw new IllegalDataException("Corrupted value: couldn't break down"
      + " into individual values (consumed " + val_idx + " bytes, but was"
      + " expecting to consume " + (kv.value().length) + "): " + kv
      + ", cells so far: " + deltas.values());
    }

    val_idx = 0;
    int qual_idx = 0;
    byte[] healed_cell = null;
    int healed_index = 0;
    
    this.value = new byte[val_length];
    this.qualifier = new byte[qual_length];
    
    if (repair && needs_repair) {
      healed_cell = new byte[val_length+qual_length];
    }
    
    for (final Cell cell: deltas.values()) {
      System.arraycopy(cell.qualifier, 0, this.qualifier, qual_idx, 
          cell.qualifier.length);
      qual_idx += cell.qualifier.length;
      System.arraycopy(cell.value, 0, this.value, val_idx, cell.value.length);
      val_idx += cell.value.length;

      if (repair && needs_repair) {
        System.arraycopy(cell.qualifier, 0, healed_cell, healed_index, 
            cell.qualifier.length);
        healed_index += cell.qualifier.length;
        System.arraycopy(cell.value, 0, healed_cell, healed_index, cell.value.length);
        healed_index += cell.value.length;          
      }
    }
      
    if (repair && needs_repair) {
      LOG.debug("Repairing appended data column " + kv);
      final PutRequest put = RequestBuilder.buildPutRequest(tsdb.getConfig(), tsdb.table, kv.key(),
          TSDB.FAMILY(), kv.qualifier(), healed_cell, kv.timestamp());
      repaired_deferred = tsdb.getClient().put(put);
    }
    
    return deltas.values();
  }
  
  /** @return the sorted qualifier in a compacted data point format after 
   * {@link #parseKeyValue(TSDB, KeyValue)} has been called */
  public byte[] qualifier() {
    return qualifier;
  }
  
  /** @return the sorted value in a compacted data point format after 
   * {@link #parseKeyValue(TSDB, KeyValue)} has been called */
  public byte[] value() {
    return value;
  }
  
  /** @return a deferred to wait on if the call to 
   * {@link #parseKeyValue(TSDB, KeyValue)} triggered a put to storage. */
  public Deferred<Object> repairedDeferred() {
    return repaired_deferred;
  }
 
  /** @return whether or not a qualifier of AppendDataPoints */
  public static boolean isAppendDataPoints(byte[] qualifier) {
    return qualifier != null && qualifier.length == 3 && qualifier[0] == APPEND_COLUMN_PREFIX;
  }
}
