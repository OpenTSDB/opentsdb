// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
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
package net.opentsdb.storage.schemas.tsdb1x;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;

import java.util.TreeMap;

/**
 * Represents a read-only sequence of continuous Rollup or Summary values
 * stored by aggregation type.
 * 
 * TODO - pre-size the arrays if possible. Rollups are often not appended
 * so it would be wise to try to allocate a reasonable amount instead of
 * all the re-alloc and copy.
 * 
 * TODO - handle nanosecond offsets
 * 
 * TODO - validate ms handling
 * 
 * @since 3.0
 */
public class NumericSummaryRowSeq implements RowSeq {
  /** The size of this class header. */
  public static int HEADER_SIZE = 12 // class header 64 bit
                                + 8  // base _timestamp
                                + 12 // data array header 64 bit
                                + 4  // size
                                + 12 // rollup interval header 64 bit
                                + 12; // dps map (TODO - better estimate)
  
  /** The base row timestamp in Unix epoch seconds. */
  protected long base_timestamp;
  
  /** The rollup interval for offset decoding. */
  protected final RollupInterval interval;
  
  /** The data in qualifier/value/qualifier/value, etc order per summary 
   * type. */
  protected Map<Integer, byte[]> summary_data;
  
  /** The number of values in this row. */
  protected int dps;
  
  /** The size of data in this sequence. */
  protected int size;
  
  /**
   * Default ctor
   * @param base_timestamp The row base timestamp in Unix epoch seconds.
   */
  public NumericSummaryRowSeq(final long base_timestamp, 
                              final RollupInterval interval) {
    if (interval == null) {
      throw new IllegalArgumentException("Interval can't be null.");
    }
    if (interval.rollupConfig() == null) {
      throw new IllegalArgumentException("The interval may not have been "
          + "initialized properly as it's RollupConfig is null.");
    }
    this.base_timestamp = base_timestamp;
    this.interval = interval;
    dps = 0;
    size = 0;
    summary_data = Maps.newHashMapWithExpectedSize(2);
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericSummaryType.TYPE;
  }
  
  @Override
  public void addColumn(final byte prefix, 
                        final byte[] qualifier, 
                        final byte[] value) {
    // so, we can have byte prefix's or we could have strings for the old
    // rollups. E.g. [ type, offset, offset ] or 
    // [ 'a', 'g', 'g', ':', offset, offset ]. Offsets may be on 2 or 
    // more bytes. So we can *guess* that if the length is 5 or less 
    // that it's a byte prefix, and > 5 then it's a string prefix.
    final int type;
    final int offset_start;
    if (qualifier.length < 6) {
      type = qualifier[0];
      offset_start = 1;
    } else {
      type = interval.rollupConfig().getIdForAggregator(qualifier);
      offset_start = interval.rollupConfig().getOffsetStartFromQualifier(qualifier);
    }
    
    byte[] data = summary_data.get(type);
    if (data == null) {
      data = new byte[qualifier.length - offset_start + value.length];
      System.arraycopy(qualifier, offset_start, data, 0, 
          qualifier.length - offset_start);
      System.arraycopy(value, 0, data, qualifier.length - offset_start, 
          value.length);
      summary_data.put(type, data);
      size += qualifier.length - offset_start + value.length;
      dps++;
    } else {
      final byte[] copy = new byte[data.length + 
                                   qualifier.length - offset_start + value.length];
      System.arraycopy(data, 0, copy, 0, data.length);
      System.arraycopy(qualifier, offset_start, copy, data.length, 
          qualifier.length - offset_start);
      System.arraycopy(value, 0, copy, 
          data.length + qualifier.length - offset_start, 
          value.length);
      summary_data.put(type, copy);
      size += qualifier.length - offset_start + value.length;
      dps++;
    }
    // assume the row was pre-filtered for the values we want, e.g. sum
    // and count or just max, etc.
  }

  /**
   * Appends the given sequence to this one. Make sure to call 
   * {@link #dedupe(boolean, boolean)} after so we can clean it up before
   * iterating over the results.
   * @param row A non-null row sequence to merge.
   */
  void appendSeq(final NumericSummaryRowSeq row) {
    for (final Entry<Integer, byte[]> entry : row.summary_data.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      byte[] data = summary_data.get(entry.getKey());
      if (data == null) {
        // woot! Just move it over
        summary_data.put(entry.getKey(), entry.getValue());
      } else {
        final byte[] copy = new byte[data.length + entry.getValue().length];
        System.arraycopy(data, 0, copy, 0, data.length);
        System.arraycopy(entry.getValue(), 0, copy, data.length, entry.getValue().length);
        summary_data.put(entry.getKey(), copy);
      }
    }
    dps += row.dps;
    size += row.size;
  }
  
  @Override
  public ChronoUnit dedupe(final boolean keep_earliest, final boolean reverse) {
    dps = 0;
    size = 0;
    ChronoUnit resolution = null;
    final Iterator<Entry<Integer, byte[]>> it = summary_data.entrySet().iterator();
    while (it.hasNext()) {
      final Entry<Integer, byte[]> entry = it.next();
      int local_dps = 0;
      // first pass, see if we even need to dedupe
      long last_offset = -1;
      long current_offset = 0;
      int idx = 0;
      final byte[] data = entry.getValue();
      
      boolean need_repair = false;
      while (idx < data.length) {
        current_offset = RollupUtils.getOffsetFromRollupQualifier(data, idx, interval);
        /*if ((data[idx] & NumericCodec.NS_BYTE_FLAG) == 
            NumericCodec.NS_BYTE_FLAG) {
          current_offset = NumericCodec.offsetFromNanoQualifier(data, idx);
          idx += NumericCodec.NS_Q_WIDTH;
          idx += NumericCodec.getValueLengthFromQualifier(data, idx - 1);
        } else*/ if ((data[idx] & NumericCodec.MS_BYTE_FLAG) == 
            NumericCodec.MS_BYTE_FLAG) {
          idx += NumericCodec.MS_Q_WIDTH;
          idx += NumericCodec.getValueLengthFromQualifier(data, idx - 1);
          if (resolution == null) {
            resolution = ChronoUnit.MILLIS;
          }
        } else {
          idx += NumericCodec.S_Q_WIDTH;
          idx += NumericCodec.getValueLengthFromQualifier(data, idx - 1);
          if (resolution == null) {
            resolution = ChronoUnit.SECONDS;
          }
        }
        local_dps++;
        if (current_offset <= last_offset) {
           need_repair = true;
          break;
        }
        last_offset = current_offset;
      }
      
      if (!need_repair) {
        if (reverse) {
          reverse();
        }
        dps += local_dps;
        size += data.length;
        continue;
      }
      
      // if we made it here we need to dedupe and sort. Normalize to longs
      // then flush.
      // TODO - any primitive tree maps out there? Or maybe there's just an
      // all around better way to do this. For now this should be fast enough.
      // The value is a concatenation of the offset and length into a long.
      // The first 32 bits are the offset, the last 32 the width to copy.
      TreeMap<Long,Long> map = new TreeMap<Long, Long>();
      idx = 0;
      local_dps = 0;
      int vlen;
      long encoded_value = 0;
      while (idx < data.length) {
        current_offset = RollupUtils.getOffsetFromRollupQualifier(data, idx, interval);
        /*if ((data[idx] & NumericCodec.NS_BYTE_FLAG) == 
            NumericCodec.NS_BYTE_FLAG) {
          current_offset = NumericCodec.offsetFromNanoQualifier(data, idx);
          vlen = NumericCodec.getValueLengthFromQualifier(data, 
              idx + NumericCodec.NS_Q_WIDTH - 1);
          encoded_value = (long) idx << 32 | 
              (long) (NumericCodec.NS_Q_WIDTH + vlen);
          idx += NumericCodec.NS_Q_WIDTH + vlen;
        } else*/ if ((data[idx] & NumericCodec.MS_BYTE_FLAG) == 
            NumericCodec.MS_BYTE_FLAG) {
          
          vlen = NumericCodec.getValueLengthFromQualifier(data, 
              idx + NumericCodec.MS_Q_WIDTH - 1);
          encoded_value = (long) idx << 32 | 
              (long) (NumericCodec.MS_Q_WIDTH + vlen);
          idx += NumericCodec.MS_Q_WIDTH + vlen;
        } else {
          vlen = NumericCodec.getValueLengthFromQualifier(data, 
              idx + NumericCodec.S_Q_WIDTH - 1);
          encoded_value = (long) idx << 32 | 
              (long) (NumericCodec.S_Q_WIDTH + vlen);
          idx += NumericCodec.S_Q_WIDTH + vlen;
        }
        
        // now copy the data into the buffer then store it
        if (keep_earliest) {
          map.putIfAbsent(current_offset, encoded_value);
        } else {
          map.put(current_offset, encoded_value);
        }
      }
      
      final byte[] sorted = new byte[data.length];
      final Iterator<Entry<Long, Long>> iterator;
      if (reverse) {
        iterator = map.descendingMap().entrySet().iterator();
      } else {
        iterator = map.entrySet().iterator();
      }
      idx = 0;
      
      int offset = 0;
      int width = 0;
      
      while (iterator.hasNext()) {
        final long value = iterator.next().getValue();
        offset = (int) (value >> 32);
        width = (int) value;
        System.arraycopy(data, offset, sorted, idx, width);
        idx += width;
        local_dps++;
      }
      
      // truncate if necessary
      if (idx < data.length) {
        summary_data.put(entry.getKey(), Arrays.copyOfRange(sorted, 0, idx));
        size += idx;
      } else {
        summary_data.put(entry.getKey(), sorted);
        size += data.length;
      }
      dps += local_dps;
    }
    return resolution;
  }
  
  @Override
  public int size() {
    return HEADER_SIZE + size;
  }
  
  @Override
  public int dataPoints() {
    return dps;
  }
  
  /**
   * Flips the data points so they are in time descending order.
   */
  void reverse() {
    final Iterator<Entry<Integer, byte[]>> it = summary_data.entrySet().iterator();
    while (it.hasNext()) {
      final Entry<Integer, byte[]> entry = it.next();
      final byte[] data = entry.getValue();
      if (data == null || data.length < 1) {
        continue;
      }
      
      final byte[] reversed = new byte[data.length];
      int read_idx = 0;
      int write_idx = reversed.length;
      
      int vlen = 0;
      while (read_idx < data.length) {
        /*if ((data[read_idx] & NumericCodec.NS_BYTE_FLAG) == 
            NumericCodec.NS_BYTE_FLAG) {
          vlen = NumericCodec.getValueLengthFromQualifier(data, 
              read_idx + NumericCodec.NS_Q_WIDTH - 1);
          write_idx -= vlen;
          System.arraycopy(data, read_idx + NumericCodec.NS_Q_WIDTH, 
              reversed, write_idx, vlen);
          write_idx -= NumericCodec.NS_Q_WIDTH;
          System.arraycopy(data, read_idx, reversed, write_idx, 
              NumericCodec.NS_Q_WIDTH);
          read_idx += (NumericCodec.NS_Q_WIDTH + vlen);
        } else*/ if ((data[read_idx] & NumericCodec.MS_BYTE_FLAG) == 
            NumericCodec.MS_BYTE_FLAG) {
          vlen = NumericCodec.getValueLengthFromQualifier(data, 
              read_idx + NumericCodec.MS_Q_WIDTH - 1);
          write_idx -= vlen;
          System.arraycopy(data, read_idx + NumericCodec.MS_Q_WIDTH, 
              reversed, write_idx, vlen);
          write_idx -= NumericCodec.MS_Q_WIDTH;
          System.arraycopy(data, read_idx, reversed, write_idx, 
              NumericCodec.MS_Q_WIDTH);
          read_idx += (NumericCodec.MS_Q_WIDTH + vlen);
        } else {
          vlen = NumericCodec.getValueLengthFromQualifier(data, 
              read_idx + NumericCodec.S_Q_WIDTH - 1);
          write_idx -= vlen;
          System.arraycopy(data, read_idx + NumericCodec.S_Q_WIDTH, 
              reversed, write_idx, vlen);
          write_idx -= NumericCodec.S_Q_WIDTH;
          System.arraycopy(data, read_idx, reversed, write_idx, 
              NumericCodec.S_Q_WIDTH);
          read_idx += (NumericCodec.S_Q_WIDTH + vlen);
        }
      }
      
      if (write_idx > 0) {
        // this shouldn't happen as we should have skipped any compacted 
        // column sentinels in the value!
        throw new RuntimeException("WTF? Write index was " + write_idx
            + " when it should have been " + data.length);
      }
      
      summary_data.put(entry.getKey(), reversed);
    }
  }
}