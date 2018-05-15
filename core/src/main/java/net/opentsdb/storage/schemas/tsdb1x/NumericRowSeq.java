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
package net.opentsdb.storage.schemas.tsdb1x;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericType;

import java.util.TreeMap;

/**
 * Represents a read-only sequence of continuous numeric columns.
 * <p>
 * This class stores in memory the data of one or more continuous
 * storage columns for a given time series. To consolidate memory, the 
 * data points are stored in one byte arrays in the format following
 * TSDB 2.0 appends with a column qualifier followed by a column value
 * then the next qualifier, next value, etc. Access is granted via pointers.
 * 
 * @since 3.0
 */
public class NumericRowSeq implements RowSeq {
  /** The size of this class header. */
  public static int HEADER_SIZE = 12 // class header 64 bit
                                + 8  // base _timestamp
                                + 12 // data array header 64 bit
                                + 4; // dps
  
  /** The base row timestamp in Unix epoch seconds. */
  protected long base_timestamp;
  
  /** The data in qualifier/value/qualifier/value, etc order. */
  protected byte[] data;
  
  /** The number of values in this row. */
  protected int dps;
  
  /**
   * Default ctor
   * @param base_timestamp The row base timestamp in Unix epoch seconds.
   */
  public NumericRowSeq(final long base_timestamp) {
    this.base_timestamp = base_timestamp;
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericType.TYPE;
  }
  
  @Override
  public void addColumn(final byte prefix, 
                        final byte[] qualifier, 
                        final byte[] value) {
    
    if (prefix == Schema.APPENDS_PREFIX) {
      if (data == null) {
        // sweet, copy
        data = Arrays.copyOf(value, value.length);
      } else {
        final byte[] copy = new byte[data.length + value.length];
        System.arraycopy(data, 0, copy, 0, data.length);
        System.arraycopy(value, 0, copy, data.length, value.length);
        data = copy;
      }
    } else {
      // two options:
      // 1) It's a raw put data point in seconds or ms (now nanos)
      // 2) It's an old-school compacted column either hetero or homogenous
      // regarding seconds or ms.
      if (qualifier.length == NumericCodec.S_Q_WIDTH) {
        // handle older versions of OpenTSDB 1.x where there were some 
        // encoding issues that only affected second values.
        final byte[] new_qualifier;
        final byte[] new_value;
        int vlen = NumericCodec.getValueLengthFromQualifier(qualifier, 
            qualifier.length - 1);
        if (value.length != vlen) {
          final long offset = NumericCodec.offsetFromSecondQualifier(
              qualifier, 0) /  1000L / 1000L / 1000L;
          // TODO - log it in a counter somewhere
          if ((qualifier[qualifier.length - 1] & NumericCodec.FLAG_FLOAT) == 
              NumericCodec.FLAG_FLOAT) {
            new_value = NumericCodec.fixFloatingPointValue(
                NumericCodec.getFlags(
                    qualifier, 0, (byte) NumericCodec.S_Q_WIDTH), value);
            if (new_value.length == 4) {
              new_qualifier = NumericCodec.buildSecondQualifier(offset, 
                  (short) (3 | NumericCodec.FLAG_FLOAT));
            } else {
              new_qualifier = NumericCodec.buildSecondQualifier(offset, 
                  (short) (7 | NumericCodec.FLAG_FLOAT));
            }
          } else {
            if (value.length == 8) {
              new_qualifier = NumericCodec.buildSecondQualifier(
                  offset, (short) 7);
            } else if (value.length == 4) {
              new_qualifier = NumericCodec.buildSecondQualifier(
                  offset, (short) 3);
            } else if (value.length == 2) {
              new_qualifier = NumericCodec.buildSecondQualifier(
                  offset, (short) 1);
            } else {
              new_qualifier = NumericCodec.buildSecondQualifier(
                  offset, (short) 0);
            }
            new_value = value;
          }
        } else {
          new_qualifier = qualifier;
          new_value = value;
        }
        
        // easy, just smoosh it together
        if (data == null) {
          data = new byte[new_qualifier.length + new_value.length];
          System.arraycopy(new_qualifier, 0, data, 0, new_qualifier.length);
          System.arraycopy(new_value, 0, data, new_qualifier.length, 
              new_value.length);
        } else {
          final byte[] copy = new byte[data.length + 
                                       new_qualifier.length + 
                                       new_value.length];
          System.arraycopy(data, 0, copy, 0, data.length);
          System.arraycopy(new_qualifier, 0, copy, data.length, 
              new_qualifier.length);
          System.arraycopy(new_value, 0, copy, 
              data.length + new_qualifier.length, new_value.length);
          data = copy;
        }
      } else {
        // instead of branching more to see if it's an ms or ns column,
        // we can just start iterating. Note that if the column is compacted
        // and has a mixed time type sentinel at the end we'll allocate an
        // extra value byte but we should never iterate or read it.
        int write_idx = 0;
        if (data == null) {
          data = new byte[qualifier.length + value.length];
        } else {
          final byte[] copy = new byte[data.length + 
                                       qualifier.length + 
                                       value.length];
          System.arraycopy(data, 0, copy, 0, data.length);
          write_idx = data.length;
          data = copy;
        }
        int qidx = 0;
        int vidx = 0;
        int vlen = 0;
        while (qidx < qualifier.length) {
          if ((qualifier[qidx] & NumericCodec.NS_BYTE_FLAG) == 
              NumericCodec.NS_BYTE_FLAG) {
            System.arraycopy(qualifier, qidx, data, write_idx, 
                NumericCodec.NS_Q_WIDTH);
            write_idx += NumericCodec.NS_Q_WIDTH;
            qidx += NumericCodec.NS_Q_WIDTH;
          } else if ((qualifier[qidx] & NumericCodec.MS_BYTE_FLAG) == 
              NumericCodec.MS_BYTE_FLAG) {
            System.arraycopy(qualifier, qidx, data, write_idx, 
                NumericCodec.MS_Q_WIDTH);
            write_idx += NumericCodec.MS_Q_WIDTH;
            qidx += NumericCodec.MS_Q_WIDTH;
          } else {
            System.arraycopy(qualifier, qidx, data, write_idx, 
                NumericCodec.S_Q_WIDTH);
            write_idx += NumericCodec.S_Q_WIDTH;
            qidx += NumericCodec.S_Q_WIDTH;
          }
          vlen = NumericCodec.getValueLengthFromQualifier(qualifier, qidx - 1);
          System.arraycopy(value, vidx, data, write_idx, vlen);
          write_idx += vlen;
          vidx += vlen;
        }
        
        if (write_idx < data.length) {
          // truncate in case there was a compacted column with the last
          // byte set to 0 or 1.
          data = Arrays.copyOfRange(data, 0, write_idx);
        }
      }
    }
  }
  
  @Override
  public ChronoUnit dedupe(final boolean keep_earliest, final boolean reverse) {
    dps = 0;
    // first pass, see if we even need to dedupe
    long last_offset = -1;
    long current_offset = 0;
    int idx = 0;
    ChronoUnit resolution = null;
    
    boolean need_repair = false;
    while (idx < data.length) {
      if ((data[idx] & NumericCodec.NS_BYTE_FLAG) == 
          NumericCodec.NS_BYTE_FLAG) {
        current_offset = NumericCodec.offsetFromNanoQualifier(data, idx);
        idx += NumericCodec.NS_Q_WIDTH;
        idx += NumericCodec.getValueLengthFromQualifier(data, idx - 1);
        if (resolution == null) {
          resolution = ChronoUnit.NANOS;
        }
      } else if ((data[idx] & NumericCodec.MS_BYTE_FLAG) == 
          NumericCodec.MS_BYTE_FLAG) {
        current_offset = NumericCodec.offsetFromMsQualifier(data, idx);
        idx += NumericCodec.MS_Q_WIDTH;
        idx += NumericCodec.getValueLengthFromQualifier(data, idx - 1);
        if (resolution == null) {
          resolution = ChronoUnit.MILLIS;
        }
      } else {
        current_offset = NumericCodec.offsetFromSecondQualifier(data, idx);
        idx += NumericCodec.S_Q_WIDTH;
        idx += NumericCodec.getValueLengthFromQualifier(data, idx - 1);
        if (resolution == null) {
          resolution = ChronoUnit.SECONDS;
        }
      }
      dps++;
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
      return resolution;
    }
    
    dps = 0;
    // if we made it here we need to dedupe and sort. Normalize to longs
    // then flush.
    // TODO - any primitive tree maps out there? Or maybe there's just an
    // all around better way to do this. For now this should be fast enough.
    // The value is a concatenation of the offset and length into a long.
    // The first 32 bits are the offset, the last 32 the width to copy.
    TreeMap<Long,Long> map = new TreeMap<Long, Long>();
    idx = 0;
    //byte[] buf;
    int vlen;
    long encoded_value = 0;
    // TODO - there's a possible optimization here to get the offset. For
    // now we're only looking at the highest resolution amongst dupes.
    while (idx < data.length) {
      if ((data[idx] & NumericCodec.NS_BYTE_FLAG) == 
          NumericCodec.NS_BYTE_FLAG) {
        current_offset = NumericCodec.offsetFromNanoQualifier(data, idx);
        vlen = NumericCodec.getValueLengthFromQualifier(data, 
            idx + NumericCodec.NS_Q_WIDTH - 1);
        encoded_value = (long) idx << 32 | 
            (long) (NumericCodec.NS_Q_WIDTH + vlen);
        idx += NumericCodec.NS_Q_WIDTH + vlen;
      } else if ((data[idx] & NumericCodec.MS_BYTE_FLAG) == 
          NumericCodec.MS_BYTE_FLAG) {
        current_offset = NumericCodec.offsetFromMsQualifier(data, idx);
        vlen = NumericCodec.getValueLengthFromQualifier(data, 
            idx + NumericCodec.MS_Q_WIDTH - 1);
        encoded_value = (long) idx << 32 | 
            (long) (NumericCodec.MS_Q_WIDTH + vlen);
        idx += NumericCodec.MS_Q_WIDTH + vlen;
      } else {
        current_offset = NumericCodec.offsetFromSecondQualifier(data, idx);
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
      dps++;
    }
    
    // truncate if necessary
    if (idx < data.length) {
      data = Arrays.copyOfRange(sorted, 0, idx);
    } else {
      data = sorted;
    }
    return resolution;
  }
  
  @Override
  public int size() {
    return HEADER_SIZE + (data == null ? 0 : data.length);
  }
  
  @Override
  public int dataPoints() {
    return dps;
  }
  
  /**
   * Flips the data points so they are in time descending order.
   */
  void reverse() {
    if (data == null || data.length < 1) {
      return;
    }
    
    final byte[] reversed = new byte[data.length];
    int read_idx = 0;
    int write_idx = reversed.length;
    
    int vlen = 0;
    while (read_idx < data.length) {
      if ((data[read_idx] & NumericCodec.NS_BYTE_FLAG) == 
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
      } else if ((data[read_idx] & NumericCodec.MS_BYTE_FLAG) == 
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
    data = reversed;
  }
}
