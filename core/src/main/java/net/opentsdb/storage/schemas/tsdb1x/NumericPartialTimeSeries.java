// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;

/**
 * An implementation that converts the column from a 1x schema into the
 * {@link NumericLongArrayType}. 
 * <b>NOTE:</b> You MUST call {@link #dedupe(boolean, boolean)} on this object
 * before passing it upstream so that the terminal long is written.
 * 
 * @since 3.0
 */
public class NumericPartialTimeSeries implements PartialTimeSeries, 
  CloseablePooledObject{
  private static final Logger LOG = LoggerFactory.getLogger(
      NumericPartialTimeSeries.class);
  
  /** Reference to the Object pool for this instance. */
  protected PooledObject pooled_object;
  
  /** A hash for the time series ID. */
  protected long id_hash;
  
  /** The set we currently belong to. */
  protected PartialTimeSeriesSet set;
  
  /** A reference counter for the array to determine when we can return it to
   * the pool. */
  protected AtomicInteger reference_counter;
  
  /** An array to store the data in. */
  protected PooledObject pooled_array;
  
  /** The current write index. */
  protected int write_idx;
  
  /** Whether or not the array has out-of-order or duplicate data. */
  protected boolean needs_repair;
  
  /** The last offset, used to determine if we need a repair. */
  protected long last_offset = -1;
    
  /**
   * Default ctor.
   */
  public NumericPartialTimeSeries() {
    reference_counter = new AtomicInteger();
  }
  
  @Override
  public void close() throws Exception {
    release();
  }

  @Override
  public Object object() {
    return this;
  }

  @Override
  public void release() {
    if (reference_counter.decrementAndGet() == 0) {
      if (pooled_array != null) {
        pooled_array.release();
        pooled_array = null;
      }
      if (pooled_object != null) {
        pooled_object.release();
      }
      set = null;
      write_idx = 0;
      needs_repair = false;
      last_offset = -1;
    }
  }
  
  /**
   * Sets the ID hash and the set but leaves the array null.
   * @param id_hash
   * @param set
   */
  void setEmpty(final long id_hash, 
                final PartialTimeSeriesSet set) {
    this.id_hash = id_hash;
    this.set = set;
  }
  
  /**
   * Adds a column to the series.
   * @param prefix The schema prefix so we know what kind of data we're dealing
   * with.
   * @param base_timestamp The base timestamp.
   * @param qualifier The non-null qualifier.
   * @param value The non-null value.
   * @param long_array_pool The array pool to claim an array from.
   * @param id_hash The hash for the time series Id.
   * @param set The set this partial belongs to.
   */
  void addColumn(final byte prefix, 
                 final TimeStamp base_timestamp,
                 final byte[] qualifier, 
                 final byte[] value,
                 final ObjectPool long_array_pool, 
                 final long id_hash, 
                 final PartialTimeSeriesSet set) {
    if (qualifier.length < 2) {
      throw new IllegalDataException("Qualifier was too short.");
    }
    if (value.length < 1) {
      throw new IllegalDataException("Value was too short.");
    }
    this.id_hash = id_hash;
    this.set = set;
    
    if (pooled_array == null) {
      pooled_array = long_array_pool.claim();
      if (pooled_array == null) {
        throw new IllegalStateException("The pooled array was null!");
      }
    }
    
    if (prefix == Schema.APPENDS_PREFIX) {
      int idx = 0;
      while (idx < value.length) {
        long timestamp = base_timestamp.epoch();
        long nanos = -1;
        byte flags = 0;
        long offset = 0;
        
        if ((value[idx] & NumericCodec.NS_BYTE_FLAG) == 
            NumericCodec.NS_BYTE_FLAG) {
          offset = NumericCodec.offsetFromNanoQualifier(value, idx);
          if (offset <= last_offset) {
            needs_repair = true;
          }
          last_offset = offset;
          timestamp += (offset / 1000L / 1000L / 1000L);
          timestamp |= NumericLongArrayType.NANOSECOND_FLAG;
          offset -= ((offset / 1000L / 1000L / 1000L) * 1000L * 1000L * 1000L);
          nanos = offset;
          
          flags = NumericCodec.getFlags(value, idx, (byte) NumericCodec.NS_Q_WIDTH);
          idx += NumericCodec.NS_Q_WIDTH;
        } else if ((value[idx] & NumericCodec.MS_BYTE_FLAG) == 
            NumericCodec.MS_BYTE_FLAG) {
          offset = NumericCodec.offsetFromMsQualifier(value, idx);
          if (offset <= last_offset) {
            needs_repair = true;
          }
          last_offset = offset;
          timestamp *= 1000;
          timestamp += (offset / 1000L / 1000L);
          timestamp |= NumericLongArrayType.MILLISECOND_FLAG;
          flags = NumericCodec.getFlags(value, idx, (byte) NumericCodec.MS_Q_WIDTH);
          idx += NumericCodec.MS_Q_WIDTH;
        } else {
          offset = NumericCodec.offsetFromSecondQualifier(value, idx);
          if (offset <= last_offset) {
            needs_repair = true;
          }
          last_offset = offset;
          timestamp += (offset / 1000L / 1000L / 1000L);
          flags = NumericCodec.getFlags(value, idx, (byte) NumericCodec.S_Q_WIDTH);
          idx += NumericCodec.S_Q_WIDTH;
        }
        
        int vlen = NumericCodec.getValueLengthFromQualifier(value, idx - 1);
        if ((flags & NumericCodec.FLAG_FLOAT) != 0) {
          timestamp |= NumericLongArrayType.FLOAT_FLAG;
          add(timestamp, nanos, NumericCodec.extractFloatingPointValue(value, idx, flags));
        } else {
          add(timestamp, nanos, NumericCodec.extractIntegerValue(value, idx, flags));
        }
        idx += vlen;
      }
    } else {
      // two options:
      // 1) It's a raw put data point in seconds or ms (now nanos)
      // 2) It's an old-school compacted column either hetero or homogenous
      // regarding seconds or ms.
      if (qualifier.length == NumericCodec.S_Q_WIDTH) {
        // handle older versions of OpenTSDB 1.x where there were some 
        // encoding issues that only affected second values.
        int vlen = NumericCodec.getValueLengthFromQualifier(qualifier, 
            qualifier.length - 1);
        long offset = NumericCodec.offsetFromSecondQualifier(qualifier, 0);
        if (offset <= last_offset) {
          needs_repair = true;
        }
        last_offset = offset;
        offset = offset / 1000L / 1000L / 1000L;
        if (value.length != vlen) {
          long timestamp = base_timestamp.epoch() + offset;
          // TODO - log it in a counter somewhere
          if ((qualifier[qualifier.length - 1] & NumericCodec.FLAG_FLOAT) == 
              NumericCodec.FLAG_FLOAT) {
            timestamp |= NumericLongArrayType.FLOAT_FLAG;
            byte[] fixed = NumericCodec.fixFloatingPointValue(
                NumericCodec.getFlags(
                    qualifier, 0, (byte) NumericCodec.S_Q_WIDTH), value);
            
            if (fixed.length == 4) {
              add(timestamp, -1, NumericCodec.extractFloatingPointValue(
                  fixed, 0, (byte) (3 | NumericCodec.FLAG_FLOAT)));
            } else {
              add(timestamp, -1, NumericCodec.extractFloatingPointValue(
                  fixed, 0, (byte) (7 | NumericCodec.FLAG_FLOAT)));
            }
          } else {
            add(timestamp, -1, NumericCodec.extractIntegerValue(
                value, 0, (byte) (value.length - 1)));
          }
        } else {
          long timestamp = base_timestamp.epoch() + offset;
          byte flags = NumericCodec.getFlags(qualifier, 0, (byte) NumericCodec.S_Q_WIDTH);
          if ((qualifier[qualifier.length - 1] & NumericCodec.FLAG_FLOAT) == 
              NumericCodec.FLAG_FLOAT) {
            timestamp |= NumericLongArrayType.FLOAT_FLAG;
            add(timestamp, -1, NumericCodec.extractFloatingPointValue(value, 0, flags));
          } else {
            add(timestamp, -1, NumericCodec.extractIntegerValue(value, 0, flags));
          }
        }
      } else {
        // could be ms, ns or compacted
        int qidx = 0;
        int vidx = 0;
        long timestamp = base_timestamp.epoch();
        long nanos = -1;
        long offset;
        
        while (qidx < qualifier.length) {
          timestamp = base_timestamp.epoch();
          nanos = -1;
          byte flags = 0;
          if ((qualifier[qidx] & NumericCodec.NS_BYTE_FLAG) == 
              NumericCodec.NS_BYTE_FLAG) {
            offset = NumericCodec.offsetFromNanoQualifier(qualifier, qidx);
            if (offset <= last_offset) {
              needs_repair = true;
            }
            last_offset = offset;
            timestamp += offset / 1000L / 1000L / 1000L;
            timestamp |= NumericLongArrayType.NANOSECOND_FLAG;
            offset -= ((offset / 1000L / 1000L / 1000L) * 1000L * 1000L * 1000L);
            nanos = offset;
            flags = NumericCodec.getFlags(qualifier, qidx, (byte) NumericCodec.NS_Q_WIDTH);
            qidx += NumericCodec.NS_Q_WIDTH;
          } else if ((qualifier[qidx] & NumericCodec.MS_BYTE_FLAG) == 
              NumericCodec.MS_BYTE_FLAG) {
            offset = NumericCodec.offsetFromMsQualifier(qualifier, qidx);
            if (offset <= last_offset) {
              needs_repair = true;
            }
            last_offset = offset;
            timestamp *= 1000;
            timestamp += (offset / 1000L / 1000L);
            timestamp |= NumericLongArrayType.MILLISECOND_FLAG;
            flags = NumericCodec.getFlags(qualifier, qidx, (byte) NumericCodec.MS_Q_WIDTH);
            qidx += NumericCodec.MS_Q_WIDTH;
          } else {
            offset = NumericCodec.offsetFromSecondQualifier(qualifier, qidx);
            if (offset <= last_offset) {
              needs_repair = true;
            }
            last_offset = offset;
            timestamp += offset / 1000L / 1000L / 1000L;
            flags = NumericCodec.getFlags(qualifier, qidx, (byte) NumericCodec.S_Q_WIDTH);
            qidx += NumericCodec.S_Q_WIDTH;
          }
          
          if ((flags & NumericCodec.FLAGS_MASK) != 0) {
            timestamp |= NumericLongArrayType.FLOAT_FLAG;
            add(timestamp, nanos, NumericCodec.extractFloatingPointValue(value, vidx, flags));
          } else {
            add(timestamp, nanos, NumericCodec.extractIntegerValue(value, vidx, flags));
          }
          vidx += NumericCodec.getValueLengthFromQualifier(qualifier, qidx - 1);
        }
      }
    }
  }

  @Override
  public void setPooledObject(final PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }

  @Override
  public long idHash() {
    return id_hash;
  }

  @Override
  public PartialTimeSeriesSet set() {
    return set;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericType.TYPE;
  }
  
  @Override
  public Object data() {
    if (pooled_array != null) {
      reference_counter.incrementAndGet();
      return pooled_array.object();
    } else {
      return null;
    }
  }
  
  /**
   * Sorts, de-duplicates and optionally reverses the data in this series. Call
   * it only after adding all of the data.
   * @param keep_earliest Whether or not to keep the earliest duplicates in the
   * array or the latest. 
   * @param reverse Whether or not to reverse the data.
   */
  public void dedupe(final boolean keep_earliest, final boolean reverse) {
    if (pooled_array == null) {
      // no-op
      return;
    }
    if (write_idx + 1 >= ((long[]) pooled_array.object()).length) {
      new ReAllocatedArray();
    }
    
    if (!needs_repair) {
      if (reverse) {
        // TODO make this more efficient
        long[] new_array = new long[write_idx + 1];
        int[] indexes = new int[write_idx + 1]; // too long I know but this'll be three pass
        int idx = 0;
        int array_idx = 0;
        long[] array = (long[]) pooled_array.object();
        while (array_idx < write_idx) {
          indexes[idx++] = array_idx;
          if ((array[array_idx] & NumericLongArrayType.NANOSECOND_FLAG) != 0) {
            array_idx += 3;
          } else {
            array_idx += 2;
          }
        }
        
        array_idx = 0;
        idx--;
        while (idx >= 0) {
          if ((array[indexes[idx]] & NumericLongArrayType.NANOSECOND_FLAG) != 0) {
            System.arraycopy(array, indexes[idx], new_array, array_idx, 3);
            array_idx += 3;
          } else {
            System.arraycopy(array, indexes[idx], new_array, array_idx, 2);
            array_idx += 2;
          }
          idx--;
        }
        
        System.arraycopy(new_array, 0, array, 0, write_idx);
      }
      ((long[]) pooled_array.object())[write_idx] = NumericLongArrayType.TERIMNAL_FLAG;
      return;
    }
    
    // dedupe
    // TODO - any primitive tree maps out there? Or maybe there's just an
    // all around better way to do this. For now this should be fast enough. We
    // can't do an in-place swap easily since we have variable lengths (thank you
    // nanos!). Though there probably is an algo for that with an extra temp var.
    // The long is the offset from the base time, the int is the offset in the
    // array.
    TreeMap<Long, Integer> map = new TreeMap<Long, Integer>();
    int idx = 0;
    long base_timestamp = -1;
    long[] array = (long[]) pooled_array.object();
    while (idx < write_idx) {
      if (base_timestamp < 0) {
        if ((array[idx] & NumericLongArrayType.MILLISECOND_FLAG) != 0) {
          base_timestamp = (array[idx] & NumericLongArrayType.TIMESTAMP_MASK) / 1000;
        } else {
          base_timestamp = array[idx] & NumericLongArrayType.TIMESTAMP_MASK;
        }
        base_timestamp = base_timestamp - (base_timestamp % 3600);
      }
      
      long offset = 0;
      if ((array[idx] & NumericLongArrayType.MILLISECOND_FLAG) != 0) {
        long ms = array[idx] & NumericLongArrayType.TIMESTAMP_MASK;
        offset = (ms / 1000) - base_timestamp;
        offset *= 1000L;
        offset += ms - ((ms / 1000L) * 1000L);
        offset *= 1000L * 1000L;
      } else {
        offset = (array[idx] & NumericLongArrayType.TIMESTAMP_MASK) - base_timestamp;
        offset *= 1000L * 1000L * 1000L;
      }
      
      if ((array[idx] & NumericLongArrayType.NANOSECOND_FLAG) != 0) {
        offset += array[idx + 1];
      }
      
      if (keep_earliest) {
        map.putIfAbsent(offset, idx);
      } else {
        map.put(offset, idx);
      }
      
      if ((array[idx] & NumericLongArrayType.NANOSECOND_FLAG) != 0) {
        idx += 3;
      } else {
        idx += 2;
      }
    }
    
    // TODO - see if we can checkout another array
    long[] new_array = new long[write_idx];
    final Iterator<Entry<Long, Integer>> iterator;
    if (reverse) {
      iterator = map.descendingMap().entrySet().iterator();
    } else {
      iterator = map.entrySet().iterator();
    }
    
    idx = 0;
    while (iterator.hasNext()) {
      final Entry<Long, Integer> entry = iterator.next();
      if ((array[entry.getValue()] & NumericLongArrayType.NANOSECOND_FLAG) != 0) {
        System.arraycopy(array, entry.getValue(), new_array, idx, 3);
        idx += 3;
      } else {
        System.arraycopy(array, entry.getValue(), new_array, idx, 2);
        idx += 2;
      }
    }
    
    // copy back
    write_idx = idx;
    System.arraycopy(new_array, 0, array, 0, idx);
    ((long[]) pooled_array.object())[write_idx] = NumericLongArrayType.TERIMNAL_FLAG;
    needs_repair = false;
  }
  
  private void add(final long timestamp, final long nanos, final double value) {
    add(timestamp, nanos, Double.doubleToRawLongBits(value));
  }
  
  private void add(final long timestamp, final long nanos, final long value) {
    if (((long[]) pooled_array.object()).length <= (write_idx + (nanos >= 0 ? 3 : 2))) {
      new ReAllocatedArray();
    }
    
    ((long[]) pooled_array.object())[write_idx++] = timestamp;
    if (nanos >= 0) {
      ((long[]) pooled_array.object())[write_idx++] = nanos;
    }
    ((long[]) pooled_array.object())[write_idx++] = value;
  }
  
  /**
   * A simple class used when we exceede the size of the pooled array.
   */
  class ReAllocatedArray implements PooledObject {
    
    long[] array;
    
    /**
     * Copies and adds 16 longs to the array.
     */
    ReAllocatedArray() {
      array = new long[write_idx + 16];
      System.arraycopy(((long[]) pooled_array.object()), 0, array, 0, write_idx);
      try {
        pooled_array.release();
      } catch (Throwable t) {
        LOG.error("Whoops, issue releasing pooled array", t);
      }
      pooled_array = this;
      // TODO - log this
    }
    
    @Override
    public Object object() {
      return array;
    }

    @Override
    public void release() {
      // no-op
    }
    
  }
}
