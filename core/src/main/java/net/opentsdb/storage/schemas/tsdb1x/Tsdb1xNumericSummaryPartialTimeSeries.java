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
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericByteArraySummaryType;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.utils.Bytes;

/**
 * An implementation that converts the rollup column from a 1x schema into the
 * {@link NumericByteArraySummaryType}. 
 * <b>NOTE:</b> You MUST call {@link #dedupe(boolean, boolean)} on this object
 * before passing it upstream to clean up the data.
 * <p>
 * <b>NOTE:</b> If PUTs are used and two or more types are written in order, e.g.
 * sum, avg @ts1, sum, avg @ts2, avg, sum @ts3 then we'll not dedupe and leave it
 * in the slightly less efficient timestamp and 1 value format. TODO see if
 * this is the right thing to do.
 * @since 3.0
 */
public class Tsdb1xNumericSummaryPartialTimeSeries extends 
    Tsdb1xPartialTimeSeries<NumericByteArraySummaryType>
      implements NumericByteArraySummaryType {
  private static final Logger LOG = LoggerFactory.getLogger(
      Tsdb1xNumericSummaryPartialTimeSeries.class);
 
  /** Whether or not the array has out-of-order or duplicate data. */
  protected boolean needs_repair;
  
  /** The last timestamp, used to determine if we need a repair. */
  protected long last_ts = -1;
  
  /** the last type written. */
  protected byte last_type = -1;
  
  /**
   * Default ctor.
   */
  public Tsdb1xNumericSummaryPartialTimeSeries() {
    super();
  }
  
  @Override
  public void release() {
    if (pooled_array != null) {
      pooled_array.release();
      pooled_array = null;
    }
    set = null;
    write_idx = 0;
    needs_repair = false;
    last_ts = -1;
    last_type = -1;
    if (pooled_object != null) {
      pooled_object.release();
    }
  }
  
  @Override
  public void reset(final TimeStamp base_timestamp, 
                    final long id_hash, 
                    final ObjectPool array_pool,
                    final PartialTimeSeriesSet set,
                    final RollupInterval interval) {
    super.reset(base_timestamp, id_hash, array_pool, set, interval);
    if (interval == null) {
      throw new IllegalArgumentException("Rollup interval cannot be null.");
    }
    needs_repair = false;
    last_ts = -1;
    last_type = -1;
  }
  
  @Override
  public void addColumn(final byte prefix,
                        final byte[] qualifier, 
                        final byte[] value) {
    if (qualifier == null || qualifier.length < 1) {
      throw new IllegalDataException("Qualifier was too short.");
    }
    if (value == null || value.length < 1) {
      throw new IllegalDataException("Value was too short.");
    }
    if (set == null) {
      throw new IllegalStateException("Call reset before trying to add data.");
    }
    if (pooled_array == null) {
      if (array_pool != null) {
        pooled_array = array_pool.claim();
        if (pooled_array == null) {
          throw new IllegalStateException("The pooled array was null!");
        }
      } else {
        pooled_array = new ReAllocatedArray(2048);
      }
    }
    
    // so, we can have byte prefix's or we could have strings for the old
    // rollups. E.g. [ type, offset, offset ] or 
    // [ 'a', 'g', 'g', ':', offset, offset ]. Offsets may be on 2 or 
    // more bytes. So we can *guess* that if the length is 5 or less 
    // that it's a byte prefix, and > 5 then it's a string prefix.
    // ALSO note that appends are <offset><value> like raw data appends though
    // the offset is interval based instead of time based.
    int type;
    final int offset_start;
    if (qualifier.length < 6) {
      type = qualifier[0];
      offset_start = qualifier.length == 1 ? -1 : 1;
    } else {
      type = interval.rollupConfig().getIdForAggregator(qualifier);
      offset_start = interval.rollupConfig().getOffsetStartFromQualifier(qualifier);
    }
    
    byte[] data = (byte[]) pooled_array.object();
    int value_length = offset_start >= 0 ? 
        NumericCodec.getValueLengthFromQualifier(qualifier, qualifier.length - 1) : 0;
    if (value.length == value_length) {
      if (write_idx + 8 + 1 + 2 + value.length >= data.length) {
        if (pooled_array instanceof ReAllocatedArray) {
          ((ReAllocatedArray) pooled_array).grow();
        } else {
          new ReAllocatedArray();
        }
        data = (byte[]) pooled_array.object();
      }
      
      long ts = RollupUtils.getTimestampFromRollupQualifier(
          qualifier, base_timestamp.epoch(), interval, offset_start);
      if (ts < last_ts || (ts == last_ts && last_type == (byte) type)) {
        needs_repair = true;
      }
      
      last_type = (byte) type;
      last_ts = ts;
      // TODO nanos and millis?
      
      Bytes.setLong(data, ts / 1000, write_idx);
      write_idx += 8;
      data[write_idx++] = 1;
      data[write_idx++] = (byte) type;
      
      if ((qualifier[offset_start] & NumericCodec.MS_BYTE_FLAG) == 
            NumericCodec.MS_BYTE_FLAG) {
        data[write_idx++] = NumericCodec.getFlags(qualifier, offset_start, 
            (byte) NumericCodec.MS_Q_WIDTH);
      } else {
        data[write_idx++] = NumericCodec.getFlags(qualifier, offset_start, 
            (byte) NumericCodec.S_Q_WIDTH);
      }
      System.arraycopy(value, 0, data, write_idx, value.length);
      write_idx += value.length;
    } else {
      // appended! So multiple offsets and values in the value array.
      // this has to be fast so we just dump data in, then we'll work on sorting
      // and deduping in that function.
      // timestamps are just 2 bytes for now.
      for (int i = 0; i < value.length;) {
        int offset = Bytes.getUnsignedShort(value, i);
        long ts = RollupUtils.getTimestampFromRollupQualifier(offset, 
            base_timestamp.epoch(), interval);
        if (ts < last_ts || (ts == last_ts && last_type == (byte) type)) {
          needs_repair = true;
        }
        
        byte flags = (byte) offset;
        value_length = (byte) ((flags & 0x7) + 1);
        if (write_idx + 8 + 1 + 2 + value_length >= data.length) {
          if (pooled_array instanceof ReAllocatedArray) {
            ((ReAllocatedArray) pooled_array).grow();
          } else {
            new ReAllocatedArray();
          }
          data = (byte[]) pooled_array.object();
        }
        
        last_type = (byte) type;
        last_ts = ts;
        
        Bytes.setLong(data, ts / 1000, write_idx);
        write_idx += 8;
        data[write_idx++] = 1;
        data[write_idx++] = (byte) type;
        
        data[write_idx++] = NumericCodec.getFlags(value, i, (byte) NumericCodec.S_Q_WIDTH);
        i += NumericCodec.S_Q_WIDTH;
        System.arraycopy(value, i, data, write_idx, value_length);
        write_idx += value_length;
        i += value_length;
      }
    }
  }
  
  @Override
  public NumericByteArraySummaryType value() {
    return this;
  }
  
  @Override
  public void dedupe(final boolean keep_earliest, final boolean reverse) {
    if (pooled_array == null) {
      // no-op
      return;
    }
    
    if (!needs_repair) {
      // TODO - reverse
      return;
    }
    
    // dedupe
    // TODO - any primitive tree maps out there? Or maybe there's just an
    // all around better way to do this. For now this should be fast enough. We
    // can't do an in-place swap easily since we have variable lengths (thank you
    // nanos!). Though there probably is an algo for that with an extra temp var.
    // The long is the offset from the base time, the map keys are the types and
    // the int values are the offset in the array. Just make it cleaner.
    TreeMap<Long, TreeMap<Byte, Integer>> map = new TreeMap<Long, TreeMap<Byte, Integer>>();
    int idx = 0;
    byte[] data = (byte[]) pooled_array.object();
    while (idx < write_idx) {
      long ts = Bytes.getLong(data, idx);
      TreeMap<Byte, Integer> summaries = map.get(ts);
      if (summaries == null) {
        summaries = Maps.newTreeMap();
        map.put(ts, summaries);
      }
      idx += 8;
      
      byte num_values = data[idx++];
      for (byte i = 0; i < num_values; i++) {
        if (keep_earliest) {
          summaries.putIfAbsent(data[idx], idx + 1);
        } else {
          summaries.put(data[idx], idx + 1);
        }
        
        idx += NumericCodec.getValueLengthFromQualifier(data, idx + 1) + 2;
      }
    }
    
    byte[] new_array;
    final PooledObject new_pooled_obj;
    if (array_pool != null && !(pooled_array instanceof ReAllocatedArray)) {
      new_pooled_obj = array_pool.claim();
      new_array = (byte[]) new_pooled_obj.object();
    } else {
      new_array = new byte[write_idx];
      new_pooled_obj = null;
    }
    final Iterator<Entry<Long, TreeMap<Byte, Integer>>> outer_iterator;
    if (reverse) {
      outer_iterator = map.descendingMap().entrySet().iterator();
    } else {
      outer_iterator = map.entrySet().iterator();
    }
    
    idx = 0;
    while (outer_iterator.hasNext()) {
      final Entry<Long, TreeMap<Byte, Integer>> entry = outer_iterator.next();
      Bytes.setLong(new_array, entry.getKey(), idx);
      idx += 8;
      new_array[idx++] = (byte) entry.getValue().size();
      final Iterator<Entry<Byte, Integer>> inner_iterator = entry.getValue().entrySet().iterator();
      while (inner_iterator.hasNext()) {
        final Entry<Byte, Integer> value = inner_iterator.next();
        new_array[idx++] = value.getKey();
        final int len = NumericCodec.getValueLengthFromQualifier(data, value.getValue());
        new_array[idx++] = data[value.getValue()];
        System.arraycopy(data, value.getValue() + 1, new_array, idx, len);
        idx += len;
      }
    }
    
    // copy back
    write_idx = idx;
    System.arraycopy(new_array, 0, data, 0, idx);
    if (new_pooled_obj == null) {
      new_pooled_obj.release();
    }
    needs_repair = false;
  }
  
  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return write_idx;
  }

  @Override
  public byte[] data() {
    if (pooled_array != null) {
      reference_counter.incrementAndGet();
      return (byte[]) pooled_array.object();
    } else {
      return null;
    }
  }
  
  /**
   * A simple class used when we exceed the size of the pooled array.
   */
  class ReAllocatedArray implements PooledObject {
    
    byte[] array;
    
    /**
     * Copies and adds 64 bytes to the array
     */
    ReAllocatedArray() {
      this(write_idx + 64);
    }
    
    ReAllocatedArray(final int amount) {
      array = new byte[amount];
      if (pooled_array != null) {
        System.arraycopy(((byte[]) pooled_array.object()), 0, array, 0, write_idx);
      }
      try {
        if (pooled_array != null) {
          pooled_array.release();
        }
      } catch (Throwable t) {
        LOG.error("Whoops, issue releasing pooled array", t);
      }
      // TODO - log this
      pooled_array = this;
    }
    
    @Override
    public Object object() {
      return array;
    }

    @Override
    public void release() {
      // no-op
    }
    
    private void grow() {
      array = new byte[write_idx + 64];
      System.arraycopy(((byte[]) pooled_array.object()), 0, array, 0, write_idx);
      try {
        if (pooled_array != null) {
          pooled_array.release();
        }
      } catch (Throwable t) {
        LOG.error("Whoops, issue releasing pooled array", t);
      }
      // TODO - log this
    }
  }
}