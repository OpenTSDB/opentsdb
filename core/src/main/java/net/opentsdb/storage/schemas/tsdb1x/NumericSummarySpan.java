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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.rollup.RollupUtils;

/**
 * 
 * NOTE: Dps for each summary are aggregated in order. If there are
 * multiple summaries but some are missing, those summaries will return 
 * a null value at that timestamp. Upstream needs to figure out if it 
 * needs both values or can interpolate or what.
 * 
 * @since 3.0
 */
public class NumericSummarySpan implements Span<NumericSummaryType> {
  
  /** Whether or not to iterate in reverse order (timestamp descending). */
  protected final boolean reversed;
  
  /** The sorted list of rows in time ascending order always. */
  protected final List<NumericSummaryRowSeq> rows;
  
  /**
   * Default ctor.
   * @param reversed Whether or not to iterate in reverse.
   */
  public NumericSummarySpan(final boolean reversed) {
    this.reversed = reversed;
    rows = Lists.newArrayList();
  }
  
  @Override
  public synchronized void addSequence(final RowSeq sequence,
                                       final boolean keep_earliest) {
    if (sequence == null) {
      throw new IllegalArgumentException("Sequence cannot be null.");
    }
    if (!(sequence instanceof NumericSummaryRowSeq)) {
      throw new IllegalArgumentException("Cannot add a "
          + "non-NumericSummaryRowSeq: " + sequence.getClass());
    }
    final NumericSummaryRowSeq seq = (NumericSummaryRowSeq) sequence;
    if (seq.summary_data == null || seq.summary_data.isEmpty()) {
      // skip empty rows.
      return;
    }
    
    if (rows.isEmpty()) {
      rows.add(seq);
      return;
    }
    
    int idx = rows.size() - 1;
    if (rows.get(idx).base_timestamp == seq.base_timestamp) {
      // in this case we had a row continuation across a scan, which
      // can easily happen with wide rows so add it.
      rows.get(idx).appendSeq(seq);
      rows.get(idx).dedupe(keep_earliest, reversed);
      return;
    } else if (rows.get(idx).base_timestamp >= seq.base_timestamp) {
      // out of order so  walk back till we find a match the location
      // to insert it.
      for (; idx >= 0; idx--) {
        final long ts = rows.get(idx).base_timestamp;
        if (ts == seq.base_timestamp) {
          rows.get(idx).appendSeq(seq);
          rows.get(idx).dedupe(keep_earliest, reversed);
          return;
        }
        
        if (ts < seq.base_timestamp) {
          rows.add(idx + 1, seq);
          return;
        }
      }
      
      // put it at the top
      rows.add(0, seq);
    } else {
      rows.add(seq);
    }
  }
  
  @Override
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator() {
    return new SequenceIterator();
  }
  
  /**
   * An iterator over the rows in the list.
   */
  public class SequenceIterator implements 
      Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>,
      TimeSeriesValue<NumericSummaryType>,
      NumericSummaryType{

    /** The index within {@link NumericSummarySpan#rows}. */
    private int rows_idx;

    // TODO - blech lots of overhead. Find a bettter way.
    Map<Integer, Integer> row_idxs;
    Map<Integer, MutableNumericType> data_points;
    long current_offset;
    boolean has_next;
    
    /** The timestamp. Since the API says consumers can't keep this 
     * reference, we can keep re-using it to save memory. */
    private ZonedNanoTimeStamp ts;
    
    SequenceIterator() {
      // TODO - init
      
      if (rows.isEmpty()) {
        has_next = false;
        return;
      }
      current_offset = reversed ? Long.MIN_VALUE : Long.MAX_VALUE;
      ts = new ZonedNanoTimeStamp(0, 0, Const.UTC);
      rows_idx = reversed ? rows.size() - 1 : 0;
      NumericSummaryRowSeq row = rows.get(0); // guaranteed non-empty
      
      row_idxs = Maps.newHashMapWithExpectedSize(row.summary_data.size());
      data_points = Maps.newHashMapWithExpectedSize(row.summary_data.size());
      for (final Entry<Integer, byte[]> entry : row.summary_data.entrySet()) {
        row_idxs.put(entry.getKey(), 0);
        data_points.put(entry.getKey(), null);
        
        long offset = RollupUtils.getOffsetFromRollupQualifier(entry.getValue(), 0, row.interval);
        if (reversed ? offset > current_offset : offset < current_offset) {
          current_offset = offset;
        }
      }
      
      if (current_offset == (reversed ? Long.MIN_VALUE : Long.MAX_VALUE)) {
        has_next = false;
      } else {
        has_next = true;
      }
    }
    
    @Override
    public TimeStamp timestamp() {
      return ts;
    }

    @Override
    public NumericSummaryType value() {
      return this;
    }

    @Override
    public TypeToken<NumericSummaryType> type() {
      return NumericSummaryType.TYPE;
    }

    @Override
    public boolean hasNext() {
      return has_next;
    }

    @Override
    public TimeSeriesValue<NumericSummaryType> next() {
      if (reversed ? rows_idx < 0 : rows_idx >= rows.size()) {
        throw new NoSuchElementException("No more data.");
      }
      
      // now set our data
      final NumericSummaryRowSeq row = rows.get(rows_idx);
      for (final Entry<Integer, byte[]> entry : row.summary_data.entrySet()) {
        final byte[] data = entry.getValue();
        int row_idx = row_idxs.get(entry.getKey());
        if (row_idx >= data.length) {
          // Another entry had a value. Skip this one since we're at the end.
          data_points.put(entry.getKey(), null);
          continue;
        }
        long offset = RollupUtils.getOffsetFromRollupQualifier(data, row_idx, row.interval);
        if (offset != current_offset) {
          data_points.put(entry.getKey(), null);
          continue;
        }
        
        final byte flags;
        final int value_idx;
        if ((data[row_idx] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
          flags = NumericCodec.getFlags(data, row_idx, (byte) NumericCodec.MS_Q_WIDTH);
          value_idx = row_idx + NumericCodec.MS_Q_WIDTH; 
        } else {
          flags = NumericCodec.getFlags(data, row_idx, (byte) NumericCodec.S_Q_WIDTH);
          value_idx = row_idx + NumericCodec.S_Q_WIDTH;
        }
        
        row_idx = value_idx + NumericCodec.getValueLength(flags);
        boolean is_integer = !((flags & NumericCodec.FLAG_FLOAT) == 
            NumericCodec.FLAG_FLOAT);
        MutableNumericType dp = data_points.get(entry.getKey());
        if (dp == null) {
          dp = new MutableNumericType();
          data_points.put(entry.getKey(), dp);
        }
        if (is_integer) {
          dp.set(NumericCodec.extractIntegerValue(data, value_idx, flags));
        } else {
          dp.set(NumericCodec.extractFloatingPointValue(data, value_idx, flags));
        }
        
        row_idxs.put(entry.getKey(), row_idx);
      }
      ts.update(row.base_timestamp, current_offset * 1000L * 1000L);
      
      // before returning, find out if we have more data.
      current_offset = nextOffset();
      if (current_offset == (reversed ? Long.MIN_VALUE : Long.MAX_VALUE)) {
        // nothing left in this row so move on
        if (reversed) {
          rows_idx--;
        } else {
          rows_idx++;
        }
        resetIndices();
      }
      if (rows_idx >= 0 && rows_idx < rows.size()) {
        current_offset = nextOffset();
        if (current_offset == (reversed ? Long.MIN_VALUE : Long.MAX_VALUE)) {
          has_next = false;
        } else {
          has_next = true;
        }
      } else {
        has_next = false;
      }
      return this;
    }

    @Override
    public Collection<Integer> summariesAvailable() {
      return data_points.keySet();
    }

    @Override
    public NumericType value(final int summary) {
      return data_points.get(summary);
    }
    
    long nextOffset() {
      NumericSummaryRowSeq seq = rows.get(rows_idx);
      long next_offset = reversed ? Long.MIN_VALUE : Long.MAX_VALUE;
      for (final Entry<Integer, byte[]> entry : seq.summary_data.entrySet()) {
        final byte[] data = entry.getValue();
        final int row_idx = row_idxs.get(entry.getKey());
        if (row_idx >= data.length) {
          continue;
        }
        long offset = RollupUtils.getOffsetFromRollupQualifier(data, row_idx, seq.interval);
        if (reversed ? offset > next_offset : offset < next_offset) {
          next_offset = offset;
        }
      }
      return next_offset;
    }
    
    void resetIndices() {
      Iterator<Entry<Integer, Integer>> it = row_idxs.entrySet().iterator();
      while (it.hasNext()) {
        final Entry<Integer, Integer> entry = it.next();
        row_idxs.put(entry.getKey(), 0);
      }
    }
  }
}