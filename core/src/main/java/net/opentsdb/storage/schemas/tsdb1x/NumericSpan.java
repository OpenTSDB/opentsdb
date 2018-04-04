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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.IllegalDataException;

/**
 * Represents a read-only sequence of continuous data points.
 * <p>
 * This class stores a continuous sequence of {@link NumericRowSeq}s in 
 * memory. Iteration is performed in forward or reverse order.
 * 
 * @since 3.0
 */
public class NumericSpan implements Span<NumericType> {
  
  /** Whether or not to iterate in reverse order (timestamp descending). */
  protected final boolean reversed;
  
  /** The sorted list of rows in time ascending order always. */
  protected final List<NumericRowSeq> rows;
  
  /**
   * Default ctor.
   * @param reversed Whether or not to iterate in reverse.
   */
  public NumericSpan(final boolean reversed) {
    this.reversed = reversed;
    rows = Lists.newArrayList();
  }
  
  @Override
  public synchronized void addSequence(final RowSeq sequence,
                                       final boolean keep_earliest) {
    if (sequence == null) {
      throw new IllegalArgumentException("Sequence cannot be null.");
    }
    if (!(sequence instanceof NumericRowSeq)) {
      throw new IllegalArgumentException("Cannot add a "
          + "non-NumericRowSeq: " + sequence.getClass());
    }
    final NumericRowSeq seq = (NumericRowSeq) sequence;
    if (seq.data == null || seq.data.length < 1) {
      // skip empty rows.
      return;
    }
    
    if (!rows.isEmpty()) {
      if (rows.get(rows.size() - 1).base_timestamp == 
          seq.base_timestamp) {
        // in this case we had a row continuation across a scan, which
        // can easily happen with wide rows so add it.
        rows.get(rows.size() - 1).addColumn(Schema.APPENDS_PREFIX, 
            new byte[] { Schema.APPENDS_PREFIX, 0, 0 }, seq.data);
        rows.get(rows.size() - 1).dedupe(keep_earliest, reversed);
        return;
      } else if (rows.get(rows.size() - 1).base_timestamp >= 
          seq.base_timestamp) {
        // out of order so  walk back till we find a match the location
        // to insert it.
        int idx = rows.size() - 2;
        if (idx < 0) {
          rows.add(0, seq);
          return;
        }
        for (; idx >= 0; idx--) {
          final long ts = rows.get(idx).base_timestamp;
          if (ts == seq.base_timestamp) {
            rows.get(idx).addColumn(Schema.APPENDS_PREFIX, 
                new byte[] { Schema.APPENDS_PREFIX, 0, 0 }, seq.data);
            rows.get(idx).dedupe(keep_earliest, reversed);
            return;
          }
          
          if (ts < seq.base_timestamp) {
            rows.add(idx + 1, seq);
            return;
          }
        }
      }
    }
    rows.add(seq);
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
      TimeSeriesValue<NumericType>, 
      NumericType {

    /** The index within {@link NumericSpan#rows}. */
    private int rows_idx = reversed ? rows.size() - 1 : 0;
    
    /** The data point index within {@link NumericRowSeq#data}. */
    private int row_idx = 0;
    
    /** The data value index within {@link NumericRowSeq#data}. */
    private int value_idx = 0;
    
    /** Whether or not the current value is an integer. */
    private boolean is_integer;
    
    /** The current data point flags. */
    private byte flags;
    
    /** The timestamp. Since the API says consumers can't keep this 
     * reference, we can keep re-using it to save memory. */
    private ZonedNanoTimeStamp ts = new ZonedNanoTimeStamp(0, 0, Const.UTC);
    
    @Override
    public boolean isInteger() {
      return is_integer;
    }

    @Override
    public long longValue() {
      if (!is_integer) {
        throw new IllegalDataException("This is not an integer!");
      }
      return NumericCodec.extractIntegerValue(
          rows.get(rows_idx).data, value_idx, flags);
    }

    @Override
    public double doubleValue() {
      if (is_integer) {
        throw new IllegalDataException("This is not a float!");
      }
      return NumericCodec.extractFloatingPointValue(
          rows.get(rows_idx).data, value_idx, flags);
    }

    @Override
    public double toDouble() {
      if (is_integer) {
        return (double) longValue();
      }
      return doubleValue();
    }

    @Override
    public TimeStamp timestamp() {
      return ts;
    }

    @Override
    public NumericType value() {
      return this;
    }

    @Override
    public TypeToken<NumericType> type() {
      return NumericType.TYPE;
    }

    @Override
    public boolean hasNext() {
      if (reversed ? rows_idx < 0 : rows_idx >= rows.size()) {
        return false;
      }
      
      if (row_idx < rows.get(rows_idx).data.length) {
        return true;
      }
      
      if (reversed) {
        return rows_idx - 1 >= 0;
      } else {
        return rows_idx  + 1< rows.size();
      }
    }

    @Override
    public TimeSeriesValue<NumericType> next() {
      if (reversed ? rows_idx < 0 : rows_idx >= rows.size()) {
        throw new NoSuchElementException("No more data.");
      }
      NumericRowSeq seq = rows.get(rows_idx);
      if (row_idx >= seq.data.length) {
        if (reversed) {
          rows_idx--;
        } else {
          rows_idx++;
        }
        row_idx = 0;
        seq = rows.get(rows_idx);
      }
      
      final long time_offset;
      if ((seq.data[row_idx] & NumericCodec.NS_BYTE_FLAG) == 
          NumericCodec.NS_BYTE_FLAG) {
        time_offset = NumericCodec.offsetFromNanoQualifier(seq.data, row_idx);
        flags = NumericCodec.getFlags(seq.data, row_idx, 
            (byte) NumericCodec.NS_Q_WIDTH);
        value_idx = row_idx + NumericCodec.NS_Q_WIDTH;
      } else if ((seq.data[row_idx] & NumericCodec.MS_BYTE_FLAG) == 
          NumericCodec.MS_BYTE_FLAG) {
        time_offset = NumericCodec.offsetFromMsQualifier(seq.data, row_idx);
        flags = NumericCodec.getFlags(seq.data, row_idx, 
            (byte) NumericCodec.MS_Q_WIDTH);
        value_idx = row_idx + NumericCodec.MS_Q_WIDTH;
      } else {
        time_offset = NumericCodec.offsetFromSecondQualifier(seq.data, row_idx);
        flags = NumericCodec.getFlags(seq.data, row_idx, 
            (byte) NumericCodec.S_Q_WIDTH);
        value_idx = row_idx + NumericCodec.S_Q_WIDTH;
      }
      
      row_idx = value_idx + NumericCodec.getValueLength(flags);
      final long seconds_offset = (time_offset / 1000L / 1000L / 1000L); 
      
      final long epoch = seq.base_timestamp + seconds_offset;
      final long nanos = time_offset - (seconds_offset * 1000L * 1000L * 1000L);
      ts.update(epoch, nanos);
      is_integer = !((flags & NumericCodec.FLAG_FLOAT) == 
          NumericCodec.FLAG_FLOAT);
      return this;
    }
    
  }
}
