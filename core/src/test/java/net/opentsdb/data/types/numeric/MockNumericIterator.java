// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import java.util.List;

import org.junit.Ignore;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.NumericFillPolicy;
import net.opentsdb.query.processor.TimeSeriesProcessor;

/**
 * Simple little class for mocking out a source.
 * Set the individual deferreds with exceptions or just leave them as nulls.
 * If you set an exception, it will be thrown or returned in the appropriate 
 * calls.
 */
@Ignore
public class MockNumericIterator implements TimeSeriesIterator<TimeSeriesValue<NumericType>> {
  
  public Deferred<Object> initialize_deferred = Deferred.fromResult(null);
  public Deferred<Object> fetch_next_deferred = Deferred.fromResult(null);
  public Deferred<Object> close_deferred = Deferred.fromResult(null);
  public TimeSeriesProcessor processor = null;
  public NumericFillPolicy fill;
  public RuntimeException ex;
  public boolean throw_ex;
  public List<List<MutableNumericType>> data;
  
  private int outer_index = 0;
  private int inner_index = 0;
  
  private final TimeSeriesId id;
  private MockNumericIterator parent;
  
  public MockNumericIterator(final TimeSeriesId id) {
    this.id = id;
    fill = NumericFillPolicy
        .newBuilder()
        .setPolicy(FillPolicy.NOT_A_NUMBER)
        .build();
  }
  
  @Override
  public Deferred<Object> initialize() {
    if (ex != null) {
      return Deferred.fromError(ex);
    }
    return initialize_deferred;
  }

  @Override
  public TypeToken<?> type() {
    return NumericType.TYPE;
  }

  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public void setProcessor(final TimeSeriesProcessor processor) {
    this.processor = processor;
  }

  @Override
  public IteratorStatus status() {
    if (ex != null) {
      return IteratorStatus.EXCEPTION;
    }
    if (outer_index >= data.size()) {
      return IteratorStatus.END_OF_DATA;
    }
    if (inner_index >= data.get(outer_index).size()) {
      return IteratorStatus.END_OF_CHUNK;
    }
    return IteratorStatus.HAS_DATA;
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    if (ex != null) {
      throw ex;
    }
    // see if we're time synced or not
    if (processor != null) {
      if (outer_index >= data.size() ||
          inner_index >= data.get(outer_index).size() || 
          data.get(outer_index).get(inner_index).timestamp().compare(
          TimeStampComparator.NE, processor.syncTimestamp())) {
        return new MutableNumericType(id, processor.syncTimestamp(), fill.getValue());
      }
      return data.get(outer_index).get(inner_index);
    }
    
    // not synced
    final MutableNumericType v = data.get(outer_index).get(inner_index);
    if (inner_index >= data.get(outer_index).size()) {
      outer_index++;
      inner_index = 0;
    } else {
      inner_index++;
    }
    return v;
  }

  @Override
  public void advance() {
    if (ex != null) {
      if (throw_ex) {
        throw ex;
      }
      processor.markStatus(IteratorStatus.EXCEPTION);
      return;
    }
    if (outer_index >= data.size()) {
      processor.markStatus(IteratorStatus.END_OF_DATA);
      return;
    }
    if (inner_index >= data.get(outer_index).size()) {
      // don't advance until fetchNext() is called.
      if (outer_index + 1 >= data.size()) {
        processor.markStatus(IteratorStatus.END_OF_DATA);
      } else {
        processor.markStatus(IteratorStatus.END_OF_CHUNK);
      }
      return;
    }
    
    final TimeStamp data_ts = data.get(outer_index).get(inner_index).timestamp();
    if (data_ts.compare(TimeStampComparator.EQ, processor.syncTimestamp())) {
      if (inner_index + 1 >= data.get(outer_index).size()) {
        if (outer_index + 1 >= data.size()) {
          processor.markStatus(IteratorStatus.END_OF_DATA);
        } else {
          processor.markStatus(IteratorStatus.END_OF_CHUNK);
        }
      } else {
        processor.markStatus(IteratorStatus.HAS_DATA);
        processor.setSyncTime(data.get(outer_index).get(inner_index + 1).timestamp());
      }
    } else {
      while (data.get(outer_index).get(inner_index).timestamp().compare(
          TimeStampComparator.LT, processor.syncTimestamp())) {
        inner_index++;
        
        if (outer_index >= data.size()) {
          processor.markStatus(IteratorStatus.END_OF_DATA);
          return;
        } else if (inner_index >= data.get(outer_index).size()) {
          processor.markStatus(IteratorStatus.END_OF_CHUNK);
          return;
        }
      }
      
      if (data.get(outer_index).get(inner_index).timestamp().compare(
          TimeStampComparator.GT, processor.syncTimestamp())) {
        processor.markStatus(IteratorStatus.HAS_DATA);
        processor.setSyncTime(data.get(outer_index).get(inner_index).timestamp());
      } else {
        if (inner_index + 1 >= data.get(outer_index).size()) {
          if (outer_index + 1 >= data.size()) {
            processor.markStatus(IteratorStatus.END_OF_DATA);
          } else {
            processor.markStatus(IteratorStatus.END_OF_CHUNK);
          }
        } else {
          processor.markStatus(IteratorStatus.HAS_DATA);
          processor.setSyncTime(data.get(outer_index).get(inner_index + 1).timestamp());
        }
      }
    }
  }

  @Override
  public TimeStamp nextTimestamp() {
    if (ex != null) {
      throw ex;
    }
    if (inner_index >= data.get(outer_index).size()) {
      return new MillisecondTimeStamp(Long.MAX_VALUE);
    }
    return data.get(outer_index).get(inner_index).timestamp();
  }

  @Override
  public Deferred<Object> fetchNext() {
    if (ex != null) {
      return Deferred.fromError(ex);
    }
    if (processor != null) {
      if (outer_index >= data.size()) {
        processor.markStatus(IteratorStatus.END_OF_DATA);
      } else {
        if (inner_index + 1 >= data.get(outer_index).size()) {
          outer_index++;
          inner_index = 0;
        } else {
          inner_index++;
        }
        
        if (outer_index >= data.size()) {
          processor.markStatus(IteratorStatus.END_OF_DATA);
        } else if (inner_index >= data.get(outer_index).size()) {
          processor.markStatus(IteratorStatus.END_OF_CHUNK);
        } else {
          processor.markStatus(IteratorStatus.HAS_DATA);
          processor.setSyncTime(data.get(outer_index).get(inner_index).timestamp());
        }
      }
    } else {
      if (inner_index + 1 >= data.get(outer_index).size()) {
        outer_index++;
        inner_index = 0;
      } else {
        inner_index++;
      }
    }
    return fetch_next_deferred;
  }

  @Override
  public TimeSeriesIterator<TimeSeriesValue<NumericType>> getCopy() {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    it.parent = this;
    return it;
  }

  @Override
  public TimeSeriesIterator<TimeSeriesValue<NumericType>> getCopyParent() {
    return parent;
  }

  @Override
  public Deferred<Object> close() {
    if (ex != null) {
      return Deferred.fromError(ex);
    }
    return close_deferred;
  }

}
