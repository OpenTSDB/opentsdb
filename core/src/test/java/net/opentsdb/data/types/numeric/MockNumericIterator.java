// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import java.util.Collections;
import java.util.List;

import org.junit.Ignore;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.NumericFillPolicy;
import net.opentsdb.query.processor.TimeSeriesProcessor;

/**
 * Simple little class for mocking out a source.
 * <p>
 * Set the individual deferreds with exceptions or just leave them as nulls.
 * If you set an exception, it will be thrown or returned in the appropriate 
 * calls.
 * <p>
 * To use this mock, add lists of 1 or more data points, sorted in time order,
 * to the {@link #data} list. At the end of each list, the status is set to
 * return {@link IteratorStatus#END_OF_CHUNK}.
 */
@Ignore
public class MockNumericIterator extends TimeSeriesIterator<NumericType> {
  
  public Deferred<Object> initialize_deferred = Deferred.fromResult(null);
  public Deferred<Object> fetch_next_deferred = Deferred.fromResult(null);
  public Deferred<Object> close_deferred = Deferred.fromResult(null);
  public TimeSeriesProcessor processor = null;
  public NumericFillPolicy fill;
  public RuntimeException ex;
  public boolean throw_ex;
  public List<List<MutableNumericType>> data = Collections.emptyList();
  
  private int outer_index = 0;
  private int inner_index = 0;
  
  public MockNumericIterator(final TimeSeriesId id) {
    super(id);
    fill = NumericFillPolicy
        .newBuilder()
        .setPolicy(FillPolicy.NOT_A_NUMBER)
        .build();
  }
  
  public MockNumericIterator(final TimeSeriesId id, final int order) {
    super(id);
    this.order = order;
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
    updateContext();
    return initialize_deferred;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericType.TYPE;
  }

  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public IteratorStatus status() {
    if (ex != null) {
      return IteratorStatus.EXCEPTION;
    }
    if (outer_index >= data.size()) {
      return IteratorStatus.END_OF_DATA;
    } else if (inner_index >= data.get(outer_index).size()) {
      if (outer_index + 1 >= data.size()) {
        return IteratorStatus.END_OF_DATA;
      } else {
        return IteratorStatus.END_OF_CHUNK;
      }
    } else {
      return IteratorStatus.HAS_DATA;
    }
  }
  
  @Override
  public TimeSeriesValue<NumericType> next() {
    if (ex != null) {
      if (context != null) {
        context.updateContext(IteratorStatus.EXCEPTION, null);
      }
      if (throw_ex) {
        throw ex;
      }
      return null;
    }
    TimeSeriesValue<NumericType> result = null;
    
    if (context != null) {
      if (outer_index < data.size() && 
          inner_index < data.get(outer_index).size()) {
        boolean end = false;
        while (data.get(outer_index).get(inner_index).timestamp().compare(
            RelationalOperator.LT, context.syncTimestamp())) {
          if (inner_index >= data.get(outer_index).size()) {
            outer_index++;
            inner_index = 0;
          } else {
            inner_index++;
          }
          
          if (outer_index >= data.size() || 
              inner_index >= data.get(outer_index).size()) {
            end = true;
            break;
          }
        }
        
        if (!end && data.get(outer_index).get(inner_index).timestamp().compare(
            RelationalOperator.EQ, context.syncTimestamp())) {
          result = data.get(outer_index).get(inner_index);
          if (inner_index >= data.get(outer_index).size()) {
            outer_index++;
            inner_index = 0;
          } else {
            inner_index++;
          }
        } else {
          result = new MutableNumericType(context.syncTimestamp(), 
            fill.getValue());
        }
      } else {
        result = new MutableNumericType(context.syncTimestamp(), 
            fill.getValue());
      }
      
      updateContext();
    } else {
      if (outer_index < data.size() && 
          inner_index < data.get(outer_index).size()) {
        result = data.get(outer_index).get(inner_index);
        if (inner_index >= data.get(outer_index).size()) {
          outer_index++;
          inner_index = 0;
        } else {
          inner_index++;
        }
      }
    }
    //System.out.println("   outer: " + outer_index + "  inner: " + inner_index);
    return result;
  }

  @Override
  public TimeSeriesValue<NumericType> peek() {
    if (outer_index >= data.size()) {
      return null;
    }
    if (inner_index >= data.get(outer_index).size()) {
      // end of chunk
      return null;
    }
    return data.get(outer_index).get(outer_index);
  }
  
  protected void updateContext() {
    if (outer_index >= data.size()) {
      context.updateContext(IteratorStatus.END_OF_DATA, null);
    } else if (inner_index >= data.get(outer_index).size()) {
      if (outer_index + 1 >= data.size()) {
        context.updateContext(IteratorStatus.END_OF_DATA, null);
      } else {
        context.updateContext(IteratorStatus.END_OF_CHUNK, null);
      }
    } else {
      context.updateContext(IteratorStatus.HAS_DATA, 
          data.get(outer_index).get(inner_index).timestamp());
    }
  }
  
  @Override
  public Deferred<Object> fetchNext() {
    if (ex != null) {
      if (context != null) {
        context.updateContext(IteratorStatus.EXCEPTION, null);
      }
      if (throw_ex) {
        throw ex;
      }
      return Deferred.fromError(ex);
    }
    if (outer_index >= data.size()) {
      // noop
    } else if (inner_index >= data.get(outer_index).size()) {
      outer_index++;
      inner_index = 0;
    } else {
      inner_index++;
    }
    if (context != null) {
      updateContext();
    }
    return fetch_next_deferred;
  }

  @Override
  public TimeSeriesIterator<NumericType> getShallowCopy(final QueryContext context) {
    final MockNumericIterator it = new MockNumericIterator(id);
    it.data = data;
    it.parent = this;
    it.context = context;
    if (context != null) {
      context.register(this);
    }
    return it;
  }

  @Override
  public TimeSeriesIterator<NumericType> getDeepCopy(final QueryContext context, 
                                                 final TimeStamp start, 
                                                 final TimeStamp end) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public Deferred<Object> close() {
    if (ex != null) {
      return Deferred.fromError(ex);
    }
    return close_deferred;
  }

  @Override
  public TimeStamp startTime() {
    if (data.isEmpty()) {
      return null;
    }
    // iterate to find the first.
    for (final List<MutableNumericType> data_set : data) {
      for (final MutableNumericType value : data_set) {
        return value.timestamp();
      }
    }
    return null;
  }

  @Override
  public TimeStamp endTime() {
    for (int i = data.size() - 1; i >= 0; i--) {
      for (int x = data.get(i).size() - 1; x >= 0; x--) {
        return data.get(i).get(x).timestamp();
      }
    }
    return null;
  }

}
