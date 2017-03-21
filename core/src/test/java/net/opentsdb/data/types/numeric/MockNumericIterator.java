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

import java.util.Collections;
import java.util.List;

import org.junit.Ignore;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.context.QueryContext;
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
  
  private final TimeSeriesId id;
  
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
  public TimeSeriesValue<NumericType> next() {
    if (ex != null) {
      context.updateContext(IteratorStatus.EXCEPTION, null);
      if (throw_ex) {
        throw ex;
      }
      return null;
    }
    TimeSeriesValue<NumericType> result = null;
    if (outer_index < data.size() && 
        inner_index < data.get(outer_index).size()) {
      boolean end = false;
      while (data.get(outer_index).get(inner_index).timestamp().compare(
          TimeStampComparator.LT, context.syncTimestamp())) {
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
          TimeStampComparator.EQ, context.syncTimestamp())) {
        result = data.get(outer_index).get(inner_index);
        if (inner_index >= data.get(outer_index).size()) {
          outer_index++;
          inner_index = 0;
        } else {
          inner_index++;
        }
      } else {
        result = new MutableNumericType(id, context.syncTimestamp(), 
          fill.getValue());
      }
    } else {
      result = new MutableNumericType(id, context.syncTimestamp(), 
          fill.getValue());
    }
    
    updateContext();
    //System.out.println("   outer: " + outer_index + "  inner: " + inner_index);
    return result;
  }

  private void updateContext() {
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
      context.updateContext(IteratorStatus.EXCEPTION, null);
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
    updateContext();
    return fetch_next_deferred;
  }

  @Override
  public TimeSeriesIterator<NumericType> getCopy(final QueryContext context) {
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
  public Deferred<Object> close() {
    if (ex != null) {
      return Deferred.fromError(ex);
    }
    return close_deferred;
  }

}
