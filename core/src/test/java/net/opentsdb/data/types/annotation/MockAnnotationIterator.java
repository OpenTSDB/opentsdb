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
package net.opentsdb.data.types.annotation;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.processor.TimeSeriesProcessor;

public class MockAnnotationIterator extends 
    TimeSeriesIterator<AnnotationType> {
  public Deferred<Object> initialize_deferred = Deferred.fromResult(null);
  public Deferred<Object> fetch_next_deferred = Deferred.fromResult(null);
  public Deferred<Object> close_deferred = Deferred.fromResult(null);
  public TimeSeriesProcessor processor = null;
  public RuntimeException ex;
  public boolean throw_ex;
  
  private MockNumericIterator parent;
  
  @Override
  public Deferred<Object> initialize() {
    if (ex != null) {
      return Deferred.fromError(ex);
    }
    return initialize_deferred;
  }
  
  public MockAnnotationIterator(final TimeSeriesId id) {
    super(id);
  }
  
  public MockAnnotationIterator(final TimeSeriesId id, final int order) {
    super(id);
    this.order = order;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return AnnotationType.TYPE;
  }

  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public IteratorStatus status() {
    // TODO
    return null;
  }
  
  @Override
  public TimeSeriesValue<AnnotationType> next() {
    if (ex != null) {
      throw ex;
    }
    // TODO
    return null;
  }

  @Override
  public Deferred<Object> fetchNext() {
    // TODO Auto-generated method stub
    if (ex != null) {
      return Deferred.fromError(ex);
    }
    return null;
  }

  @Override
  public TimeSeriesIterator<AnnotationType> getCopy(final QueryContext context) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TimeSeriesIterator<AnnotationType> getCopy(final QueryContext context, 
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TimeStamp endTime() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected void updateContext() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public TimeSeriesValue<AnnotationType> peek() {
    // TODO Auto-generated method stub
    return null;
  }

}
