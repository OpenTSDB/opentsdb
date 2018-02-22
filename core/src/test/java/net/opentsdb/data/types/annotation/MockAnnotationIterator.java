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
package net.opentsdb.data.types.annotation;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
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
  
  public MockAnnotationIterator(final TimeSeriesStringId id) {
    super(id);
  }
  
  public MockAnnotationIterator(final TimeSeriesStringId id, final int order) {
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
  public TimeSeriesIterator<AnnotationType> getShallowCopy(final QueryContext context) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TimeSeriesIterator<AnnotationType> getDeepCopy(final QueryContext context, 
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
