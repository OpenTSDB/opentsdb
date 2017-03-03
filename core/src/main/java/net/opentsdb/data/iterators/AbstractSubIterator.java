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
package net.opentsdb.data.iterators;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.processor.TimeSeriesProcessor;

/**
 * An starting point for single series iterators that passes through calls to
 * the source iterator. Implementations can override the methods they need.
 * @param <T> The type of {@link net.opentsdb.data.TimeSeriesDataType} the 
 * iterator works with.
 *  
 * @since 3.0
 */
public abstract class AbstractSubIterator<T extends TimeSeriesValue<?>> implements 
  TimeSeriesIterator<TimeSeriesValue<?>> {
  
  /** The processor this iterator belongs to. May be null. */
  protected TimeSeriesProcessor processor;
  
  /** The source of data for this iterator. */
  protected TimeSeriesIterator<?> source;
  
  /** The parent iterator if this was cloned from an existing iterator. */
  protected TimeSeriesIterator<?> parent_copy;
  
  /**
   * Default ctore that accepts a time series as the source.
   * @param source An optional source time series iterator.
   */
  public AbstractSubIterator(final TimeSeriesIterator<?> source) {
    this.source = source;
  }
  
  @Override
  public Deferred<Object> initialize() {
    return source != null ? source.initialize() : 
      Deferred.fromError(new UnsupportedOperationException("Not implemented"));
  }
  
  @Override
  public TimeSeriesId id() {
    if (source != null) {
      return source.id();
    }
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setProcessor(final TimeSeriesProcessor processor) {
    this.processor = processor;
    // NOTE: Does not set the source's processor. Just this one.
  }
  
  @Override
  public IteratorStatus status() {
    if(source != null) {
      return source.status();
    }
    throw new UnsupportedOperationException("Not implemented");
  }
  
  @Override
  public void advance() {
    if (source != null) {
      source.advance();
    } else {
      throw new UnsupportedOperationException("Not implemented");
    }
  }
  
  @Override
  public TimeStamp nextTimestamp(){
    if (source != null) {
      return source.nextTimestamp();
    }
    throw new UnsupportedOperationException("Not implemented");
  }
  
  @Override
  public Deferred<Object> fetchNext() {
    return source != null ? source.fetchNext() : 
      Deferred.fromError(new UnsupportedOperationException("Not implemented"));
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public TimeSeriesIterator<TimeSeriesValue<?>> getCopyParent() {
    return (TimeSeriesIterator<TimeSeriesValue<?>>) parent_copy;
  }
  
  @Override
  public Deferred<Object> close() {
    return source != null ? source.close() : 
      Deferred.fromError(new UnsupportedOperationException("Not implemented"));
  }
}
