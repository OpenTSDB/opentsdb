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
   * @param source A non-null time series iterator.
   * @throws IllegalArgumentException if the source was null.
   */
  public AbstractSubIterator(final TimeSeriesIterator<?> source) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    this.source = source;
  }
  
  /**
   * A copy constructor that make a copy of the source and take a reference
   * to the parent.
   * @param source A non-null source to copy from.
   * @param parent A non-null parent.
   * @throws IllegalArgumentException if the source or parent was null.
   */
  protected AbstractSubIterator(final TimeSeriesIterator<?> source, 
      final TimeSeriesIterator<?> parent) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (parent == null) {
      throw new IllegalArgumentException("Parent cannot be null.");
    }
    this.source = source.getCopy();
    parent_copy = parent;
  }
  
  @Override
  public Deferred<Object> initialize() {
    return source.initialize();
  }
  
  @Override
  public TimeSeriesId id() {
    return source.id();
  }

  @Override
  public void setProcessor(final TimeSeriesProcessor processor) {
    this.processor = processor;
    // NOTE: Does not set the source's processor. Just this one.
  }
  
  @Override
  public IteratorStatus status() {
    return source.status();
  }
  
  @Override
  public void advance() {
    source.advance();
  }
  
  @Override
  public TimeStamp nextTimestamp(){
    return source.nextTimestamp();
  }
  
  @Override
  public Deferred<Object> fetchNext() {
    return source.fetchNext();
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public TimeSeriesIterator<TimeSeriesValue<?>> getCopyParent() {
    return (TimeSeriesIterator<TimeSeriesValue<?>>) parent_copy;
  }
  
  @Override
  public Deferred<Object> close() {
    return source.close();
  }
}
