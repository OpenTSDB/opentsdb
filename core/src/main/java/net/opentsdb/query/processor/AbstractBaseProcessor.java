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
package net.opentsdb.query.processor;

import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;

/**
 * A processor base that tracks sync times and status based on it's child
 * iterators. This base is useful for processors that adjust timing such as 
 * downsamplers.
 * 
 * @param <T>  The type of the implementing processor. Just pass in the class
 * name.
 * 
 * @since 3.0
 */
public abstract class AbstractBaseProcessor<T> implements TimeSeriesProcessor {

  /** A non-null source for the processor. */
  protected final TimeSeriesProcessor source;
  
  /** An optional config for this processor. May be null. */
  protected final TimeSeriesProcessorConfig<T> config;
  
  /** A local map of iterators that may be initialized to include processor
   * specific sub-iterators the source's iterators pass through. */
  protected Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators;
  
  /** An option parent if this was copied. */
  protected TimeSeriesProcessor copy_parent;
  
  /** The "current" timestamp returned when {@link #setSyncTime(TimeStamp)} is 
   * called. */
  protected TimeStamp sync_time = new MillisecondTimeStamp(Long.MAX_VALUE);
  
  /** The "next" timestamp updated when child iterators call into 
   * {@link #setSyncTime(TimeStamp)}. */
  protected TimeStamp next_sync_time = new MillisecondTimeStamp(Long.MAX_VALUE);
  
  /** The current ierator status. */
  protected IteratorStatus status = IteratorStatus.END_OF_DATA;
  
  /**
   * Default Ctor used by the registry.
   * @param source A non-null source to operate on.
   * @param config An optional config.
   */
  public AbstractBaseProcessor(final TimeSeriesProcessor source, 
      final TimeSeriesProcessorConfig<T> config) {
    if (source == null) {
      throw new IllegalArgumentException("Source for the processor cannot be null");
    }
    this.source = source;
    this.config = config;
  }
  
  /**
   * Copy constructor that sets the parent reference
   * @param source A non-null source to operate on.
   * @param config An optional config.
   * @param parent A non-null parent.
   */
  protected AbstractBaseProcessor(final TimeSeriesProcessor source, 
      final TimeSeriesProcessorConfig<T> config, TimeSeriesProcessor parent) {
    if (parent == null) {
      throw new IllegalArgumentException("Parent cannot be null when making a copy");
    }
    this.source = source;
    this.config = config;
    copy_parent = parent;
  }

  @Override
  public Deferred<Object> initialize() {
    return source.initialize(); 
  }
  
  @Override
  public void addSeries(final TimeSeriesGroupId group, 
      final TimeSeriesIterator<?> series) {
    throw new UnsupportedOperationException("Not implemented.");
  }
  
  @Override
  public Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>> iterators() {
    return iterators;
  }
  
  @Override
  public TimeStamp syncTimestamp() {
    // TODO - make this returned timestamp immutable (after copying from next_sync_time
    return sync_time;
  }
  
  @Override
  public IteratorStatus status(){
    return status;
  }
  
  @Override
  public void next() {
    source.next();
  }
  
  @Override
  public void markStatus(final IteratorStatus status) {
    this.status = IteratorStatus.updateStatus(this.status, status);
  }
  
  @Override
  public void setSyncTime(final TimeStamp timestamp) {
    if (timestamp.compare(TimeStampComparator.LT, next_sync_time)) {
      next_sync_time.update(timestamp);
    }
  }
  
  @Override
  public Deferred<Object> fetchNext() {
    return source.fetchNext();
  }
  
  @Override
  public TimeSeriesProcessor getCopyParent() {
    return copy_parent;
  }
  
  @Override
  public Deferred<Object> close() {
    return source.close();
  }
  
}
