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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.utils.Deferreds;

/**
 * A simple iterator that stores and iterates over a set 
 * {@link TimeSeriesIterator}s synchronized on time.
 * <p>
 * This is most often the source for a processor pipeline.
 * <p>
 * Note that because {@link #fetchNext()} can be executed asynchronously,
 * we have to sync the calls to {@link #setSyncTime(TimeStamp)} and 
 * {@link #markStatus(IteratorStatus)} to avoid thread issues. The rest of the
 * calls shouldn't require synchronization as the iterator should only be
 * used within a single thread.
 *  
 * @since 3.0
 */
public class IteratorGroup implements TimeSeriesProcessor, 
  Callback<Deferred<Object>, Exception> {

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
  
  /** The parent if this was a copy. */
  protected TimeSeriesProcessor parent;
  
  /**
   * Default Ctor that simply initializes the map.
   */
  public IteratorGroup() {
    iterators = Maps.newHashMap();
  }
  
  @Override
  public Deferred<Object> initialize() {
    // nothing to do here as the iterators should have been initialized.
    return Deferred.fromResult(null);
  }

  @Override
  public Map<TimeSeriesGroupId, Map<TypeToken<?>, 
    List<TimeSeriesIterator<?>>>> iterators() {
    return Collections.unmodifiableMap(iterators);
  }

  @Override
  public void addSeries(final TimeSeriesGroupId group, 
      final TimeSeriesIterator<?> series) {
    if (group == null) {
      throw new IllegalArgumentException("Group ID cannot be null.");
    }
    if (series == null) {
      throw new IllegalArgumentException("Series cannot be null.");
    }
    Map<TypeToken<?>, List<TimeSeriesIterator<?>>> type_map = iterators.get(group);
    if (type_map == null) {
      type_map = Maps.newHashMap();
      iterators.put(group, type_map);
    }
    List<TimeSeriesIterator<?>> its = type_map.get(series.type());
    if (its == null) {
      its = Lists.newArrayList();
      type_map.put(series.type(), its);
    }
    if (its.contains(series)) {
      throw new IllegalStateException("The series " + series + " was already added.");
    }
    its.add(series);
    
//    if (series.nextTimestamp().compare(TimeStampComparator.LT, sync_time)) {
//      sync_time.update(series.nextTimestamp());
//      next_sync_time.update(sync_time);
//    }
//    status = IteratorStatus.updateStatus(status, series.status());
//    series.setProcessor(this);
  }

  @Override
  public TimeStamp syncTimestamp() {
    return sync_time;
  }

  @Override
  public IteratorStatus status() {
    return status;
  }

  @Override
  public void next() {
    try {
      sync_time.update(next_sync_time);
      next_sync_time.setMax();
      status = IteratorStatus.END_OF_DATA;
      for (final Map<TypeToken<?>, List<TimeSeriesIterator<?>>> type : 
          iterators.values()) {
//        for (final List<TimeSeriesIterator<?>> its : type.values()) {
//          for (final TimeSeriesIterator<?> it : its) {
//            if (it.status() == IteratorStatus.HAS_DATA || 
//                it.status() == IteratorStatus.END_OF_CHUNK) {
//              it.advance();
//            } else if (it.status() == IteratorStatus.EXCEPTION) {
//              status = IteratorStatus.EXCEPTION;
//              break;
//            }
//          }
//        }
      }
    } catch (RuntimeException e) {
      synchronized (this) {
        status = IteratorStatus.EXCEPTION;
      }
      throw e;
    }
  }

  @Override
  public synchronized void markStatus(final IteratorStatus status) {
    if (status == null) {
      throw new IllegalArgumentException("Status cannot be null.");
    }
    this.status = IteratorStatus.updateStatus(this.status, status);
  }

  @Override
  public synchronized void setSyncTime(final TimeStamp timestamp) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (timestamp.compare(TimeStampComparator.LT, next_sync_time)) {
      next_sync_time.update(timestamp);
    }
  }

  @Override
  public Deferred<Object> fetchNext() {
    next_sync_time.setMax();
    status = IteratorStatus.END_OF_DATA;
    final List<Deferred<Object>> deferreds = Lists.newArrayList();
    for (final Map<TypeToken<?>, List<TimeSeriesIterator<?>>> type : iterators.values()) {
      for (final List<TimeSeriesIterator<?>> its : type.values()) {
        for (final TimeSeriesIterator<?> it : its) {
          deferreds.add(it.fetchNext());
        }
      }
    }
    return Deferred.group(deferreds)
        .addCallback(Deferreds.NULL_GROUP_CB)
        .addErrback(this);
  }

  @Override
  public TimeSeriesProcessor getCopy() {
    final IteratorGroup new_iterator = new IteratorGroup();
    for (final Entry<TimeSeriesGroupId, Map<TypeToken<?>, 
        List<TimeSeriesIterator<?>>>> entry : iterators.entrySet()) {
      for (final List<TimeSeriesIterator<?>> its : entry.getValue().values()) {
        for (final TimeSeriesIterator<?> it : its) {
          new_iterator.addSeries(entry.getKey(), it.getCopy(null));
        }
      }
    }
    new_iterator.parent = this;
    return new_iterator;
  }

  @Override
  public TimeSeriesProcessor getCopyParent() {
    return parent;
  }

  @Override
  public Deferred<Object> close() {
    final List<Deferred<Object>> deferreds = Lists.newArrayList();
    for (final Map<TypeToken<?>, List<TimeSeriesIterator<?>>> type : iterators.values()) {
      for (final List<TimeSeriesIterator<?>> its : type.values()) {
        for (final TimeSeriesIterator<?> it : its) {
          deferreds.add(it.close());
        }
      }
    }
    return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
  }

  
  @Override
  public Deferred<Object> call(final Exception ex) throws Exception {
    status = IteratorStatus.EXCEPTION;
    return Deferred.fromError(ex);
  }

}
