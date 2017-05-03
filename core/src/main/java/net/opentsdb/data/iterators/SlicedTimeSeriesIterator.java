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

import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.query.context.QueryContext;

/**
 * A logical view on top of one or more time series iterators. For example, if
 * a query for 24 hours is split into 1 hour chunks with individual cache
 * entries for each hour, the hours can be re-merged into a logical view using
 * this class. 
 * <p>
 * Adding data is done via {@link #addIterator(TimeSeriesIterator)}. The first
 * iterator added sets the ID of the iterator and the starting timestamp. The
 * following rules apply to iterators:
 * <ol>
 * <li>The {@link #startTime()} of the inserting iterator must be equal to or
 * greater than the {@link #endTime()} of the previous iterator.</li>
 * <li>The {@link #order()} of the inserting iterator must be greater than
 * the order of the previous iterator.</li>
 * <li>The first iterator's order must be 0 or greater.</li>
 * </ol>
 *
 * @param <T> The type of data stored by the iterators.
 */
public class SlicedTimeSeriesIterator<T extends TimeSeriesDataType> 
  extends TimeSeriesIterator<T> {

  /** The list of iterators to combine into a logical view. */
  private List<TimeSeriesIterator<T>> iterators;
  
  /** The current iterator index. */
  private int iterator_index;
  
  /** The last timestamp, used for asserting order when adding iterators. */
  private TimeStamp last_timestamp;
  
  /**
   * Default ctor. Sets up an array and instantiates a timestamp.
   */
  public SlicedTimeSeriesIterator() {
    iterators = Lists.newArrayListWithExpectedSize(1);
    iterator_index = 0;
    // TODO - use something better.
    last_timestamp = new MillisecondTimeStamp(0L);
  }
  
  /**
   * Adds an iterator to the set in order. Must have the same ID as the previous
   * iterators and a greater or equivalent start timestamp to the end timestamp
   * of the previous iterator. Also must have a greater order than the previous.
   * <b>Note:</b> The first iterator inserted sets the ID for the set.
   * @param iterator A non-null iterator to add.
   */
  public void addIterator(final TimeSeriesIterator<T> iterator) {
    if (iterator == null) {
      throw new IllegalArgumentException("Iterator cannot be null.");
    }
    if (iterator.order() < 0) {
      throw new IllegalArgumentException("Iterator must have an order "
          + "set starting at 0.");
    }
    if (iterators.isEmpty()) {
      iterators.add(iterator);
      id = iterator.id;
      return;
    }
    
    // validate
    final TimeSeriesIterator<T> previous = iterators.get(iterators.size() - 1);
    if (iterator.order() <= previous.order()) {
      throw new IllegalArgumentException("Iterator must have a later order "
          + "than the previous iterator. Previous order; " + previous.order());
    }
    if (iterator.startTime().compare(TimeStampComparator.LT, previous.endTime())) {
      throw new IllegalArgumentException("Iterator must start at or after the "
          + "previous iterator's end time. Previous end: " + previous.endTime());
    }
    iterators.add(iterator);
    
    if (context != null) {
      context.register(this, iterator);
    }
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return iterators.isEmpty() ? null : iterators.get(0).type();
  }

  @Override
  public TimeStamp startTime() {
    return iterators.isEmpty() ? null : iterators.get(0).startTime();
  }

  @Override
  public TimeStamp endTime() {
    return iterators.isEmpty() ? null : 
      iterators.get(iterators.size() - 1).endTime();
  }

  @Override
  public IteratorStatus status() {
    if (iterator_index >= iterators.size()) {
      return IteratorStatus.END_OF_DATA;
    }
    return advance(false);
  }

  @Override
  public TimeSeriesValue<T> next() {
    if (iterator_index >= iterators.size()) {
      throw new NoSuchElementException("No more data in shard");
    }

    TimeSeriesIterator<T> iterator = iterators.get(iterator_index);
    IteratorStatus status = iterators.get(iterator_index).status();
    
    if (status == IteratorStatus.END_OF_DATA) {
      status = advance(true);
    } 
    if (status != IteratorStatus.HAS_DATA) {
      throw new IllegalStateException("Iterator status was: " + status);
    }
    
    TimeSeriesValue<T> value = iterator.next();
    while (last_timestamp != null && 
        last_timestamp.compare(TimeStampComparator.EQ, value.timestamp())) {
      // need to advance as it the new chunk had the same start time as the 
      // end time of our previous chunk (inclusive ranges).
      if (iterator.status() == IteratorStatus.HAS_DATA) {
        value = iterator.next();
      } else {
        status = advance(true);
        if (status == IteratorStatus.HAS_DATA) {
          iterator = iterators.get(iterator_index);
          value = iterator.next();
        } else {
          return null;
        }
      }
    }
    last_timestamp.update(value.timestamp());
    
    if (context != null && 
        iterators.get(iterator_index).status() == IteratorStatus.END_OF_DATA) {
      advance(false);
    }
    return value;
  }

  @Override
  public TimeSeriesValue<T> peek() {
    if (iterator_index >= iterators.size()) {
      return null;
    }
    return iterators.get(iterator_index).peek();
  }
  
  @Override
  public Deferred<Object> fetchNext() {
    if (iterator_index >= iterators.size()) {
      throw new NoSuchElementException("No more data in set");
    }
    return iterators.get(iterator_index).fetchNext();
  }
  
  @Override
  public void setContext(final QueryContext context) {
    if (this.context != null && this.context != context) {
      this.context.unregister(this);
    }
    this.context = context;
    if (context != null) {
      context.register(this);
    }
    for (final TimeSeriesIterator<T> iterator : iterators) {
      iterator.setContext(context);
      context.register(this, iterator);
    }
  }
  
  @Override
  public Deferred<Object> initialize() {
    if (iterators.isEmpty()) {
      return Deferred.fromResult(null);
    }
    
    // init in reverse order.
    class ErrCB implements Callback<Deferred<Object>, Exception> {
      @Override
      public Deferred<Object> call(final Exception e) throws Exception {
        return Deferred.fromError(e);
      }
    }
    
    class InitPrevious implements Callback<Deferred<Object>, Object> {
      final int previous;
      InitPrevious(final int previous) {
        this.previous = previous;
      }
      @Override
      public Deferred<Object> call(final Object ignored) throws Exception {
        if (previous - 1 >= 0) {
          return iterators.get(previous - 1).initialize()
              .addCallbackDeferring(new InitPrevious(previous - 1))
              .addErrback(new ErrCB());
        }
        // roll forward in case we have empty iterators
        while (iterator_index < iterators.size()) {
          final TimeSeriesIterator<T> iterator = iterators.get(iterator_index);
          if (iterator.status() == IteratorStatus.END_OF_DATA) {
            iterator_index++;
          } else {
            iterator.updateContext();
            break;
          }
        }
        return Deferred.fromResult(null);
      }
    }
    
    return iterators.get(iterators.size() - 1).initialize()
      .addCallbackDeferring(new InitPrevious(iterators.size() - 1))
      .addErrback(new ErrCB());
  }
  
  @Override
  public TimeSeriesIterator<T> getCopy(final QueryContext context) {
    final SlicedTimeSeriesIterator<T> clone = new SlicedTimeSeriesIterator<T>();
    for (final TimeSeriesIterator<T> iterator : iterators) {
      clone.addIterator(iterator.getCopy(context));
    }
    return clone;
  }
  
  @Override
  protected void updateContext() {
    if (iterator_index >= iterators.size()) {
      return;
    }
    iterators.get(iterator_index).updateContext();
  }
  
  /**
   * Helper to roll forward to the next good slice and update the
   * context if configured.
   * @param throw_exception Whether or not to throw an exception when the end
   * of data is reached, i.e. from {@link #next()}.
   * @return The current iterator status.
   */
  private IteratorStatus advance(final boolean throw_exception) {
    IteratorStatus status = iterators.get(iterator_index).status();
    if (status == IteratorStatus.EXCEPTION) {
      if (context != null) {
        context.updateContext(IteratorStatus.EXCEPTION, null);
      }
      return status;
    }
    while (status == IteratorStatus.END_OF_DATA) {
      ++iterator_index;
      if (iterator_index >= iterators.size()) {
        if (throw_exception) {
          throw new NoSuchElementException("No more data in shard");
        } else {
          if (context != null) {
            context.updateContext(IteratorStatus.END_OF_DATA, null);
          }
          return IteratorStatus.END_OF_DATA;
        }
      }
      
      // need to make sure that if the next iterator's start time matches
      // our last time that we advance by consuming the first value. Otherwise
      // when we call updateContext() below it can leave result in an infinite
      // loop.
      final TimeSeriesIterator<T> iterator = iterators.get(iterator_index);
      if (iterator.status() == IteratorStatus.EXCEPTION) {
        if (context != null) {
          context.updateContext(IteratorStatus.EXCEPTION, null);
        }
        return status;
      }
      final TimeSeriesValue<T> peek = iterator.peek();
      if (last_timestamp != null && peek != null && 
          last_timestamp.compare(TimeStampComparator.EQ, peek.timestamp())) {
        // consume a value
        iterator.next();
        status = iterator.status();
      } else {
        status = iterator.status();
      }
    }
    if (context != null) {
      // wont' advance but we need to set the next timestamp in the context.
      iterators.get(iterator_index).updateContext();
    }
    return status;
  }
  
}
