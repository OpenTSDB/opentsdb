// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.expression;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.FillPolicy;
import net.opentsdb.core.SeekableView;
import net.opentsdb.utils.ByteSet;

/**
 * Holds the results of a sub query (a single metric) and iterates over each
 * resultant series in lock-step for expression evaluation.
 * @since 2.3
 */
public class TimeSyncedIterator implements ITimeSyncedIterator {
  
  /** The name of this sub query given by the user */
  private final String id;
  
  /** The set of tag keys issued with the query */
  private final ByteSet query_tagks;

  /** The data point interfaces fetched from storage */
  private final DataPoints[] dps;
  
  /** The current value used for iterating */
  private final DataPoint[] current_values;
  
  /** References to the MutableDataObjects the ExpressionIterator will read */
  private final ExpressionDataPoint[] emitter_values;
  
  /** A list of the iterators used for fetching the next value */
  private final SeekableView[] iterators;

  /** Set by the ExpressionIterator when it computes the intersection */ 
  private int index;
  
  /** A policy to use for emitting values when a timestamp is missing data */
  private NumericFillPolicy fill_policy;
  
  /**
   * Instantiates an iterator based on the results of a TSSubQuery. 
   * This will setup the emitters so it's safe to call {@link #values()}
   * @param id The name of the query.
   * @param query_tagks The set of tags used in filters on the query.
   * @param dps The data points fetched from storage.
   * @throws IllegalArgumentException if one of the parameters is null or the ID
   * is empty
   */
  public TimeSyncedIterator(final String id, final ByteSet query_tagks, 
      final DataPoints[] dps) {
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("Missing ID string");
    }
    if (dps == null) {
      // it's ok for these to be empty, but they canna be null ya ken?
      throw new IllegalArgumentException("Missing data points");
    }
    this.id = id;
    this.query_tagks = query_tagks;
    this.dps = dps;
    // TODO - load from a default or something
    fill_policy = new NumericFillPolicy(FillPolicy.ZERO);
    current_values = new DataPoint[dps.length];
    emitter_values = new ExpressionDataPoint[dps.length];
    iterators = new SeekableView[dps.length];
    setupEmitters();
  }
  
  /**
   * A copy constructor that loads from an existing iterator.
   * @param iterator The iterator to load from
   */
  private TimeSyncedIterator(final TimeSyncedIterator iterator) {
    id = iterator.id;
    query_tagks = iterator.query_tagks; // sharing is ok here
    dps = iterator.dps; // TODO ?? OK?
    fill_policy = iterator.fill_policy;
    current_values = new DataPoint[dps.length];
    emitter_values = new ExpressionDataPoint[dps.length];
    iterators = new SeekableView[dps.length];
    setupEmitters();
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("TimeSyncedIterator(id=")
       .append(id)
       .append(", index=")
       .append(index)
       .append(", dpsSize=")
       .append(dps.length)
       .append(")");
    return buf.toString();
  }
  
  @Override
  public int size() {
    return dps.length;
  }
  
  @Override
  public boolean hasNext() {
    for (final DataPoint dp : current_values) {
      if (dp != null) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public ExpressionDataPoint[] next(final long timestamp) {
    for (int i = 0; i < current_values.length; i++) {
      if (current_values[i] == null) {
        emitter_values[i].reset(timestamp, fill_policy.getValue());
        continue;
      }
      
      if (current_values[i].timestamp() > timestamp) {
        emitter_values[i].reset(timestamp, fill_policy.getValue());
      } else {
        emitter_values[i].reset(current_values[i]);
        if (!iterators[i].hasNext()) {
          current_values[i] = null;
        } else {
          current_values[i] = iterators[i].next();
        }
      }
    }
    return emitter_values;
  }
  
  @Override
  public long nextTimestamp() {
    long ts = Long.MAX_VALUE;
    for (final DataPoint dp : current_values) {
      if (dp != null) {
        long t = dp.timestamp();
        if (t < ts) {
          ts = t;
        }
      }
    }
    return ts;
  }
  
  @Override
  public void next(final int i) {
    if (current_values[i] == null) {
      throw new RuntimeException("No more elements");
    }
    emitter_values[i].reset(current_values[i]);
    if (iterators[i].hasNext()) {
      current_values[i] = iterators[i].next();
    } else {
      current_values[i] = null;
    }
  }
  
  @Override
  public boolean hasNext(final int i) {
    return current_values[i] != null;
  }
  
  @Override
  public int getIndex() {
    return index;
  }
  
  @Override
  public void setIndex(final int index) {
    this.index = index;
  }

  @Override
  public String getId() {
    return id;
  }

  /** @return the set of data points */
  public DataPoints[] getDataPoints() {
    return dps;
  }
  
  @Override
  public void nullIterator(final int index) {
    if (index < 0 || index > current_values.length) {
      throw new IllegalArgumentException("Index out of range: " + index);
    }
    current_values[index] = null;
  }
  
  @Override
  public ExpressionDataPoint[] values() {
    return emitter_values;
  }
  
  @Override
  public ByteSet getQueryTagKs() {
    return query_tagks;
  }

  @Override
  public void setFillPolicy(final NumericFillPolicy policy) {
    fill_policy = policy;
  }

  @Override
  public NumericFillPolicy getFillPolicy() {
    return fill_policy;
  }

  @Override
  public ITimeSyncedIterator getCopy() {
    return new TimeSyncedIterator(this);
  }
  
  /**
   * Iterates over the values and sets up the current and emitter values
   */
  private void setupEmitters() {
    // set the iterators
    for (int i = 0; i < dps.length; i++) {
      iterators[i] = dps[i].iterator();
      if (!iterators[i].hasNext()) {
        current_values[i] = null;
        emitter_values[i] = null;
      } else {
        current_values[i] = iterators[i].next();
        emitter_values[i] = new ExpressionDataPoint(dps[i]);
        emitter_values[i].setIndex(i);
      }
    }
  }
}
