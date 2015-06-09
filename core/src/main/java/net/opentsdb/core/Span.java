// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

package net.opentsdb.core;

import static com.google.common.base.Preconditions.checkArgument;

import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.IdUtils;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.SignedBytes;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

/**
 * Represents a read-only sequence of continuous data points for a single TSUID.
 */
class Span implements DataPoints {
  /** All the {@link DataPoints} in this span. */
  private final ImmutableSortedSet<DataPoints> rows;
  private final DataPoints first;

  /**
   * Default constructor. The provided {@code dps} must not be empty
   *
   * @param dps The {@link DataPoints} this Span should encapsulate, it must not be empty.
   */
  Span(SortedSet<DataPoints> dps) {
    checkArgument(!dps.isEmpty(), "dps must not be empty but was");

    final String first_tsuid = dps.first().getTSUIDs().get(0);
    final String last_tsuid = dps.last().getTSUIDs().get(0);

    checkArgument(first_tsuid.equals(last_tsuid),
        "The TSUIDS in the provided DataPoints must match but didn't");

    rows = ImmutableSortedSet.copyOf(dps);
    first = dps.first();
  }

  /**
   * @return the id of the metric associated with the rows in this span
   * @see DataPoints#metric()
   */
  @Override
  public byte[] metric() {
    return first.metric();
  }

  /**
   * @return the list of tag id pairs for the rows in this span
   * @see DataPoints#tags()
   */
  @Override
  public Map<byte[], byte[]> tags() {
    return first.tags();
  }

  /**
   * @see DataPoints#aggregatedTags()
   */
  @Override
  public List<byte[]> aggregatedTags() {
    return Collections.emptyList();
  }

  /**
   * @return the number of data points in this span, O(n) Unfortunately we must walk the entire
   * array for every row as there may be a mix of second and millisecond timestamps
   */
  @Override
  public int size() {
    int size = 0;
    for (final DataPoints row : rows) {
      size += row.size();
    }
    return size;
  }

  /**
   * @return 0 since aggregation cannot happen at the span level
   */
  @Override
  public int aggregatedSize() {
    return 0;
  }

  /**
   * Returns all TSUIDS associated with this {@link DataPoints} implementation.
   */
  @Override
  public List<String> getTSUIDs() {
    List<String> tsuids = first.getTSUIDs();
    return ImmutableList.of(tsuids.get(0));
  }

  /**
   * @return a list of annotations associated with this span. May be empty
   */
  @Override
  public List<Annotation> getAnnotations() {
    ImmutableList.Builder<Annotation> annot_builder = ImmutableList.builder();
    for (DataPoints row : rows) {
      annot_builder.addAll(row.getAnnotations());
    }

    return annot_builder.build();
  }

  /**
   * @return an iterator to run over the list of data points
   */
  @Override
  public SeekableView iterator() {
    return spanIterator();
  }

  /**
   * Get the {@link net.opentsdb.core.DataPoint} at index {@code i}.
   */
  DataPoint dataPointForIndex(final int i) {
    int offset = 0;
    for (final DataPoints row : rows) {
      final int sz = row.size();

      if (offset + sz > i) {
        return Iterables.get(row, i - offset);
      }

      offset += sz;
    }

    throw new IllegalArgumentException("i (" + i + ") is outside the range of " + this);
  }

  /**
   * Returns the timestamp for a data point at index {@code i} if it exists. <b>Note:</b> To get to
   * a timestamp this method must walk the entire byte array, i.e. O(n) so call this sparingly. Use
   * the iterator instead.
   *
   * @param i A 0 based index incremented per the number of data points in the span.
   * @return A Unix epoch timestamp in milliseconds
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   */
  @Override
  public long timestamp(final int i) {
    return dataPointForIndex(i).timestamp();
  }

  /**
   * Determines whether or not the value at index {@code i} is an integer
   *
   * @param i A 0 based index incremented per the number of data points in the span.
   * @return True if the value is an integer, false if it's a floating point
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   */
  @Override
  public boolean isInteger(final int i) {
    return dataPointForIndex(i).isInteger();
  }

  /**
   * Returns the value at index {@code i}
   *
   * @param i A 0 based index incremented per the number of data points in the span.
   * @return the value as a long
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   * @throws ClassCastException if the value is a float instead. Call {@link #isInteger} first
   * @throws IllegalDataException if the data is malformed
   */
  @Override
  public long longValue(final int i) {
    return dataPointForIndex(i).longValue();
  }

  /**
   * Returns the value at index {@code i}
   *
   * @param i A 0 based index incremented per the number of data points in the span.
   * @return the value as a double
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   * @throws ClassCastException if the value is an integer instead. Call {@link #isInteger} first
   * @throws IllegalDataException if the data is malformed
   */
  @Override
  public double doubleValue(final int i) {
    return dataPointForIndex(i).doubleValue();
  }

  /** Returns a human readable string representation of the object. */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("size", rows.size())
        .add("rows", Joiner.on(',').join(rows))
        .toString();
  }

  /** Package private iterator method to access it as a Span.Iterator. */
  Span.Iterator spanIterator() {
    return new Span.Iterator();
  }

  @Override
  public int compareTo(final DataPoints other) {
    final byte[] this_tsuid = IdUtils.stringToUid(getTSUIDs().get(0));
    final byte[] other_tsuid = IdUtils.stringToUid(other.getTSUIDs().get(0));

    return SignedBytes.lexicographicalComparator().compare(this_tsuid, other_tsuid);
  }

  /**
   * Package private iterator method to access data while downsampling.
   *
   * @param interval_ms The interval in milli seconds wanted between each data point.
   * @param downsampler The downsampling function to use.
   */
  Downsampler downsampler(final long interval_ms,
                          final Aggregator downsampler) {
    return new Downsampler(spanIterator(), interval_ms, downsampler);
  }

  /** Iterator for {@link Span}s. */
  final class Iterator implements SeekableView {
    private final PeekingIterator<DataPoint> iterator;

    Iterator() {
      iterator = Iterators.peekingIterator(Iterables.concat(rows).iterator());
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public DataPoint next() {
      return iterator.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seek(final long timestamp) {
      while (iterator.hasNext() && iterator.peek().timestamp() < timestamp) {
        iterator.next();
      }
    }

    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("span", Span.this)
          .add("iterator", iterator)
          .toString();
    }
  }
}
