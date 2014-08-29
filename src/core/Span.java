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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.UniqueId;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.SignedBytes;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Represents a read-only sequence of continuous data points.
 * <p>
 * This class stores a continuous sequence of {@link net.opentsdb.storage.hbase.CompactedRow}s in memory.
 */
final class Span implements DataPoints {
  /** All the rows in this span. */
  private final ImmutableSortedSet<DataPoints> rows;
  private final DataPoints first;
  
  /**
   * Default constructor.
   */
  Span(ImmutableSortedSet<DataPoints> dps) {
    checkArgument(!dps.isEmpty(), "dps must not be empty but was");

    final String first_tsuid = dps.first().getTSUIDs().get(0);
    final String last_tsuid = dps.last().getTSUIDs().get(0);

    checkArgument(first_tsuid.equals(last_tsuid),
            "The TSUIDS in the provided DataPoints must match but didn't");

    rows = dps;
    first = dps.first();
  }

  /**
   * @return the id of the metric associated with the rows in this span
   * @throws IllegalStateException if the span was empty
   * @see DataPoints#metric()
   */
  @Override
  public byte[] metric() {
    return first.metric();
  }

  /**
   * @return the list of tag id pairs for the rows in this span
   * @throws IllegalStateException if the span was empty
   * @see DataPoints#tags()
   */
  @Override
  public Map<byte[],byte[]> tags() {
    return first.tags();
  }

  /**
   * @see DataPoints#aggregatedTags()
   */
  @Override
  public List<byte[]> aggregatedTags() {
    return Collections.emptyList();
  }

  /** @return the number of data points in this span, O(n)
   * Unfortunately we must walk the entire array for every row as there may be a 
   * mix of second and millisecond timestamps */
  public int size() {
    int size = 0;
    for (final DataPoints row : rows) {
      size += row.size();
    }
    return size;
  }

  /** @return 0 since aggregation cannot happen at the span level */
  public int aggregatedSize() {
    return 0;
  }

  public List<String> getTSUIDs() {
    if (rows.size() < 1) {
      return null;
    }

    List<String> tsuids = first.getTSUIDs();
    return ImmutableList.of(tsuids.get(0));
  }
  
  /** @return a list of annotations associated with this span. May be empty */
  public List<Annotation> getAnnotations() {
    ImmutableList.Builder<Annotation> annot_builder = ImmutableList.builder();
    for (DataPoints row : rows) {
      annot_builder.addAll(row.getAnnotations());
    }

    return annot_builder.build();
  }

  /** @return an iterator to run over the list of data points */
  public SeekableView iterator() {
    return spanIterator();
  }

  // TODO Needs a lot of tests. See https://github
  // .com/hi3g/opentsdb/blob/77e4c239e91a7752764c6723621292d7cc0944ce/src
  // /core/Span.java#L202-L215 for a reference of what it used to do.
  private DataPoint getDataPointForIndex(final int i) {
    int offset = 0;
    for (final DataPoints row : rows) {
      final int sz = row.size();

      if (offset + sz > i) {
        return Iterables.get(row, i - offset - 1);
      }

      offset += sz;
    }

    throw new IllegalArgumentException("i (" + i + ") is outside the range of " + this);
  }

  /**
   * Returns the timestamp for a data point at index {@code i} if it exists.
   * <b>Note:</b> To get to a timestamp this method must walk the entire byte
   * array, i.e. O(n) so call this sparingly. Use the iterator instead.
   * @param i A 0 based index incremented per the number of data points in the
   * span.
   * @return A Unix epoch timestamp in milliseconds
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   */
  public long timestamp(final int i) {
    return getDataPointForIndex(i).timestamp();
  }

  /**
   * Determines whether or not the value at index {@code i} is an integer
   * @param i A 0 based index incremented per the number of data points in the
   * span.
   * @return True if the value is an integer, false if it's a floating point
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   */
  public boolean isInteger(final int i) {
    return getDataPointForIndex(i).isInteger();
  }

  /**
   * Returns the value at index {@code i}
   * @param i A 0 based index incremented per the number of data points in the
   * span.
   * @return the value as a long
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   * @throws ClassCastException if the value is a float instead. Call 
   * {@link #isInteger} first
   * @throws IllegalDataException if the data is malformed
   */
  public long longValue(final int i) {
    return getDataPointForIndex(i).longValue();
  }

  /**
   * Returns the value at index {@code i}
   * @param i A 0 based index incremented per the number of data points in the
   * span.
   * @return the value as a double
   * @throws IndexOutOfBoundsException if the index would be out of bounds
   * @throws ClassCastException if the value is an integer instead. Call 
   * {@link #isInteger} first
   * @throws IllegalDataException if the data is malformed
   */
  public double doubleValue(final int i) {
    return getDataPointForIndex(i).doubleValue();
  }

  /** Returns a human readable string representation of the object. */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
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
    final byte[] this_tsuid = UniqueId.stringToUid(getTSUIDs().get(0));
    final byte[] other_tsuid = UniqueId.stringToUid(other.getTSUIDs().get(0));

    return SignedBytes.lexicographicalComparator().compare(this_tsuid, other_tsuid);
  }

  /** Iterator for {@link Span}s. */
  final class Iterator implements SeekableView {
    private final PeekingIterator<DataPoint> iterator;

    Iterator() {
      iterator = Iterators.peekingIterator(Iterables.concat(rows).iterator());
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public DataPoint next() {
      return iterator.next();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    public void seek(final long timestamp) {
      while (iterator.hasNext() && iterator.peek().timestamp() < timestamp) {
        iterator.next();
      }
    }

    public String toString() {
      return Objects.toStringHelper(this)
              .add("span", Span.this)
              .add("iterator", iterator)
              .toString();
    }
  }

  /**
   * Package private iterator method to access data while downsampling.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   */
  Downsampler downsampler(final long interval_ms,
                          final Aggregator downsampler) {
    return new Downsampler(spanIterator(), interval_ms, downsampler);
  }
}
