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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.query.context.QueryContext;

/**
 * A list of zero or more {@link TimeSeriesIterator}s all of the same type.
 * <p>
 * The lists returned are immutable. While calls to {@link #addIterator(TimeSeriesIterator)}
 * can be made at any time, it's recommended not to after using one of the 
 * methods to read out the iterators.
 * <p>
 * <b>WARNING:</b> This method is not thread safe. If multiple threads will be
 * adding iterators, make sure they synchronize on the container.
 * 
 * @since 3.0
 */
public class TypedIteratorList implements Iterable<TimeSeriesIterator<?>> {

  /** Used for calls when the list is null. */
  private static final List<TimeSeriesIterator<?>> EMPTY_LIST = 
      new ArrayList<TimeSeriesIterator<?>>(0);
  
  /** The type of iterators this list holds. Set on the first call to 
   * {@link #addIterator(TimeSeriesIterator)}. */
  private TypeToken<?> type;
  
  /** The list of iterators initialized to null. */
  private List<TimeSeriesIterator<?>> iterators;
  
  /**
   * Default ctor.
   */
  public TypedIteratorList() {
    
  }
  
  /**
   * Ctor that initializes the array list with the expected size.
   * @param expected_size The expected count of iterators.
   */
  public TypedIteratorList(final int expected_size) {
    iterators = new ArrayList<TimeSeriesIterator<?>>(expected_size);
  }
  
  /** @return The type of data stored in the list. Null if no data has been stored. */
  public TypeToken<?> type() {
    return type;
  }
  
  /**
   * Adds the iterator to the list. Does not check for duplicates and sets the
   * type of the set to that of the first iterator. If another iterator with a
   * different type is passed, an exception is thrown.
   * @param iterator A non-null iterator to store in the list.
   * @throws IllegalArgumentException if the iterator was null or it's type did
   * not match that of previous iterators in the list.
   */
  public void addIterator(final TimeSeriesIterator<?> iterator) {
    if (iterator == null) {
      throw new IllegalArgumentException("Iterator cannot be null.");
    }
    if (type != null && !(type.equals(iterator.type()))) {
      throw new IllegalArgumentException("Iterator with type [" + iterator.type() 
      + "] must be of the same type: " + type);
    } else if (type == null) {
      type = iterator.type();
    }
    if (iterators == null) {
      iterators = new ArrayList<TimeSeriesIterator<?>>();
    }
    iterators.add(iterator);
  }
  
  /** @return An unmodifiable list of iterators. May be empty. */
  public List<TimeSeriesIterator<?>> iterators() {
    return Collections.unmodifiableList(
        iterators != null ? iterators : EMPTY_LIST);
  }

  @Override
  public Iterator<TimeSeriesIterator<?>> iterator() {
    return iterators == null ? EMPTY_LIST.iterator() : 
      Collections.unmodifiableList(iterators).iterator();
  }

  /**
   * Returns a collection of cloned iterators using the given context.
   * @param context A query context.
   * @return A cloned iterator list.
   */
  public TypedIteratorList getClone(final QueryContext context) {
    final TypedIteratorList clone = new TypedIteratorList(iterators.size());
    for (final TimeSeriesIterator<?> iterator : iterators) {
      clone.addIterator(iterator.getCopy(context));
    }
    return clone;
  }
}
