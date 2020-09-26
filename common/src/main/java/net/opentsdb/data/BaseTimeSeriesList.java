// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.data;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * For complex structures that manage multiple time series it's more efficient
 * to implement a {@link List} interface over the underlying structure than to
 * create a new list and populate it with references, particularly when we're 
 * looking at thousands or hundreds of thousands of time series. 
 * <p>
 * This is a base class that throws {@link UnsupportedOperationException}
 * exceptions for all of the modification and other methods that aren't 
 * implemented.
 * 
 * @since 3.0
 */
public abstract class BaseTimeSeriesList implements List<TimeSeries> {

  /**
   * The final size of the list. Must be set by the implementation as this
   * value is used in the {@link #isEmpty()} and {@link #size()} methods.
   */
  protected int size;
  
  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size < 1;
  }

  @Override
  public boolean contains(final Object o) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public <T> T[] toArray(final T[] a) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean add(final TimeSeries e) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean remove(final Object o) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean containsAll(final Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean addAll(final Collection<? extends TimeSeries> c) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean addAll(final int index, 
                        final Collection<? extends TimeSeries> c) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public TimeSeries set(final int index, final TimeSeries element) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public void add(final int index, final TimeSeries element) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public TimeSeries remove(final int index) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public int indexOf(final Object o) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public int lastIndexOf(final Object o) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public ListIterator<TimeSeries> listIterator() {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public ListIterator<TimeSeries> listIterator(final int index) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public List<TimeSeries> subList(final int fromIndex, final int toIndex) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

}
