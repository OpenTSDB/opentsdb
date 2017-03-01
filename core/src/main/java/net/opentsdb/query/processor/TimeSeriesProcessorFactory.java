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
// see <http://www.gnu.org/licenses/>.package net.opentsdb.data;
package net.opentsdb.query.processor;

import java.lang.reflect.Constructor;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.iterators.TimeSeriesIterator;

/**
 * A registry and factory used to instantiate processors given a unique token.
 * The factory is also responsible for registering and instantiating 
 * {@link net.opentsdb.data.TimeSeriesDataType} specific sub-iterators.
 * 
 * @param <T> A {@link TimeSeriesProcessor} object.
 * @since 3.0
 */
public interface TimeSeriesProcessorFactory<T> {
  
  /**
   * The case-insensitive token name of the processor. E.g. "downsampler".
   * @return A name for the processor.
   */
  public String name();
  
  /**
   * The {@link TimeSeriesProcessor} type of the processor this factory represents.
   * @return A non-null type token.
   */
  public TypeToken<?> type();
  
  /**
   * Instantiates a new processor of the current type.
   * @param source A non-null source. (Use TODO for instantiating an iterator grouping)
   * @param config An optional configuration for the processor. May be null.
   * @return An instantiated but uninitialized processor.
   */
  public T newProcessor(final TimeSeriesProcessor source, 
      final TimeSeriesProcessorConfig<T> config);
  
  /**
   * Instantiates a sub-iterator that processes the source and applies functions
   * on the data coming out at each iteration. This method should only be called
   * from within the processor.
   * <b>Note:</b> The factory should return a pass-through iterator for types
   * that are not supported.
   * @param data_type A non-null data type.
   * @param source A non-null iterator source to run through the processor.
   * @param config An optional configuration for the processor and sub processor. 
   * May be null. 
   * @return An instantiated and uninitialized iterator for the processor.
   */
  public TimeSeriesIterator<?> newSubIterator(final TypeToken<?> data_type, 
      final TimeSeriesIterator<?> source, final TimeSeriesProcessorConfig<T> config);
  
  /**
   * Registers (optionally replacing) a sub iterator for the processor type.
   * @param data_type A non-null data type to associate with the iterator.
   * @param ctor A non-null constructor for the iterator.
   * TODO - doc ctor.
   */
  public void registerSubIteratorType(TypeToken<?> data_type, Constructor<?> ctor);
  
  /**
   * Returns a config builder for the given processor type if supported.
   * @return A config builder or null if the processor does not require configs.
   */
  public TimeSeriesProcessorConfigBuilder<T> newConfig();
}
