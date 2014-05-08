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

import java.util.NoSuchElementException;

import net.opentsdb.core.Aggregators.Interpolation;

/**
 * A function capable of aggregating multiple {@link DataPoints} together.
 * <p>
 * All aggregators must be stateless.  All they can do is run through a
 * sequence of {@link Longs Longs} or {@link Doubles Doubles} and return an
 * aggregated value.
 */
public interface Aggregator {

  /**
   * A sequence of {@code long}s.
   * <p>
   * This interface is semantically equivalent to
   * {@code Iterator<long>}.
   */
  public interface Longs {

    /**
     * Returns {@code true} if this sequence has more values.
     * {@code false} otherwise.
     */
    boolean hasNextValue();

    /**
     * Returns the next {@code long} value in this sequence.
     * @throws NoSuchElementException if calling {@link #hasNextValue} returns
     * {@code false}.
     */
    long nextLongValue();

  }

  /**
   * A sequence of {@code double}s.
   * <p>
   * This interface is semantically equivalent to
   * {@code Iterator<double>}.
   */
  public interface Doubles {

    /**
     * Returns {@code true} if this sequence has more values.
     * {@code false} otherwise.
     */
    boolean hasNextValue();

    /**
     * Returns the next {@code double} value in this sequence.
     * @throws NoSuchElementException if calling {@link #hasNextValue} returns
     * {@code false}.
     */
    double nextDoubleValue();

  }

  /**
   * Aggregates a sequence of {@code long}s.
   * @param values The sequence to aggregate.
   * @return The aggregated value.
   */
  long runLong(Longs values);

  /**
   * Aggregates a sequence of {@code double}s.
   * @param values The sequence to aggregate.
   * @return The aggregated value.
   */
  double runDouble(Doubles values);

  /** 
   * Returns the interpolation method to use when working with data points
   * across time series.
   * @return The interpolation method to use
   */
  Interpolation interpolationMethod();
}
