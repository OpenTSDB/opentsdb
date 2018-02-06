// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.NoSuchElementException;

import net.opentsdb.data.types.numeric.Aggregators.Interpolation;

/**
 * A function capable of aggregating multiple {@code DataPoints} together.
 * <p>
 * All aggregators must be stateless.  All they can do is run through a
 * sequence of {@link Longs Longs} or {@link Doubles Doubles} and return an
 * aggregated value.
 */
public abstract class Aggregator {
  
  /** Interpolation method this aggregator uses across time series */
  private final Interpolation interpolation_method;
  
  /** String name of the aggregator */
  private final String name;

  /**
   * Create a new instance of this class.
   * @param interpolationMethod The interpolation method to use.
   * @param name The name of this aggregator.
   */
  protected Aggregator(final Interpolation interpolationMethod, final String name) {
    this.interpolation_method = interpolationMethod;
    this.name = name;
  }
  
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
  public abstract long runLong(Longs values);

  /**
   * Aggregates a sequence of {@code double}s.
   * @param values The sequence to aggregate.
   * @return The aggregated value.
   */
  public abstract double runDouble(Doubles values);

  /** 
   * Returns the interpolation method to use when working with data points
   * across time series.
   * @return The interpolation method to use
   */
  Interpolation interpolationMethod() {
    return interpolation_method;
  }
  
  @Override
  public String toString() {
    return name;
  }
}
