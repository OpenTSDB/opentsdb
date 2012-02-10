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

import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Utility class that provides common, generally useful aggregators.
 */
public final class Aggregators {

  /** Aggregator that sums up all the data points. */
  public static final Aggregator SUM = new Sum();

  /** Aggregator that returns the minimum data point. */
  public static final Aggregator MIN = new Min();

  /** Aggregator that returns the maximum data point. */
  public static final Aggregator MAX = new Max();

  /** Aggregator that returns the average value of the data point. */
  public static final Aggregator AVG = new Avg();

  /** Aggregator that returns the Standard Deviation of the data points. */
  public static final Aggregator STD = new Std();

  /** Maps an aggregator name to its instance. */
  private static final HashMap<String, Aggregator> aggregators;

  static {
    aggregators = new HashMap<String, Aggregator>(4);
    aggregators.put("sum", SUM);
    aggregators.put("min", MIN);
    aggregators.put("max", MAX);
    aggregators.put("avg", AVG);
    aggregators.put("std", STD);
  }

  private Aggregators() {
    // Can't create instances of this utility class.
  }

  /**
   * Returns the set of the names that can be used with {@link #get get}.
   */
  public static Set<String> set() {
    return aggregators.keySet();
  }

  /**
   * Returns the aggregator corresponding to the given name.
   * @param name The name of the aggregator to get.
   * @throws NoSuchElementException if the given name doesn't exist.
   * @see #set
   */
  public static Aggregator get(final String name) {
    final Aggregator agg = aggregators.get(name);
    if (agg != null) {
      return agg;
    }
    throw new NoSuchElementException("No such aggregator: " + name);
  }

  private static final class Sum implements Aggregator {

    public long runLong(final Longs values) {
      long result = values.nextLongValue();
      while (values.hasNextValue()) {
        result += values.nextLongValue();
      }
      return result;
    }

    public double runDouble(final Doubles values) {
      double result = values.nextDoubleValue();
      while (values.hasNextValue()) {
        result += values.nextDoubleValue();
      }
      return result;
    }

    public String toString() {
      return "sum";
    }

  }

  private static final class Min implements Aggregator {

    public long runLong(final Longs values) {
      long min = values.nextLongValue();
      while (values.hasNextValue()) {
        final long val = values.nextLongValue();
        if (val < min) {
          min = val;
        }
      }
      return min;
    }

    public double runDouble(final Doubles values) {
      double min = values.nextDoubleValue();
      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (val < min) {
          min = val;
        }
      }
      return min;
    }

    public String toString() {
      return "min";
    }

  }

  private static final class Max implements Aggregator {

    public long runLong(final Longs values) {
      long max = values.nextLongValue();
      while (values.hasNextValue()) {
        final long val = values.nextLongValue();
        if (val > max) {
          max = val;
        }
      }
      return max;
    }

    public double runDouble(final Doubles values) {
      double max = values.nextDoubleValue();
      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (val > max) {
          max = val;
        }
      }
      return max;
    }

    public String toString() {
      return "max";
    }

  }

  private static final class Avg implements Aggregator {

    public long runLong(final Longs values) {
      long result = values.nextLongValue();
      int n = 1;
      while (values.hasNextValue()) {
        result += values.nextLongValue();
        n++;
      }
      return result / n;
    }

    public double runDouble(final Doubles values) {
      double result = values.nextDoubleValue();
      int n = 1;
      while (values.hasNextValue()) {
        result += values.nextDoubleValue();
        n++;
      }
      return result / n;
    }

    public String toString() {
      return "avg";
    }
  }

  /**
   * A Standard Devation aggregator that can compute without storing all
   * of the data points in memory at the same time. This implementation
   * is based upon a paper by John D. Cook that can be found here:
   * http://www.johndcook.com/standard_deviation.html
   */
  protected static final class Std implements Aggregator {

    @Override
    public long runLong(Longs values) {
      long n = 1;
      double oldMean = 0, newMean = 0, oldVariance = 0, newVariance = 0;

      while (values.hasNextValue()) {
        double x = values.nextLongValue();
        if (n == 1) {
          oldMean = newMean = x;
          oldVariance = 0;
        }
        else {
          newMean = oldMean + (x - oldMean) / n;
          newVariance = oldVariance + (x - oldMean) * (x - newMean);

          // set up for next iteration
          oldMean = newMean;
          oldVariance = newVariance;
        }
        n++;
      }

      if (n > 1) {
        return (long) Math.sqrt(newVariance / (n - 1));
      } else {
        return 0;
      }
    }

    @Override
    public double runDouble(Doubles values) {
      long n = 1;
      double oldMean = 0, newMean = 0, oldVariance = 0, newVariance = 0;

      while (values.hasNextValue()) {
        double x = values.nextDoubleValue();
        if (n == 1) {
          oldMean = newMean = x;
          oldVariance = 0;
        }
        else {
          newMean = oldMean + (x - oldMean) / n;
          newVariance = oldVariance + (x - oldMean) * (x - newMean);

          // set up for next iteration
          oldMean = newMean;
          oldVariance = newVariance;
        }
        n++;
      }

      if (n > 1) {
        return Math.sqrt(newVariance / (n - 1));
      } else {
        return 0;
      }
    }


    public String toString() {
      return "std";
    }
  }

}
