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

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.descriptive.rank.Percentile.EstimationType;
import org.apache.commons.math3.util.ResizableDoubleArray;

import com.google.common.base.Preconditions;

/**
 * Utility class that provides common, generally useful aggregators.
 */
public final class Aggregators {

  /**
   * Different interpolation methods
   */
  public enum Interpolation {
    LERP,   /* Regular linear interpolation */
    ZIM,    /* Returns 0 when a data point is missing */
    MAX,    /* Returns the <type>.MaxValue when a data point is missing */
    MIN     /* Returns the <type>.MinValue when a data point is missing */
  }
  
  /** Aggregator that sums up all the data points. */
  public static final Aggregator SUM = new Sum(
      Interpolation.LERP, "sum");

  /** Aggregator that returns the minimum data point. */
  public static final Aggregator MIN = new Min(
      Interpolation.LERP, "min");

  /** Aggregator that returns the maximum data point. */
  public static final Aggregator MAX = new Max(
      Interpolation.LERP, "max");

  /** Aggregator that returns the average value of the data point. */
  public static final Aggregator AVG = new Avg(
      Interpolation.LERP, "avg");

  /** Aggregator that returns the Standard Deviation of the data points. */
  public static final Aggregator DEV = new StdDev(
      Interpolation.LERP, "dev");
  
  /** Sums data points but will cause the SpanGroup to return a 0 if timesamps
   * don't line up instead of interpolating. */
  public static final Aggregator ZIMSUM = new Sum(
      Interpolation.ZIM, "zimsum");

  /** Returns the minimum data point, causing SpanGroup to set <type>.MaxValue
   * if timestamps don't line up instead of interpolating. */
  public static final Aggregator MIMMIN = new Min(
      Interpolation.MAX, "mimmin");
  
  /** Returns the maximum data point, causing SpanGroup to set <type>.MinValue
   * if timestamps don't line up instead of interpolating. */
  public static final Aggregator MIMMAX = new Max(
      Interpolation.MIN, "mimmax");
  
  /** Maps an aggregator name to its instance. */
  private static final HashMap<String, Aggregator> aggregators;

  /** Aggregator that returns 99.9th percentile. */
  public static final PercentileAgg p999 = new PercentileAgg(99.9d, "p999");
  /** Aggregator that returns 99th percentile. */
  public static final PercentileAgg p99 = new PercentileAgg(99d, "p99");
  /** Aggregator that returns 95th percentile. */
  public static final PercentileAgg p95 = new PercentileAgg(95d, "p95");
  /** Aggregator that returns 99th percentile. */
  public static final PercentileAgg p90 = new PercentileAgg(90d, "p90");
  /** Aggregator that returns 75th percentile. */
  public static final PercentileAgg p75 = new PercentileAgg(75d, "p75");
  /** Aggregator that returns 50th percentile. */
  public static final PercentileAgg p50 = new PercentileAgg(50d, "p50");

  /** Aggregator that returns estimated 99.9th percentile. */
  public static final PercentileAgg ep999 = new PercentileAgg(99.9d, "ep999", EstimationType.R_3);
  /** Aggregator that returns estimated 99th percentile. */
  public static final PercentileAgg ep99 = new PercentileAgg(99d, "ep99", EstimationType.R_3);
  /** Aggregator that returns estimated 95th percentile. */
  public static final PercentileAgg ep95 = new PercentileAgg(95d, "ep95", EstimationType.R_3);
  /** Aggregator that returns estimated 75th percentile. */
  public static final PercentileAgg ep90 = new PercentileAgg(90d, "ep90", EstimationType.R_3);
  /** Aggregator that returns estimated 50th percentile. */
  public static final PercentileAgg ep75 = new PercentileAgg(75d, "ep75", EstimationType.R_3);
  /** Aggregator that returns estimated 50th percentile. */
  public static final PercentileAgg ep50 = new PercentileAgg(50d, "ep50", EstimationType.R_3);

  static {
    aggregators = new HashMap<String, Aggregator>(8);
    aggregators.put("sum", SUM);
    aggregators.put("min", MIN);
    aggregators.put("max", MAX);
    aggregators.put("avg", AVG);
    aggregators.put("dev", DEV);
    aggregators.put("zimsum", ZIMSUM);
    aggregators.put("mimmin", MIMMIN);
    aggregators.put("mimmax", MIMMAX);

    PercentileAgg[] percentiles = {
       p999, p99, p95, p90, p75, p50, ep999, ep99, ep95, ep90, ep75, ep50
    };
    for (PercentileAgg agg : percentiles) {
        aggregators.put(agg.getName(), agg);
    }
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
    private final Interpolation method;
    private final String name;
    
    public Sum(final Interpolation method, final String name) {
      this.method = method;
      this.name = name;
    }
    
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
      return name;
    }

    public Interpolation interpolationMethod() {
      return method;
    }
    
  }

  private static final class Min implements Aggregator {
    private final Interpolation method;
    private final String name;
    
    public Min(final Interpolation method, final String name) {
      this.method = method;
      this.name = name;
    }
    
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
      return name;
    }

    public Interpolation interpolationMethod() {
      return method;
    }
    
  }

  private static final class Max implements Aggregator {
    private final Interpolation method;
    private final String name;
    
    public Max(final Interpolation method, final String name) {
      this.method = method;
      this.name = name;
    }
    
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
      return name;
    }

    public Interpolation interpolationMethod() {
      return method;
    }
    
  }

  private static final class Avg implements Aggregator {
    private final Interpolation method;
    private final String name;
    
    public Avg(final Interpolation method, final String name) {
      this.method = method;
      this.name = name;
    }
    
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
      return name;
    }
  
    public Interpolation interpolationMethod() {
      return method;
    }
   
  }

  /**
   * Standard Deviation aggregator.
   * Can compute without storing all of the data points in memory at the same
   * time.  This implementation is based upon a
   * <a href="http://www.johndcook.com/standard_deviation.html">paper by John
   * D. Cook</a>, which itself is based upon a method that goes back to a 1962
   * paper by B.  P. Welford and is presented in Donald Knuth's Art of
   * Computer Programming, Vol 2, page 232, 3rd edition
   */
  private static final class StdDev implements Aggregator {
    private final Interpolation method;
    private final String name;
    
    public StdDev(final Interpolation method, final String name) {
      this.method = method;
      this.name = name;
    }
    
    public long runLong(final Longs values) {
      double old_mean = values.nextLongValue();

      if (!values.hasNextValue()) {
        return 0;
      }

      long n = 2;
      double new_mean = 0;
      double variance = 0;
      do {
        final double x = values.nextLongValue();
        new_mean = old_mean + (x - old_mean) / n;
        variance += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      } while (values.hasNextValue());

      return (long) Math.sqrt(variance / (n - 1));
    }

    public double runDouble(final Doubles values) {
      double old_mean = values.nextDoubleValue();

      if (!values.hasNextValue()) {
        return 0;
      }

      long n = 2;
      double new_mean = 0;
      double variance = 0;
      do {
        final double x = values.nextDoubleValue();
        new_mean = old_mean + (x - old_mean) / n;
        variance += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      } while (values.hasNextValue());

      return Math.sqrt(variance / (n - 1));
    }

    public String toString() {
      return name;
    }
    
    public Interpolation interpolationMethod() {
      return method;
    }
    
  }

  /**
   * Percentile aggregator based on apache commons math3 implementation
   */
  private static final class PercentileAgg implements Aggregator {
    private final Double percentile;
    private final String name;
    private final EstimationType estimation;

    PercentileAgg(final Double percentile, final String name) {
        this(percentile, name, null);
    }
    public String getName() {
        return name;
    }
    PercentileAgg(final Double percentile, final String name, final EstimationType est) {
      Preconditions.checkArgument(percentile > 0 && percentile <= 100, "Invalid percentile value");
      this.percentile = percentile;
      this.name = name;
      this.estimation = est;
    }

    public long runLong(final Longs values) {
      final Percentile percentile =
        this.estimation == null
            ? new Percentile(this.percentile)
            : new Percentile(this.percentile).withEstimationType(estimation);
      final ResizableDoubleArray local_values = new ResizableDoubleArray();
      while(values.hasNextValue()) {
        local_values.addElement(values.nextLongValue());
      }
      percentile.setData(local_values.getElements());
      return (long) percentile.evaluate();
    }

    public double runDouble(final Doubles values) {
        final Percentile percentile = new Percentile(this.percentile);
        final ResizableDoubleArray local_values = new ResizableDoubleArray();
        while(values.hasNextValue()) {
          local_values.addElement(values.nextDoubleValue());
        }
        percentile.setData(local_values.getElements());
        return percentile.evaluate();
    }

    public String toString() {
      return name;
    }

    @Override
    public Interpolation interpolationMethod() {
        return Aggregators.Interpolation.LERP;
    }

  }
}
