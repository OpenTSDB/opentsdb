// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
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
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.descriptive.rank.Percentile.EstimationType;
import org.apache.commons.math3.util.ResizableDoubleArray;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
    MIN,     /* Returns the <type>.MinValue when a data point is missing */
    PREV    /* Returns the previous value stored, when a data point is missing */
  }
  
  /** Aggregator that sums up all the data points. */
  public static final Aggregator SUM = new Sum(
      Interpolation.LERP, "sum");

  /**
   * Aggregator that sums up all the data points,and uses interpolation where
   * previous value is used for data point missing.
   */
  public static final Aggregator PFSUM= new Sum(
      Interpolation.PREV, "pfsum");
  
  /** Aggregator that returns the minimum data point. */
  public static final Aggregator MIN = new Min(
      Interpolation.LERP, "min");

  /** Aggregator that returns the maximum data point. */
  public static final Aggregator MAX = new Max(
      Interpolation.LERP, "max");

  /** Aggregator that returns the average value of the data point. */
  public static final Aggregator AVG = new Avg(
      Interpolation.LERP, "avg");

  /** Aggregator that returns the emedian of the data points. */
  public static final Aggregator MEDIAN = new Median(Interpolation.LERP, 
      "median");
  
  /** Aggregator that skips aggregation/interpolation and/or downsampling. */
  public static final Aggregator NONE = new None(Interpolation.ZIM, "raw");
  
  /** Return the product of two time series 
   * @since 2.3 */
  public static final Aggregator MULTIPLY = new Multiply(
      Interpolation.LERP, "multiply");
  
  /** Aggregator that returns the Standard Deviation of the data points. */
  public static final Aggregator DEV = new StdDev(
      Interpolation.LERP, "dev");
  
  /** Sums data points but will cause the SpanGroup to return a 0 if timestamps
   * don't line up instead of interpolating. */
  public static final Aggregator ZIMSUM = new Sum(
      Interpolation.ZIM, "zimsum");

  /** Returns the minimum data point, causing SpanGroup to set &lt;type&gt;.MaxValue
   * if timestamps don't line up instead of interpolating. */
  public static final Aggregator MIMMIN = new Min(
      Interpolation.MAX, "mimmin");
  
  /** Returns the maximum data point, causing SpanGroup to set &lt;type&gt;.MinValue
   * if timestamps don't line up instead of interpolating. */
  public static final Aggregator MIMMAX = new Max(
      Interpolation.MIN, "mimmax");

  /** Aggregator that returns the number of data points.
   * WARNING: This currently interpolates with zero-if-missing. In this case 
   * counts will be off when counting multiple time series. Only use this when
   * downsampling until we support NaNs.
   * @since 2.2 */
  public static final Aggregator COUNT = new Count(Interpolation.ZIM, "count");

  /** Aggregator that returns the first data point. */
  public static final Aggregator FIRST = new First(Interpolation.ZIM, "first");

  /** Aggregator that returns the last data point. */
  public static final Aggregator LAST = new Last(Interpolation.ZIM, "last");
  
  /** Maps an aggregator name to its instance. */
  private static final HashMap<String, Aggregator> aggregators;

  /** Aggregator that returns 99.9th percentile. */
  public static final PercentileAgg p999 = new PercentileAgg(99.9d, "p999");
  /** Aggregator that returns 99th percentile. */
  public static final PercentileAgg p99 = new PercentileAgg(99d, "p99");
  /** Aggregator that returns 95th percentile. */
  public static final PercentileAgg p95 = new PercentileAgg(95d, "p95");
  /** Aggregator that returns 90th percentile. */
  public static final PercentileAgg p90 = new PercentileAgg(90d, "p90");
  /** Aggregator that returns 75th percentile. */
  public static final PercentileAgg p75 = new PercentileAgg(75d, "p75");
  /** Aggregator that returns 50th percentile. */
  public static final PercentileAgg p50 = new PercentileAgg(50d, "p50");

  /** Aggregator that returns estimated 99.9th percentile. */
  public static final PercentileAgg ep999r3 = 
      new PercentileAgg(99.9d, "ep999r3", EstimationType.R_3);
  /** Aggregator that returns estimated 99th percentile. */
  public static final PercentileAgg ep99r3 = 
      new PercentileAgg(99d, "ep99r3", EstimationType.R_3);
  /** Aggregator that returns estimated 95th percentile. */
  public static final PercentileAgg ep95r3 = 
      new PercentileAgg(95d, "ep95r3", EstimationType.R_3);
  /** Aggregator that returns estimated 90th percentile. */
  public static final PercentileAgg ep90r3 = 
      new PercentileAgg(90d, "ep90r3", EstimationType.R_3);
  /** Aggregator that returns estimated 75th percentile. */
  public static final PercentileAgg ep75r3 = 
      new PercentileAgg(75d, "ep75r3", EstimationType.R_3);
  /** Aggregator that returns estimated 50th percentile. */
  public static final PercentileAgg ep50r3 = 
      new PercentileAgg(50d, "ep50r3", EstimationType.R_3);

  /** Aggregator that returns estimated 99.9th percentile. */
  public static final PercentileAgg ep999r7 = 
      new PercentileAgg(99.9d, "ep999r7", EstimationType.R_7);
  /** Aggregator that returns estimated 99th percentile. */
  public static final PercentileAgg ep99r7 = 
      new PercentileAgg(99d, "ep99r7", EstimationType.R_7);
  /** Aggregator that returns estimated 95th percentile. */
  public static final PercentileAgg ep95r7 = 
      new PercentileAgg(95d, "ep95r7", EstimationType.R_7);
  /** Aggregator that returns estimated 90th percentile. */
  public static final PercentileAgg ep90r7 = 
      new PercentileAgg(90d, "ep90r7", EstimationType.R_7);
  /** Aggregator that returns estimated 75th percentile. */
  public static final PercentileAgg ep75r7 = 
      new PercentileAgg(75d, "ep75r7", EstimationType.R_7);
  /** Aggregator that returns estimated 50th percentile. */
  public static final PercentileAgg ep50r7 = 
      new PercentileAgg(50d, "ep50r7", EstimationType.R_7);

  static {
    aggregators = new HashMap<String, Aggregator>(8);
    aggregators.put("sum", SUM);
    aggregators.put("min", MIN);
    aggregators.put("max", MAX);
    aggregators.put("avg", AVG);
    aggregators.put("none", NONE);
    aggregators.put("median", MEDIAN);
    aggregators.put("mult", MULTIPLY);
    aggregators.put("dev", DEV);
    aggregators.put("count", COUNT);
    aggregators.put("zimsum", ZIMSUM);
    aggregators.put("mimmin", MIMMIN);
    aggregators.put("mimmax", MIMMAX);
    aggregators.put("first", FIRST);
    aggregators.put("last", LAST);
    aggregators.put("pfsum", PFSUM);

    PercentileAgg[] percentiles = {
       p999, p99, p95, p90, p75, p50, 
       ep999r3, ep99r3, ep95r3, ep90r3, ep75r3, ep50r3,
       ep999r7, ep99r7, ep95r7, ep90r7, ep75r7, ep50r7
    };
    for (PercentileAgg agg : percentiles) {
        aggregators.put(agg.toString(), agg);
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


  private static final class Sum extends Aggregator {
    public Sum(final Interpolation method, final String name) {
      super(method, name);
    }

    @Override
    public long runLong(final Longs values) {
      long result = values.nextLongValue();
      while (values.hasNextValue()) {
        result += values.nextLongValue();
      }
      return result;
    }

    @Override
    public double runDouble(final Doubles values) {
      double result = 0.;
      long n = 0L;

      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (!Double.isNaN(val)) {
          result += val;
          ++n;
        }
      }

      return (0L == n) ? Double.NaN : result;
    }
    
  }

  private static final class Min extends Aggregator {
    public Min(final Interpolation method, final String name) {
      super(method, name);
    }

    @Override
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

    @Override
    public double runDouble(final Doubles values) {
      final double initial = values.nextDoubleValue();
      double min = Double.isNaN(initial) ? Double.POSITIVE_INFINITY : initial;

      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (!Double.isNaN(val) && val < min) {
          min = val;
        }
      }

      return (Double.POSITIVE_INFINITY == min) ? Double.NaN : min;
    }
    
  }

  private static final class Max extends Aggregator {
    public Max(final Interpolation method, final String name) {
      super(method, name);
    }

    @Override
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

    @Override
    public double runDouble(final Doubles values) {
      final double initial = values.nextDoubleValue();
      double max = Double.isNaN(initial) ? Double.NEGATIVE_INFINITY : initial;

      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (!Double.isNaN(val) && val > max) {
          max = val;
        }
      }

      return (Double.NEGATIVE_INFINITY == max) ? Double.NaN : max;
    }
    
  }

  private static final class Avg extends Aggregator {
    public Avg(final Interpolation method, final String name) {
      super(method, name);
    }

    @Override
    public long runLong(final Longs values) {
      long result = values.nextLongValue();
      int n = 1;
      while (values.hasNextValue()) {
        result += values.nextLongValue();
        n++;
      }
      return result / n;
    }

    @Override
    public double runDouble(final Doubles values) {
      double result = 0.;
      int n = 0;
      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (!Double.isNaN(val)) {
          result += val;
          n++;
        }
      }
      return (0 == n) ? Double.NaN : result / n;
    }
   
  }

  private static final class Median extends Aggregator {
    public Median(final Interpolation method, final String name) {
      super(method, name);
    }

    @Override
    public long runLong(final Longs values) {
      final List<Long> collection = Lists.newArrayList();
      while (values.hasNextValue()) {
        collection.add(values.nextLongValue());
      }
      if (collection.isEmpty()) {
        throw new IllegalStateException("Shouldn't be here without any data");
      }
      Collections.sort(collection);
      return collection.get(collection.size() / 2);
    }

    @Override
    public double runDouble(final Doubles values) {
      final List<Double> collection = Lists.newArrayList();
      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (!Double.isNaN(val)) {
          collection.add(val);
        }
      }
      if (collection.isEmpty()) {
        // in this case we may have had lots of NaNs so just drop em.
        return Double.NaN;
      }
      Collections.sort(collection);
      return collection.get(collection.size() / 2);
    }
  }
  
  /**
   * An aggregator that isn't meant for aggregation. Paradoxical!!
   * Really it's used as a flag to indicate that, during sorting and iteration,
   * that the pipeline should not perform any aggregation and should emit 
   * raw time series.
   */
  private static final class None extends Aggregator {
    public None(final Interpolation method, final String name) {
      super(method, name);
    }
    
    @Override
    public long runLong(final Longs values) {
      final long v = values.nextLongValue();
      if (values.hasNextValue()) {
        throw new IllegalDataException("More than one value in aggregator " + values);
      }
      return v;
    }
    
    @Override
    public double runDouble(final Doubles values) {
      final double v = values.nextDoubleValue();
      if (values.hasNextValue()) {
        throw new IllegalDataException("More than one value in aggregator " + values);
      }
      return v;
    }
  }
  
  private static final class Multiply extends Aggregator {
    
    public Multiply(final Interpolation method, final String name) {
      super(method, name);
    }

    @Override
    public long runLong(Longs values) {
      long result = values.nextLongValue();
      while (values.hasNextValue()) {
        result *= values.nextLongValue();
      }
      return result;
    }

    @Override
    public double runDouble(Doubles values) {
      double result = values.nextDoubleValue();
      while (values.hasNextValue()) {
        result *= values.nextDoubleValue();
      }
      return result;
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
  private static final class StdDev extends Aggregator {
    public StdDev(final Interpolation method, final String name) {
      super(method, name);
    }

    @Override
    public long runLong(final Longs values) {
      double old_mean = values.nextLongValue();

      if (!values.hasNextValue()) {
        return 0;
      }

      long n = 2;
      double new_mean = 0.;
      double M2 = 0.;
      do {
        final double x = values.nextLongValue();
        new_mean = old_mean + (x - old_mean) / n;
        M2 += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      } while (values.hasNextValue());

      return (long) Math.sqrt(M2 / (n - 1));
    }

    @Override
    public double runDouble(final Doubles values) {
      // Try to get at least one non-NaN value.
      double old_mean = values.nextDoubleValue();
      while (Double.isNaN(old_mean) && values.hasNextValue()) {
        old_mean = values.nextDoubleValue();
      }

      if (Double.isNaN(old_mean)) {
        // Couldn't find any non-NaN values.
        // The stddev of NaNs is NaN.
        return Double.NaN;
      }
      if (!values.hasNextValue()) {
        // Only found one non-NaN value.
        // The stddev of one value is zero.
        return 0.;
      }

      // If we got here, then we have one non-NaN value, and there are more
      // values to aggregate; however, some or all of these values may be NaNs.

      long n = 2;
      double new_mean = 0.;

      // This is not strictly the second central moment (i.e., variance), but
      // rather a multiple of it.
      double M2 = 0.;
      do {
        final double x = values.nextDoubleValue();
        if (!Double.isNaN(x)) {
          new_mean = old_mean + (x - old_mean) / n;
          M2 += (x - old_mean) * (x - new_mean);
          old_mean = new_mean;
          n++;
        }
      } while (values.hasNextValue());

      // If n is still 2, then we never found another non-NaN value; therefore,
      // we should return zero.
      //
      // Otherwise, we calculate the actual variance, and then we find its
      // positive square root, which is the standard deviation.
      return (2 == n) ? 0. : Math.sqrt(M2 / (n - 1));
    }

  }

  private static final class Count extends Aggregator {
    public Count(final Interpolation method, final String name) {
      super(method, name);
    }
    
    @Override
    public long runLong(Longs values) {
      long result = 0;
      while (values.hasNextValue()) {
        values.nextLongValue();
        result++;
      }
      return result;
    }

    @Override
    public double runDouble(Doubles values) {
      double result = 0;
      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (!Double.isNaN(val)) {
          result++;
        }
      }
      return result;
    }

  }

  /**
   * Percentile aggregator based on apache commons math3 implementation
   * The default calculation is:
   * index=(N+1)p 
   * estimate=x⌈h−1/2⌉
   * minLimit=0
   * maxLimit=1
   */
  private static final class PercentileAgg extends Aggregator {
    private final Double percentile;
    private final EstimationType estimation;

    public PercentileAgg(final Double percentile, final String name) {
        this(percentile, name, null);
    }

    public PercentileAgg(final Double percentile, final String name, 
        final EstimationType est) {
      super(Aggregators.Interpolation.LERP, name);
      Preconditions.checkArgument(percentile > 0 && percentile <= 100, 
          "Invalid percentile value");
      this.percentile = percentile;
      this.estimation = est;
    }

    @Override
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

    @Override
    public double runDouble(final Doubles values) {
      final Percentile percentile = new Percentile(this.percentile);
      final ResizableDoubleArray local_values = new ResizableDoubleArray();
      int n = 0;
      while(values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (!Double.isNaN(val)) {
          local_values.addElement(val);
          n++;
        }
      }
      if (n > 0) {
        percentile.setData(local_values.getElements());
        return percentile.evaluate();
      } else {
        return Double.NaN;
      }
    }

  }
  public static final class MovingAverage extends Aggregator {
    private LinkedList<SumPoint> list = new LinkedList<SumPoint>();
    private final long numPoints;
    private final boolean isTimeUnit;

    public MovingAverage(final Interpolation method, final String name, long numPoints, boolean isTimeUnit) {
      super(method, name);
      this.numPoints = numPoints;
      this.isTimeUnit = isTimeUnit;
    }

    public long runLong(final Longs values) {
      long sum = values.nextLongValue();
      while (values.hasNextValue()) {
        sum += values.nextLongValue();
      }

      if (values instanceof DataPoint) {
        long ts = ((DataPoint) values).timestamp();
        list.addFirst(new SumPoint(ts, sum));
      }

      long result = 0;
      int count = 0;

      Iterator<SumPoint> iter = list.iterator();
      SumPoint first = iter.next();
      boolean conditionMet = false;

      // now sum up the preceeding points
      while (iter.hasNext()) {
        SumPoint next = iter.next();
        result += (Long) next.val;
        count++;
        if (!isTimeUnit && count >= numPoints) {
          conditionMet = true;
          break;
        } else if (isTimeUnit && ((first.ts - next.ts) > numPoints)) {
          conditionMet = true;
          break;
        }
      }

      if (!conditionMet || count == 0) {
        return 0;
      }

      return result / count;
    }

    @Override
    public double runDouble(Doubles values) {
      double sum = values.nextDoubleValue();
      while (values.hasNextValue()) {
        sum += values.nextDoubleValue();
      }

      if (values instanceof DataPoint) {
        long ts = ((DataPoint) values).timestamp();
        list.addFirst(new SumPoint(ts, sum));
      }

      double result = 0;
      int count = 0;

      Iterator<SumPoint> iter = list.iterator();
      SumPoint first = iter.next();
      boolean conditionMet = false;

      // now sum up the preceeding points
      while (iter.hasNext()) {
        SumPoint next = iter.next();
        result += (Double) next.val;
        count++;
        if (!isTimeUnit && count >= numPoints) {
          conditionMet = true;
          break;
        } else if (isTimeUnit && ((first.ts - next.ts) > numPoints)) {
          conditionMet = true;
          break;
        }
      }

      if (!conditionMet || count == 0) {
        return 0;
      }

      return result / count;
    }
  
    class SumPoint {
      long ts;
      Object val;

      public SumPoint(long ts, Object val) {
        this.ts = ts;
        this.val = val;
      }
    }
  }
  
  private static final class First extends Aggregator {
    public First(final Interpolation method, final String name) {
      super(method, name);
    }
    
    public long runLong(final Longs values) {
      long val = values.nextLongValue();
      while (values.hasNextValue()) {
    	  values.nextLongValue();
      }
      return val;
    }

    public double runDouble(final Doubles values) {
      double val = values.nextDoubleValue();
      while (values.hasNextValue()) {
    	  values.nextDoubleValue();
      }
      return val;
    }
  }
  
  private static final class Last extends Aggregator {
    public Last(final Interpolation method, final String name) {
      super(method, name);
    }
    
    public long runLong(final Longs values) {
      long val = values.nextLongValue();
      while (values.hasNextValue()) {
        val = values.nextLongValue();
      }
      return val;
    }

    public double runDouble(final Doubles values) {
      double val = values.nextDoubleValue();
      while (values.hasNextValue()) {
        val = values.nextDoubleValue();
      }
      return val;
    }
  }
}
