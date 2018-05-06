// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.descriptive.rank.Percentile.EstimationType;
import org.apache.commons.math3.util.ResizableDoubleArray;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.exceptions.IllegalDataException;

/**
 * Utility class that provides common, generally useful numeric aggregators.
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
  public static final NumericAggregator SUM = new Sum("sum");

  /** TEMP - Just for 2.x parsing. */
  public static final NumericAggregator PFSUM = new Sum("pfsum");
  
  /** Aggregator that returns the minimum data point. */
  public static final NumericAggregator MIN = new Min("min");

  /** Aggregator that returns the maximum data point. */
  public static final NumericAggregator MAX = new Max("max");

  /** Aggregator that returns the average value of the data point. */
  public static final NumericAggregator AVG = new Avg("avg");

  /** Aggregator that returns the emedian of the data points. */
  public static final NumericAggregator MEDIAN = new Median("median");
  
  /** Aggregator that skips aggregation/interpolation and/or downsampling. */
  public static final NumericAggregator NONE = new None("raw");
  
  /** Return the product of two time series 
   * @since 2.3 */
  public static final NumericAggregator MULTIPLY = new Multiply("multiply");
  
  /** Aggregator that returns the Standard Deviation of the data points. */
  public static final NumericAggregator DEV = new StdDev("dev");
  
  /** TEMP - Just for 2.x parsing. */
  public static final NumericAggregator ZIMSUM = new Sum("zimsum");

  /** TEMP - Just for 2.x parsing. */
  public static final NumericAggregator MIMMIN = new Min("mimmin");
  
  /** TEMP - Just for 2.x parsing. */
  public static final NumericAggregator MIMMAX = new Max("mimmax");

  /** Aggregator that returns the number of data points.
   * WARNING: This currently interpolates with zero-if-missing. In this case 
   * counts will be off when counting multiple time series. Only use this when
   * downsampling until we support NaNs.
   * @since 2.2 */
  public static final NumericAggregator COUNT = new Count("count");

  /** Aggregator that returns the first data point. */
  public static final NumericAggregator FIRST = new First("first");

  /** Aggregator that returns the last data point. */
  public static final NumericAggregator LAST = new Last("last");
  
  /** Maps an aggregator name to its instance. */
  private static final HashMap<String, NumericAggregator> aggregators;

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
    aggregators = Maps.newHashMap();
    aggregators.put("sum", SUM);
    aggregators.put("min", MIN);
    aggregators.put("max", MAX);
    aggregators.put("avg", AVG);
    aggregators.put("none", NONE);
    aggregators.put("median", MEDIAN);
    aggregators.put("mult", MULTIPLY);
    aggregators.put("multiply", MULTIPLY);
    aggregators.put("dev", DEV);
    aggregators.put("stdev", DEV);
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
  public static NumericAggregator get(final String name) {
    final NumericAggregator agg = aggregators.get(name);
    if (agg != null) {
      return agg;
    }
    throw new NoSuchElementException("No such aggregator: " + name);
  }

  /**
   * Aggregator that simply sums all of the values in the array.
   * TODO - handle integer overflows.
   */
  private static final class Sum extends BaseNumericAggregator {
    public Sum(final String name) {
      super(name);
    }

    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      // TODO - overflow check
      long sum = 0;
      for (int i = 0; i < limit; i++) {
        sum += values[i];
      }
      dp.resetValue(sum);
    }
    
    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      double sum = 0;
      int nans = 0;
      for (int i = 0; i < limit; i++) {
        if (Double.isNaN(values[i]) && !infectious_nans) {
          nans++;
          continue;
        }
        sum += values[i];
      }
      if (nans == limit || (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(sum);
      }
    }
  }

  /**
   * Finds the smallest value in the array.
   */
  private static final class Min extends BaseNumericAggregator {
    public Min(final String name) {
      super(name);
    }

    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      long min = values[0];
      for (int i = 1; i < limit; i++) {
        if (values[i] < min) {
          min = values[i];
        }
      }
      dp.resetValue(min);
    }
    
    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      double min = values[0];
      int nans = 0;
      for (int i = 1; i < limit; i++) {
        if (Double.isNaN(values[i]) && infectious_nans) {
          nans++;
          continue;
        }
        if (values[i] < min) {
          min = values[i];
        }
      }
      if (nans == limit || (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(min);
      }
    }
    
  }

  /**
   * Finds the largest value in the array.
   */
  private static final class Max extends BaseNumericAggregator {
    public Max(final String name) {
      super(name);
    }
    
    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      long max = values[0];
      for (int i = 1; i < limit; i++) {
        if (values[i] > max) {
          max = values[i];
        }
      }
      dp.resetValue(max);
    }
    
    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      double max = values[0];
      int nans = 0;
      for (int i = 1; i < limit; i++) {
        if (Double.isNaN(values[i]) && infectious_nans) {
          nans++;
          continue;
        }
        if (values[i] > max) {
          max = values[i];
        }
      }
      if (nans == limit || (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(max);
      }
    }
    
  }

  /**
   * Computes the average. For longs, if the result is a whole number, it will
   * return a long, otherwise it will return a double.
   * TODO - handle integer overflows.
   */
  private static final class Avg extends BaseNumericAggregator {
    public Avg(final String name) {
      super(name);
    }

    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      // short circuit
      if (limit == 1) {
        dp.resetValue(values[0]);
        return;
      } else if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      
      // TODO - overflow check
      long sum = 0;
      for (int i = 0; i < limit; i++) {
        sum += values[i];
      }
      double avg = (double) sum / (double) limit;
      if (avg % 1 == 0) {
        dp.resetValue(sum / limit);
      } else {
        dp.resetValue(avg);
      }
    }
    
    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      // short circuit
      if (limit == 1) {
        dp.resetValue(values[0]);
        return;
      } else if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      
      double sum = 0;
      int nans = 0;
      for (int i = 0; i < limit; i++) {
        if (Double.isNaN(values[i]) && !infectious_nans) {
          nans++;
          continue;
        }
        sum += values[i];
      }
      if (nans == limit|| (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(sum / (double) (limit - nans));
      }
    }
  }

  /**
   * Returns the median value of the set. For even set sizes, the upper most
   * value of the median is returned.
   */
  private static final class Median extends BaseNumericAggregator {
    public Median(final String name) {
      super(name);
    }

    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      if (limit == 1) {
        dp.resetValue(values[0]);
        return;
      } else if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      
      // ugg, we can't violate the sorting of the source and we can't
      // sort anyway since the limit may be less than the length with
      // garbage in a previously used array. so we have to copy.
      final long[] copy = limit == values.length ? values : 
          Arrays.copyOf(values, limit);
      Arrays.sort(copy);
      
      dp.resetValue(copy[copy.length / 2]);
    }

    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (limit == 1) {
        dp.resetValue(values[0]);
        return;
      } else if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      
      final double[] copy = limit == values.length ? values : 
        Arrays.copyOf(values, limit);
      Arrays.sort(copy);
      if (Double.isNaN(copy[copy.length - 1]) && infectious_nans) {
        dp.resetValue(Double.NaN);
      } else {
        int end = copy.length - 1;
        while (Double.isNaN(copy[end])) {
          end--;
          if (end < 0) {
            dp.resetValue(Double.NaN);
            return;
          }
        }
        dp.resetValue(copy[(end + 1) / 2]);
      }
    }
    
  }
  
  /**
   * An aggregator that isn't meant for aggregation. Paradoxical!!
   * Really it's used as a flag to indicate that, during sorting and iteration,
   * that the pipeline should not perform any aggregation and should emit 
   * raw time series. Any calls to the {@link #run(double[], int)} or 
   * {@link #run(long[], int)} methods will throw 
   * {@link UnsupportedOperationException}.
   */
  private static final class None extends BaseNumericAggregator {
    public None(final String name) {
      super(name);
    }
    
    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      throw new UnsupportedOperationException("None cannot actually be called.");
    }
    
    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      throw new UnsupportedOperationException("None cannot actually be called.");
    }
    
  }
  
  /**
   * Calculates the product of all values in the array.
   * TODO - handle integer overflows.
   */
  private static final class Multiply extends BaseNumericAggregator {
    public Multiply(final String name) {
      super(name);
    }

    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      
      // TODO - overflow
      long product = 1;
      for (int i = 0; i < limit; i++) {
        product *= values[i];
      }
      dp.resetValue(product);
    }
    
    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      
      double product = 1;
      int nans = 0;
      for (int i = 0; i < limit; i++) {
        if (Double.isNaN(values[i]) && !infectious_nans) {
          nans++;
          continue;
        }
        product *= values[i];
      }
      if (nans == limit|| (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(product);
      }
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
  private static final class StdDev extends BaseNumericAggregator {
    public StdDev(final String name) {
      super(name);
    }

    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      if (limit == 1) {
        dp.resetValue(0L);
        return;
      } else if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      
      double old_mean = values[0];
      long n = 2;
      double new_mean = 0.;
      double M2 = 0.;
      for (int i = 1; i < limit; i++) {
        final double x = values[i];
        new_mean = old_mean + (x - old_mean) / n;
        M2 += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      }

      double stdev = Math.sqrt(M2 / (n - 1));
      if (stdev % 1 == 0) {
        dp.resetValue((long) stdev);
      } else {
        dp.resetValue(stdev);
      }
    }

    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (limit == 1) {
        dp.resetValue(0.0);
        return;
      } else if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      
      int nans = Double.isNaN(values[0]) ? 1 : 0;
      double old_mean = values[0];
      long n = 2;
      double new_mean = 0.;
      double M2 = 0.;
      for (int i = 1; i < limit; i++) {
        if (Double.isNaN(values[i]) && !infectious_nans) {
          nans++;
          continue;
        }
        final double x = values[i];
        new_mean = old_mean + (x - old_mean) / n;
        M2 += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      }
      
      if (nans == limit || (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        dp.resetValue(Math.sqrt(M2 / (n - 1)));
      }
    }
    
  }

  /**
   * Simply returns the {@code limit} value of the {@link #run(double[], int)} 
   * or {@link #run(long[], int)} calls.
   */
  private static final class Count extends BaseNumericAggregator {
    public Count(final String name) {
      super(name);
    }
    
    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      dp.resetValue(limit);
    }
    
    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      dp.resetValue(limit);
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
  private static final class PercentileAgg extends BaseNumericAggregator {
    private final Double percentile;
    private final EstimationType estimation;

    public PercentileAgg(final Double percentile, final String name) {
        this(percentile, name, null);
    }

    public PercentileAgg(final Double percentile, final String name, 
        final EstimationType est) {
      super(name);
      Preconditions.checkArgument(percentile > 0 && percentile <= 100, 
          "Invalid percentile value");
      this.percentile = percentile;
      this.estimation = est;
    }

    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      
      final Percentile percentile =
        this.estimation == null
            ? new Percentile(this.percentile)
            : new Percentile(this.percentile).withEstimationType(estimation);
      final ResizableDoubleArray local_values = new ResizableDoubleArray();
      for (int i = 0; i < limit; i++) {
        local_values.addElement(values[i]);
      }
      percentile.setData(local_values.getElements());
      final double p = percentile.evaluate();
      if (p % 1 == 0) {
        dp.resetValue((long) p);
      } else {
        dp.resetValue(p);
      }
    }

    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      
      final Percentile percentile = new Percentile(this.percentile);
      final ResizableDoubleArray local_values = new ResizableDoubleArray();
      int nans = 0;
      for (int i = 0; i < limit; i++) {
        if (Double.isNaN(values[i]) && !infectious_nans) {
          nans++;
          continue;
        }
        local_values.addElement(values[i]);
      }
      if (nans == limit || (nans > 0 && infectious_nans)) {
        dp.resetValue(Double.NaN);
      } else {
        percentile.setData(local_values.getElements());
        dp.resetValue(percentile.evaluate());
      }
    }

  }
//  public static final class MovingAverage extends Aggregator {
//    private LinkedList<SumPoint> list = new LinkedList<SumPoint>();
//    private final long numPoints;
//    private final boolean isTimeUnit;
//
//    public MovingAverage(final Interpolation method, final String name, long numPoints, boolean isTimeUnit) {
//      super(method, name);
//      this.numPoints = numPoints;
//      this.isTimeUnit = isTimeUnit;
//    }
//
//    public long runLong(final Longs values) {
//      long sum = values.nextLongValue();
//      while (values.hasNextValue()) {
//        sum += values.nextLongValue();
//      }
//
//      if (values instanceof DataPoint) {
//        long ts = ((DataPoint) values).timestamp();
//        list.addFirst(new SumPoint(ts, sum));
//      }
//
//      long result = 0;
//      int count = 0;
//
//      Iterator<SumPoint> iter = list.iterator();
//      SumPoint first = iter.next();
//      boolean conditionMet = false;
//
//      // now sum up the preceeding points
//      while (iter.hasNext()) {
//        SumPoint next = iter.next();
//        result += (Long) next.val;
//        count++;
//        if (!isTimeUnit && count >= numPoints) {
//          conditionMet = true;
//          break;
//        } else if (isTimeUnit && ((first.ts - next.ts) > numPoints)) {
//          conditionMet = true;
//          break;
//        }
//      }
//
//      if (!conditionMet || count == 0) {
//        return 0;
//      }
//
//      return result / count;
//    }
//
//    @Override
//    public double runDouble(Doubles values) {
//      double sum = values.nextDoubleValue();
//      while (values.hasNextValue()) {
//        sum += values.nextDoubleValue();
//      }
//
//      if (values instanceof DataPoint) {
//        long ts = ((DataPoint) values).timestamp();
//        list.addFirst(new SumPoint(ts, sum));
//      }
//
//      double result = 0;
//      int count = 0;
//
//      Iterator<SumPoint> iter = list.iterator();
//      SumPoint first = iter.next();
//      boolean conditionMet = false;
//
//      // now sum up the preceeding points
//      while (iter.hasNext()) {
//        SumPoint next = iter.next();
//        result += (Double) next.val;
//        count++;
//        if (!isTimeUnit && count >= numPoints) {
//          conditionMet = true;
//          break;
//        } else if (isTimeUnit && ((first.ts - next.ts) > numPoints)) {
//          conditionMet = true;
//          break;
//        }
//      }
//
//      if (!conditionMet || count == 0) {
//        return 0;
//      }
//
//      return result / count;
//    }
//  
//    class SumPoint {
//      long ts;
//      Object val;
//
//      public SumPoint(long ts, Object val) {
//        this.ts = ts;
//        this.val = val;
//      }
//    }
//  }
  
  /**
   * Returns the first value in the array.
   */
  private static final class First extends BaseNumericAggregator {
    public First(final String name) {
      super(name);
    }
    
    @Override
    public void run(final long[] values, 
                    final int limit,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      dp.resetValue(values[0]);
    }

    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      dp.resetValue(values[0]);
      if (infectious_nans) {
        for (int i = 0; i < limit; i++) {
          if (Double.isNaN(values[i])) {
            dp.resetValue(Double.NaN);
            return;
          }
        }
      }
    }
    
  }
  
  /**
   * Returns the last value in the array.
   */
  private static final class Last extends BaseNumericAggregator {
    public Last(final String name) {
      super(name);
    }
    
    @Override
    public void run(final long[] values, 
                    final int limit, 
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      dp.resetValue(values[limit - 1]);
    }

    @Override
    public void run(final double[] values, 
                    final int limit, 
                    final boolean infectious_nans,
                    final MutableNumericValue dp) {
      if (limit < 1) {
        throw new IllegalDataException("Limit must be greater than 0");
      }
      dp.resetValue(values[limit - 1]);
      if (infectious_nans) {
        for (int i = 0; i < limit; i++) {
          if (Double.isNaN(values[i])) {
            dp.resetValue(Double.NaN);
            return;
          }
        }
      }
    }
    
  }
}
