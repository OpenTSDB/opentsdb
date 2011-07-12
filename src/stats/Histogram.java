// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.stats;

import java.util.Arrays;

/**
 * A histogram to keep track of the approximation of a distribution of values.
 * <p>
 * This is not a general purpose implementation of histogram.  It's
 * specifically designed for "small" values (close to 0) as the primary
 * use case is latency histograms.
 * <p>
 * All values must be positive ({@code >= 0}).
 * <p>
 * The histogram is linear (fixed size buckets) up to a given cutoff
 * point.  Beyond that point, the histogram becomes exponential (each
 * bucket is twice as large as the previous one).  This gives good
 * granularity for lower values while still allowing a rough
 * classification for the "long tail" of larger values.
 * <p>
 * Note that this implementation doesn't allow you to directly control
 * the number of buckets in the histogram.  The number will depend on
 * the arguments given to the constructor.
 * <p>
 * This class is not synchronized.
 */
public final class Histogram {

  /** Interval between each bucket for the linear part of the histogram. */
  private final short interval;

  /** Inclusive value beyond which we switch to exponential buckets. */
  private final int cutoff;

  /**
   * How many linear buckets we have.
   * Technically we don't need to store this value but we do in order to
   * avoid having to re-compute it in the fast path each time we add a
   * new value.
   */
  private final short num_linear_buckets;

  /**
   * The power of 2 used by the first exponential bucket.
   * Technically we don't need to store this value but we do in order to
   * avoid having to re-compute it in the fast path each time we add a
   * new value.
   */
  private final short exp_bucket_shift;

  /** Buckets where we actually store the values. */
  private final int[] buckets;

  /**
   * Constructor.
   * @param max The maximum value of the histogram.  Any value greater
   * than this will be considered to be "infinity".
   * @param interval The interval (size) of each linear bucket.
   * @param cutoff The value beyond which to switch to exponential
   * buckets.  The histogram may actually use this value or a value up
   * to {@code interval} greater.
   * @throws IllegalArgumentException if any of following conditions are
   * not met:
   * <pre>
   *   0 &lt; interval &lt;= max
   *   0 &lt;= cutoff &lt;= max
   * </pre>
   */
  public Histogram(final int max,
                   final short interval, final int cutoff) {
    if (interval > max) {
      throw new IllegalArgumentException("interval > max! interval="
                                         + interval + ", max=" + max);
    } else if (cutoff > max) {
      throw new IllegalArgumentException("cutoff > max! cutoff="
                                         + cutoff + ", max=" + max);
    } else if (interval < 1) {
      throw new IllegalArgumentException("interval < 1! interval=" + interval);
    } else if (cutoff < 0) {
      throw new IllegalArgumentException("cutoff < 0! interval=" + cutoff);
    }
    this.interval = interval;
    // One linear bucket every `interval' up to `cutoff'.
    num_linear_buckets = (short) (cutoff / interval);
    this.cutoff = num_linear_buckets * interval;
    this.exp_bucket_shift = (short) log2rounddown(interval);
    this.buckets = new int[num_linear_buckets
      // Find how many exponential buckets we need, starting from the
      // first power of 2 that's less than or equal to `interval'.
      + log2roundup((max - cutoff) >> exp_bucket_shift)
      // Add an extra overflow bucket at the end.
      + 1];
  }

  /**
   * Computes the logarithm base 2 (rounded up) of an integer.
   * <p>
   * This is essentially equivalent to
   *   {@code Math.ceil(Math.log(n) / Math.log(2))}
   * except it's 3 times faster.
   * @param n A strictly positive integer.
   * @return The logarithm base 2.  As a special case, if the integer
   * given in argument is 0, this function returns 0.  If the integer
   * given in argument is negative, the return value is undefined.
   * @see #log2rounddown
   */
  static final int log2roundup(final int n) {
    int log2 = 0;
    while (n > 1 << log2) {
      log2++;
    }
    return log2;
  }

  /**
   * Computes the logarithm base 2 (rounded down) of an integer.
   * <p>
   * This is essentially equivalent to
   *   {@code Math.floor(Math.log(n) / Math.log(2))}
   * except it's 4.5 times faster.  This function is also almost 70%
   * faster than {@link #log2roundup}.
   * @param n A strictly positive integer.
   * @return The logarithm base 2.  As a special case, if the integer
   * given in argument is 0, this function returns 0.  If the integer
   * given in argument is negative, the return value is undefined.
   * @see #log2roundup
   */
  static final int log2rounddown(int n) {
    int log2 = 0;
    while (n > 1) {
      n >>>= 1;
      log2++;
    }
    return log2;
  }

  /** Returns the number of buckets in this histogram. */
  public int buckets() {
    return buckets.length;
  }

  /**
   * Adds a value to the histogram.
   * <p>
   * This method works in {@code O(1)}.
   * @param value The value to add.
   * @throws IllegalArgumentException if the value given is negative.
   */
  public void add(final int value) {
    if (value < 0) {
      throw new IllegalArgumentException("negative value: " + value);
    }
    buckets[bucketIndexFor(value)]++;
  }

  /**
   * Returns the value of the <i>p</i>th  percentile in this histogram.
   * <p>
   * This method works in {@code O(N)} where {@code N} is the number of
   * {@link #buckets buckets}.
   * @param p A strictly positive integer in the range {@code [1; 100]}
   * @throws IllegalArgumentException if {@code p} is not valid.
   */
  public int percentile(int p) {
    if (p < 1 || p > 100) {
      throw new IllegalArgumentException("invalid percentile: " + p);
    }
    int count = 0;  // Count of values in the histogram.
    for (int i = 0; i < buckets.length; i++) {
      count += buckets[i];
    }
    if (count == 0) {  // Empty histogram.  Need to special-case it, otherwise
      return 0;        // the `if (count <= p)' below will be erroneously true.
    }
    // Find the number of elements at or below which the pth percentile is.
    p = count * p / 100;
    // Now walk the array backwards and decrement the count until it reaches p.
    for (int i = buckets.length - 1; i >= 0; i--) {
      count -= buckets[i];
      if (count <= p) {
        return bucketHighInterval(i);
      }
    }
    return 0;
  }

  /**
   * Prints this histogram in a human readable ASCII format.
   * <p>
   * This is equivalent to calling {@link #printAsciiBucket} on every
   * bucket.
   * @param out The buffer to which to write the output.
   */
  public void printAscii(final StringBuilder out) {
    for (int i = 0; i < buckets.length; i++) {
      printAsciiBucket(out, i);
    }
  }

  /**
   * Prints a bucket of this histogram in a human readable ASCII format.
   * @param out The buffer to which to write the output.
   * @see #printAscii
   */
  final void printAsciiBucket(final StringBuilder out, final int i) {
    out.append('[')
      .append(bucketLowInterval(i))
      .append('-')
      .append(i == buckets.length - 1 ? "Inf" : bucketHighInterval(i))
      .append("): ")
      .append(buckets[i])
      .append('\n');
  }

  /** Helper for unit tests that returns the value in the given bucket. */
  final int valueInBucket(final int index) {
    return buckets[index];
  }

  /** Finds the index of the bucket in which the given value should be. */
  private int bucketIndexFor(final int value) {
    if (value < cutoff) {
      return value / interval;
    }
    int bucket = num_linear_buckets  // Skip all linear buckets.
      // And find which bucket the rest (after `cutoff') should be in.
      // Reminder: the first exponential bucket ends at 2^exp_bucket_shift.
      + log2rounddown((value - cutoff) >> exp_bucket_shift);
    if (bucket >= buckets.length) {
      return buckets.length - 1;
    }
    return bucket;
  }

  /** Returns the low interval (inclusive) of the given bucket. */
  private int bucketLowInterval(final int index) {
    if (index <= num_linear_buckets) {
      return index * interval;
    } else {
      return cutoff + (1 << (index - num_linear_buckets + exp_bucket_shift));
    }
  }

  /** Returns the high interval (exclusive) of the given bucket. */
  private int bucketHighInterval(final int index) {
    if (index == buckets.length - 1) {
      return Integer.MAX_VALUE;
    } else {
      return bucketLowInterval(index + 1);
    }
  }

  public String toString() {
    return "Histogram(interval=" + interval + ", cutoff=" + cutoff
      + ", num_linear_buckets=" + num_linear_buckets
      + ", exp_bucket_shift=" + exp_bucket_shift
      + ", buckets=" + Arrays.toString(buckets) + ')';
  }

}
