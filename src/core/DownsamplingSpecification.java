// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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

import com.google.common.base.MoreObjects;
import net.opentsdb.utils.DateTime;

/**
 * Representation of a downsampling specification in a TSDB query.
 * @since 2.2
 */
public final class DownsamplingSpecification {
  /** Instance of a specification indicating no downsampling requested. */
  public static final DownsamplingSpecification NO_DOWNSAMPLER =
    new DownsamplingSpecification();

  /** Special value representing no downsampling interval given. */
  public static final long NO_INTERVAL = 0L;

  /** Special value representing no downsampling function given. */
  public static final Aggregator NO_FUNCTION = null;

  /** The default fill policy. */
  public static final FillPolicy DEFAULT_FILL_POLICY = FillPolicy.NONE;

  // Parsed downsample interval.
  private final long interval;

  // Parsed downsampler function.
  private final Aggregator function;
  
  // Parsed fill policy: whether to interpolate or to fill.
  private final FillPolicy fill_policy;

  /**
   * A specification indicating no downsampling is requested.
   */
  private DownsamplingSpecification() {
    interval = NO_INTERVAL;
    function = NO_FUNCTION;
    fill_policy = DEFAULT_FILL_POLICY;
  }

  /**
   * Non-stringified, piecewise c-tor.
   * @param interval The downsampling interval, in milliseconds.
   * @param function The downsampling function.
   * @param fill_policy The policy specifying how to deal with missing data.
   * @throws IllegalArgumentException if any argument is invalid.
   */
  public DownsamplingSpecification(final long interval,
      final Aggregator function, final FillPolicy fill_policy) {
    if (null == function) {
      throw new IllegalArgumentException("downsampling function cannot be null");
    }
    if (interval <= 0L) {
      throw new IllegalArgumentException("interval not > 0: " + interval);
    }
    if (null == fill_policy) {
      throw new IllegalArgumentException("fill policy cannot be null");
    }
    if (function == Aggregators.NONE) {
      throw new IllegalArgumentException("cannot use the NONE "
          + "aggregator for downsampling");
    }

    this.interval = interval;
    this.function = function;
    this.fill_policy = fill_policy;
  }

  /**
   * C-tor for string representations.
   * The argument to this c-tor should have the following format:
   * {@code interval-function[-fill_policy]}.
   * @param specification String representation of a downsample specifier.
   * @throws IllegalArgumentException if the specification is null or invalid.
   */
  public DownsamplingSpecification(final String specification) {
    if (null == specification) {
      throw new IllegalArgumentException("Downsampling specifier cannot be " +
        "null");
    }

    final String[] parts = specification.split("-");
    if (parts.length < 2) {
      // Too few items.
      throw new IllegalArgumentException("Invalid downsampling specifier '" +
        specification + "': must provide at least interval and function");
    } else if (parts.length > 3) {
      // Too many items.
      throw new IllegalArgumentException("Invalid downsampling specifier '" +
        specification + "': must consist of interval, function, and optional " +
        "fill policy");
    }

    // This porridge is just right.

    // INTERVAL.
    // This will throw if interval is invalid.
    interval = DateTime.parseDuration(parts[0]);

    // FUNCTION.
    try {
      function = Aggregators.get(parts[1]);
    } catch (final NoSuchElementException e) {
      throw new IllegalArgumentException("No such downsampling function: " +
        parts[1]);
    }
    if (function == Aggregators.NONE) {
      throw new IllegalArgumentException("cannot use the NONE "
          + "aggregator for downsampling");
    }

    // FILL POLICY.
    if (3 == parts.length) {
      // If the user gave us three parts, then the third must be a fill
      // policy.
      fill_policy = FillPolicy.fromString(parts[2]);
      if (null == fill_policy) {
        final StringBuilder oss = new StringBuilder();
        oss.append("No such fill policy: '").append(parts[2])
           .append("': must be one of:");
        for (final FillPolicy policy : FillPolicy.values()) {
          oss.append(" ").append(policy.getName());
        }

        throw new IllegalArgumentException(oss.toString());
      }
    } else {
      // Default to linear interpolation.
      fill_policy = FillPolicy.NONE;
    }
  }

  /**
   * Get the downsampling interval, in milliseconds.
   * @return the downsampling interval, in milliseconds.
   */
  public long getInterval() {
    return interval;
  }

  /**
   * Get the downsampling function.
   * @return the downsampling function.
   */
  public Aggregator getFunction() {
    return function;
  }

  /**
   * Get the policy specifying how to deal with missing data.
   * @return the policy specifying how to deal with missing data.
   */
  public FillPolicy getFillPolicy() {
    return fill_policy;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("interval", getInterval())
      .add("function", getFunction())
      .add("fillPolicy", getFillPolicy())
      .toString();
  }
}

