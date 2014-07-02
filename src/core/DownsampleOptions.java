// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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


public final class DownsampleOptions {

  /** {@link DownsampleOptions} instance to not downsample. */
  public static final DownsampleOptions NONE =
      new DownsampleOptions(0, null);

  /**
   * The interval in milli seconds wanted between each data point.
   * zero turns off downsampling.
   */
  private final long interval_ms;
  /** The downsampling function to use. */
  private final Aggregator downsampler;

  /**
   * Constructor.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   */
  public DownsampleOptions(final long interval_ms,
                           final Aggregator downsampler) {
    this.interval_ms = interval_ms;
    this.downsampler = downsampler;
  }

  /** The interval in milli seconds wanted between each data point. */
  public long getIntervalMs() {
    return interval_ms;
  }

  /** @return Specified downsampling function. */
  public Aggregator getDownsampler() {
    return downsampler;
  }

  /** @return True if the downsampilng is enabled. */
  public boolean enabled() {
    return interval_ms > 0;
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("DownsampleOptions(interval_ms=")
        .append(interval_ms)
        .append(", downsampler=")
        .append(downsampler)
        .append(')');
    return buf.toString();
  }
}
