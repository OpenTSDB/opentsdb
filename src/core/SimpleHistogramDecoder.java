// This file is part of OpenTSDB.
// Copyright (C) 2016-2017  The OpenTSDB Authors.
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

import java.util.Arrays;

/**
 * <p>
 * Histogram decoder for Simple style histograms.
 * </p>
 * <p>
 * It currently checks the first byte of the encoded data and decide the type of
 * the histogram. Then it uses that histogram types builtin factory method to
 * create the instance.
 * </p>
 * <p>
 * This class is thread safe as it has no state.
 * </p>
 * @since 2.4
 */
public class SimpleHistogramDecoder extends HistogramDataPointCodec {
  @Override
  public Histogram decode(final byte[] raw_data,
                                   final boolean includes_type) {
    if (raw_data == null) {
      throw new IllegalArgumentException("The data array cannot be null.");
    }
    if (includes_type && raw_data.length < 1) {
      throw new IllegalArgumentException("The data array cannot be empty.");
    }
    if (includes_type && (int) raw_data[0] != id) {
      throw new IllegalArgumentException("Data ID " + (int) raw_data[0] 
          + " did not match the codec ID " + id);
    }
    final Histogram histogram = new SimpleHistogram(id);
    histogram.fromHistogram(raw_data, includes_type);
    return histogram;
  }
  
  @Override
  public byte[] encode(final Histogram data_point,
                       final boolean include_id) {
    if (!(data_point instanceof SimpleHistogram)) {
      throw new IllegalArgumentException("The given histogram is not a "
          + "SimpleHistogram: " + data_point.getClass());
    }
    return data_point.histogram(include_id);
  }
}
