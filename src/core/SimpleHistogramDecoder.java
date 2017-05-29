// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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
 * Histogram decoder for Yamas style histograms.
 * </p>
 * <p>
 * It currently checks the first byte of the encoded data and decide the type of
 * the histogram. Then it uses that histogram types builtin factory method to
 * create the instance.
 * </p>
 * <p>
 * This class is thread safe as it has no state.
 * </p>
 */
public class SimpleHistogramDecoder implements HistogramDataPointDecoder {
  @Override
  public HistogramDataPoint decode(final byte[] raw_data, final long timestamp) {
    final Histogram histogram;
    switch (raw_data[0]) {
    case 0x0: // should be 0, refer to core-library/src/test/java/com/yahoo/yamas/metrics/Yamas1HistogramTest.java
    {
      histogram = new SimpleHistogram();
      byte[] hist_raw_data = Arrays.copyOfRange(raw_data, 1, raw_data.length);
      histogram.fromHistogram(hist_raw_data);
    }
    break;
    default:
      throw new IllegalDataException("Unknown header of histogram data, the header is: " + raw_data[0]);
    }

    return new SimpleHistogramDataPointAdapter(histogram, timestamp);
  }
}
