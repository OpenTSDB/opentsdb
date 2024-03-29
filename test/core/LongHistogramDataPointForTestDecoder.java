// This file is part of OpenTSDB.
// Copyright (C) 2016-2023  The OpenTSDB Authors.
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

import org.junit.Ignore;

@Ignore
public class LongHistogramDataPointForTestDecoder extends HistogramDataPointCodec {

  @Override
  public Histogram decode(byte[] raw_data, final boolean includes_id) {
    final LongHistogramDataPointForTest dp = new LongHistogramDataPointForTest(id);
    dp.fromHistogram(raw_data, includes_id);
    return dp;
  }

  
  @Override
  public byte[] encode(Histogram data_point, final boolean include_id) {
    // TODO Auto-generated method stub
    return null;
  }

}
