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

import java.util.List;
import java.util.Map;

public interface Histogram {
  
  public byte[] histogram(final boolean include_id);
  
  public void fromHistogram(final byte[] raw, final boolean includes_id);
  
  public double percentile(final double p);
  
  public List<Double> percentiles(List<Double> p);
  
  public Map getHistogram();
  
  public Histogram clone();
  
  public int getId();
  
  void aggregate(Histogram histo, HistogramAggregation func);
  
  void aggregate(List<Histogram> histos, HistogramAggregation func);
}
