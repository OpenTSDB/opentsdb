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
package net.opentsdb.query.expression;

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSQuery;

/**
 * Modifies each data point in the series with the absolute value, tossing away
 * the signed component.
 * @since 2.3
 */
public class Absolute implements Expression {

  @Override
  public DataPoints[] evaluate(final TSQuery data_query, 
      final List<DataPoints[]> query_results, final List<String> params) {
    if (data_query == null) {
      throw new IllegalArgumentException("Missing time series query");
    }
    if (query_results == null || query_results.isEmpty()) {
      return new DataPoints[]{};
    }
    
    int num_results = 0;
    for (DataPoints[] results: query_results) {
      num_results += results.length;
    }
    
    final DataPoints[] results = new DataPoints[num_results];
    int ix = 0;
    // one or more sub queries (m=...&m=...&m=...)
    for (final DataPoints[] sub_query_result : query_results) {
      // group bys (m=sum:foo{host=*})
      for (final DataPoints dps : sub_query_result) {
        results[ix++] = abs(dps);
      }
    }
    return results;
  }

  /**
   * Iterate over each data point and store the absolute value
   * @param points The data points to modify
   * @return The resulting data points
   */
  private DataPoints abs(final DataPoints points) {
    // TODO(cl) - Using an array as the size function may not return the exact
    // results and we should figure a way to avoid copying data anyway.
    final List<DataPoint> dps = new ArrayList<DataPoint>();

    final SeekableView view = points.iterator();
    while (view.hasNext()) {
      DataPoint pt = view.next();
      if (pt.isInteger()) {
        dps.add(MutableDataPoint.ofLongValue(
            pt.timestamp(), Math.abs(pt.longValue())));
      } else {
        dps.add(MutableDataPoint.ofDoubleValue(
            pt.timestamp(), Math.abs(pt.doubleValue())));
      }
    }
    final DataPoint[] results = new DataPoint[dps.size()];
    dps.toArray(results);
    return new PostAggregatedDataPoints(points, results);
  }

  @Override
  public String writeStringField(List<String> queryParams, String innerExpression) {
    return "absolute(" + innerExpression + ")";
  }

}
