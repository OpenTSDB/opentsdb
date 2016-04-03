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

import com.google.common.base.Joiner;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSQuery;

/**
 * Returns an alias if provided or the original metric name if not. The alias 
 * may optionally contain a template for tag replacement so that tags are 
 * advanced to the metric name for systems that require it. (e.g. flatten
 * a name for Graphite).
 * @since 2.3
 */
public class Alias implements Expression {

  static Joiner COMMA_JOINER = Joiner.on(',').skipNulls();

  @Override
  public DataPoints[] evaluate(final TSQuery data_query, 
      final List<DataPoints[]> query_results, final List<String> params) {
    if (data_query == null) {
      throw new IllegalArgumentException("Missing time series query");
    }
    if (query_results == null || query_results.isEmpty()) {
      return new DataPoints[]{};
    }
    if (params == null || params.isEmpty()) {
      throw new IllegalArgumentException("Missing the alias");
    }
    final String alias_template = COMMA_JOINER.join(params);
    
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
        // TODO(cl) - Using an array as the size function may not return the exact
        // results and we should figure a way to avoid copying data anyway.
        final List<DataPoint> new_dps_list = new ArrayList<DataPoint>();
        final SeekableView view = dps.iterator();
        while (view.hasNext()) {
          DataPoint pt = view.next();
          if (pt.isInteger()) {
            new_dps_list.add(MutableDataPoint.ofLongValue(
                pt.timestamp(), Math.abs(pt.longValue())));
          } else {
            new_dps_list.add(MutableDataPoint.ofDoubleValue(
                pt.timestamp(), Math.abs(pt.doubleValue())));
          }
        }
        
        final DataPoint[] new_dps = new DataPoint[dps.size()];
        new_dps_list.toArray(new_dps);
        final PostAggregatedDataPoints padps = new PostAggregatedDataPoints(
            dps, new_dps);
        
        padps.setAlias(alias_template);
        results[ix++] = padps;
      }
    }
    return results;
  }

  @Override
  public String writeStringField(final List<String> query_params, 
      final String innerExpression) {
    final StringBuilder buf = new StringBuilder();
    buf.append("alias(")
      .append(innerExpression)
      .append(query_params == null || query_params.isEmpty() 
        ? "" : "," + COMMA_JOINER.join(query_params))
      .append(")");
    return buf.toString();
  }
}