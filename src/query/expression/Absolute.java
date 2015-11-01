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

import java.util.List;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSQuery;

public class Absolute implements Expression {

  @Override
  public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults, List<String> params) {
    if (queryResults == null || queryResults.isEmpty()) {
      throw new NullPointerException("Query results cannot be empty");
    }

    DataPoints[] inputPoints = queryResults.get(0);
    DataPoints[] outputPoints = new DataPoints[inputPoints.length];

    for (int i=0; i<inputPoints.length; i++) {
      outputPoints[i] = abs(inputPoints[i]);
    }

    return outputPoints;
  }

  protected DataPoints abs(DataPoints points) {
    int size = points.size();
    DataPoint[] dps = new DataPoint[size];

    SeekableView view = points.iterator();
    int i=0;
    while (view.hasNext()) {
      DataPoint pt = view.next();
      if (pt.isInteger()) {
        dps[i] = MutableDataPoint.ofDoubleValue(pt.timestamp(), Math.abs(pt.longValue()));
      } else {
        dps[i] = MutableDataPoint.ofDoubleValue(pt.timestamp(), Math.abs(pt.doubleValue()));
      }
      i++;
    }

    return new PostAggregatedDataPoints(points, dps);
  }

  @Override
  public String writeStringField(List<String> queryParams, String innerExpression) {
    return "absolute(" + innerExpression + ")";
  }

}
