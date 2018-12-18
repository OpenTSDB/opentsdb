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
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.Aggregators.Interpolation;

/**
 * Implements a difference function, calculates the first difference of a given series
 *
 * @since 2.3
 */
public class FirstDifference implements net.opentsdb.query.expression.Expression {

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
        for (final DataPoints[] results : query_results) {
            num_results += results.length;
        }
        final DataPoints[] results = new DataPoints[num_results];

        int ix = 0;
        // one or more sub queries (m=...&m=...&m=...)
        for (final DataPoints[] sub_query_result : query_results) {
            // group bys (m=sum:foo{host=*})
            for (final DataPoints dps : sub_query_result) {
                results[ix++] = firstDiff(dps);
            }
        }

        return results;

    }

    /**
     * return the first difference of datapoints
     *
     * @param points       The data points to do difference
     * @return The resulting data points
     */
    private DataPoints firstDiff(final DataPoints points) {
        final List<DataPoint> dps = new ArrayList<DataPoint>();
        final SeekableView view = points.iterator();
        List<Double> nums = new ArrayList<Double>();
        List<Long> times = new ArrayList<Long>();
        while (view.hasNext()) {
            DataPoint pt = view.next();
            nums.add(pt.toDouble());
            times.add(pt.timestamp());
        }
        List<Double> diff = new ArrayList<Double>();
        diff.add(0.0);
        for (int j =0;j<nums.size()-1;j++){
            diff.add(nums.get(j+1) - nums.get(j));
        }
        for (int j =0;j<nums.size();j++){
            dps.add(MutableDataPoint.ofDoubleValue(times.get(j), diff.get(j)));
        }
        final DataPoint[] results = new DataPoint[dps.size()];
        dps.toArray(results);
        return new net.opentsdb.query.expression.PostAggregatedDataPoints(points, results);
    }



    @Override
    public String writeStringField(final List<String> query_params,
                                   final String inner_expression) {
        return "firstDiff(" + inner_expression + ")";
    }

}