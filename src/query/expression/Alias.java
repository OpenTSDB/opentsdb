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
import java.util.Map;
import com.google.common.base.Joiner;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.TSQuery;

/**
 * Returns an alias if provided or the original metric name if not.
 * @since 2.3
 */
public class Alias implements Expression {

    static Joiner COMMA_JOINER = Joiner.on(',').skipNulls();

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults,
                                 List<String> queryParams) {
        if (queryResults == null || queryResults.size() == 0) {
            throw new NullPointerException("No query results");
        }

        String aliasTemplate = "__default";

        if (queryParams != null && queryParams.size() >= 0) {
            aliasTemplate = COMMA_JOINER.join(queryParams);
        }

        DataPoints[] inputPoints = queryResults.get(0);

        DataPoint[][] dps = new DataPoint[inputPoints.length][];

        for (int j = 0; j < dps.length; j++) {
            DataPoints base = inputPoints[j];
            dps[j] = new DataPoint[base.size()];
            int i = 0;

            for (DataPoint pt : base) {
                if (pt.isInteger()) {
                    dps[j][i] = MutableDataPoint.ofDoubleValue(pt.timestamp(), pt.longValue());
                } else {
                    dps[j][i] = MutableDataPoint.ofDoubleValue(pt.timestamp(), pt.doubleValue());
                }
                i++;
            }
        }

        DataPoints[] resultArray = new DataPoints[queryResults.get(0).length];
        for (int i = 0; i < resultArray.length; i++) {
            PostAggregatedDataPoints result = new PostAggregatedDataPoints(inputPoints[i],
                    dps[i]);

            String alias = aliasTemplate;
            for (Map.Entry<String, String> e : inputPoints[i].getTags().entrySet()) {
                alias = alias.replace("@" + e.getKey(), e.getValue());
            }

            result.setAlias(alias);
            resultArray[i] = result;
        }

        return resultArray;
    }

    @Override
    public String writeStringField(List<String> queryParams, String innerExpression) {
        if (queryParams == null || queryParams.size() == 0) {
            return "NULL";
        }

        return queryParams.get(0);
    }
}