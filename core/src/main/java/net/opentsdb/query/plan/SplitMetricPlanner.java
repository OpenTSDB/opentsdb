// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.plan;

import com.google.common.base.Strings;

import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;

/**
 * Simple planner that splits the query up by metric, placing each metric
 * in a separate child query of the parent. Expressions and outputs are ignored
 * for the child queries.
 * 
 * @since 3.0
 */
public class SplitMetricPlanner extends QueryPlanner<IteratorGroups> {

  /**
   * Default ctor.
   * @param query A non-null time series query.
   */
  public SplitMetricPlanner(final TimeSeriesQuery query) {
    super(query);
    generatePlan();
  }

  @Override
  protected void generatePlan() {
    if (query.getMetrics() == null || query.getMetrics().isEmpty()) {
      throw new IllegalStateException("The query must have 1 or more metrics.");
    }
    planned_query = TimeSeriesQuery.newBuilder(query).build();
    
    for (final Metric metric : query.getMetrics()) {
      final TimeSeriesQuery.Builder builder = TimeSeriesQuery.newBuilder();
      builder.setTime(query.getTime());
      builder.addMetric(metric);
      
      if (!Strings.isNullOrEmpty(metric.getFilter())) {
        boolean matched = false;
        for (final Filter filter : query.getFilters()) {
          if (filter.getId().equals(metric.getFilter())) {
            builder.addFilter(filter);
            matched = true;
            break;
          }
        }
        if (!matched) {
          throw new IllegalStateException("Couldn't find filter: " 
              + metric.getFilter());
        }
      }
      
      planned_query.addSubQuery(builder.build());
    }
  }
  
}
