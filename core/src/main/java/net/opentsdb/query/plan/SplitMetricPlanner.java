// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
