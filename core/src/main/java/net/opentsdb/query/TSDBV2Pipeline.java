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
package net.opentsdb.query;

import java.util.Collection;

import com.google.common.base.Strings;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.pojo.Metric;

/**
 * Context pipeline that implements OpenTSDB 2.x's query operations.
 * 
 * TODO - implement the rest
 * 
 * @since 3.0
 */
public class TSDBV2Pipeline extends AbstractQueryPipelineContext {
  
  /**
  * Default ctor.
  * @param tsdb A non-null TSDB to work with.
  * @param query A non-null query to execute.
  * @param context The user's query context.
  * @param sinks A collection of one or more sinks to publish to.
  * @throws IllegalArgumentException if any argument was null.
  */
  public TSDBV2Pipeline(final TSDB tsdb, 
                        final TimeSeriesQuery query, 
                        final QueryContext context,
                        final Collection<QuerySink> sinks) {
    super(tsdb, query, context, sinks);
  }
  
  @Override
  public void initialize() {
    net.opentsdb.query.pojo.TimeSeriesQuery q = 
        (net.opentsdb.query.pojo.TimeSeriesQuery) query;
    // TODO - pick metric executors
    for (Metric metric : q.getMetrics()) {
      // TODO - push down gb and any other operators we can
      final net.opentsdb.query.pojo.TimeSeriesQuery.Builder sub_query = 
          net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
          .setTime(q.getTime())
          .addMetric(metric);
      if (!Strings.isNullOrEmpty(metric.getFilter())) {
        sub_query.addFilter(q.getFilter(metric.getFilter()));
      }
      
      final QuerySourceConfig config = QuerySourceConfig.newBuilder()
          .setId(metric.getId())
          .setQuery(sub_query.build())
          .build();
      
      // TODO - get a proper source. For now just the default.
      final QueryNode node = tsdb.getRegistry()
          .getQueryNodeFactory(null)
          .newNode(this, config);
      addVertex(node);
      addDagEdge(this, node);
    }
    
    // TODO - expressions
    
    initializeGraph();
  }
  
  @Override
  public String id() {
    return "TsdbV2Pipeline";
  }

}