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

import java.time.ZoneId;
import java.util.Collection;

import com.google.common.base.Strings;

import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.processor.groupby.GroupByFactory;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.query.processor.groupby.GroupByConfig;

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
  public TSDBV2Pipeline(final DefaultTSDB tsdb, 
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
      QueryNode node = ((QueryNodeFactory) tsdb.getRegistry()
          .getDefaultPlugin(TimeSeriesDataStore.class))
          .newNode(this, config);
      addVertex(node);

      final Downsampler downsampler = metric.getDownsampler() != null ? 
          metric.getDownsampler() : q.getTime().getDownsampler();
      // downsample
      if (downsampler != null) {
        DownsampleConfig.Builder ds = DownsampleConfig.newBuilder()
            .setId("downsample_" + metric.getId())
            .setAggregator(downsampler.getAggregator())
            .setInterval(downsampler.getInterval())
            .setQuery(q);
        if (!Strings.isNullOrEmpty(downsampler.getTimezone())) {
          ds.setTimeZone(ZoneId.of(downsampler.getTimezone()));
        }
        final NumericInterpolatorConfig nic = 
            NumericInterpolatorFactory.parse(downsampler.getAggregator());
        ds.setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
          .setQueryIteratorInterpolatorConfig(nic);
        QueryNode down = new DownsampleFactory("Downsample").newNode(this, ds.build());
        addVertex(down);
        addDagEdge(down, node);
        node = down;
      }
      
      Filter filter = Strings.isNullOrEmpty(metric.getFilter()) ? null : q.getFilter(metric.getFilter());
      if (filter != null) {
        GroupByConfig.Builder gb_config = null;
        for (TagVFilter v : filter.getTags()) {
          if (v.isGroupBy()) {
            NumericInterpolatorConfig nic = NumericInterpolatorFactory.parse(
                !Strings.isNullOrEmpty(metric.getAggregator()) ?
                    metric.getAggregator() : q.getTime().getAggregator());
            if (gb_config == null) {
              gb_config = GroupByConfig.newBuilder()
                  .setQueryIteratorInterpolatorFactory(new NumericInterpolatorFactory.Default())
                  .setQueryIteratorInterpolatorConfig(nic)
                  .setId("groupBy_" + metric.getId());
            }
            gb_config.addTagKey(v.getTagk());
          }
        }
        
        if (gb_config != null) {
          gb_config.setAggregator( 
              !Strings.isNullOrEmpty(metric.getAggregator()) ?
              metric.getAggregator() : q.getTime().getAggregator());
          
          QueryNode gb = new GroupByFactory("GroupBy").newNode(this, gb_config.build());
          addVertex(gb);
          addDagEdge(gb, node);
          node = gb;
        }
      }
      
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