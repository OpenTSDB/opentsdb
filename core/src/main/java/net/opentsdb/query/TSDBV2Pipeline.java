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
package net.opentsdb.query;

import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.ConfigurationException;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorFactory;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.RateOptions;
import net.opentsdb.query.processor.groupby.GroupByFactory;
import net.opentsdb.query.processor.rate.RateFactory;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;
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
          .setConfiguration(tsdb.getConfig())
          .build();
      
      // TODO - get a proper source. For now just the default.
      final TimeSeriesDataStoreFactory factory = tsdb.getRegistry()
          .getDefaultPlugin(TimeSeriesDataStoreFactory.class);
      if (factory == null) {
        throw new ConfigurationException("No default "
            + "TimeSeriesDataStoreFactory loaded");
      }
      final TimeSeriesDataStore store = factory.newInstance(tsdb, null /* TODO - implement span */);
      if (store == null) {
        throw new QueryExecutionException("Unable to get a data store "
            + "instance from factory: " + factory.id(), 0);
      }

      QueryNode node = store.newNode(this, config);
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
      
      if (metric.isRate()) {
        QueryNode rate = new RateFactory("Rate").newNode(this, 
            metric.getRateOptions() == null ? RateOptions.newBuilder().build() : metric.getRateOptions());
        addVertex(rate);
        addDagEdge(rate, node);
        node = rate;
      }
      
      Filter filter = Strings.isNullOrEmpty(metric.getFilter()) ? null : q.getFilter(metric.getFilter());
      if (filter != null) {
        GroupByConfig.Builder gb_config = null;
        final Set<String> join_keys = Sets.newHashSet();
        for (TagVFilter v : filter.getTags()) {
          if (v.isGroupBy()) {
            String agg = !Strings.isNullOrEmpty(metric.getAggregator()) ?
                metric.getAggregator() : q.getTime().getAggregator();
            NumericInterpolatorConfig nic = NumericInterpolatorFactory.parse(agg);
            if (gb_config == null) {
              QueryIteratorInterpolatorFactory nif;
              // TODO - find a better way
              if (agg.contains("zimsum") || 
                  agg.contains("mimmax") ||
                  agg.contains("mimmin")) {
                nif = tsdb.getRegistry().getPlugin(
                    QueryIteratorInterpolatorFactory.class, "Default");
              } else {
                nif = tsdb.getRegistry().getPlugin(
                    QueryIteratorInterpolatorFactory.class, "LERP");
              }
              if (nif == null) {
                throw new QueryExecutionException("Unable to find the LERP interpolator.", 0);
              }
              gb_config = GroupByConfig.newBuilder()
                  .setQueryIteratorInterpolatorFactory(nif)
                  .setQueryIteratorInterpolatorConfig(nic)
                  .setId("groupBy_" + metric.getId());
            }
            join_keys.add(v.getTagk());
          }
        }
        
        if (gb_config != null) {
          if (factory.idType() == Const.TS_BYTE_ID) {
            try {
              List<byte[]> keys = store.encodeJoinKeys(
                  Lists.newArrayList(join_keys), null /* TODO */)
                  .join(); // TODO <--- DO NOT JOIN here! Find a way to async it.
              gb_config.setTagKeys(keys);
            } catch (InterruptedException e) {
              throw new QueryExecutionException("Unexpected interruption", 0, e);
            } catch (Exception e) {
              throw new QueryExecutionException("Unexpected exception", 0, e);
            }
          }
          gb_config.setTagKeys(join_keys);
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
