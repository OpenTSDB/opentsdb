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
package net.opentsdb.query.processor.downsample;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.downsample.DownsampleConfig.Builder;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Pair;

/**
 * Simple class for generating Downsample processors.
 * 
 * @since 3.0
 */
public class DownsampleFactory extends BaseQueryNodeFactory {
  private static final Logger LOG = LoggerFactory.getLogger(
      DownsampleFactory.class);
  
  public static final String TYPE = "Downsample";
  
  public static final String AUTO_KEY = "tsd.query.downsample.auto.config";
  
  public static final TypeReference<Map<String, String>> AUTO_REF = 
      new TypeReference<Map<String, String>>() { };
  
  /** The auto downsample intervals. */
  private List<Pair<Long, String>> intervals;
  
  /**
   * Default ctor.
   */
  public DownsampleFactory() {
    super();
    registerIteratorFactory(NumericType.TYPE, new NumericIteratorFactory());
    registerIteratorFactory(NumericSummaryType.TYPE, 
        new NumericSummaryIteratorFactory());
    registerIteratorFactory(NumericArrayType.TYPE, 
        new NumericArrayIteratorFactory());
  }
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    
    // default intervals
    intervals = Lists.newArrayListWithExpectedSize(6);
    intervals.add(new Pair<Long, String>(86_400L * 365L * 1000L, "1w")); // 1y
    intervals.add(new Pair<Long, String>(86_400L * 30L * 1000L, "1d")); // 1n
    intervals.add(new Pair<Long, String>(86_400L * 7L * 1000L, "6h")); // 1w
    intervals.add(new Pair<Long, String>(86_400L * 3L * 1000L, "1h")); // 3d
    intervals.add(new Pair<Long, String>(3_600L * 12L * 1000L, "15m")); // 12h
    intervals.add(new Pair<Long, String>(3_600L * 6L * 1000L, "1m")); // 6h
    intervals.add(new Pair<Long, String>(0L, "1m")); // default
    
    registerConfigs(tsdb);
    
    return Deferred.fromResult(null);
  }

  @Override
  public QueryNode newNode(final QueryPipelineContext context,
                           final QueryNodeConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    return new Downsample(this, context, (DownsampleConfig) config);
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public QueryNodeConfig parseConfig(final ObjectMapper mapper,
                                     final TSDB tsdb,
                                     final JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("interval");
    if (n != null) {
      builder.setInterval(n.asText());
    }
    
    n = node.get("id");
    if (n != null) {
      builder.setId(n.asText());
    }
    
    n = node.get("timezone");
    if (n != null) {
      builder.setTimeZone(n.asText());
    }
    
    n = node.get("aggregator");
    if (n != null) {
      builder.setAggregator(n.asText());
    }
    
    n = node.get("infectiousNan");
    if (n != null) {
      builder.setInfectiousNan(n.asBoolean());
    }
    
    n = node.get("runAll");
    if (n != null) {
      builder.setRunAll(n.asBoolean());
    }
    
    n = node.get("fill");
    if (n != null) {
      builder.setFill(n.asBoolean());
    }
    
    n = node.get("start");
    if (n != null) {
      builder.setStart(n.asText());
    }
    
    n = node.get("end");
    if (n != null) {
      builder.setEnd(n.asText());
    }
    
    n = node.get("interpolatorConfigs");
    for (final JsonNode config : n) {
      JsonNode type_json = config.get("type");
      final QueryInterpolatorFactory factory = tsdb.getRegistry().getPlugin(
          QueryInterpolatorFactory.class, 
          type_json == null || type_json.isNull() ? 
             null : type_json.asText());
      if (factory == null) {
        throw new IllegalArgumentException("Unable to find an "
            + "interpolator factory for: " + 
            (type_json == null || type_json.isNull() ? "Default" :
             type_json.asText()));
      }
      
      final QueryInterpolatorConfig interpolator_config = 
          factory.parseConfig(mapper, tsdb, config);
      builder.addInterpolatorConfig(interpolator_config);
    }
    
    n = node.get("sources");
    if (n != null && !n.isNull()) {
      try {
        builder.setSources(mapper.treeToValue(n, List.class));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Failed to parse json", e);
      }
    }
    
    builder.setIntervals(intervals);
    
    return (DownsampleConfig) builder.build();
  }
  
  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final QueryNodeConfig config, 
                         final QueryPlanner plan) {
    // For downsampling we need to set the config start and end times
    // to the query start and end times. The config will then align them.
    DownsampleConfig.Builder builder = DownsampleConfig
        .newBuilder((DownsampleConfig) config)
        .setStart(context.query().getStart())
        .setEnd(context.query().getEnd())
        .setId(config.getId());
    
    QueryNodeConfig newConfig = builder.build();

    // and we need to find our sources if we have a rollup as well as set the 
    // padding.
    final List<QueryNodeConfig> sources = Lists.newArrayList(
        plan.terminalSourceNodes(newConfig));
    for (final QueryNodeConfig source : sources) {
      if (!(source instanceof TimeSeriesDataSourceConfig)) {
        LOG.debug("Hmmm, wasn't a data source config? " + source);
        continue;
      }
      TimeSeriesDataSourceConfig.Builder new_source = 
          (TimeSeriesDataSourceConfig.Builder)
            ((TimeSeriesDataSourceConfig) source).toBuilder();
      new_source.setSummaryInterval(((DownsampleConfig) newConfig).getInterval());
      if (((DownsampleConfig) newConfig).getAggregator().equalsIgnoreCase("avg")) {
        new_source.addSummaryAggregation("sum");
        new_source.addSummaryAggregation("count");
      } else {
        new_source.addSummaryAggregation(((DownsampleConfig) newConfig).getAggregator());
      }
      
      plan.replace(source, new_source.build());
    }
    
    plan.replace(config, newConfig);
  }
  
  /**
   * Returns the intervals for this factory.
   * <b>WARNING:</b> Do NOT modify the list or entries.
   * @return The non-null intervals list.
   */
  public List<Pair<Long, String>> intervals() {
    return intervals;
  }
  
  /**
   * Returns the proper auto interval based on the query width and the interval
   * config.
   * @param delta A non-negative delta in milliseconds.
   * @param intervals The non-null reference to auto intervals.
   * @return The configured auto downsample interval.
   * @throws IllegalStateException if the downsampler is not configured properly.
   */
  public static String getAutoInterval(final long delta, 
                                       final List<Pair<Long, String>> intervals) {
    synchronized (intervals) {
      for (final Pair<Long, String> interval : intervals) {
        if (delta >= interval.getKey()) {
          return interval.getValue();
        }
      }
    }
    throw new IllegalStateException("The host is miss configured and was "
        + "unable to find a default auto downsample interval.");
  }
  
  /** A callback for the auto downsample config. */
  class SettingsCallback implements ConfigurationCallback<Object> {
    @Override
    public void update(final String key, final Object value) {
      if (key.equals(AUTO_KEY)) {
        if (value == null || ((Map<String, String>) value).isEmpty()) {
          return;
        }
        
        @SuppressWarnings("unchecked")
        final Map<String, String> new_intervals = (Map<String, String>) value;
        if (new_intervals.isEmpty()) {
          LOG.error("The auto downsample config is empty. Using the defaults.");
          return;
        }
        if (new_intervals.get("0") == null) {
          LOG.error("The auto downsample config is missing the '0' config. "
              + "Using the defaults.");
          return;
        }
      
        final List<Pair<Long, String>> pairs = 
            Lists.newArrayListWithExpectedSize(new_intervals.size());
        for (final Entry<String, String> entry : new_intervals.entrySet()) {
          try {
            final long interval = entry.getKey().equals("0") ? 0 :
              DateTime.parseDuration(entry.getKey());
            DateTime.parseDuration(entry.getValue()); // validation
            pairs.add(new Pair<Long, String>(interval, entry.getValue()));
          } catch (Exception e) {
            LOG.error("Failed to parse entry: " + entry + ". Using defaults", e);
            return;
          }
        }
        
        // all good, sync it. Ugly yeah but this should run infrequently and only
        // have up to a dozen entries.
        synchronized (intervals) {
          for (final Pair<Long, String> entry : pairs) {
            boolean present = false;
            final Iterator<Pair<Long, String>> iterator = intervals.iterator();
            while (iterator.hasNext()) {
              final Pair<Long, String> extant = iterator.next();
              if (extant.getKey() == entry.getKey() && 
                  !extant.getValue().equalsIgnoreCase(entry.getValue())) {
                // value changed so we need to update it
                extant.setValue(entry.getValue());
                present = true;
                break;
              } else if (extant.getKey() == entry.getKey()) {
                present = true;
                break;
              }
            }
            
            if (!present) {
              intervals.add(entry);
            }
          }
          
          // yank the stragglers
          final Iterator<Pair<Long, String>> iterator = intervals.iterator();
          while (iterator.hasNext()) {
            final Pair<Long, String> extant = iterator.next();
            boolean present = false;
            for (final Pair<Long, String> new_entry : pairs) {
              if (extant.getKey() == new_entry.getKey()) {
                present = true;
                break;
              }
            }
            
            if (!present) {
              iterator.remove();
            }
          }
          
          Collections.sort(intervals, REVERSE_PAIR_CMP);
          LOG.info("Updated auto downsample intervals: " + intervals);
        }
      }
    }
  }
  
  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(AUTO_KEY)) {
      tsdb.getConfig().register(
          ConfigurationEntrySchema.newBuilder()
          .setKey(AUTO_KEY)
          .setType(AUTO_REF)
          .setDescription("A map of 1 or more pairs of auto downsample steps "
              + "where the key is a TSDB style duration and the value is "
              + "another duration to use as the downsample. The query duration "
              + "is compared against the key duration and if it is greater than "
              + "or equal to the key, then the value duration is substituted. "
              + "There must be at least one key of '0' that is treated as the "
              + "default duration.")
          .isDynamic()
          .isNullable()
          .setSource(getClass().getName())
          .build());
    }
    tsdb.getConfig().bind(AUTO_KEY, new SettingsCallback());
  }
  
  /**
   * A comparator for the pair keys in reverse numeric order.
   */
  static class ReversePairComparator implements Comparator<Pair<Long, ?>> {

    @Override
    public int compare(final Pair<Long, ?> a, final Pair<Long, ?> b) {
      return -a.getKey().compareTo(b.getKey());
    }
    
  }
  static final ReversePairComparator REVERSE_PAIR_CMP = new ReversePairComparator();
  
  /**
   * The default numeric iterator factory.
   */
  protected class NumericIteratorFactory implements QueryIteratorFactory {

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new DownsampleNumericIterator(node, result, sources.iterator().next());
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new DownsampleNumericIterator(node, result, sources.values().iterator().next());
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericType.TYPE);
    }
        
  }

  /**
   * Handles summaries.
   */
  protected class NumericSummaryIteratorFactory implements QueryIteratorFactory {

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new DownsampleNumericSummaryIterator(node, result, sources.iterator().next());
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new DownsampleNumericSummaryIterator(node, result, sources.values().iterator().next());
    }
    
    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericSummaryType.TYPE);
    }
  }
  
  /**
   * Handles arrays.
   */
  protected class NumericArrayIteratorFactory implements QueryIteratorFactory {

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Collection<TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new DownsampleNumericArrayIterator(node, result, sources.iterator().next());
    }

    @Override
    public TypedTimeSeriesIterator newIterator(final QueryNode node,
                                               final QueryResult result,
                                               final Map<String, TimeSeries> sources,
                                               final TypeToken<? extends TimeSeriesDataType> type) {
      return new DownsampleNumericArrayIterator(node, result, sources.values().iterator().next());
    }
    
    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericArrayType.TYPE);
    }
  }
  
}
