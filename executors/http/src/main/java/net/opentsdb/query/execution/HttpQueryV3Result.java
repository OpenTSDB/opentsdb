// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.utils.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.event.EventGroupType;
import net.opentsdb.data.types.event.EventType;
import net.opentsdb.data.types.event.EventsGroupValue;
import net.opentsdb.data.types.event.EventsValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.readcache.CachedQueryNode;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.DateTime;

/**
 * A result for Graph queries that takes in the JSON and maintains a 
 * reference, parsing it as needed. We will parse out the time spec and
 * time series IDs but the actual values are pulled out when the
 * iterators are constructed.
 * 
 * TODO - summaries
 * TODO - split it out.
 * 
 * @since 3.0
 */
public class HttpQueryV3Result implements QueryResult {
  private static final Logger LOG = LoggerFactory.getLogger(HttpQueryV3Result.class);
  
  /** The node that owns us. */
  private final QueryNode node;
  
  /** The ID of this result. */
  private final QueryResultId id;
  
  /** The time spec parsed out. */
  private TimeSpecification time_spec;
  
  /** The list of series we found. */
  private List<TimeSeries> series;
  
  /** An optional exception. */
  private Exception exception;
  
  /** An optional rollup config from summaries. */
  private RollupConfig rollup_config;

  /** Object Mapper for serdes. */
  private final ObjectMapper mapper = JSON.getMapper();

  /** Flag to know if we need to process series parallely */
  private boolean process_parallel = true;
  
  /**
   * Default ctor without an exception.
   * @param node The non-null parent node.
   * @param root The non-null root node.
   */
  HttpQueryV3Result(final QueryNode node, 
                    final JsonNode root, 
                    final RollupConfig rollup_config) {
    this(node, root, rollup_config, null);
  }
  
  /**
   * Ctor with an exception. If the exception isn't null then the root 
   * must be set.
   * @param node The non-null parent node.
   * @param root The root node. Cannot be null if the exception is null.
   * @param exception An optional exception.
   */
  HttpQueryV3Result(final QueryNode node, 
                    final JsonNode root, 
                    final RollupConfig rollup_config,
                    final Exception exception) {
    this.exception = exception;
    this.rollup_config = rollup_config;
    this.node = new CachedQueryNode(node.config().getId(), node);
    TimeSeriesDataSourceConfig cfg = (TimeSeriesDataSourceConfig) node.config();
    if (exception == null && root != null) {
      JsonNode n = root.get("source");
      if (n == null || n.isNull()) {
        throw new IllegalStateException("No source from the JSON response.");
      }
      String[] src = n.asText().split(":");
      if (src == null || src.length < 2) {
        throw new IllegalStateException("Failed to parse the source: " + n.asText());
      }

      if (((HttpQueryV3Factory) node.factory()).skipIdParsing()) {
        TimeSeriesDataSourceConfig tsdsc = (TimeSeriesDataSourceConfig) node.config();
        if (tsdsc.getPushDownNodes() != null && !tsdsc.getPushDownNodes().isEmpty()) {
          final List<QueryNodeConfig> pushDowns = tsdsc.getPushDownNodes();
          id = (QueryResultId) pushDowns.get(pushDowns.size() - 1).resultIds().get(0);
        } else {
          id = (QueryResultId) node.config().resultIds().get(0);
        }
      } else {
        id = new DefaultQueryResultId(src[0], src[1]);
      }
      n = root.get("timeSpecification");
      if (n != null && !n.isNull()) {
        time_spec = new TimeSpec(n);
      }
      
      n = root.get("data");
      if (n != null && !n.isNull()) {
        series = Lists.newArrayList();
        int i = 0;
        for (final JsonNode ts : n) {
          series.add(new HttpTimeSeries(ts));
          if (i++ == 0) {
            // check for numerics
            JsonNode rollups = ts.get("NumericSummaryType");
            if (rollup_config == null) {
              if (rollups != null && !rollups.isNull()) {
                this.rollup_config = new RollupData(ts);
              } else {
                this.rollup_config = null;
              }
            }
          }
        }
      } else {
        series = Collections.emptyList();
      }
    } else {
      series = Collections.emptyList();
      // TODO - Important! If we have a lot of results expected and some are
      // not serializing to a result, we need to handle those.
      if (node.config().resultIds().isEmpty()) {
        LOG.warn("Empty result from downstream. Using the node's ID as the "
            + "result ID: " + cfg.getId());
        id = new DefaultQueryResultId(cfg.getId(), cfg.getId());
      } else {
        id = (QueryResultId) node.config().resultIds().get(0);
      }
    }
    
  }

  @Override
  public TimeSpecification timeSpecification() {
    return time_spec;
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return series;
  }

  @Override
  public String error() {
    return exception != null ? exception.getMessage() : null;
  }
  
  @Override
  public Throwable exception() {
    return exception;
  }
  
  @Override
  public long sequenceId() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public QueryResultId dataSource() {
    return id;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public ChronoUnit resolution() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RollupConfig rollupConfig() {
    return rollup_config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean processInParallel() {
    return process_parallel;
  }

  class TimeSpec implements TimeSpecification {

    private final TimeStamp start;
    private final TimeStamp end;
    private final String string_interval;
    private final TemporalAmount interval;
    private final ChronoUnit units;
    private final ZoneId time_zone;
    
    TimeSpec(final JsonNode node) {
      long st = node.get("start").asLong();
      long e = node.get("end").asLong();
      if (st == HttpQueryV3Result.this.node.pipelineContext().query().startTime().epoch() &&
          e == HttpQueryV3Result.this.node.pipelineContext().query().endTime().epoch()) {
        start = HttpQueryV3Result.this.node.pipelineContext().query().startTime();
        end = HttpQueryV3Result.this.node.pipelineContext().query().endTime();
      } else {
        start = new SecondTimeStamp(node.get("start").asLong());
        end = new SecondTimeStamp(node.get("end").asLong());
      }
      string_interval = node.get("interval").asText();
      if (string_interval.equalsIgnoreCase("0all") ||
          string_interval.equalsIgnoreCase(DownsampleConfig.AUTO)) {
        interval = null;
      } else {
        interval = DateTime.parseDuration2(string_interval);
      }
      // TODO - get the proper units.
      //units = ChronoUnit(node.get("units").asText());
      units = null;
      time_zone = ZoneId.of(node.get("timeZone").asText());
    }
    
    @Override
    public TimeStamp start() {
      return start;
    }

    @Override
    public TimeStamp end() {
      return end;
    }

    @Override
    public TemporalAmount interval() {
      return interval;
    }

    @Override
    public String stringInterval() {
      return string_interval;
    }

    @Override
    public ChronoUnit units() {
      return units;
    }

    @Override
    public ZoneId timezone() {
      return time_zone;
    }

    @Override
    public void updateTimestamp(int offset, TimeStamp timestamp) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void nextTimestamp(TimeStamp timestamp) {
      // TODO Auto-generated method stub
      
    }
    
  }

  /**
   * The base time series that parses out the ID and sets the root node
   * of the data.
   */
  class HttpTimeSeries implements TimeSeries {

    /** The parsed ID. */
    private final TimeSeriesStringId id;
    
    /** The time series root node. */
    private final JsonNode node;
    
    /** The parsed out types. */
    private final List<TypeToken<? extends TimeSeriesDataType>> types;
    
    /**
     * Default ctor.
     * @param node The non-null time series root node.
     */
    HttpTimeSeries(final JsonNode node) {
      this.node = node;
      final BaseTimeSeriesStringId.Builder builder =
          BaseTimeSeriesStringId.newBuilder();
      JsonNode temp = node.get("metric");
      if (temp != null && !temp.isNull()) {
        builder.setMetric(temp.asText());
      }
      temp = node.get("tags");
      if (temp != null && !temp.isNull()) {
        final Iterator<Entry<String, JsonNode>> iterator = temp.fields();
        while (iterator.hasNext()) {
          final Entry<String, JsonNode> entry = iterator.next();
          String tag = entry.getValue().asText();
          if (tag.contains("\u0000")) {
            LOG.warn("Stripping null UTF8 from tag: " + tag + " from source: " 
                + HttpQueryV3Result.this.node.config().getId());
            tag = tag.replace("\u0000", "");
          }
          if (tag.endsWith("\n")) {
            LOG.warn("Stripping null from tag: " + tag + " from source: " 
                + HttpQueryV3Result.this.node.config().getId());
            tag = tag.substring(0, tag.length() - 1);
          }
          builder.addTags(entry.getKey(), tag);
        }
      }

      temp = node.get("hits");
      if (temp != null && !temp.isNull()) {
        builder.setHits(temp.asLong());
      }

      temp = node.get("aggregateTags");
      if (temp != null && !temp.isNull()) {
        for (final JsonNode tag : temp) {
          builder.addAggregatedTag(tag.asText());
        }
      }
      id = builder.build();

      types = Lists.newArrayList();
      temp = node.get("NumericType");
      if (temp != null && !temp.isNull()) {
        if (time_spec != null) {
          types.add(NumericArrayType.TYPE);
        } else {
          types.add(NumericType.TYPE);
        }
      }

      temp = node.get("NumericSummaryType");
      if (temp != null && !temp.isNull()) {
        types.add(NumericSummaryType.TYPE);
      }

      temp = node.get("EventsType");
      if (temp != null && !temp.isNull()) {
        types.add(EventType.TYPE);
        process_parallel = false;
      }

      temp = node.get("EventsGroupType");
      if (temp != null && !temp.isNull()) {
        types.add(EventGroupType.TYPE);
        process_parallel = false;
      }
    }
    
    @Override
    public TimeSeriesId id() {
      return id;
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        final TypeToken<? extends TimeSeriesDataType> type) {
      // TODO - cleanup
      if (types.contains(type)) {
        TypedTimeSeriesIterator<? extends TimeSeriesDataType> data = null;
        if (type == NumericType.TYPE) {
          data = new NumericData(node.get("NumericType"));
        } else if (type == NumericArrayType.TYPE) {
          data = new ArrayData(node.get("NumericType"));
        } else if (type == NumericSummaryType.TYPE) {
          data = new SummaryData(node.get("NumericSummaryType"));
        } else if (type == EventType.TYPE) {
          data = new EventData(node.get("EventsType"));
        } else if (type == EventGroupType.TYPE) {
          data = new EventGroupData(node.get("EventsGroupType"));
        }
        if (data != null) {
          return Optional.of(data);
        }
        return Optional.empty();
      }
      return Optional.empty();
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      // TODO - cleanup
      List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> results = Lists
          .newArrayListWithExpectedSize(1);
      if (types.contains(NumericType.TYPE)) {
        results.add(new NumericData(node.get("NumericType")));
      } else if (types.contains(NumericArrayType.TYPE)) {
        results.add(new ArrayData(node.get("NumericType")));
      } else if (types.contains(EventType.TYPE)) {
        results.add(new EventData(node.get("EventsType")));
      } else if (types.contains(EventGroupType.TYPE)) {
        results.add(new EventGroupData(node.get("EventsGroupType")));
      }
      return results;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return types;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }
    
  }

  /**
   * Iterator for the NumericType.
   */
  class NumericData implements TypedTimeSeriesIterator {
    
    /** The data point we populate each time. */
    private MutableNumericValue dp;
    
    /** The iterator over the data points. */
    private Iterator<Entry<String, JsonNode>> iterator;
    
    /** The timestamp populated each round. */
    private TimeStamp timestamp;
    
    NumericData(final JsonNode data) {
      iterator = data.fields();
      timestamp = new SecondTimeStamp(0);
      dp = new MutableNumericValue();
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericType.TYPE;
    }

    @Override
    public void close() {
      // no-op for now
    }
    
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> next() {
      Entry<String, JsonNode> entry = iterator.next();
      timestamp.updateEpoch(Long.parseLong(entry.getKey()));
      if (entry.getValue().isDouble()) {
        dp.reset(timestamp, entry.getValue().asDouble());
      } else {
        dp.reset(timestamp, entry.getValue().asLong());
      }
      return dp;
    }
    
  }
  
  /**
   * Implementation for the NumericArrayType.
   */
  class ArrayData implements TypedTimeSeriesIterator, 
      TimeSeriesValue<NumericArrayType>,
      NumericArrayType{

    /** The data arrays we'll populate. */
    private long[] long_data;
    private double[] double_data;
    
    /** Whether or not we were read. */
    private boolean was_read = false;
    
    /** The write index into the arrays. */
    private int idx = 0;
    
    ArrayData(final JsonNode data) {
      long_data = new long[data.size()];
      for (final JsonNode node : data) {
        if (node.isDouble() || node.isTextual() && 
            node.asText().toLowerCase().equals("nan")) {
          // TODO - infinites?
          if (double_data == null) {
            double_data = new double[long_data.length];
            for (int i = 0; i < idx; i++) {
              double_data[i] = long_data[i];
            }
            long_data = null;
          }
          
          if (idx >= double_data.length) {
            double[] temp = new double[idx < 1024 ? idx * 2 : idx + 8];
            for (int i = 0; i < idx; i++) {
              temp[i] = double_data[i];
            }
          }
          double_data[idx++] = node.asDouble();
        } else {
          if (long_data == null) {
            if (idx >= double_data.length) {
              double[] temp = new double[idx < 1024 ? idx * 2 : idx + 8];
              for (int i = 0; i < idx; i++) {
                temp[i] = double_data[i];
              }
            }
            double_data[idx++] = node.asDouble();
          } else {
            if (idx >= long_data.length) {
              long[] temp = new long[idx < 1024 ? idx * 2 : idx + 8];
              for (int i = 0; i < idx; i++) {
                temp[i] = long_data[i];
              }
            }
            long_data[idx++] = node.asLong();
          }
        }
      }
    }
    
    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericArrayType.TYPE;
    }

    @Override
    public void close() {
      // no-op for now
    }
    
    @Override
    public boolean hasNext() {
      return !was_read;
    }

    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> next() {
      was_read = true;
      return this;
    }

    @Override
    public TimeStamp timestamp() {
      return time_spec.start();
    }

    @Override
    public NumericArrayType value() {
      return this;
    }

    @Override
    public TypeToken<NumericArrayType> type() {
      return NumericArrayType.TYPE;
    }

    @Override
    public int offset() {
      return 0;
    }

    @Override
    public int end() {
      return idx;
    }

    @Override
    public boolean isInteger() {
      return long_data != null;
    }

    @Override
    public long[] longArray() {
      return long_data;
    }

    @Override
    public double[] doubleArray() {
      return double_data;
    }
    
  }

  class SummaryData implements TypedTimeSeriesIterator {
    
    /** The data point we populate each time. */
    private MutableNumericSummaryValue dp;
    
    /** The timestamp populated each round. */
    private TimeStamp timestamp;
    
    private Iterator<JsonNode> iterator;
    
    SummaryData(final JsonNode data) {
      iterator = data.get("data").iterator();
      timestamp = new SecondTimeStamp(0);
      dp = new MutableNumericSummaryValue();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> next() {
      final JsonNode node = iterator.next();
      final Entry<String, JsonNode> entry = node.fields().next();
      timestamp.updateEpoch(Long.parseLong(entry.getKey()));
      dp.resetTimestamp(timestamp);
      
      int i = 0;
      for (final JsonNode agg : entry.getValue()) {
        if (agg.isDouble()) {
          dp.resetValue(i++, agg.asDouble());
        } else {
          dp.resetValue(i++, agg.asLong());
        }
      }
      return dp;
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericSummaryType.TYPE;
    }
    
    @Override
    public void close() {
      // no-op for now
    }
    
  }

  class EventData implements TypedTimeSeriesIterator {

    private JsonNode node;

    EventData(final JsonNode data) {
      node = data;
    }

    @Override
    public boolean hasNext() {
      return node == null ? false : true;
    }

    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> next() {

      EventsValue events_value;
      try {
        events_value  = mapper.treeToValue(node, EventsValue.class);
        node = null;
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Unable to parse config", e);
      }
      return events_value;
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return EventType.TYPE;
    }

    @Override
    public void close() {
      // no-op for now
    }
    
  }

  class EventGroupData implements TypedTimeSeriesIterator {

    private JsonNode node;

    EventGroupData(final JsonNode data) {
      node = data;
    }

    @Override
    public boolean hasNext() {
      return node == null ? false : true;
    }

    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> next() {

      EventsGroupValue events_group_value;
      try {
        events_group_value = mapper.treeToValue(node, EventsGroupValue.class);
        node = null;
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Unable to parse config", e);
      }
      return events_group_value;
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return EventGroupType.TYPE;
    }

    @Override
    public void close() {
      // no-op for now
    }
    
  }
  
  /**
   * A parsed rollup config.
   */
  class RollupData implements RollupConfig {
    
    /** Forward and reverse maps. */
    private Map<String, Integer> name_to_id;
    private Map<Integer, String> id_to_name;
    
    RollupData(JsonNode node) {
      name_to_id = Maps.newHashMap();
      id_to_name = Maps.newHashMap();
      
      // from "time series" object root.
      node = node.get("NumericSummaryType");
      if (node == null || node.isNull()) {
        return;
      }
      
      node = node.get("aggregations");
      int i = 0;
      for (final JsonNode agg : node) {
        name_to_id.put(agg.asText(), i);
        id_to_name.put(i++, agg.asText());
      }
    }
    
    @Override
    public Map<String, Integer> getAggregationIds() {
      return name_to_id;
    }

    @Override
    public String getAggregatorForId(final int id) {
      return id_to_name.get(id);
    }

    @Override
    public int getIdForAggregator(final String aggregator) {
      return name_to_id.get(aggregator);
    }

    @Override
    public List<String> getIntervals() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<String> getPossibleIntervals(String interval) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<RollupInterval> getRollupIntervals(long interval, String str_interval, boolean skip_default) {
      return null;
    }

    @Override
    public RollupInterval getRollupInterval(String interval) {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public RollupInterval getDefaultInterval() {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public int getIdForAggregator(byte[] qualifier) {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public int getOffsetStartFromQualifier(byte[] qualifier) {
      throw new UnsupportedOperationException("TODO");
    }

  }
  
}
