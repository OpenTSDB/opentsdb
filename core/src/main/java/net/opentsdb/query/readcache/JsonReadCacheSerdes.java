// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.readcache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
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
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.event.EventGroupType;
import net.opentsdb.data.types.event.EventType;
import net.opentsdb.data.types.event.EventsGroupValue;
import net.opentsdb.data.types.event.EventsValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.status.StatusIterator;
import net.opentsdb.data.types.status.StatusType;
import net.opentsdb.data.types.status.StatusValue;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.summarizer.Summarizer;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * A JSON serializer that converts the data into cacheable segments and vice-versa.
 * 
 * TODO - Clean this up and UTs
 * 
 * TODO - set the rollup config
 * 
 * TODO - may need the QueryPipelineContext in the dummy node.
 * 
 * TODO - properly handle events and status message
 * 
 * @since 3.0
 */
public class JsonReadCacheSerdes implements ReadCacheSerdes, 
                                            ReadCacheSerdesFactory {
  private static final Logger LOG = LoggerFactory.getLogger(
      JsonReadCacheSerdes.class);

  // TODO - UGLY TEMP!!
  static RollupConfig ROLLUP_CONFIG = DefaultRollupConfig.newBuilder()
      .addAggregationId("sum", 0)
      .addAggregationId("count", 1)
      .addAggregationId("max", 2)
      .addAggregationId("min", 3)
      .addAggregationId("avg", 5)
      .addAggregationId("first", 6)
      .addAggregationId("last", 7)
      .addInterval(RollupInterval.builder()
          .setInterval("sum")
          .setTable("tsdb")
          .setPreAggregationTable("tsdb")
          .setInterval("1h")
          .setRowSpan("1d"))
      .build();
  
  @Override
  public byte[] serialize(Collection<QueryResult> results) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonGenerator json;
    QueryPipelineContext ctx = null;
    try {
      json = JSON.getFactory().createGenerator(baos);
      json.writeStartObject();
      json.writeArrayFieldStart("results");
      
      for (final QueryResult result : results) {
        if (ctx != null) {
          ctx = result.source().pipelineContext();
        }
        // TODO make sure ids are strings.
        json.writeStartObject();
        json.writeStringField("source", result.source().config().getId() 
            + ":" + result.dataSource());
        // TODO - array of data sources
        // serdes time spec if present
        if (result.timeSpecification() != null) {
          json.writeObjectFieldStart("timeSpecification");
          // TODO - ms, second, nanos, etc
          json.writeNumberField("start", result.timeSpecification().start().epoch());
          json.writeNumberField("end", result.timeSpecification().end().epoch());
          json.writeStringField("intervalISO", result.timeSpecification().interval() != null ?
              result.timeSpecification().interval().toString() : "null");
          json.writeStringField("interval", result.timeSpecification().stringInterval());
          if (result.timeSpecification().timezone() != null) {
            json.writeStringField("timeZone", result.timeSpecification().timezone().toString());
          }
          json.writeStringField("units", result.timeSpecification().units() != null ?
              result.timeSpecification().units().toString() : "null");
          json.writeEndObject();
        }

        json.writeArrayFieldStart("data");
        int idx = 0;

        boolean wasStatus = false;
        boolean wasEvent =false;
        String namespace = null;
        TimeStamp ts = new SecondTimeStamp(0);
        for (final TimeSeries series : result.timeSeries()) {
          serializeSeries(series, 
              (TimeSeriesStringId) series.id(),
              json,
              null,
              ts,
              result);
          if (!wasStatus ) {
            for (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator :
                series.iterators()) {
              if (iterator.getType() == StatusType.TYPE) {
                namespace = ((StatusIterator) iterator).namespace();
                wasStatus = true;
                break;
              }
            }
          }

        }
        // end of the data array
        json.writeEndArray();
        json.writeNumberField("lastValueTimestamp", ts.epoch());
        if (wasStatus && null != namespace && !namespace.isEmpty()) {
          json.writeStringField("namespace", namespace);
        }

        json.writeEndObject();
      }
      
      json.writeEndArray();
      json.writeEndObject();
      json.close();
    } catch (IOException e1) {
      throw new RuntimeException("Failed to instantiate a JSON "
          + "generator", e1);
    }
    
    final byte[] array = baos.toByteArray();
    if (ctx != null && ctx.query().isTraceEnabled()) {
      ctx.queryContext().logTrace("Caching single segment: " 
          + new String(array, Const.UTF8_CHARSET));
    }
    return array;
  }

  @Override
  public Map<String, ReadCacheQueryResult> deserialize(
      final QueryNode node, 
      final byte[] data) {
    if (node.pipelineContext().query().isTraceEnabled()) {
      node.pipelineContext().queryContext().logTrace("Parsing cached segment: " 
          + new String(data, Const.UTF8_CHARSET));
    }
    Map<String, ReadCacheQueryResult> map = Maps.newHashMap();
    try {
      JsonNode results = JSON.getMapper().readTree(data);
      results = results.get("results");
      for (final JsonNode result : results) {
        ReadCacheQueryResult r = new JsonCachedResult(node, result, ROLLUP_CONFIG);
        map.put(r.source().config().getId() + ":" + r.dataSource(), r);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse data", e);
    }
    return map;
  }
  
  private void serializeSeries(
      final TimeSeries series,
      final TimeSeriesStringId id,
      final JsonGenerator json,
      final List<String> sets,
      final TimeStamp last_value,
      final QueryResult result) throws IOException {
    boolean wrote_values = false;
    boolean was_status = false;
    boolean was_event = false;
    boolean was_event_group = false;
    for (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator 
          : series.iterators()) {
      while (iterator.hasNext()) {
        TimeSeriesValue<? extends TimeSeriesDataType> value = iterator.next();
        if (iterator.getType() == StatusType.TYPE) {
          if (!was_status) {
            was_status = true;
          }
          json.writeStartObject();
          writeStatus((StatusValue) value, json);
          wrote_values = true;
        } else if (iterator.getType() == EventType.TYPE) {
          was_event = true;
          json.writeStartObject();
          json.writeObjectFieldStart("EventsType");
          writeEvents((EventsValue) value, json);
          json.writeEndObject();
          wrote_values = true;
        } else if (iterator.getType() == EventGroupType.TYPE) {
          was_event_group = true;
          json.writeStartObject();
          writeEventGroup((EventsGroupValue) value, json, id);
          wrote_values = true;
        } else {
          if (iterator.getType() == NumericType.TYPE) {
            if (writeNumeric((TimeSeriesValue<NumericType>) value, 
                             iterator, json, result, last_value, wrote_values)) {
              wrote_values = true;
            }
          } else if (iterator.getType() == NumericSummaryType.TYPE) {
            if (writeNumericSummary(value, iterator, json, result, 
                                    last_value, wrote_values)) {
              wrote_values = true;
            }
          } else if (iterator.getType() == NumericArrayType.TYPE) {
            if (writeNumericArray((TimeSeriesValue<NumericArrayType>) value, 
                                  iterator, json, result, last_value, wrote_values)) {
              wrote_values = true;
            }
          }
        }
      }
    }

    if (wrote_values) {
      // serialize the ID
      if(!was_status && !was_event) {
        json.writeStringField("metric", id.metric());
      }
      if (! was_event_group) {
        json.writeObjectFieldStart("tags");
        for (final Entry<String, String> entry : id.tags().entrySet()) {
          json.writeStringField(entry.getKey(), entry.getValue());
        }
        json.writeEndObject();
      }
      if (was_event) {
        json.writeNumberField("hits", id.hits());
      } else {
        json.writeArrayFieldStart("aggregateTags");
        for (final String tag : id.aggregatedTags()) {
          json.writeString(tag);
        }
        json.writeEndArray();
      }
      json.writeEndObject();
    }
  }

  private boolean writeNumeric(
      TimeSeriesValue<NumericType> value,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      final TimeStamp last_value,
      boolean wrote_values) throws IOException {
    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      // just the values
      TimeStamp ts = result.timeSpecification().start().getCopy();
      while (value != null) {
        if (!wrote_values) {
          json.writeStartObject();
          wrote_values = true;
        }
        if (!wrote_type) {
          json.writeArrayFieldStart("NumericType"); // yeah, it's numeric.
          wrote_type = true;
        }

        if (value.value() == null) {
          json.writeNull();
        } else {
          if ((value).value().isInteger()) {
            json.writeNumber(
                (value).value().longValue());
            if (ts.compare(Op.GT, last_value)) {
              last_value.update(ts);
            }
          } else {
            json.writeNumber(
                (value).value().doubleValue());
            if (!Double.isNaN(value.value().doubleValue())) {
              if (ts.compare(Op.GT, last_value)) {
                last_value.update(ts);
              }
            }
          }
        }

        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericType>) iterator.next();
        } else {
          value = null;
        }
        ts.add(result.timeSpecification().interval());
      }
      json.writeEndArray();
      return wrote_type;
    }

    // timestamp and values
    boolean wrote_local = false;
    while (value != null) {
      long ts = value.timestamp().epoch();
      final String ts_string = Long.toString(ts);

      if (!wrote_values) {
        json.writeStartObject();
        wrote_values = true;
      }
      if (!wrote_type) {
        json.writeObjectFieldStart("NumericType"); // yeah, it's numeric.
        wrote_type = true;
      }

      if (value.value() == null) {
        json.writeNullField(ts_string);
      } else {
        if ((value).value().isInteger()) {
          json.writeNumberField(ts_string,
              (value).value().longValue());
          if (ts > last_value.epoch()) {
            last_value.updateEpoch(ts);
          }
        } else {
          json.writeNumberField(ts_string,
              (value).value().doubleValue());
          if (!Double.isNaN(value.value().doubleValue())) {
            if (ts > last_value.epoch()) {
              last_value.updateEpoch(ts);
            }
          }
        }
      }

      if (iterator.hasNext()) {
        value = (TimeSeriesValue<NumericType>) iterator.next();
      } else {
        value = null;
      }
      wrote_local = true;
    }
    if (wrote_local) {
      json.writeEndObject();
    }
    return wrote_type;
  }

  private boolean writeRollupNumeric(
      TimeSeriesValue<NumericSummaryType> value,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      final TimeStamp last_value,
      boolean wrote_values) throws IOException {

    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      Collection<Integer> summaries = null;
      Integer summary = null;

      // just the values
      TimeStamp ts = result.timeSpecification().start().getCopy();
      while (value != null) {
        if (!wrote_values) {
          json.writeStartObject();
          wrote_values = true;
        }
        if (!wrote_type) {
          json.writeArrayFieldStart("NumericType"); // yeah, it's numeric.
          wrote_type = true;
        }

        if (value.value() == null) {
          //TODO, should we use json.writeNull() instead?
          json.writeNumber(Double.NaN);
        } else {

          // Will fetch summaries from the first non null dps.
          if (summaries == null) {
            summaries =
                (value).value().summariesAvailable();
            summary = summaries.iterator().next();
          }
          if ((value).value().value(summary).isInteger()) {
            json.writeNumber(
                (value).value().value(summary).longValue());
            if (ts.compare(Op.GT, last_value)) {
              last_value.update(ts);
            }
          } else {
            json.writeNumber(
                (value).value().value(summary).doubleValue());
            if (!Double.isNaN(value.value().value(summary).doubleValue())) {
              if (ts.compare(Op.GT, last_value)) {
                last_value.update(ts);
              }
            }
          }
        }

        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
        } else {
          value = null;
        }
        ts.add(result.timeSpecification().interval());
      }
      json.writeEndArray();
      return wrote_type;
    }

    Collection<Integer> summaries = null;
    Integer summary = null;

    // timestamp and values
    while (value != null) {
      long ts = value.timestamp().epoch();
      final String ts_string = Long.toString(ts);

      if (!wrote_values) {
        json.writeStartObject();
        wrote_values = true;
      }
      if (!wrote_type) {
        json.writeObjectFieldStart("NumericType"); // yeah, it's numeric.
        wrote_type = true;
      }

      if (summaries == null) {
        summaries =
            (value).value().summariesAvailable();
        summary = summaries.iterator().next();
      }

      if (value.value() == null) {
        json.writeNullField(ts_string);
      } else {
        if ((value).value().value(summary).isInteger()) {
          json.writeNumberField(ts_string,
              (value).value().value(summary).longValue());
          if (ts > last_value.epoch()) {
            last_value.updateEpoch(ts);
          }
        } else {
          json.writeNumberField(ts_string,
              (value).value().value(summary).doubleValue());
          if (!Double.isNaN(value.value().value(summary).doubleValue())) {
            if (ts > last_value.epoch()) {
              last_value.updateEpoch(ts);
            }
          }
        }
      }

      if (iterator.hasNext()) {
        value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      } else {
        value = null;
      }
    }
    json.writeEndObject();
    return wrote_type;
  }

  private boolean writeNumericSummary(
      TimeSeriesValue value,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      final TimeStamp last_value,
      boolean wrote_values) throws IOException {

    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      if (!(result.source() instanceof Summarizer)) {
        return writeRollupNumeric(
            (TimeSeriesValue<NumericSummaryType>) value, 
            iterator, 
            json,
            result, 
            last_value, 
            wrote_values);
      }

      Collection<Integer> summaries =
          ((TimeSeriesValue<NumericSummaryType>) value)
              .value()
              .summariesAvailable();

      value = (TimeSeriesValue<NumericSummaryType>) value;
      while (value != null) {
        long ts = value.timestamp().epoch();

        if (!wrote_values) {
          json.writeStartObject();
          wrote_values = true;
        }
        if (!wrote_type) {
          json.writeObjectFieldStart("NumericSummaryType");
          json.writeArrayFieldStart("aggregations");
          for (final int summary : summaries) {
            json.writeString(result.rollupConfig().getAggregatorForId(summary));
          }
          json.writeEndArray();

          json.writeArrayFieldStart("data");
          wrote_type = true;
        }

        if (value.value() == null) {
          json.writeNull();
        } else {
          final NumericSummaryType v = ((TimeSeriesValue<NumericSummaryType>) value).value();
          json.writeStartArray();
          for (final int summary : summaries) {
            final NumericType summary_value = v.value(summary);
            if (summary_value == null) {
              json.writeNull();
            } else if (summary_value.isInteger()) {
              json.writeNumber(summary_value.longValue());
              if (ts > last_value.epoch()) {
                last_value.updateEpoch(ts);
              }
            } else {
              json.writeNumber(summary_value.doubleValue());
              if (!Double.isNaN(summary_value.doubleValue())) {
                if (ts > last_value.epoch()) {
                  last_value.updateEpoch(ts);
                }  
              }
            }
          }
          json.writeEndArray();
        }

        if (iterator.hasNext()) {
          value = iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndArray();
      json.writeEndObject();
      return wrote_type;
    }

    // NOTE: This is assuming all values have the same summaries available.

    // Rollups result would typically be a groupby and not a summarizer
    if (!(result.source() instanceof Summarizer)) {
      return writeRollupNumeric(
          (TimeSeriesValue<NumericSummaryType>) value,
          iterator, 
          json, 
          result, 
          last_value,
          wrote_values);
    }

    if (((TimeSeriesValue<NumericSummaryType>) value).value() != null) {
      Collection<Integer> summaries =
          ((TimeSeriesValue<NumericSummaryType>) value).value().summariesAvailable();

      value = (TimeSeriesValue<NumericSummaryType>) value;
      while (value != null) {
        long ts = value.timestamp().epoch();
        final String ts_string = Long.toString(ts);

        if (!wrote_values) {
          json.writeStartObject();
          wrote_values = true;
        }
        if (!wrote_type) {
          json.writeObjectFieldStart("NumericSummaryType");
          json.writeArrayFieldStart("aggregations");
          for (final int summary : summaries) {
            json.writeString(result.rollupConfig().getAggregatorForId(summary));
          }
          json.writeEndArray();

          json.writeArrayFieldStart("data");
          wrote_type = true;
        }
        if (value.value() == null) {
          json.writeNullField(ts_string);
        } else {
          json.writeStartObject();
          final NumericSummaryType v = ((TimeSeriesValue<NumericSummaryType>) value).value();
          json.writeArrayFieldStart(ts_string);
          for (final int summary : summaries) {
            final NumericType summary_value = v.value(summary);
            if (summary_value == null) {
              json.writeNull();
            } else if (summary_value.isInteger()) {
              json.writeNumber(summary_value.longValue());
              if (ts > last_value.epoch()) {
                last_value.updateEpoch(ts);
              }
            } else {
              json.writeNumber(summary_value.doubleValue());
              if (!Double.isNaN(summary_value.doubleValue())) {
                if (ts > last_value.epoch()) {
                  last_value.updateEpoch(ts);
                }
              }
            }
          }
          json.writeEndArray();
          json.writeEndObject();
        }

        if (iterator.hasNext()) {
          value = iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndArray();
      json.writeEndObject();
    }
    return wrote_type;
  }

  private boolean writeNumericArray(
      TimeSeriesValue<NumericArrayType> value,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      final TimeStamp last_value,
      boolean wrote_values) throws IOException {

    if (value.value().end() < 1) {
      // no data
      return false;
    }

    // we can assume here that we have a time spec as we can't get arrays
    // without it.
    TimeStamp ts = result.timeSpecification().start().getCopy();
    boolean wrote_type = false;
    for (int i = value.value().offset(); i < value.value().end(); i++) {
      if (!wrote_values) {
        json.writeStartObject();
        wrote_values = true;
      }
      if (!wrote_type) {
        json.writeArrayFieldStart("NumericType"); // yeah, it's numeric.
        wrote_type = true;
      }
      if (value.value().isInteger()) {
        json.writeNumber(value.value().longArray()[i]);
        if (ts.compare(Op.GT, last_value)) {
          last_value.update(ts);
        }
      } else {
        json.writeNumber(value.value().doubleArray()[i]);
        if (!Double.isNaN(value.value().doubleArray()[i])) {
          if (ts.compare(Op.GT, last_value)) {
            last_value.update(ts);
          }
        }
      }
      ts.add(result.timeSpecification().interval());
    }
    json.writeEndArray();
    return wrote_type;
  }

  private void writeEventGroup(EventsGroupValue eventsGroupValue, final JsonGenerator json,
      final TimeSeriesStringId id)
      throws IOException {
    json.writeObjectFieldStart("EventsGroupType");
    if (eventsGroupValue.group() != null) {
      json.writeObjectFieldStart("group");
      for (Map.Entry<String, String> e : eventsGroupValue.group().entrySet()) {
        json.writeStringField(e.getKey(), String.valueOf(e.getValue()));
      }
      json.writeEndObject();
    }

    json.writeObjectFieldStart("event");
    writeEvents(eventsGroupValue.event(), json);
    json.writeObjectFieldStart("tags");
    for (final Entry<String, String> entry : id.tags().entrySet()) {
      json.writeStringField(entry.getKey(), entry.getValue());
    }
    json.writeEndObject();
    json.writeEndObject();

    json.writeEndObject();
  }
  
  private void writeEvents(EventsValue eventsValue, final JsonGenerator json) throws IOException {
    json.writeStringField("namespace", eventsValue.namespace());
    json.writeStringField("source", eventsValue.source());
    json.writeStringField("title", eventsValue.title());
    json.writeStringField("message", eventsValue.message());
    json.writeStringField("priority", eventsValue.priority());
    json.writeStringField("timestamp", Long.toString(eventsValue.timestamp().epoch()));
    json.writeStringField("endTimestamp", Long.toString(eventsValue.endTimestamp().epoch()));
    json.writeStringField("userId", eventsValue.userId());
    json.writeBooleanField("ongoing", eventsValue.ongoing());
    json.writeStringField("eventId", eventsValue.eventId());
    json.writeEndArray();

    if (eventsValue.additionalProps() != null) {
      json.writeObjectFieldStart("additionalProps");
      for (Map.Entry<String, Object> e : eventsValue.additionalProps().entrySet()) {
        json.writeStringField(e.getKey(), String.valueOf(e.getValue()));
      }
      json.writeEndObject();
    }

  }
  
  private void writeStatus(StatusValue statusValue, final JsonGenerator json) throws IOException {

    byte[] statusCodeArray = statusValue.statusCodeArray();
    if (null == statusCodeArray) {
      json.writeNumberField("statusCode", statusValue.statusCode());
    } else {
      json.writeArrayFieldStart("statusCodeArray");
      for (byte code : statusCodeArray) {
        json.writeNumber(code);
      }
      json.writeEndArray();
    }

    TimeStamp[] timeStampArray = statusValue.timestampArray();
    if (null == timeStampArray) {
      json.writeNumberField("lastUpdateTime", statusValue.lastUpdateTime().msEpoch());
    } else {
      json.writeArrayFieldStart("timestampArray");
      for (TimeStamp timeStamp : timeStampArray) {
        json.writeNumber(timeStamp.epoch());
      }
      json.writeEndArray();
    }

    json.writeNumberField("statusType", statusValue.statusType());
    json.writeStringField("message", statusValue.message());
    json.writeStringField("application", statusValue.application());
  }
  
  public class JsonCachedResult implements ReadCacheQueryResult {

    /** The node that owns us. */
    private final QueryNode node;
    
    /** The name of this data source. */
    private String data_source;
    
    /** The time spec parsed out. */
    private TimeSpecification time_spec;
    
    /** The list of series we found. */
    private List<TimeSeries> series;
    
    /** An optional exception. */
    private Exception exception;
    
    /** An optional rollup config from summaries. */
    private RollupConfig rollup_config;
    
    private TimeStamp last_value_ts;

    /** Object Mapper for serdes. */
    private final ObjectMapper mapper = new ObjectMapper();
    
    /**
     * Default ctor without an exception.
     * @param context The non-null context.
     * @param root The non-null root node.
     */
    JsonCachedResult(final QueryNode node,
                     final JsonNode root, 
                     final RollupConfig rollup_config) {
      this(node, root, rollup_config, null);
    }
    
    /**
     * Ctor with an exception. If the exception isn't null then the root 
     * must be set.
     * @param context The non-null context.
     * @param root The root node. Cannot be null if the exception is null.
     * @param exception An optional exception.
     */
    JsonCachedResult(final QueryNode node,
                     final JsonNode root, 
                     final RollupConfig rollup_config,
                     final Exception exception) {
      try {
        String temp = root.get("source").asText();
        data_source = temp.substring(temp.indexOf(":") + 1);
        this.node = new CachedQueryNode(temp.substring(0, temp.indexOf(":")), 
            node);
        
        this.exception = exception;
        this.rollup_config = rollup_config;
        if (exception == null) {
          
          JsonNode n = root.get("timeSpecification");
          if (n != null && !n.isNull()) {
            time_spec = new TimeSpec(n);
          }
          
          n = root.get("lastValueTimestamp");
          if (n != null && !n.isNull()) {
            last_value_ts = new SecondTimeStamp(n.asInt());
          } else {
            throw new RuntimeException("Missing last value timestamp!");
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
        }
      } catch (Exception e) {
        LOG.error("Failed to parse cache result: " + root, e);
        throw new QueryExecutionException("Cache exception", 500, e);
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
    public String dataSource() {
      return data_source;
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
      return false;
    }

    class TimeSpec implements TimeSpecification {

      private final TimeStamp start;
      private final TimeStamp end;
      private final String string_interval;
      private final TemporalAmount interval;
      private final ChronoUnit units;
      private final ZoneId time_zone;
      
      TimeSpec(final JsonNode node) {
        start = new SecondTimeStamp(node.get("start").asLong());
        end = new SecondTimeStamp(node.get("end").asLong());
        string_interval = node.get("interval").asText();
        if (string_interval.toLowerCase().equals("0all")) {
          interval = null;
        } else {
          interval = DateTime.parseDuration2(string_interval);
        }
        // TODO - get the proper units.
        //units = ChronoUnit(node.get("units").asText());
        units = null;
        JsonNode n = node.get("timeZone");
        if (n != null && !n.isNull()) {
          time_zone = ZoneId.of(n.asText());
        } else {
          time_zone = Const.UTC;
        }
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
            builder.addTags(entry.getKey(), entry.getValue().asText());
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
        }

        temp = node.get("EventsGroupType");
        if (temp != null && !temp.isNull()) {
          types.add(EventGroupType.TYPE);
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
      
    }

    @Override
    public TimeStamp lastValueTimestamp() {
      return last_value_ts;
    }
  }
  
  @Override
  public String type() {
    return "JSONReadCacheSerdes";
  }

  @Override
  public String id() {
    return "JSONReadCacheSerdes";
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public ReadCacheSerdes getSerdes() {
    return this;
  }

  @Override
  public byte[][] serialize(final int[] timestamps, 
                            final Collection<QueryResult> results) {
    final byte[][] cache_data = new byte[timestamps.length][];
    final QR[] serializers = new QR[results.size()];
    QueryPipelineContext ctx = null;
    int i = 0;
    for (final QueryResult result : results) {
      if (ctx == null) {
        ctx = result.source().pipelineContext();
      }
      serializers[i++] = new QR(result);
    }
    
    for (i = 0; i < timestamps.length; i++) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      JsonGenerator json;
      try {
        json = JSON.getFactory().createGenerator(baos);
        json.writeStartObject();
        json.writeArrayFieldStart("results");
        
        int start = timestamps[i];
        int end = 0;
        if (i + 1 >= timestamps.length) {
          long delta = (long) timestamps[i] - (long) timestamps[i - 1];
          end = (int) (start + delta);
        } else {
          end = timestamps[i + 1];
        }
        for (int x = 0; x < serializers.length; x++) {
          serializers[x].serialize(json, start, end);
        }
        
        json.writeEndArray();
        json.writeEndObject();
        json.close();
        cache_data[i] = baos.toByteArray();
        
        if (ctx != null && ctx.query().isTraceEnabled()) {
          ctx.queryContext().logTrace("Caching sub segment: " 
              + new String(cache_data[i], Const.UTF8_CHARSET));
        }
      } catch (Throwable t) {
        LOG.error("WTF?", t);
      }
    }
    
    return cache_data;
  }
  
  class QR {
    TimeSeries[] series;
    TypedTimeSeriesIterator[] iterators;
    TimeSeriesValue<? extends TimeSeriesDataType>[] values;
    QueryResult result;
    
    QR(QueryResult result) {
      this.result = result;
      final int size = result.timeSeries().size();
      series = new TimeSeries[size];
      iterators = new TypedTimeSeriesIterator[size];
      values = new TimeSeriesValue[size]; 
      
      int i = 0;
      for (final TimeSeries ts : result.timeSeries()) {
        series[i] = ts;
        // TODO - temp, just the first
        iterators[i] = ts.iterators().iterator().next();
        if (iterators[i].hasNext()) {
          values[i] = (TimeSeriesValue<? extends TimeSeriesDataType>) iterators[i].next();
        }
        i++;
      }
    }
    
    void clear(int index) {
      values[index] = null;
      iterators[index] = null;
      series[index].close();
      series[index] = null;
    }
    
    void serialize(final JsonGenerator json, final int start, final int end) 
        throws IOException {
      json.writeStartObject();
      json.writeStringField("source", result.source().config().getId() + ":" 
          + result.dataSource());
      
      TimeStamp last_value = new SecondTimeStamp(0);
      if (result.timeSpecification() != null) {
        json.writeObjectFieldStart("timeSpecification");
        // TODO - ms, second, nanos, etc
        json.writeNumberField("start", start);
        json.writeNumberField("end", end);
        json.writeStringField("intervalISO", result.timeSpecification().interval() != null ?
            result.timeSpecification().interval().toString() : "null");
        json.writeStringField("interval", result.timeSpecification().stringInterval());
        if (result.timeSpecification().timezone() != null) {
          json.writeStringField("timeZone", result.timeSpecification().timezone().toString());
        }
        json.writeStringField("units", result.timeSpecification().units() != null ?
            result.timeSpecification().units().toString() : "null");
        json.writeEndObject();
      }
      
      // data
      json.writeArrayFieldStart("data");
      
      // loop de loop
      for (int i = 0; i < iterators.length; i++) {
        if (values[i] == null) {
          continue;
        }
        
        // no data for this segment, skip it.
        if (values[i].timestamp().epoch() > end) {
          continue;
        }
        
        if (values[i].timestamp().epoch() < start && iterators[i].getType() 
            != NumericArrayType.TYPE) {
          while (values[i] != null && values[i].timestamp().epoch() < start) {
            if (iterators[i].hasNext()) {
              values[i] = (TimeSeriesValue<? extends TimeSeriesDataType>) 
                  iterators[i].next();
            } else {
              clear(i);
            }
          }
        }
        
        
        // null check again... sniff
        if (values[i] == null) {
          continue;
        }
        
        boolean wrote_data = false;
        // got something to write!
        if (iterators[i].getType() == NumericType.TYPE) {
          if (result.timeSpecification() != null) {
            StringBuilder buf = new StringBuilder();
            int z = 0;
            long delta = (long) start - result.timeSpecification().start().epoch();
            long interval = result.timeSpecification().interval().get(ChronoUnit.SECONDS);
            int array_ts = start;
            while (array_ts < end) {
              if (values[i].timestamp().epoch() == array_ts) {
                if (z++ > 0) {
                  buf.append(",");
                }
                if (((NumericType) values[i].value()).isInteger()) {
                  wrote_data = true;
                  last_value.update(values[i].timestamp());
                  buf.append(Long.toString(((NumericType) values[i].value()).longValue()));
                } else {
                  if (!Double.isNaN(((NumericType) values[i].value()).doubleValue())) {
                    wrote_data = true;
                    last_value.update(values[i].timestamp());
                  }
                  buf.append(Double.toString(((NumericType) values[i].value()).doubleValue()));
                }
                if (iterators[i].hasNext()) {
                  values[i] = (TimeSeriesValue<? extends TimeSeriesDataType>) iterators[i].next();
                } else {
                  clear(i);
                }
              } else {
                // fill
                if (z++ > 0) {
                  buf.append(",");
                }
                buf.append("\"NaN\"");
              }
              array_ts += interval;
            }
            
            if (wrote_data) {
              json.writeStartObject();
              json.writeArrayFieldStart("NumericType");
              json.writeRaw(buf.toString());
              json.writeEndArray();
            }
          } else {
            // regular
            json.writeStartObject();
            json.writeObjectFieldStart("NumericType");
            while (true) {
              TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) 
                  values[i];
              if (v.timestamp().epoch() >= end) {
                break;
              }
              
              if (v.value().isInteger()) {
                json.writeNumberField(Long.toString(v.timestamp().epoch()), 
                    v.value().longValue());
              } else {
                json.writeNumberField(Long.toString(v.timestamp().epoch()), 
                    v.value().doubleValue());
              }
              if (iterators[i].hasNext()) {
                values[i] = (TimeSeriesValue<? extends TimeSeriesDataType>) 
                    iterators[i].next();
              } else {
                //clear(i);
                break;
              }
            }
            json.writeEndObject();
            wrote_data = true;
          }
        } else if (iterators[i].getType() == NumericArrayType.TYPE) {
          // offset
          long delta = (long) start - result.timeSpecification().start().epoch();
          long interval = result.timeSpecification().interval().get(
              ChronoUnit.SECONDS);
          int offset = (int) (delta / interval);
          int array_ts = start;
          if (((NumericArrayType) values[i].value()).isInteger()) {
            long[] data = ((NumericArrayType) values[i].value()).longArray();
            wrote_data = true;
            json.writeStartObject();
            json.writeArrayFieldStart("NumericType");
            while (array_ts < end) {
              json.writeNumber(data[offset++]);
              array_ts += (int) interval;
            }
            json.writeEndArray();
          } else {
            double[] data = ((NumericArrayType) values[i].value()).doubleArray();
            StringBuilder buf = new StringBuilder();
            int z = 0;
            while (array_ts < end) {
              if (z++ > 0) {
                buf.append(",");
              }
              if (!Double.isNaN(data[offset])) {
                wrote_data = true;
              }
              buf.append(Double.toString(data[offset++]));
              array_ts += (int) interval;
            }
            
            if (wrote_data) {
              json.writeStartObject();
              json.writeArrayFieldStart("NumericType");
              json.writeRaw(buf.toString());
              json.writeEndArray();
            }
          }
        }
        
        if (wrote_data) {
          json.writeStringField("metric", 
              ((TimeSeriesStringId) series[i].id()).metric());
          json.writeObjectFieldStart("tags");
          for (final Entry<String, String> entry : 
              ((TimeSeriesStringId) series[i].id()).tags().entrySet()) {
            json.writeStringField(entry.getKey(), entry.getValue());
          }
          json.writeEndObject();
          json.writeEndObject();
        }
        
      } // end serdes loop
      
      
      json.writeEndArray();  // end of data
      json.writeNumberField("lastValueTimestamp", last_value.epoch());
      json.writeEndObject(); // end of result
    }
  }
}
