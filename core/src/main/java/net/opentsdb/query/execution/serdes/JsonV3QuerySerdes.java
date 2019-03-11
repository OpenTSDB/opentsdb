// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.execution.serdes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.query.processor.summarizer.Summarizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.opentsdb.common.Const;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery.LogLevel;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

public class JsonV3QuerySerdes implements TimeSeriesSerdes {
  private static final Logger LOG = LoggerFactory.getLogger(
      JsonV3QuerySerdes.class);
  
  /** The query context. */
  private final QueryContext context;

  /** The options for this serialization. */
  private final SerdesOptions options;
  
  /** The generator. */
  private final JsonGenerator json;

  /** The query start and end timestamps. */
  private final TimeStamp start;
  private final TimeStamp end;
  
  /** Whether or not we've serialized the first result set. */
  private boolean initialized;
  
  /** TEMP */
  private Map<String, TIntObjectMap<SetWrapper>> partials = Maps.newHashMap();
  private Map<String, TLongSet> ids = Maps.newHashMap();
  
  /**
   * Default ctor.
   */
  public JsonV3QuerySerdes(final QueryContext context,
                           final SerdesOptions options,
                           final OutputStream stream) {
    if (options == null) {
      throw new IllegalArgumentException("Options cannot be null.");
    }
    if (!(options instanceof JsonV2QuerySerdesOptions)) {
      throw new IllegalArgumentException("Options must be an instance of "
          + "JsonV2QuerySerdesOptions.");
    }
    if (stream == null) {
      throw new IllegalArgumentException("Stream cannot be null.");
    }
    this.context = context;
    this.options = options;
    try {
      json = JSON.getFactory().createGenerator(stream);
    } catch (IOException e) {
      throw new RuntimeException("WTF? Failed to instantiate a JSON "
          + "generator", e);
    }
    start = context.query().startTime();
    end = context.query().endTime();
  }
  
  // TODO - find a better way to not sync
  @Override
  public synchronized Deferred<Object> serialize(final QueryResult result, 
                                                 final Span span) {
    if (result == null) {
      throw new IllegalArgumentException("Data may not be null.");
    }
    final JsonV2QuerySerdesOptions opts = (JsonV2QuerySerdesOptions) options;
    
    if (!initialized) {
      try {
        json.writeStartObject();
        json.writeArrayFieldStart("results");
      } catch (IOException e) {
        throw new RuntimeException("WTF?", e);
      }
      initialized = true;
    }
    
    final List<TimeSeries> series;
    final List<Deferred<TimeSeriesStringId>> deferreds;
    if (result.idType() == Const.TS_BYTE_ID) {
      series = Lists.newArrayList(result.timeSeries());
      deferreds = Lists.newArrayListWithCapacity(series.size());
      for (final TimeSeries ts : result.timeSeries()) {
        deferreds.add(((TimeSeriesByteId) ts.id()).decode(false, span));
      }
    } else {
      series = null;
      deferreds = null;
    }
    
    /**
     * Performs the serialization after determining if the serializations
     * need to resolve series IDs.
     */
    class ResolveCB implements Callback<Object, ArrayList<TimeSeriesStringId>> {

      @Override
      public Object call(final ArrayList<TimeSeriesStringId> ids) 
            throws Exception {
        try {
          json.writeStartObject();
          json.writeStringField("source", result.source().config().getId() + ":" + result.dataSource());
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
            //json.writeNumberField("intervalNumeric", result.timeSpecification().interval().get(result.timeSpecification().units()));
            if (result.timeSpecification().timezone() != null) {
              json.writeStringField("timeZone", result.timeSpecification().timezone().toString());
            }
            json.writeStringField("units", result.timeSpecification().units() != null ? 
                result.timeSpecification().units().toString() : "null");
            json.writeEndObject();
          }
          
          json.writeArrayFieldStart("data");
          int idx = 0;
          
          if (opts.getParallelThreshold() > 0 && 
              result.timeSeries().size() > opts.getParallelThreshold()) {
            final List<Pair<Integer, TimeSeries>> pairs = 
                Lists.newArrayListWithExpectedSize(result.timeSeries().size());
            idx = 0;
            for (final TimeSeries ts : result.timeSeries()) {
              pairs.add(new Pair<Integer, TimeSeries>(idx++, ts));
            }

            final List<String> sets =
                Lists.newArrayListWithExpectedSize(result.timeSeries().size());
            pairs.stream().parallel().forEach((pair) -> {
              try {
                serializeSeries(opts, 
                    pair.getValue(), 
                    ids != null ? ids.get(pair.getKey()) : 
                      (TimeSeriesStringId) pair.getValue().id(),
                    json, 
                    null, 
                    result);
              } catch (Exception e) {
                LOG.error("Failed to serialize ts: " + series, e);
                throw new QueryExecutionException("Unexpected exception "
                    + "serializing ts: " + series, 0, e);
              }
            });
            
            idx = 0;
            for (final String set : sets) {
              if (idx++ > 0) {
                json.writeRaw(",");
              }
              json.writeRaw(set);
            }
          } else {
            for (final TimeSeries series :
              series != null ? series : result.timeSeries()) {
              serializeSeries(opts, 
                  series,
                  ids != null ? ids.get(idx++) : (TimeSeriesStringId) series.id(),
                  json, 
                  null, 
                  result);
            }
          }
          // end of the data array
          json.writeEndArray();
          
          json.writeEndObject();
        } catch (Exception e) {
          LOG.error("Unexpected exception", e);
          return Deferred.fromError(new QueryExecutionException(
              "Unexpected exception "
              + "serializing: " + result, 500, e));
        }
        return Deferred.fromResult(null);
      }
      
    }
    
    class ErrorCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception ex) throws Exception {
        if (ex instanceof DeferredGroupException) {
          throw (Exception) Exceptions.getCause((DeferredGroupException) ex);
        }
        throw ex;
      }
    }
    
    try {
      if (deferreds != null) {
        return Deferred.group(deferreds)
          .addCallback(new ResolveCB())
          .addErrback(new ErrorCB());
      } else {
        return Deferred.fromResult(new ResolveCB().call(null));
      }
    } catch (InterruptedException e) {
      throw new QueryExecutionException("Failed to resolve IDs", 500, e);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
      throw new QueryExecutionException("Failed to resolve IDs", 500, e);
    }
  }

  @Override
  public void serializeComplete(final Span span) {
    try {
      // TODO - other bits like the query and trace data
      json.writeEndArray();
      
      if (context.query().getLogLevel() != LogLevel.OFF) {
        json.writeArrayFieldStart("log");
        for (final String log : context.logs()) {
          json.writeString(log);
        }
        json.writeEndArray();
      }
      
      json.writeEndObject();
      json.flush();
    } catch (IOException e) {
      throw new QueryExecutionException("Failure closing serializer", 500, e);
    }
  }
  
  @Override
  public Deferred<Object> serialize(final PartialTimeSeries series, 
                                    final Span span) {
    // TODO - break out
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = 0;
    try {
      // simple serdes for a json fragment.
      final TypedTimeSeriesIterator iterator = series.iterator();
      while (iterator.hasNext()) {
        if (count++ > 0) {
          stream.write(',');
        }
        TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) iterator.next();
        stream.write('"');
        stream.write(Long.toString(v.timestamp().epoch()).getBytes());
        stream.write('"');
        stream.write(':');
        if (v.value() == null) {
          stream.write("null".getBytes());
        } else if (v.value().isInteger()) {
          stream.write(Long.toString(v.value().longValue()).getBytes());
        } else {
          if (Double.isNaN(v.value().doubleValue())) {
            stream.write("NaN".getBytes());
          } else {
            // TODO - check for finite
            stream.write(Double.toString(v.value().doubleValue()).getBytes());
          }
        }
      }
//      try {
//        iterator.close();
//      } catch (Exception e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    synchronized (this) {
      final String set_id = series.set().node().config().getId() + ":" 
          + series.set().dataSource();
      TLongSet group_ids = ids.get(set_id);
      if (group_ids == null) {
        group_ids = new TLongHashSet();
        ids.put(set_id, group_ids);
      }
      group_ids.add(series.idHash());
      
      TIntObjectMap<SetWrapper> source_shards = partials.get(set_id);
      if (source_shards == null) {
        source_shards = new TIntObjectHashMap<SetWrapper>();
        partials.put(set_id, source_shards);
      }
      
      SetWrapper set = source_shards.get((int) series.set().start().epoch());
      if (set == null) {
        set = new SetWrapper();
        source_shards.put((int) series.set().start().epoch(), set);
      }
      set.series.put(series.idHash(), stream.toByteArray());

      try {
        series.close();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
      if (set.set == null) {
        return Deferred.fromResult(null);
      } else if (set.series.size() != set.set.timeSeriesCount()) {
        return Deferred.fromResult(null);
      }    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Object> complete(final PartialTimeSeriesSet set, 
                                   final Span span) {
    final String set_id = set.node().config().getId() + ":" + set.dataSource();
    synchronized (this) {
      TIntObjectMap<SetWrapper> source_shards = partials.get(set_id);
      if (source_shards == null) {
        source_shards = new TIntObjectHashMap<SetWrapper>();
        partials.put(set_id, source_shards);
      }
      
      SetWrapper wrapper = source_shards.get((int) set.start().epoch());
      if (wrapper == null) {
        wrapper = new SetWrapper();
        wrapper.set = set;
        source_shards.put((int) set.start().epoch(), wrapper);
      } else {
        wrapper.set = set;
      }
      
      if (source_shards.size() != set.totalSets()) {
        return Deferred.fromResult(null);
      }
      
      // TODO don't loop every time, maintain counter outside
      for (final TIntObjectMap<SetWrapper> entry : partials.values()) {
        for (final SetWrapper w : entry.valueCollection()) {
          if (wrapper.set == null) {
            return Deferred.fromResult(null);
          }
           if (wrapper.series.size() != w.set.timeSeriesCount()) {
            return Deferred.fromResult(null);
          }
        }
      }
    }
    
    serializePush();
    return Deferred.fromResult(true);
  }
  
  @Override
  public void deserialize(final QueryNode node, 
                          final Span span) {
    node.onError(new UnsupportedOperationException("Not implemented for this "
        + "class: " + getClass().getCanonicalName()));
  }

  private void serializeSeries(
      final JsonV2QuerySerdesOptions options,
      final TimeSeries series,
      final TimeSeriesStringId id,
      JsonGenerator json,
      final List<String> sets,
      final QueryResult result) throws IOException {
    
    final ByteArrayOutputStream baos;
    if (json == null) {
      baos = new ByteArrayOutputStream();
      json = JSON.getFactory().createGenerator(baos);
    } else {
      baos = null;
    }
    
    boolean wrote_values = false;
    for (final TypedTimeSeriesIterator iterator : series.iterators()) {
      while (iterator.hasNext()) {

        TimeSeriesValue<? extends TimeSeriesDataType> value = iterator.next();
        while (value != null && value.timestamp().compare(Op.LT, start)) {
          if (iterator.hasNext()) {
            value = iterator.next();
          } else {
            value = null;
          }
        }

        if (value == null) {
          continue;
        }
        if (value.timestamp().compare(Op.LT, start) || value.timestamp().compare(Op.GT, end)) {
          continue;
        }

        if (iterator.getType() == NumericType.TYPE) {
          if (writeNumeric((TimeSeriesValue<NumericType>) value, options, iterator, json, result, wrote_values)) {
            wrote_values = true;
          }
        } else if (iterator.getType() == NumericSummaryType.TYPE) {
          if (writeNumericSummary(value, options, iterator, json, result, wrote_values)) {
            wrote_values = true;
          }
        } else if (iterator.getType() == NumericArrayType.TYPE) {
          if(writeNumericArray((TimeSeriesValue<NumericArrayType>) value, options, iterator, json, result, wrote_values)) {
            wrote_values = true;
          }
        }
      }
    }
    
    if (wrote_values) {
      // serialize the ID
      json.writeStringField("metric", id.metric());
      json.writeObjectFieldStart("tags");
      for (final Entry<String, String> entry : id.tags().entrySet()) {
        json.writeStringField(entry.getKey(), entry.getValue());
      }
      json.writeEndObject();
      json.writeArrayFieldStart("aggregateTags");
      for (final String tag : id.aggregatedTags()) {
        json.writeString(tag);
      }
      json.writeEndArray();
      json.writeEndObject();
    }
    
    if (baos != null) {
      json.close();
      synchronized(sets) {
        sets.add(new String(baos.toByteArray(), Const.UTF8_CHARSET));
      }
      baos.close();
    } else {
      json.flush();
    }
  }
  
  private boolean writeNumeric(
      TimeSeriesValue<NumericType> value,
      final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final QueryResult result,
      boolean wrote_values) throws IOException {
    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      // just the values
      while (value != null) {
        if (value.timestamp().compare(Op.GT, end)) {
          break;
        }
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
          if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
            json.writeNumber(
                ((TimeSeriesValue<NumericType>) value).value().longValue());
          } else {
            json.writeNumber(
                ((TimeSeriesValue<NumericType>) value).value().doubleValue());
          }
        }
        
        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericType>) iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndArray();
      return wrote_type;
    }
    
    // timestamp and values
    while (value != null) {
      if (value.timestamp().compare(Op.GT, end)) {
        break;
      }
      long ts = (options != null && options.getMsResolution()) 
          ? value.timestamp().msEpoch() 
          : value.timestamp().msEpoch() / 1000;
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
        if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
          json.writeNumberField(ts_string, 
              ((TimeSeriesValue<NumericType>) value).value().longValue());
        } else {
          json.writeNumberField(ts_string, 
              ((TimeSeriesValue<NumericType>) value).value().doubleValue());
        }
      }
      
      if (iterator.hasNext()) {
        value = (TimeSeriesValue<NumericType>) iterator.next();
      } else {
        value = null;
      }
    }
    json.writeEndObject();
    return wrote_type;
  }

  private boolean writeRollupNumeric(
      TimeSeriesValue<NumericSummaryType> value,
      final JsonV2QuerySerdesOptions options,
      final Iterator<TimeSeriesValue<?>> iterator,
      final JsonGenerator json,
      final QueryResult result,
      boolean wrote_values) throws IOException {

    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      Collection<Integer> summaries = null;
      Integer summary = null;
      
      // just the values
      while (value != null) {
        if (value.timestamp().compare(Op.GT, end)) {
          break;
        }
        
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
                ((TimeSeriesValue<NumericSummaryType>) value).value().summariesAvailable();
            summary = summaries.iterator().next();
          }
          if (((TimeSeriesValue<NumericSummaryType>) value).value().value(summary).isInteger()) {
            json.writeNumber(
                ((TimeSeriesValue<NumericSummaryType>) value).value().value(summary).longValue());
          } else {
            json.writeNumber(
                ((TimeSeriesValue<NumericSummaryType>) value).value().value(summary).doubleValue());
          }
        }

        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndArray();
      return wrote_type;
    }

    Collection<Integer> summaries = null;
    Integer summary = null;

    // timestamp and values
    while (value != null) {
      if (value.timestamp().compare(Op.GT, end)) {
        break;
      }
      long ts = (options != null && options.getMsResolution())
          ? value.timestamp().msEpoch()
          : value.timestamp().msEpoch() / 1000;
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
            ((TimeSeriesValue<NumericSummaryType>) value).value().summariesAvailable();
        summary = summaries.iterator().next();
      }

      if (value.value() == null) {
        json.writeNullField(ts_string);
      } else {
        if (((TimeSeriesValue<NumericSummaryType>) value).value().value(summary).isInteger()) {
          json.writeNumberField(ts_string,
              ((TimeSeriesValue<NumericSummaryType>) value).value().value(summary).longValue());
        } else {
          json.writeNumberField(ts_string,
              ((TimeSeriesValue<NumericSummaryType>) value).value().value(summary).doubleValue());
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
      final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final QueryResult result,
      boolean wrote_values) throws IOException {

    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      if (!(result.source() instanceof Summarizer)) {
        return writeRollupNumeric((TimeSeriesValue<NumericSummaryType>) value, options, iterator, json,
            result, wrote_values);
      }

      Collection<Integer> summaries =
          ((TimeSeriesValue<NumericSummaryType>) value)
              .value()
              .summariesAvailable();

      value = (TimeSeriesValue<NumericSummaryType>) value;
      while (value != null) {
        if (value.timestamp().compare(Op.GT, end)) {
          break;
        }
        long ts = (options != null && options.getMsResolution()) 
            ? value.timestamp().msEpoch() 
            : value.timestamp().msEpoch() / 1000;
            
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
            } else {
              json.writeNumber(summary_value.doubleValue());
            }
          }
          json.writeEndArray();
        }
        
        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
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
      return writeRollupNumeric((TimeSeriesValue<NumericSummaryType>) value,
          options, iterator, json, result, wrote_values);
    }

    if (((TimeSeriesValue<NumericSummaryType>) value).value() != null) {
      Collection<Integer> summaries =
          ((TimeSeriesValue<NumericSummaryType>) value).value().summariesAvailable();
  
      value = (TimeSeriesValue<NumericSummaryType>) value;
      while (value != null) {
        if (value.timestamp().compare(Op.GT, end)) {
          break;
        }
        long ts = (options != null && options.getMsResolution()) 
            ? value.timestamp().msEpoch() 
            : value.timestamp().msEpoch() / 1000;
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
            } else {
              json.writeNumber(summary_value.doubleValue());
            }
          }
          json.writeEndArray();
          json.writeEndObject();
        }
        
        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
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
      final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final QueryResult result,
      boolean wrote_values) throws IOException {
    
    if (value.value().end() < 1) {
      // no data
      return false;
    }
    
    // we can assume here that we have a time spec as we can't get arrays
    // without it.
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
      } else {
        json.writeNumber(value.value().doubleArray()[i]);
      }
    }
    json.writeEndArray();
    return wrote_type;
  }

  /**
   * Scratch class used to collect the serialized time series.
   */
  class SetWrapper {
    public PartialTimeSeriesSet set;
    public TLongObjectMap<byte[]> series = new TLongObjectHashMap<byte[]>();
  }
  
  void serializePush() {
    final JsonV2QuerySerdesOptions opts = (JsonV2QuerySerdesOptions) options;
   
    try {
      json.writeStartObject();
      json.writeArrayFieldStart("results");
      
      for (final Entry<String, TIntObjectMap<SetWrapper>> entry : partials.entrySet()) {
        int[] keys = entry.getValue().keys();
        Arrays.sort(keys);
        
        final SetWrapper shard = entry.getValue().get(keys[0]);
        
        json.writeStartObject();
        json.writeStringField("source", shard.set.node().config().getId() + ":" 
            + shard.set.dataSource());
        // TODO - array of data sources
        
        // serdes time spec if present
//        if (result.timeSpecification() != null) {
//          json.writeObjectFieldStart("timeSpecification");
//          // TODO - ms, second, nanos, etc
//          json.writeNumberField("start", result.timeSpecification().start().epoch());
//          json.writeNumberField("end", result.timeSpecification().end().epoch());
//          json.writeStringField("intervalISO", result.timeSpecification().interval().toString());
//          json.writeStringField("interval", result.timeSpecification().stringInterval());
//          //json.writeNumberField("intervalNumeric", result.timeSpecification().interval().get(result.timeSpecification().units()));
//          if (result.timeSpecification().timezone() != null) {
//            json.writeStringField("timeZone", result.timeSpecification().timezone().toString());
//          }
//          json.writeStringField("units", result.timeSpecification().units().toString());
//          json.writeEndObject();
//        }
        
        json.writeArrayFieldStart("data");

        TLongSet group_ids = ids.get(entry.getKey());
        final TLongIterator iterator = group_ids.iterator();
        while (iterator.hasNext()) {
          final long id_hash = iterator.next();
          // serialize the ID
          json.writeStartObject();
          TimeSeriesId raw_id = shard.set.id(id_hash);
          if (raw_id == null) {
            // MISSING! Fill
            continue;
          }
          TimeSeriesStringId id = (TimeSeriesStringId) raw_id;
          json.writeStringField("metric", id.metric());
          json.writeObjectFieldStart("tags");
          for (final Entry<String, String> pair : id.tags().entrySet()) {
            json.writeStringField(pair.getKey(), pair.getValue());
          }
          json.writeEndObject();
          json.writeArrayFieldStart("aggregateTags");
          for (final String tag : id.aggregatedTags()) {
            json.writeString(tag);
          }
          json.writeEndArray();
          
          json.writeObjectFieldStart("NumericType");
          int count = 0;
          for (final int idx : keys) {
            final SetWrapper w = entry.getValue().get(idx);
            byte[] s = w.series.get(id_hash);
            if (s == null) {
              // TODO - fill!!
            } else {
              if (count++ > 0) {
                json.writeRaw(",");
              }
              json.writeRaw(new String(s));
            }
          }
          json.writeEndObject();
          json.writeEndObject();
        }
        json.writeEndArray();
        json.writeEndObject();
      }
      
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
  }
}
