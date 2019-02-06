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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
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
    
    json.writeStartObject();

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
          writeNumeric((TimeSeriesValue<NumericType>) value, options, iterator, json, result);
        } else if (iterator.getType() == NumericSummaryType.TYPE) {
          writeNumericSummary(value, options, iterator, json, result);
        } else if (iterator.getType() == NumericArrayType.TYPE) {
          writeNumericArray((TimeSeriesValue<NumericArrayType>) value, options, iterator, json,
              result);
        }
      }
    }
    
    json.writeEndObject();
    
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
  
  private void writeNumeric(
      TimeSeriesValue<NumericType> value,
      final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final QueryResult result) throws IOException {
    if (result.timeSpecification() != null) {
      json.writeArrayFieldStart("NumericType");
      // just the values
      while (value != null) {
        if (value.timestamp().compare(Op.GT, end)) {
          break;
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
      return;
    }
    
    // timestamp and values
    json.writeObjectFieldStart("NumericType");
    while (value != null) {
      if (value.timestamp().compare(Op.GT, end)) {
        break;
      }
      long ts = (options != null && options.getMsResolution()) 
          ? value.timestamp().msEpoch() 
          : value.timestamp().msEpoch() / 1000;
      final String ts_string = Long.toString(ts);
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
  }

  private void writeRollupNumeric(
      TimeSeriesValue<NumericSummaryType> value,
      final JsonV2QuerySerdesOptions options,
      final Iterator<TimeSeriesValue<?>> iterator,
      final JsonGenerator json,
      final QueryResult result) throws IOException {


    if (result.timeSpecification() != null) {

      final List<Integer> summaries = Lists.newArrayList(
          ((TimeSeriesValue<NumericSummaryType>) value).value().summariesAvailable());

      json.writeArrayFieldStart("NumericType");
      // just the values
      while (value != null) {
        if (value.timestamp().compare(Op.GT, end)) {
          break;
        }
        if (value.value() == null) {
          json.writeNull();
        } else {
          if (((TimeSeriesValue<NumericSummaryType>) value).value().value(summaries.get(0)).isInteger()) {
            json.writeNumber(
                ((TimeSeriesValue<NumericSummaryType>) value).value().value(summaries.get(0)).longValue());
          } else {
            json.writeNumber(
                ((TimeSeriesValue<NumericSummaryType>) value).value().value(summaries.get(0)).doubleValue());
          }
        }

        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndArray();
      return;

    }

    final List<Integer> summaries = Lists.newArrayList(
        ((TimeSeriesValue<NumericSummaryType>) value).value().summariesAvailable());

    // timestamp and values
    json.writeObjectFieldStart("NumericType");
    while (value != null) {
      if (value.timestamp().compare(Op.GT, end)) {
        break;
      }
      long ts = (options != null && options.getMsResolution())
          ? value.timestamp().msEpoch()
          : value.timestamp().msEpoch() / 1000;
      final String ts_string = Long.toString(ts);
      if (value.value() == null) {
        json.writeNullField(ts_string);
      } else {
        if (((TimeSeriesValue<NumericSummaryType>) value).value().value(summaries.get(0)).isInteger()) {
          json.writeNumberField(ts_string,
              ((TimeSeriesValue<NumericSummaryType>) value).value().value(summaries.get(0)).longValue());
        } else {
          json.writeNumberField(ts_string,
              ((TimeSeriesValue<NumericSummaryType>) value).value().value(summaries.get(0)).doubleValue());
        }
      }

      if (iterator.hasNext()) {
        value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      } else {
        value = null;
      }
    }
    json.writeEndObject();

  }
  
  private void writeNumericSummary(
      TimeSeriesValue value,
      final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final QueryResult result) throws IOException {

    if (result.timeSpecification() != null) {

      final List<Integer> summaries = Lists.newArrayList(
          ((TimeSeriesValue<NumericSummaryType>) value).value().summariesAvailable());

      if (summaries != null && summaries.size() == 1) {
        // If the number of summaries are 1, going to force a NumericType on the data
        // TODO: find a clean way of doing this
        writeRollupNumeric((TimeSeriesValue<NumericSummaryType>) value, options, iterator, json,
            result);
        return;
      }

      value = (TimeSeriesValue<NumericSummaryType>) value;

      json.writeObjectFieldStart("NumericSummaryType");
      // just the values
      // NOTE: This is assuming all values have the same summaries available.

      json.writeArrayFieldStart("aggregations");
      for (final int summary : summaries) {
        json.writeString(result.rollupConfig().getAggregatorForId(summary));
      }
      json.writeEndArray();
      
      json.writeArrayFieldStart("data");
      while (value != null) {
        if (value.timestamp().compare(Op.GT, end)) {
          break;
        }
        long ts = (options != null && options.getMsResolution()) 
            ? value.timestamp().msEpoch() 
            : value.timestamp().msEpoch() / 1000;
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
      return;
    }

    // NOTE: This is assuming all values have the same summaries available.
    final List<Integer> summaries = Lists.newArrayList(
        ((TimeSeriesValue<NumericSummaryType>) value).value().summariesAvailable());

    if (summaries != null && summaries.size() == 1) {
      writeNumeric((TimeSeriesValue<NumericType>) value,
          options, iterator, json, result);
      return;
    }

    value = (TimeSeriesValue<NumericSummaryType>) value;
    json.writeObjectFieldStart("NumericSummaryType");

    json.writeArrayFieldStart("aggregations");
    for (final int summary : summaries) {
      json.writeString(result.rollupConfig().getAggregatorForId(summary));
    }
    json.writeEndArray();
    
    json.writeArrayFieldStart("data");
    while (value != null) {
      if (value.timestamp().compare(Op.GT, end)) {
        break;
      }
      long ts = (options != null && options.getMsResolution()) 
          ? value.timestamp().msEpoch() 
          : value.timestamp().msEpoch() / 1000;
      final String ts_string = Long.toString(ts);
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



  private void writeNumericArray(
      TimeSeriesValue<NumericArrayType> value,
      final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final QueryResult result) throws IOException {
    
    if (value.value().end() < 1) {
      // no data
      return;
    }
    
    json.writeArrayFieldStart("NumericType"); // yeah, it's numeric.
    // we can assume here that we have a time spec as we can't get arrays
    // without it.
    for (int i = value.value().offset(); i < value.value().end(); i++) {
      if (value.value().isInteger()) {
        json.writeNumber(value.value().longArray()[i]);
      } else {
        json.writeNumber(value.value().doubleArray()[i]);
      }
    }
    json.writeEndArray();
  }

}
