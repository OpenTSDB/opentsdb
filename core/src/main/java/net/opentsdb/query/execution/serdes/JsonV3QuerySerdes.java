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
import java.io.InputStream;
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
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedIterator;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

public class JsonV3QuerySerdes implements TimeSeriesSerdes, TSDBPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      JsonV3QuerySerdes.class);
  
  @Override
  public String id() {
    return "V3";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
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
  public Deferred<Object> serialize(final QueryContext context, 
                                    final SerdesOptions options,
                                    final OutputStream stream, 
                                    final QueryResult result, 
                                    final Span span) {
    if (stream == null) {
      throw new IllegalArgumentException("Output stream may not be null.");
    }
    if (result == null) {
      throw new IllegalArgumentException("Data may not be null.");
    }
    final JsonV2QuerySerdesOptions opts;
    if (options == null) {
      throw new IllegalArgumentException("Options cannot be null.");
    }
     if (!(options instanceof JsonV2QuerySerdesOptions)) {
      throw new IllegalArgumentException("Options were of the wrong type: " 
          + options.getClass());
    }
    opts = (JsonV2QuerySerdesOptions) options;
    
    final JsonGenerator json;
    try {
      json = JSON.getFactory().createGenerator(stream);
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
    
    try {
      json.writeStartObject();
      
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
    
    final List<TimeSeries> series;
    final List<Deferred<TimeSeriesStringId>> deferreds;
    if (result.idType() == Const.TS_BYTE_ID) {
      series = Lists.newArrayList(result.timeSeries());
      deferreds = Lists.newArrayListWithCapacity(series.size());
      for (final TimeSeries ts : result.timeSeries()) {
        deferreds.add(((TimeSeriesByteId) ts.id()).decode(false, 
            null /* TODO - implement tracing */));
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
          // serdes time spec if present
          if (result.timeSpecification() != null) {
            json.writeObjectFieldStart("timeSpecification");
            // TODO - ms, second, nanos, etc
            json.writeNumberField("start", result.timeSpecification().start().epoch());
            json.writeNumberField("end", result.timeSpecification().end().epoch());
            json.writeStringField("intervalISO", result.timeSpecification().interval().toString());
            json.writeStringField("interval", result.timeSpecification().stringInterval());
            //json.writeNumberField("intervalNumeric", result.timeSpecification().interval().get(result.timeSpecification().units()));
            if (result.timeSpecification().timezone() != null) {
              json.writeStringField("timeZone", result.timeSpecification().timezone().toString());
            }
            json.writeStringField("units", result.timeSpecification().units().toString());
            json.writeEndObject();
          }
          
          json.writeArrayFieldStart("data");
          int idx = 0;
          
          if (opts.parallelThreshold() > 0 && 
              result.timeSeries().size() > opts.parallelThreshold()) {
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
            // end of the data array
            json.writeEndArray();
            
            // TODO - flag
            //json.writeObjectField("executionGraph", context.query());
            
            // final end of the object.
            json.writeEndObject();
            json.close();
          }
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
  public void deserialize(final SerdesOptions options, 
                          final InputStream stream,
                          final QueryNode node, 
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
    
    for (final TypedIterator<TimeSeriesValue<?>> iterator : series.iterators()) {
      if (!iterator.hasNext()) {
        continue;
      }
      
      TimeSeriesValue<? extends TimeSeriesDataType> value = iterator.next();
      while (value != null && value.timestamp().compare(
          Op.LT, options.start)) {
        if (iterator.hasNext()) {
          value = iterator.next();
        } else {
          value = null;
        }
      }
      
      if (value == null) {
        continue;
      }
      if (value.timestamp().compare(Op.LT, options.start()) ||
          value.timestamp().compare(Op.GT, options.end)) {
        continue;
      }
     
      if (iterator.getType() == NumericType.TYPE) {
        writeNumeric((TimeSeriesValue<NumericType>) value,
            options, iterator, json, result);
      } else if (iterator.getType() == NumericSummaryType.TYPE) {
        writeNumericSummary((TimeSeriesValue<NumericSummaryType>) value,
            options, iterator, json, result);
      } else if (iterator.getType() == NumericArrayType.TYPE) {
        writeNumericArray((TimeSeriesValue<NumericArrayType>) value,
            options, iterator, json, result);
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
        if (value.timestamp().compare(Op.GT, options.end())) {
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
      if (value.timestamp().compare(Op.GT, options.end())) {
        break;
      }
      long ts = (options != null && options.msResolution()) 
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
  
  private void writeNumericSummary(
      TimeSeriesValue<NumericSummaryType> value,
      final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final QueryResult result) throws IOException {
    if (result.timeSpecification() != null) {
      json.writeArrayFieldStart("NumericSummaryType");
      // just the values
      while (value != null) {
        if (value.timestamp().compare(Op.GT, options.end())) {
          break;
        }
        if (value.value() == null) {
          json.writeNull();
        } else {
          final NumericSummaryType v = ((TimeSeriesValue<NumericSummaryType>) value).value();
          json.writeStartArray();
          for (final int summary : v.summariesAvailable()) {
            final NumericType summary_value = v.value(summary);
            if (summary_value == null) {
              json.writeNumberField(Integer.toString(summary), null);
            } else if (summary_value.isInteger()) {
              json.writeNumberField(Integer.toString(summary),
                  summary_value.longValue());
            } else {
              json.writeNumberField(Integer.toString(summary),
                  summary_value.doubleValue());
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
      return;
    }
    
    json.writeObjectFieldStart("NumericSummaryType");
    while (value != null) {
      if (value.timestamp().compare(Op.GT, options.end())) {
        break;
      }
      long ts = (options != null && options.msResolution()) 
          ? value.timestamp().msEpoch() 
          : value.timestamp().msEpoch() / 1000;
      final String ts_string = Long.toString(ts);
      if (value.value() == null) {
        json.writeNullField(ts_string);
      } else {
        json.writeObjectFieldStart(ts_string);
        final NumericSummaryType v = ((TimeSeriesValue<NumericSummaryType>) value).value();
        json.writeStartArray();
        for (final int summary : v.summariesAvailable()) {
          final NumericType summary_value = v.value(summary);
          if (summary_value == null) {
            json.writeNumberField(Integer.toString(summary), null);
          } else if (summary_value.isInteger()) {
            json.writeNumberField(Integer.toString(summary),
                summary_value.longValue());
          } else {
            json.writeNumberField(Integer.toString(summary),
                summary_value.doubleValue());
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
