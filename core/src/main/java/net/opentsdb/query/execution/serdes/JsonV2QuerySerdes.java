// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
import java.util.Collection;
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
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.serdes.SerdesCallback;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

/**
 * Simple serializer that outputs the time series in the same format as
 * OpenTSDB 2.x's /api/query endpoint.
 * <b>NOTE:</b> The serializer will write the individual query results to 
 * a JSON array but will not start or close the array (so additional data
 * can be added like the summary, query, etc).
 * <b>NOTE:</b> The module will execute a JOIN() on a deferred if it has
 * to resolve byte encoded time series IDs into string IDs.
 * 
 * @since 3.0
 */
public class JsonV2QuerySerdes implements TimeSeriesSerdes {
  private static final Logger LOG = LoggerFactory.getLogger(
      JsonV2QuerySerdes.class);
  
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
  public JsonV2QuerySerdes(final QueryContext context,
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
    this.options = options;
    try {
      json = JSON.getFactory().createGenerator(stream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to instantiate a JSON "
          + "generator", e);
    }
    start = context.query().startTime();
    end = context.query().endTime();
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Deferred<Object> serialize(final QueryResult result,
                                    final Span span) {
    synchronized(this) {
      if (result == null) {
        throw new IllegalArgumentException("Data may not be null.");
      }
      final JsonV2QuerySerdesOptions opts = (JsonV2QuerySerdesOptions) options;
      
      if (!initialized) {
        try {
          json.writeStartArray();
        } catch (IOException e) {
          throw new RuntimeException("Unexpected exception: " + e.getMessage(), e);
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
                  final Collection<TypedTimeSeriesIterator> iterators = 
                      pair.getValue().iterators();
                  if (iterators.isEmpty()) {
                    return;
                  }
                  // NOTE: We should only have one here.
                  final TypedTimeSeriesIterator iterator = 
                      iterators.iterator().next();
                  if (!iterator.hasNext()) {
                    return;
                  }
                  TimeSeriesValue<? extends TimeSeriesDataType> value = 
                      (TimeSeriesValue<TimeSeriesDataType>) iterator.next();
                  while (value != null && value.timestamp().compare(Op.LT, start)) {
                    if (iterator.hasNext()) {
                      value = (TimeSeriesValue<NumericType>) iterator.next();
                    } else {
                      value = null;
                    }
                  }
                  
                  if (value == null) {
                    return;
                  }
                  if (value.timestamp().compare(Op.LT, start) ||
                      value.timestamp().compare(Op.GT, end)) {
                    return;
                  }
                  
                  final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                  final JsonGenerator json = JSON.getFactory().createGenerator(baos);
                  json.writeStartObject();
                  
                  final TimeSeriesStringId id;
                  if (ids != null) {
                    id = (ids.get(pair.getKey()));
                  } else {
                    id = (TimeSeriesStringId) pair.getValue().id();
                  }
                  
                  serializeSeries(opts, value, iterator, id, json, result);
                  json.close();
                  synchronized(sets) {
                    sets.add(new String(baos.toByteArray(), Const.UTF8_CHARSET));
                  }
                  baos.close();
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
                final Collection<TypedTimeSeriesIterator> iterators = 
                    series.iterators();
                if (iterators.isEmpty()) {
                  continue;
                }
                // NOTE: We should only have one here.
                final TypedTimeSeriesIterator iterator = 
                    iterators.iterator().next();
                if (!iterator.hasNext()) {
                  continue;
                }
                
                TimeSeriesValue<? extends TimeSeriesDataType> value = 
                    (TimeSeriesValue<TimeSeriesDataType>) iterator.next();
                while (value != null && value.timestamp().compare(
                    Op.LT, start)) {
                  if (iterator.hasNext()) {
                    value = (TimeSeriesValue<NumericType>) iterator.next();
                  } else {
                    value = null;
                  }
                }
                
                if (value == null) {
                  continue;
                }
                if (value.timestamp().compare(Op.LT, start) ||
                    value.timestamp().compare(Op.GT, end)) {
                  continue;
                }
                
                json.writeStartObject();
                
                final TimeSeriesStringId id;
                if (ids != null) {
                  id = (ids.get(idx++));
                } else {
                  id = (TimeSeriesStringId) series.id();
                }
                serializeSeries(opts, value, iterator, id, json, result);
                json.flush();
              }
            }
            
            json.flush();
          } catch (Exception e) {
            LOG.error("Unexpected exception", e);
            return Deferred.fromError(new QueryExecutionException(
                "Unexpected exception "+ "serializing: " + result, 500, e));
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
        throw new QueryExecutionException("Unexpected exception", 500, e);
      }
    }
  }

  @Override
  public void serialize(final PartialTimeSeries series, 
                        final SerdesCallback callback,
                        final Span span) {
    callback.onError(series, new UnsupportedOperationException(
        "Not implemented yet."));
  }
  
  @Override
  public void serializeComplete(final Span span) {
    try {
      json.writeEndArray();
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
      final TimeSeriesValue<?> value,
      final TypedTimeSeriesIterator iterator,
      final TimeSeriesStringId id,
      final JsonGenerator json,
      final QueryResult result) throws IOException {
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
    json.writeObjectFieldStart("dps");
    
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

    json.writeEndObject();
    json.writeEndObject();
  }
  
  private void writeNumeric(
      TimeSeriesValue<NumericType> value,
      final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final QueryResult result) throws IOException {
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
  }
  
  private void writeNumericSummary(
      TimeSeriesValue<NumericSummaryType> value,
      final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final QueryResult result) throws IOException {
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
        final Collection<Integer> summaries = 
            ((TimeSeriesValue<NumericSummaryType>) value).value()
            .summariesAvailable();
        if (!summaries.isEmpty()) {
          // JUST grab the first
          int summary = summaries.iterator().next();
          final NumericType v = ((TimeSeriesValue<NumericSummaryType>) value)
              .value().value(summary);
          if (v == null) {
            json.writeNullField(ts_string);
          } else if (v.isInteger()) {
            json.writeNumberField(ts_string, v.longValue());
          } else {
            json.writeNumberField(ts_string, v.doubleValue());
          }
        } else {
          json.writeNullField(ts_string);
        }
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
    
    final TimeStamp timestamp = 
        result.timeSpecification().start().getCopy();
    for (int i = value.value().offset(); i < value.value().end(); i++) {
      long ts = (options != null && options.getMsResolution()) 
          ? timestamp.msEpoch() 
          : timestamp.msEpoch() / 1000;
      final String ts_string = Long.toString(ts);
      if (value.value() == null) {
        json.writeNullField(ts_string);
      } else {
        if (value.value().isInteger()) {
          json.writeNumberField(ts_string, 
              value.value().longArray()[i]);
        } else {
          json.writeNumberField(ts_string, 
              value.value().doubleArray()[i]);
        }
      }
      
      timestamp.add(result.timeSpecification().interval());
    }
  }
}
