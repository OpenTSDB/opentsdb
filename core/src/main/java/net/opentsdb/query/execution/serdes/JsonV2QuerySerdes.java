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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
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
import net.opentsdb.data.TimeStamp.Op;
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
public class JsonV2QuerySerdes implements TimeSeriesSerdes, TSDBPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      JsonV2QuerySerdes.class);
  
  /** The JSON generator used for writing. */
  private final JsonGenerator json;
  
  /**
   * Unused ctor for the plugin for now. TEMP!
   */
  public JsonV2QuerySerdes() {
    json = null;
  }
  
  /**
   * Default ctor.
   * @param generator A non-null JSON generator.
   */
  public JsonV2QuerySerdes(final JsonGenerator generator) {
    if (generator == null) {
      throw new IllegalArgumentException("Generator can not be null.");
    }
    this.json = generator;
  }
  
  @SuppressWarnings("unchecked")
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
    if (options != null) {
      if (!(options instanceof JsonV2QuerySerdesOptions)) {
        throw new IllegalArgumentException("Options were of the wrong type: " 
            + options.getClass());
      }
      opts = (JsonV2QuerySerdesOptions) options;
    } else {
      opts = null;
    }
    final net.opentsdb.query.pojo.TimeSeriesQuery query = 
        (net.opentsdb.query.pojo.TimeSeriesQuery) context.query();
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
          int idx = 0;
          for (final TimeSeries series : 
            series != null ? series : result.timeSeries()) {
            
            final TypeToken<? extends TimeSeriesDataType> type;
            Optional<Iterator<TimeSeriesValue<?>>> optional = 
                series.iterator(NumericType.TYPE);
            if (!optional.isPresent()) {
              optional = series.iterator(NumericSummaryType.TYPE);
              if (!optional.isPresent()) {
                continue;
              } else {
                type = NumericSummaryType.TYPE;
              }
            } else {
              type = NumericType.TYPE;
            }
            
            final Iterator<TimeSeriesValue<?>> iterator = optional.get();
            if (!iterator.hasNext()) {
              continue;
            }
            TimeSeriesValue<? extends TimeSeriesDataType> value = 
                (TimeSeriesValue<TimeSeriesDataType>) iterator.next();
            while (value != null && value.timestamp().compare(
                Op.LT, query.getTime().startTime())) {
              if (iterator.hasNext()) {
                value = (TimeSeriesValue<NumericType>) iterator.next();
              } else {
                value = null;
              }
            }
            
            if (value == null) {
              continue;
            }
            if (value.timestamp().compare(Op.LT, 
                      query.getTime().startTime()) ||
                value.timestamp().compare(Op.GT, 
                      query.getTime().endTime())) {
              continue;
            }
            
            json.writeStartObject();
            
            final TimeSeriesStringId id;
            if (ids != null) {
              id = (ids.get(idx++));
            } else {
              id = (TimeSeriesStringId) series.id();
            }
            
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
            
            long ts = 0;
            while(value != null) {
              if (value.timestamp().compare(Op.GT, 
                  query.getTime().endTime())) {
                break;
              }
              ts = (opts != null && opts.msResolution()) 
                  ? value.timestamp().msEpoch() 
                  : value.timestamp().msEpoch() / 1000;
              final String ts_string = Long.toString(ts);
              if (value.value() == null) {
                json.writeNullField(ts_string);
              } else if (type == NumericType.TYPE) {
                if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
                  json.writeNumberField(ts_string, 
                      ((TimeSeriesValue<NumericType>) value).value().longValue());
                } else {
                  json.writeNumberField(ts_string, 
                      ((TimeSeriesValue<NumericType>) value).value().doubleValue());
                }
              } else if (type == NumericSummaryType.TYPE) {
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
                value = (TimeSeriesValue<NumericType>) iterator.next();
              } else {
                value = null;
              }
            }
            
            json.writeEndObject();
            json.writeEndObject();
            json.flush();
          }
          
          json.flush();
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

  @Override
  public String id() {
    return "JsonV2QuerySerdes";
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

}
