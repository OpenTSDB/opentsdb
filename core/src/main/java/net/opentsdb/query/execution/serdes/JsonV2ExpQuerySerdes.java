// This file is part of OpenTSDB.
// Copyright (C) 2012018  The OpenTSDB Authors.
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
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

/**
 * A serializer mimicking the output of OpenTSDB 2.x's /api/query/exp 
 * endpoint.
 * 
 * TODO - a lot of cleanup needed here.
 * 
 * @since 3.0
 */
public class JsonV2ExpQuerySerdes implements TimeSeriesSerdes, TSDBPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      JsonV2ExpQuerySerdes.class);
  
  /** The TSDB to which we belong for pulling out the factory. */
  private TSDB tsdb;
  
  @Override
  public String id() {
    return "JsonV2ExpQuerySerdes";
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb) {
    this.tsdb = tsdb;
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
    opts = (JsonV2QuerySerdesOptions) options;
    
    final JsonGenerator json;
    try {
      json = JSON.getFactory().createGenerator(stream);
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
    
    try {
      json.writeStartObject();
      json.writeArrayFieldStart("outputs");
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
        
        // TODO - proper interpolation config
        // TODO - interpolation config for summaries.
        NumericInterpolatorConfig nic = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
            .setFillPolicy(FillPolicy.NOT_A_NUMBER)
            .setRealFillPolicy(FillWithRealPolicy.NONE)
            .setType(NumericType.TYPE.toString())
            .build();
        
        QueryInterpolatorFactory factory = tsdb
            .getRegistry().getPlugin(QueryInterpolatorFactory.class, 
                nic.id());
        
        if (factory == null) {
          throw new IllegalArgumentException("No interpolator factory found for: " + 
              nic.id() == null ? "Default" : nic.id());
        }
        
        try {
          int idx = 0;
          final Map<String, List<Pair<TimeSeriesStringId, TimeSeries>>> outputs = Maps.newHashMap();
          
          for (final TimeSeries series : 
              series != null ? series : result.timeSeries()) {
            final TimeSeriesStringId id;
            if (ids != null) {
              id = (ids.get(idx++));
            } else {
              id = (TimeSeriesStringId) series.id();
            }
            
            List<Pair<TimeSeriesStringId, TimeSeries>> s = outputs.get(id.alias());
            if (s == null) {
              s = Lists.newArrayList();
              outputs.put(id.alias(), s);
            }
            s.add(new Pair<>(id, series));
          }
          
          
          for (final Entry<String, List<Pair<TimeSeriesStringId, TimeSeries>>> entry : outputs.entrySet()) {
            List<QueryInterpolator<?>> iterators = Lists.newArrayList();
            TimeStamp next_ts = new MillisecondTimeStamp(0);
            TimeStamp next_next_ts = new MillisecondTimeStamp(0);
            next_ts.setMax();
            
            json.writeStartObject();
            json.writeStringField("id", entry.getKey() == null ? "null!?!?!" : entry.getKey());
            json.writeArrayFieldStart("dps");
            boolean has_next = false;
            for (final Pair<TimeSeriesStringId, TimeSeries> pair : entry.getValue()) {
              final QueryInterpolator<?> interp;
              Optional<Iterator<TimeSeriesValue<?>>> optional = 
                  pair.getValue().iterator(NumericType.TYPE);
              if (!optional.isPresent()) {
                optional = pair.getValue().iterator(NumericSummaryType.TYPE);
                if (!optional.isPresent()) {
                  continue;
                }
                final Iterator<TimeSeriesValue<?>> iterator = optional.get();
                if (!iterator.hasNext()) {
                  continue;
                }
                interp = (QueryInterpolator<NumericSummaryType>) 
                    factory.newInterpolator(NumericSummaryType.TYPE, iterator, nic);
              } else {
                final Iterator<TimeSeriesValue<?>> iterator = optional.get();
                if (!iterator.hasNext()) {
                  continue;
                }
                interp = (QueryInterpolator<NumericType>) 
                    factory.newInterpolator(NumericType.TYPE, iterator, nic);
              }
              
              // TODO - filter on start timestamp.
              if (interp.hasNext()) {
                has_next = true;
                next_ts.update(interp.nextReal());
                iterators.add(interp);
              }
            }
            
            TimeStamp first_ts = next_ts.getCopy();
            int set_count = 0;
            while (has_next) {
              
              // TODO - filter on end time stamp
              set_count++;
              json.writeStartArray();
              json.writeNumber(next_ts.epoch());
              
              for (int i = 0; i < iterators.size(); i++) {
                has_next = false;
                next_next_ts.setMax();
                TimeSeriesValue<?> value = 
                    (TimeSeriesValue<?>) iterators.get(i).next(next_ts);
                if (value.type() == NumericType.TYPE) {
                  if (value.value() == null) {
                    json.writeNull();
                  } else if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
                    json.writeNumber(((TimeSeriesValue<NumericType>) value).value().longValue());
                  } else {
                    json.writeNumber(((TimeSeriesValue<NumericType>) value).value().doubleValue());
                  }
                } else if (value.type() == NumericSummaryType.TYPE) {
                  final Collection<Integer> summaries = 
                      ((TimeSeriesValue<NumericSummaryType>) value).value()
                      .summariesAvailable();
                  if (!summaries.isEmpty()) {
                    // JUST grab the first
                    int summary = summaries.iterator().next();
                    final NumericType v = ((TimeSeriesValue<NumericSummaryType>) value)
                        .value().value(summary);
                    if (v == null) {
                      json.writeNull();
                    } else if (v.isInteger()) {
                      json.writeNumber(v.longValue());
                    } else {
                      json.writeNumber(v.doubleValue());
                    }
                  } else {
                    json.writeNull();
                  }
                }
                
                if (iterators.get(i).hasNext()) {
                  has_next = true;
                  next_next_ts.update(iterators.get(i).nextReal());
                }
              }
              
              json.writeEndArray();
              if (has_next) {
                next_ts.update(next_next_ts);
              }
            }
            
            json.writeEndArray();
            
            // dpsMeta
            json.writeObjectFieldStart("dpsMeta");
            json.writeNumberField("firstTimestamp", first_ts.epoch());
            json.writeNumberField("lastTimestamp", next_ts.epoch());
            json.writeNumberField("setCount", set_count);
            json.writeNumberField("series", iterators.size());
            json.writeEndObject();
            
            json.writeArrayFieldStart("meta");
            
            // timestamp
            json.writeStartObject();
            json.writeNumberField("index", 0);
            json.writeArrayFieldStart("metrics");
            json.writeString("timestamp");
            json.writeEndArray();
            json.writeEndObject();
            
            // time series IDs
            for (int i = 0; i < entry.getValue().size(); i++) {
              TimeSeriesStringId id = entry.getValue().get(i).getKey();
              json.writeStartObject();
              json.writeNumberField("index", i + 1);
              
              json.writeArrayFieldStart("metrics");
              json.writeString(id.metric());
              json.writeEndArray();
              
              json.writeObjectFieldStart("commonTags");
              for (final Entry<String, String> tag : id.tags().entrySet()) {
                json.writeStringField(tag.getKey(), tag.getValue());
              }
              json.writeEndObject();
              
              json.writeArrayFieldStart("aggregatedTags");
              for (final String agg : id.aggregatedTags()) {
                json.writeString(agg);
              }
              json.writeEndArray();
              
              json.writeEndObject();
            }
            
            json.writeEndArray();
            
            json.writeEndObject();
          }
          
          json.flush();
          
          json.writeEndArray();
          json.close();
          
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
  public void deserialize(SerdesOptions options, InputStream stream,
      QueryNode node, Span span) {
    node.onError(new UnsupportedOperationException("Not implemented for this "
        + "class: " + getClass().getCanonicalName()));
  }

}
