// This file is part of OpenTSDB.
// Copyright (C) 2012-2020 The OpenTSDB Authors.
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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map.Entry;

import net.opentsdb.data.TimeSeriesDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
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
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.serdes.SerdesCallback;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.JSON;

/**
 * A serializer mimicking the output of OpenTSDB 2.x's /api/query/exp 
 * endpoint.
 * 
 * TODO - a lot of cleanup needed here.
 * 
 * @since 3.0
 */
public class JsonV2ExpQuerySerdes implements TimeSeriesSerdes {
  private static final Logger LOG = LoggerFactory.getLogger(
      JsonV2ExpQuerySerdes.class);
  
  // TODO - proper interpolation config
  // TODO - interpolation config for summaries.
  private static final NumericInterpolatorConfig NIC = 
      (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.NONE)
      .setDataType(NumericType.TYPE.toString())
      .build();
  
  /** The TSDB we belong to. */
  private final TSDB tsdb;
  
  /** The options for this serialization. */
  private final SerdesOptions options;
  
  /** The generator. */
  private final JsonGenerator json;
  
  /** Whether or not we've serialized the first result set. */
  private final AtomicBoolean initialized;
  
  private final QueryContext context;
  
  /**
   * Default ctor.
   */
  public JsonV2ExpQuerySerdes(final QueryContext context,
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
    tsdb = context.tsdb();
    this.context = context;
    this.options = options;
    try {
      json = JSON.getFactory().createGenerator(stream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to instantiate a JSON "
          + "generator", e);
    }
    initialized = new AtomicBoolean();
  }
  
  // TODO - better way to avoid sync I hope.
  @Override
  public Deferred<Object> serialize(final QueryResult result, 
                                    final Span span) {
    if (result == null) {
      throw new IllegalArgumentException("Data may not be null.");
    }
    final JsonV2QuerySerdesOptions opts = (JsonV2QuerySerdesOptions) options;
    
    synchronized(this) {
      if (initialized.compareAndSet(false, true)) {
        try {
          json.writeStartObject();
          json.writeArrayFieldStart("outputs");
        } catch (IOException e) {
          throw new RuntimeException("Unexpected exception: " + e.getMessage(), e);
        }
      }
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
      public synchronized Object call(final ArrayList<TimeSeriesStringId> ids) 
            throws Exception {
        QueryInterpolatorFactory factory = tsdb
            .getRegistry().getPlugin(QueryInterpolatorFactory.class, 
                NIC.getType());
        if (factory == null) {
          return Deferred.fromError(new IllegalArgumentException("No interpolator factory found for: " + 
              NIC.getType() == null ? "Default" : NIC.getType()));
        }
        
        try {
          // Note we only have one expression result per QueryResult so don't 
          // bother with the outer set or alias now.
          final List<TimeSeries> results = series != null ? series : result.timeSeries();
          final TimeSeriesStringId[] id_array = new TimeSeriesStringId[results.size()];
          final TimeSeries[] series_array = new TimeSeries[results.size()];
          for (int i = 0; i < results.size(); i++) {
            final TimeSeries series = results.get(i);
            id_array[i] = ids != null ? ids.get(i) : (TimeSeriesStringId) series.id();
            series_array[i] = series;
          }
          
          json.writeStartObject();
          json.writeStringField("id", result.dataSource().dataSource());
          json.writeArrayFieldStart("dps");
          
          final List<TypedTimeSeriesIterator<?>> iterators = Lists.newArrayList();
          TimeStamp next_ts = new MillisecondTimeStamp(0);
          TimeStamp next_next_ts = new MillisecondTimeStamp(0);
          next_ts.setMax();
          
          boolean has_next = false;
          
          TypeToken<? extends TimeSeriesDataType> type = null;
          for (int x = 0; x < id_array.length; x++) {
            if (type == null) {
              // TODO may throw if the underlying iterator doesn't have a type.
              type = series_array[x].types().iterator().next();
            }
            
            final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> optional =
                series_array[x].iterator(type);
            if (!optional.isPresent()) {
              continue;
            }
            
            final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
                optional.get();
            if (!iterator.hasNext()) {
              iterator.close();
              continue;
            }
            
            if (type == NumericArrayType.TYPE) {
              iterators.add(iterator);
              has_next = true;
              next_ts.update(result.timeSpecification().start());
            } else if (type == NumericType.TYPE) {
              final QueryInterpolator<?> interp = 
                  factory.newInterpolator(NumericType.TYPE, iterator, NIC);
              if (interp.hasNext()) {
                has_next = true;
                next_ts.update(interp.nextReal());
                iterators.add(interp);
              }
            } else if (type == NumericSummaryType.TYPE) {
              final QueryInterpolator<?> interp = 
                  factory.newInterpolator(NumericSummaryType.TYPE, iterator, NIC);
              if (interp.hasNext()) {
                has_next = true;
                next_ts.update(interp.nextReal());
                iterators.add(interp);
              }
            } else {
              throw new IllegalStateException("Unknown type: " + type);
            }
          }
          
          TimeStamp first_ts = next_ts.getCopy();
          
          final int set_count;
          if (!has_next) {
            set_count = 0;
          } else if (type == NumericArrayType.TYPE) {
            set_count = serializeArray(result, iterators);
          } else {
            set_count = serializeRawOrSummary(iterators, next_ts, next_next_ts);
          }
          
          // shut-em down.
          for (int i = 0; i < iterators.size(); i++) {
            iterators.get(i).close();
            iterators.set(i, null);
          }
          
          json.writeEndArray();
          
          // dpsMeta
          json.writeObjectFieldStart("dpsMeta");
          json.writeNumberField("firstTimestamp", 
              first_ts.msEpoch() == Long.MAX_VALUE ? 0 : first_ts.msEpoch());
          json.writeNumberField("lastTimestamp", 
              next_ts.msEpoch() == Long.MAX_VALUE ? 0 : next_ts.msEpoch());
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
          for (int i = 0; i < id_array.length; i++) {
            TimeSeriesStringId id = id_array[i];
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
  
  int serializeRawOrSummary(final List<TypedTimeSeriesIterator<?>> iterators,
                            final TimeStamp next_ts, 
                            final TimeStamp next_next_ts) throws IOException {
    boolean has_next = true;
    int set_count = 0;
    
    while (has_next) {
      if (next_ts.compare(Op.LT, context.query().startTime())) {
        has_next = false;
        for (int i = 0; i < iterators.size(); i++) {
          final QueryInterpolator<?> interpolator = 
              (QueryInterpolator<?>) iterators.get(i);
          if (iterators.get(i).hasNext()) {
            has_next = true;
            interpolator.next(next_ts);
            next_next_ts.update(interpolator.nextReal());
          }
        }
        
        if (has_next) {
          next_ts.update(next_next_ts);
          if (next_ts.compare(Op.GTE, context.query().endTime())) {
            return set_count;
          }
        }
        
        continue;
      }
      
      set_count++;
      json.writeStartArray();
      json.writeNumber(next_ts.msEpoch());
      
      has_next = false;
      for (int i = 0; i < iterators.size(); i++) {
        final QueryInterpolator<?> interpolator = 
            (QueryInterpolator<?>) iterators.get(i);
        next_next_ts.setMax();
        TimeSeriesValue<?> value = 
            (TimeSeriesValue<?>) interpolator.next(next_ts);
        if (value.type() == NumericType.TYPE) {
          if (value.value() == null) {
            // TODO - actual null or keep it NaN?
            json.writeNumber(Double.NaN);
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
              // TODO - actual null or keep it NaN?
              json.writeNumber(Double.NaN);
            } else if (v.isInteger()) {
              json.writeNumber(v.longValue());
            } else {
              json.writeNumber(v.doubleValue());
            }
          } else {
            json.writeNumber(Double.NaN);
          }
        }
        
        if (iterators.get(i).hasNext()) {
          has_next = true;
          next_next_ts.update(interpolator.nextReal());
        }
      }
      
      json.writeEndArray();
      if (has_next) {
        if (next_ts.compare(Op.EQ, next_next_ts)) {
          LOG.error("Unexpected exception: Same timestamp as before: " + next_ts);
          return set_count;
        }
        next_ts.update(next_next_ts);
      } else {
        return set_count;
      }

      if (next_ts.compare(Op.GTE, context.query().endTime())) {
        return set_count;
      }
    }
    
    return set_count;
  }

  int serializeArray(final QueryResult result, 
                     final List<TypedTimeSeriesIterator<?>> iterators) 
                         throws IOException {
    int index = 0;
    final TimeStamp next_ts = result.timeSpecification().start().getCopy();
    
    while (next_ts.compare(Op.LT, context.query().startTime())) {
      if (result.timeSpecification().interval() == null) {
        // it's a run-all
        break;
      }
      index++;
      next_ts.add(result.timeSpecification().interval());
    }
    
    final TimeSeriesValue[] values = new TimeSeriesValue[iterators.size()];
    for (int i = 0; i < iterators.size(); i++) {
      values[i] = iterators.get(i).next();
    }
    
    while (true) {
      json.writeStartArray();
      json.writeNumber(next_ts.msEpoch());
      
      for (int i = 0; i < values.length; i++) {
        final NumericArrayType value = 
            ((TimeSeriesValue<NumericArrayType>) values[i]).value();
        
        if (value.offset() + index >= value.end()) {
          // done!
          return iterators.size();
        }
        
        if (value.isInteger()) {
          json.writeNumber(value.longArray()[value.offset() + index]);
        } else {
          json.writeNumber(value.doubleArray()[value.offset() + index]);
        }
      }
    
      index++;
      if (result.timeSpecification().interval() != null) {
        next_ts.add(result.timeSpecification().interval());
      } else {
        next_ts.update(result.timeSpecification().end());
      }
      json.writeEndArray();
      if (next_ts.compare(Op.GTE, context.query().endTime())) {
        return iterators.size();
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
  public synchronized void serializeComplete(final Span span) {
    try {
      json.writeEndArray();
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

}
