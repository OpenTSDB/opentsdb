// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.execution.prometheus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
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

/**
 * Super simple prometheus query range serialization implementation. It only 
 * handles actual data, returning a status of "success" regardless of whether or
 * not data has been found.
 * 
 * TODO - parallelization.
 * 
 * @since 3.0
 */
public class PrometheusQuerySerdes implements TimeSeriesSerdes {
  private static final Logger LOG = LoggerFactory.getLogger(
      PrometheusQuerySerdes.class);
  
  /** The query context. */
  private final QueryContext context;
  
  /** The options for this serialization. */
  private final SerdesOptions options;
  
  /** The query start and end timestamps. */
  private final TimeStamp start;
  private final TimeStamp end;
  private final OutputStream stream;
  
  private List<byte[]> serialized_results = Lists.newArrayList();
  
  
  public PrometheusQuerySerdes(final QueryContext context,
                               final SerdesOptions options,
                               final OutputStream stream) {
    if (stream == null) {
      throw new IllegalArgumentException("Stream cannot be null.");
    }
    this.context = context;
    this.options = options;
    this.stream = stream;

    start = context.query().startTime();
    end = context.query().endTime();
  }
  
  @Override
  public Deferred<Object> serialize(final QueryResult result, final Span span) {
    if (result == null) {
      throw new IllegalArgumentException("Data may not be null.");
    }
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonGenerator json;
    try {
      json = JSON.getFactory().createGenerator(baos);
    } catch (IOException e1) {
      throw new RuntimeException("Failed to instantiate a JSON "
          + "generator", e1);
    }
    
    final List<TimeSeries> series;
    final List<Deferred<TimeSeriesStringId>> deferreds;
    if (result.idType() == Const.TS_BYTE_ID) {
      series = result.timeSeries();
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
          // assume array context here.
          for (int i = 0; i < (series != null ? series.size() : result.timeSeries().size()); i++) {
            final TimeSeries ts = series != null ? series.get(i) : result.timeSeries().get(i);
            serializeSeries(ts, ids != null ? ids.get(i) : (TimeSeriesStringId) ts.id(), json, result);
          }
          
          json.close();
          final byte[] serialized = baos.toByteArray();
          synchronized (serialized_results) {
            serialized_results.add(serialized);
          }
          return Deferred.fromResult(null);
        } catch (Exception e) {
          LOG.error("Unexpected exception", e);
          return Deferred.fromError(new QueryExecutionException(
              "Unexpected exception "
                  + "serializing: " + result, 500, e));
        }
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
        return Deferred.groupInOrder(deferreds)
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
  public void serialize(final PartialTimeSeries series, 
                        final SerdesCallback callback, 
                        final Span span) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public void serializeComplete(Span span) {
    JsonGenerator json;
    try {
      json = JSON.getFactory().createGenerator(stream);
    } catch (IOException e1) {
      throw new RuntimeException("Failed to instantiate a JSON "
          + "generator", e1);
    }
    try {
      json.writeStartObject();
      // TODO - errors!
      json.writeStringField("status", "success");
      json.writeObjectFieldStart("data");
      // TODO - other types?
      json.writeStringField("resultType", "matrix");
      json.writeArrayFieldStart("result");
      for (int i = 0; i < serialized_results.size(); i++) {
        if (serialized_results.get(i) == null) {
          continue;
        }
        if (i > 0) {
          json.writeRaw(',');
        }
        json.writeRaw(new String(serialized_results.get(i), Const.UTF8_CHARSET));
        serialized_results.set(i, null);
      }
      // end results.
      json.writeEndArray();

      // end data
      json.writeEndObject();
      
      // end outer object
      json.writeEndObject();
      json.flush();
    } catch (IOException e) {
      throw new QueryExecutionException("Failure closing serializer", 500, e);
    }
  }

  @Override
  public void deserialize(final QueryNode node, final Span span) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  void serializeSeries(final TimeSeries series, final TimeSeriesStringId id,
      final JsonGenerator json, final QueryResult result) throws IOException {
    synchronized (this) {
      json.writeStartObject();
      
      final Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators = 
          series.iterators();
      if (iterators.isEmpty()) {
        return;
      }
      
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
          iterators.iterator().next();
      if (!iterator.hasNext()) {
        return;
      }
      
      // ID first
      json.writeObjectFieldStart("metric");
      json.writeStringField("__name__", id.metric());
      for (final Entry<String, String> pair : id.tags().entrySet()) {
        json.writeStringField(pair.getKey(), pair.getValue());
      }
      json.writeEndObject();
      
      json.writeArrayFieldStart("values");
      
      if (iterator.getType() == NumericType.TYPE) {
        while (iterator.hasNext()) {
          final TimeSeriesValue<NumericType> value = 
              (TimeSeriesValue<NumericType>) iterator.next();
          if (value.timestamp().compare(Op.LT, start)) {
            continue;
          }
          if (value.timestamp().compare(Op.GTE, end)) {
            break;
          }
          if (value.value() == null) {
            continue;
          }
          
          json.writeStartArray();
          json.writeNumber(value.timestamp().epoch());
          if (value.value().isInteger()) {
            json.writeString(Long.toString(value.value().longValue()));
          } else {
            json.writeString(Double.toString(value.value().doubleValue()));
          }
          json.writeEndArray();
        }
      } else if (iterator.getType() == NumericSummaryType.TYPE) {
        int summary = -1;
        while (iterator.hasNext()) {
          final TimeSeriesValue<NumericSummaryType> value = 
              (TimeSeriesValue<NumericSummaryType>) iterator.next();
          if (value.timestamp().compare(Op.LT, start)) {
            continue;
          }
          if (value.timestamp().compare(Op.GTE, end)) {
            break;
          }
          if (value.value() == null) {
            continue;
          }
          
          json.writeStartArray();
          json.writeNumber(value.timestamp().epoch());
          if (summary < 0) {
            summary = value.value().summariesAvailable().iterator().next();
          }
          
          final NumericType data = value.value().value(summary);
          if (data.isInteger()) {
            json.writeString(Long.toString(data.longValue()));
          } else {
            json.writeString(Double.toString(data.doubleValue()));
          }
          json.writeEndArray();
        }
      } else if (iterator.getType() == NumericArrayType.TYPE) {
        final TimeStamp ts = result.timeSpecification().start().getCopy();
        final TimeSeriesValue<NumericArrayType> value = 
            (TimeSeriesValue<NumericArrayType>) iterator.next();
        if (value.value().isInteger()) {
          long[] values = value.value().longArray();
          for (int i = value.value().offset(); i < value.value().end(); i++) {
            json.writeStartArray();
            json.writeNumber(ts.epoch());
            ts.add(result.timeSpecification().interval());
            json.writeString(Long.toString(values[i]));
          }
        } else {
          double[] values = value.value().doubleArray();
          for (int i = value.value().offset(); i < value.value().end(); i++) {
            json.writeStartArray();
            json.writeNumber(ts.epoch());
            ts.add(result.timeSpecification().interval());
            json.writeString(Double.toString(values[i]));
          }
        }
      }
      
      json.writeEndArray();
      json.writeEndObject();
    }
  }
}
