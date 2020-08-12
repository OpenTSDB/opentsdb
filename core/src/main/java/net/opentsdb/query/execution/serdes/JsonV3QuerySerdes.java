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
package net.opentsdb.query.execution.serdes;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;
import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.alert.AlertType;
import net.opentsdb.data.types.event.EventGroupType;
import net.opentsdb.data.types.event.EventType;
import net.opentsdb.data.types.event.EventsGroupValue;
import net.opentsdb.data.types.event.EventsValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.status.StatusGroupType;
import net.opentsdb.data.types.status.StatusGroupValue;
import net.opentsdb.data.types.status.StatusType;
import net.opentsdb.data.types.status.StatusValue;
import net.opentsdb.data.types.status.Summary;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.pools.StringBuilderPool;
import net.opentsdb.query.AbstractQueryPipelineContext;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery.LogLevel;
import net.opentsdb.query.processor.summarizer.Summarizer;
import net.opentsdb.query.serdes.SerdesCallback;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JsonV3QuerySerdes implements TimeSeriesSerdes {
  private static final Logger LOG = LoggerFactory.getLogger(
      JsonV3QuerySerdes.class);

  /** The query context. */
  private final QueryContext context;

  /** The options for this serialization. */
  private final SerdesOptions options;

  /** The query start and end timestamps. */
  private final TimeStamp start;
  private final TimeStamp end;
  private final OutputStream stream;

  /** Whether or not we've serialized the first result set. */
  private boolean initialized;

  private static final String NAMESPACE = "namespace";

  /** TEMP */
  //<set ID, <ts hash, ts wrapper>>
  private Map<String, Map<Long, SeriesWrapper>> partials = Maps.newConcurrentMap();
  private List<byte[]> serialized_results = Lists.newArrayList();

  /** Used for the parallel stream*/
  private static ForkJoinPool customThreadPool = new ForkJoinPool(8);
  /** Lock to parallelize the iterator*/
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Default ctor.
   */
  public JsonV3QuerySerdes(final QueryContext context,
                           final SerdesOptions options,
                           final OutputStream stream) {
    if (options == null) {
      throw new IllegalArgumentException("Options cannot be null.");
    }
    if (!(options instanceof JsonV3QuerySerdesOptions)) {
      throw new IllegalArgumentException("Options must be an instance of "
          + "JsonV3QuerySerdesOptions.");
    }
    if (stream == null) {
      throw new IllegalArgumentException("Stream cannot be null.");
    }
    this.context = context;
    this.options = options;
    this.stream = stream;

    start = context.query().startTime();
    end = context.query().endTime();
  }

  // TODO - find a better way to not sync
  @Override
  public Deferred<Object> serialize(final QueryResult result,
                                    final Span span) {
    if (result == null) {
      throw new IllegalArgumentException("Data may not be null.");
    }
    final JsonV3QuerySerdesOptions opts = (JsonV3QuerySerdesOptions) options;

    final QueryStats stats = context.stats();
    if (stats != null && result.timeSeries() != null) {
      stats.incrementSerializedTimeSeriesCount(result.timeSeries().size());
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
          json.writeStringField("source", result.dataSource().toString());
          final TimeStamp spec_start;
          final TimeStamp spec_end;

          // serdes time spec if present
          if (result.timeSpecification() != null) {
            if (result.timeSpecification().start().compare(Op.LT, start)) {
              spec_start = start.getCopy();
              int interval = DateTime.getDurationInterval(
                  result.timeSpecification().stringInterval());
              ChronoUnit u = DateTime.unitsToChronoUnit(
                  DateTime.getDurationUnits(result.timeSpecification().stringInterval()));
              spec_start.snapToPreviousInterval(interval, u);
              if (spec_start.compare(Op.LT, start)) {
                spec_start.add(result.timeSpecification().interval());
              }
            } else {
              spec_start = result.timeSpecification().start();
            }

            if (result.timeSpecification().end().compare(Op.GT, end)) {
              spec_end = end.getCopy();
              int interval = DateTime.getDurationInterval(
                  result.timeSpecification().stringInterval());
              ChronoUnit u = DateTime.unitsToChronoUnit(
                  DateTime.getDurationUnits(result.timeSpecification().stringInterval()));
              end.snapToPreviousInterval(interval, u);
            } else {
              spec_end = result.timeSpecification().end();
            }

            json.writeObjectFieldStart("timeSpecification");
            // TODO - ms, second, nanos, etc
            json.writeNumberField("start", spec_start.epoch());
            json.writeNumberField("end", spec_end.epoch());
            json.writeStringField("intervalISO", result.timeSpecification().interval() != null ?
                result.timeSpecification().interval().toString() : "null");
            json.writeStringField("interval", result.timeSpecification().stringInterval());
            if (result.timeSpecification().timezone() != null) {
              json.writeStringField("timeZone", result.timeSpecification().timezone().toString());
            }
            json.writeStringField("units", result.timeSpecification().units() != null ?
                result.timeSpecification().units().toString() : "null");
            json.writeEndObject();
          } else {
            spec_start = start;
            spec_end = end;
          }

          json.writeArrayFieldStart("data");
          int idx = 0;
          AtomicInteger ai = new AtomicInteger(0);
          AtomicBoolean wasStatus = new AtomicBoolean(false);
          StringBuilder namespace = new StringBuilder();

          if (result.processInParallel()) {
            List<TimeSeries> tss = result.timeSeries();
            LOG.debug("Processing the iterators parallelly: " + tss.size());
            final List<Pair<Integer, TimeSeries>> pairs =
                Lists.newArrayListWithExpectedSize(tss.size());
            idx = 0;
            for (int i = 0; i < tss.size(); i++) {
              TimeSeries ts = tss.get(i);
              pairs.add(new Pair<Integer, TimeSeries>(idx++, ts));
            }

            final List<String> sets =
                Lists.newArrayListWithExpectedSize(result.timeSeries().size());
            customThreadPool.submit(() ->
                pairs.stream().parallel().forEach((pair) -> {
                  try {
                    serializeSeries(opts,
                        pair.getValue(),
                        ids != null ? ids.get(pair.getKey()) :
                            (TimeSeriesStringId) pair.getValue().id(),
                        json,
                        null,
                        spec_start,
                        spec_end,
                        result);
                  } catch (Exception e) {
                    LOG.error("Failed to serialize ts: " + series, e);
                    throw new QueryExecutionException("Unexpected exception "
                        + "serializing ts: " + series, 0, e);
                  }
                  if (pair.getValue().types().contains(StatusType.TYPE)
                      && pair.getValue().id() instanceof BaseTimeSeriesStringId) {
                    BaseTimeSeriesStringId bid = (BaseTimeSeriesStringId) pair.getValue().id();
                    namespace.append(bid.namespace());
                    wasStatus.getAndSet(true);
                  }
                })).get();

            idx = 0;
            for (final String set : sets) {
              if (idx++ > 0) {
                json.writeRaw(",");
              }
              json.writeRaw(set);
            }
          } else {
            LOG.debug("Processing the iterators sequentially");
            for (final TimeSeries series : series != null ? series : result.timeSeries()) {

              serializeSeries(opts,
                  series,
                  ids != null ? ids.get(ai.getAndIncrement()) : (TimeSeriesStringId) series.id(),
                  json,
                  null,
                  spec_start,
                  spec_end,
                  result);
              if (series.types().contains(StatusType.TYPE) && series.id() instanceof BaseTimeSeriesStringId) {
                BaseTimeSeriesStringId bid = (BaseTimeSeriesStringId) series.id();
                namespace.append(bid.namespace());
                wasStatus.getAndSet(true);
              }
            }
          }
          // end of the data array
          json.writeEndArray();

          if(wasStatus.get() && namespace.length() != 0) {
            json.writeStringField(NAMESPACE, namespace.toString());
          }

          if (result instanceof AbstractQueryPipelineContext.StatusGroupResultWrapper) {
            AbstractQueryPipelineContext.StatusGroupResultWrapper statusGroupQueryResult = (AbstractQueryPipelineContext.StatusGroupResultWrapper) result;
            Summary resultSummary = statusGroupQueryResult.resultSummary();
            Summary totalSummary = statusGroupQueryResult.totalSummary();
            if (null != resultSummary) {
              writeSummary(resultSummary, "resultSummary", json);
            }
            if (null != totalSummary) {
              writeSummary(totalSummary, "totalSummary", json);
            }
          }

          json.writeEndObject();
        } catch (Exception e) {
          LOG.error("Unexpected exception", e);
          return Deferred.fromError(new QueryExecutionException(
              "Unexpected exception "
                  + "serializing: " + result, 500, e));
        }

        json.close();
        final byte[] serialized = baos.toByteArray();
        synchronized (serialized_results) {
          serialized_results.add(serialized);
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
  public void serializeComplete(final Span span) {
    if (!initialized /* Only on QueryResult */ && partials.size() > 0) {
      serializePush();
      return;
    }

    JsonGenerator json;
    try {
      json = JSON.getFactory().createGenerator(stream);
    } catch (IOException e1) {
      throw new RuntimeException("Failed to instantiate a JSON "
          + "generator", e1);
    }
    try {
      // TODO - other bits like the query and trace data
      json.writeStartObject();
      json.writeArrayFieldStart("results");
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
  public void serialize(final PartialTimeSeries series,
                        final SerdesCallback callback,
                        final Span span) {
    if (series.set().timeSeriesCount() < 1 || series.value() == null) {
      // no data
      callback.onComplete(series);
      return;
    }

    // TODO - break out
    final PooledObject pooled_object = context.tsdb().getRegistry()
        .getObjectPool(StringBuilderPool.TYPE).claim();
    try {
      if (pooled_object == null) {
        LOG.error("Failed to claim a string builder.");
        callback.onError(series, new RuntimeException("WTF? Failed to claim a string builder."));
        return;
      }
      final StringBuilder stream = (StringBuilder) pooled_object.object();
      stream.setLength(0);
      int count = 0;
      long[] values = ((NumericLongArrayType) series.value()).data();
      int idx = ((NumericLongArrayType) series.value()).offset();

      while (idx < ((NumericLongArrayType) series.value()).end()) {
        long ts = 0;
        if((values[idx] & NumericLongArrayType.MILLISECOND_FLAG) != 0) {
          ts = (values[idx] & NumericLongArrayType.TIMESTAMP_MASK) / 1000;
        } else {
          ts = values[idx] & NumericLongArrayType.TIMESTAMP_MASK;
        }
        if (ts < context.query().startTime().epoch()) {
          idx += 2;
          continue;
        }
        if (ts > context.query().endTime().epoch()) {
          break;
        }

        if (count > 0) {
          stream.append(',');
        }
        stream.append('"');
        stream.append(ts);

        stream.append('"');
        stream.append(':');
        if ((values[idx] & NumericLongArrayType.FLOAT_FLAG) != 0) {
          double d = Double.longBitsToDouble(values[idx + 1]);
          if (Double.isNaN(d)) {
            stream.append("NaN".getBytes());
          } else {
            stream.append(d);
          }
        } else {
          stream.append(values[idx + 1]);
        }
        idx += 2;
        count++;
      }

      if (count < 1) {
        callback.onComplete(series);
        return;
      }

      final String set_id = series.set().node().config().getId() + ":"
          + series.set().dataSource();

      Map<Long, SeriesWrapper> source_shards = partials.get(set_id);
      if (source_shards == null) {
        source_shards = Maps.newConcurrentMap();
        Map<Long, SeriesWrapper> extant = partials.putIfAbsent(set_id, source_shards);
        if (extant != null) {
          source_shards = extant;
        }
      }

      SeriesWrapper set = null;
      set = source_shards.get(series.idHash());
      if (set == null) {
        set = new SeriesWrapper();
        set.id_hash = series.idHash();
        set.id_type = series.idType();
        set.set = series.set();
        final SeriesWrapper extant = source_shards.putIfAbsent(series.idHash(), set);
        if (extant != null) {
          set = extant;
        }
      }

      set.series.put(series.set().start().epoch(), stream.toString());
      callback.onComplete(series);
    } finally {
      pooled_object.release();
    }
  }

  @Override
  public void deserialize(final QueryNode node,
                          final Span span) {
    node.onError(new UnsupportedOperationException("Not implemented for this "
        + "class: " + getClass().getCanonicalName()));
  }
  private void serializeSeries(
      final JsonV3QuerySerdesOptions options,
      final TimeSeries series,
      final TimeSeriesStringId id,
      JsonGenerator json,
      final List<String> sets,
      final TimeStamp start,
      final TimeStamp end,
      final QueryResult result) throws IOException {

    final ByteArrayOutputStream baos;
    if (json == null) {
      baos = new ByteArrayOutputStream();
      json = JSON.getFactory().createGenerator(baos);
    } else {
      baos = null;
    }

    boolean wrote_values = false;
    boolean was_status = false;
    boolean was_event = false;
    boolean was_event_group = false;
    lock.writeLock().lock(); // since json is not thread safe and we need to form the json in order
    try {
      final Iterator<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> 
        iterators = series.iterators().iterator();
      while (iterators.hasNext()) {
        try (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
              iterators.next()) {
          if (!iterator.hasNext()) {
            continue;
          }
  
          TimeSeriesValue<? extends TimeSeriesDataType> value = iterator.next();
          if (value == null) {
            continue;
          }
  
          if (iterator.getType() == StatusType.TYPE) {
            if (!was_status) {
              was_status = true;
            }
            json.writeStartObject();
            writeStatus((StatusValue) value, json);
            wrote_values = true;
          } else if(iterator.getType() == StatusGroupType.TYPE) {
            if (!was_status) {
              was_status = true;
            }
            json.writeStartObject();
            writeStatusGroup((StatusGroupValue) value, json);
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
          } else if (iterator.getType() == NumericType.TYPE) {
            if (writeNumeric((TimeSeriesValue<NumericType>) value, options,
                iterator, json, result, start, end, wrote_values)) {
              wrote_values = true;
            }
          } else if (iterator.getType() == NumericSummaryType.TYPE) {
            if (writeNumericSummary(value, options, iterator, json, result,
                start, end, wrote_values)) {
              wrote_values = true;
            }
          } else if (iterator.getType() == NumericArrayType.TYPE) {
            if(writeNumericArray((TimeSeriesValue<NumericArrayType>) value,
                options, iterator, json, result, start, end, wrote_values)) {
              wrote_values = true;
            }
          } else if (iterator.getType() == AlertType.TYPE) {
            if (writeAlert((TimeSeriesValue<AlertType>) value, options,
                iterator, json, result, start, end, wrote_values)) {
              wrote_values = true;
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
        if (was_event || was_event_group) {
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

      if (baos != null) {
        json.close();
        synchronized(sets) {
          sets.add(new String(baos.toByteArray(), Const.UTF8_CHARSET));
        }
        baos.close();
      } else {
        json.flush();
      }

    } finally {
      lock.writeLock().unlock();
    }
  }

  private boolean writeNumeric(
      TimeSeriesValue<NumericType> value,
      final JsonV3QuerySerdesOptions options,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      final TimeStamp start,
      final TimeStamp end,
      boolean wrote_values) throws IOException {
    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      // just the values
      while (value != null) {
        if (value.timestamp().compare(Op.LT, start)) {
          if (iterator.hasNext()) {
            value = (TimeSeriesValue<NumericType>) iterator.next();
          } else {
            value = null;
          }
          continue;
        }

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
          if ((value).value().isInteger()) {
            json.writeNumber(
                (value).value().longValue());
          } else {
            json.writeNumber(
                (value).value().doubleValue());
          }
        }

        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericType>) iterator.next();
        } else {
          value = null;
        }
      }
      if (wrote_type) {
        json.writeEndArray();
      }
      return wrote_type;
    }

    // timestamp and values
    boolean wrote_local = false;
    while (value != null) {
      if (value.timestamp().compare(Op.LT, start)) {
        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericType>) iterator.next();
        } else {
          value = null;
        }
        continue;
      }

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
        if ((value).value().isInteger()) {
          json.writeNumberField(ts_string,
              (value).value().longValue());
        } else {
          json.writeNumberField(ts_string,
              (value).value().doubleValue());
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
      final JsonV3QuerySerdesOptions options,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      final TimeStamp start,
      final TimeStamp end,
      boolean wrote_values) throws IOException {

    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      Collection<Integer> summaries = null;
      Integer summary = null;

      // just the values
      while (value != null) {
        if (value.timestamp().epoch() > 0 && value.timestamp().compare(Op.LT, start)) {
          if (iterator.hasNext()) {
            value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
          } else {
            value = null;
          }
          continue;
        }

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
                (value).value().summariesAvailable();
            summary = summaries.iterator().next();
          }
          if ((value).value().value(summary).isInteger()) {
            json.writeNumber(
                (value).value().value(summary).longValue());
          } else {
            json.writeNumber(
                (value).value().value(summary).doubleValue());
          }
        }

        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
        } else {
          value = null;
        }
      }
      if (wrote_type) {
        json.writeEndArray();
      }
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
            (value).value().summariesAvailable();
        summary = summaries.iterator().next();
      }

      if (value.value() == null) {
        json.writeNullField(ts_string);
      } else {
        if ((value).value().value(summary).isInteger()) {
          json.writeNumberField(ts_string,
              (value).value().value(summary).longValue());
        } else {
          json.writeNumberField(ts_string,
              (value).value().value(summary).doubleValue());
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
      final JsonV3QuerySerdesOptions options,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      final TimeStamp start,
      final TimeStamp end,
      boolean wrote_values) throws IOException {

    boolean wrote_type = false;
    if (result.timeSpecification() != null) {
      if (!(result.source() instanceof Summarizer)) {
        return writeRollupNumeric((TimeSeriesValue<NumericSummaryType>) value,
            options, iterator, json, result, start, end, wrote_values);
      }

      Collection<Integer> summaries =
          ((TimeSeriesValue<NumericSummaryType>) value)
              .value()
              .summariesAvailable();

      value = (TimeSeriesValue<NumericSummaryType>) value;
      while (value != null) {
        if (value.timestamp().epoch() > 0 && value.timestamp().compare(Op.LT, start)) {
          if (iterator.hasNext()) {
            value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
          } else {
            value = null;
          }
          continue;
        }

        if (value.timestamp().compare(Op.GT, end)) {
          break;
        }

        long ts = (options != null && options.getMsResolution())
            ? value.timestamp().msEpoch()
            : value.timestamp().msEpoch() / 1000;
        if (value.timestamp().epoch() == 0) {
          ts = start.epoch();
        }

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
      return writeRollupNumeric((TimeSeriesValue<NumericSummaryType>) value,
          options, iterator, json, result, start, end, wrote_values);
    }

    if (((TimeSeriesValue<NumericSummaryType>) value).value() != null) {
      Collection<Integer> summaries =
          ((TimeSeriesValue<NumericSummaryType>) value).value().summariesAvailable();

      value = (TimeSeriesValue<NumericSummaryType>) value;
      while (value != null) {
        if (value.timestamp().epoch() > 0 && value.timestamp().compare(Op.LT, start)) {
          if (iterator.hasNext()) {
            value = (TimeSeriesValue<NumericSummaryType>) iterator.next();
          } else {
            value = null;
          }
          continue;
        }

        if (value.timestamp().compare(Op.GT, end)) {
          break;
        }
        long ts = (options != null && options.getMsResolution())
            ? value.timestamp().msEpoch()
            : value.timestamp().msEpoch() / 1000;
        if (value.timestamp().epoch() == 0) {
          ts = start.epoch();
        }
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
      final JsonV3QuerySerdesOptions options,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      final TimeStamp start,
      final TimeStamp end,
      boolean wrote_values) throws IOException {
    if (value.value().end() <= value.value().offset() ||
        result.timeSpecification() == null) {
      // no data
      return false;
    }
    if (!start.compare(Op.EQ, result.timeSpecification().start()) ||
        !end.compare(Op.EQ, result.timeSpecification().end())) {
      TimeStamp cur = result.timeSpecification().start().getCopy();
      int idx = value.value().offset();
      while (idx < value.value().end() && cur.compare(Op.LT, start)) {
        idx++;
        cur.add(result.timeSpecification().interval());
      }

      boolean wrote_type = false;
      for (; idx < value.value().end(); idx++) {
        if (cur.compare(Op.GTE, end)) {
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
        if (value.value().isInteger()) {
          json.writeNumber(value.value().longArray()[idx]);
        } else {
          json.writeNumber(value.value().doubleArray()[idx]);
        }
        cur.add(result.timeSpecification().interval());
      }
      if (wrote_type) {
        json.writeEndArray();
      }
      return wrote_type;
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
    if (wrote_type) {
      json.writeEndArray();
    }
    return wrote_type;
  }

  private boolean writeAlert(
      TimeSeriesValue<AlertType> value,
      final JsonV3QuerySerdesOptions options,
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final JsonGenerator json,
      final QueryResult result,
      final TimeStamp start,
      final TimeStamp end,
      boolean wrote_values) throws IOException {
    boolean wrote_type = false;
    while (value != null) {
      if (!wrote_values) {
        json.writeStartObject();
        wrote_values = true;
      }
      if (!wrote_type) {
        json.writeObjectFieldStart("AlertType"); // yeah, it's numeric.
        wrote_type = true;
      }

      json.writeObjectFieldStart(Long.toString(value.timestamp().epoch()));
      json.writeStringField("level", value.value().state().toString());
      json.writeStringField("message", value.value().message());
      if (value.value().dataPoint() == null) {
        json.writeNullField("value");
      } else if (value.value().dataPoint().isInteger()) {
        json.writeNumberField("value", value.value().dataPoint().longValue());
      } else {
        json.writeNumberField("value", value.value().dataPoint().doubleValue());
      }
      if (value.value().threshold() != null) {
        if (value.value().threshold().isInteger()) {
          json.writeNumberField("threshold", value.value().threshold().longValue());
        } else {
          json.writeNumberField("threshold", value.value().threshold().doubleValue());
        }
      }
      json.writeStringField("type", value.value().thresholdType());
      json.writeEndObject();

      if (iterator.hasNext()) {
        value = (TimeSeriesValue<AlertType>) iterator.next();
      } else {
        value = null;
      }
    }

    if (wrote_type) {
      json.writeEndObject();
    }
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

    if (eventsGroupValue.event() != null) {
      json.writeObjectFieldStart("event");
      writeEvents(eventsGroupValue.event(), json);
      json.writeObjectFieldStart("tags");
      for (final Entry<String, String> entry : id.tags().entrySet()) {
        json.writeStringField(entry.getKey(), entry.getValue());
      }
      json.writeEndObject();
      json.writeEndObject();
    }
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
    if (eventsValue.parentIds() != null) {
      json.writeArrayFieldStart("parentIds");
      for (String p : eventsValue.parentIds()) {
        json.writeString(p);
      }
    }
    json.writeEndArray();
    if (eventsValue.childIds() != null) {
      json.writeArrayFieldStart("childIds");
      for (String c : eventsValue.childIds()) {
        json.writeString(c);
      }
    }
    json.writeEndArray();

    if (eventsValue.additionalProps() != null) {
      json.writeObjectFieldStart("additionalProps");
      for (Map.Entry<String, Object> e : eventsValue.additionalProps().entrySet()) {
        json.writeStringField(e.getKey(), String.valueOf(e.getValue()));
      }
      json.writeEndObject();
    }

  }

  private void writeStatusGroup(StatusGroupValue statusGroupValue, JsonGenerator json)
      throws IOException {
    StatusValue[] statuses = statusGroupValue.statuses();
    if (null != statuses) {
      json.writeArrayFieldStart("statuses");
      for (StatusValue statusValue : statuses) {
        json.writeStartObject();
        writeStatus(statusValue, json);
        json.writeEndObject();
      }
      json.writeEndArray();
    }

    Summary summary = statusGroupValue.summary();
    if (null != summary) {
      writeSummary(summary, "summary", json);
    }
  }

  private void writeSummary(Summary summary, String name, JsonGenerator json) throws IOException {
    json.writeObjectFieldStart(name);
    json.writeNumberField("good", summary.good);
    json.writeNumberField("bad", summary.bad);
    json.writeNumberField("warn", summary.warn);
    json.writeNumberField("unknown", summary.unknown);
    json.writeNumberField("missing", summary.missing);
    json.writeEndObject();
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
    Map<String, String> tags = statusValue.tags();
    if (tags != null) {
      json.writeObjectFieldStart("tags");
      for (final Entry<String, String> entry : tags.entrySet()) {
        json.writeStringField(entry.getKey(), entry.getValue());
      }
      json.writeEndObject();
    }
  }

  /**
   * Scratch class used to collect the serialized time series.
   */
  class SeriesWrapper {
    public PartialTimeSeriesSet set;
    public long id_hash;
    public TypeToken<? extends TimeSeriesId> id_type;
    public ConcurrentSkipListMap<Long, String> series =
        new ConcurrentSkipListMap<Long, String>();
  }

  class SetWrapper {
    public PartialTimeSeriesSet set;
    public boolean closed;
  }

  void serializePush() {
    try {
      JsonGenerator json;
      try {
        json = JSON.getFactory().createGenerator(stream);
      } catch (IOException e1) {
        throw new RuntimeException("Failed to instantiate a JSON "
            + "generator", e1);
      }
      json.writeStartObject();
      json.writeArrayFieldStart("results");

      String src_id = null;
      for (final Entry<String, Map<Long, SeriesWrapper>> entry : partials.entrySet()) {
        for (final SeriesWrapper shard : entry.getValue().values()) {
          if (shard.series == null || shard.series.isEmpty()) {
            continue;
          }

          final String source_id = shard.set.node().config().getId() + ":"
              + shard.set.dataSource();
          if (src_id == null) {
            src_id = source_id;
            json.writeStartObject();
            json.writeStringField("source", src_id);
            json.writeArrayFieldStart("data");
          } else if (!(src_id.equals(source_id))) {
            src_id = source_id;
            json.writeStartObject();
            json.writeStringField("source", src_id);
            json.writeArrayFieldStart("data");
          }

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

          // serialize the ID
          json.writeStartObject();
          TimeSeriesId raw_id = context.getId(shard.id_hash, shard.id_type);
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
          for (Entry<Long, String> e : shard.series.entrySet()) {
            //byte[] s = w.series.get(id_hash);
            if (e.getValue() == null) {
              // TODO - fill!!
            } else {
              if (count++ > 0) {
                json.writeRaw(",");
              }
              //json.writeRaw(new String(e.getValue()));
              json.writeRaw(e.getValue());
            }
          }
          json.writeEndObject();
          json.writeEndObject();
        }

        json.writeEndArray();
        json.writeEndObject();
      }

    } catch (IOException e) {
      throw new RuntimeException("Unexpected exception: " + e.getMessage(), e);
    }
  }
}