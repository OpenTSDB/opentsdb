// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly.prophet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.anomaly.AnomalyPredictionJob;
import net.opentsdb.query.anomaly.AnomalyPredictionResult;
import net.opentsdb.query.anomaly.AnomalyConfig.ExecutionMode;
import net.opentsdb.query.anomaly.BaseAnomalyFactory;
import net.opentsdb.query.anomaly.BaseAnomalyNode;
import net.opentsdb.utils.JSON;

/**
 * <b>TEMPORARY<b> - This is a node to interface with the Python version of 
 * Facebook's Prophet that handles time series forecasting. This implementation
 * is a hack to integrate with a UI and start playing around with what settings
 * are needed. Once we finalize what works best we will find a way to create a
 * much more efficient (and secure) implementation maybe by adding the code to
 * EGADs as a Java implementation.
 * 
 * Note that we have predictions as an array BECAUSE we may be stradling a 
 * prediction boundary (e.g. a query covers midnight) in which case we need
 * two predictions during evaluation. 
 * 
 * To keep the number of queries small, if there is a cache miss on a prediction
 * then we'll query for both predictions and only write the one(s) that are
 * missing.
 * 
 * TODO - TONS!
 * TODO - The forecast predictions don't seem to align properly. Right now if 
 * you query at night in the PST time zone, Prophet seems to return predictions
 * farther in the future. WTF?
 * 
 * TODO - Proper error handling
 * 
 * @since 3.o
 */
public class Prophet extends BaseAnomalyNode {
  private static final Logger LOG = LoggerFactory.getLogger(Prophet.class);
  
  /** Pandas data frame column headers that Prophet looks for. */
  private static final String DS = "ds";
  private static final String Y = "y";
  
  public static final DateTimeFormatter FORMATTER = DateTimeFormatter
      .ofPattern("yyyy-MM-dd hh:mm:ss")
      // NOTE: It looks like Prophet use's the system time. Crap.
      .withZone(ZoneId.systemDefault());
  
  /** Counter used for traking individual time series. */
  protected AtomicInteger job_counter;
  
  /** The job deferred called when all of the jobs are finished. */
  protected Deferred<Void> job_deferred;
  
  /** The results of parsing to send upstream. */
  protected List<TimeSeries> prediction_series;
  
  /**
   * Default ctor.
   * @param factory The non-null factory.
   * @param context The non-null context.
   * @param config The non-null prophet config.
   */
  public Prophet(final QueryNodeFactory factory, 
                 final QueryPipelineContext context,
                 final ProphetConfig config) {
    super(factory, context, config);
  }

  @Override
  public Deferred<Void> predictAndSet(final QueryResult result,
                                      final int prediction_index) {
    job_counter = new AtomicInteger(result.timeSeries().size());
    job_deferred = new Deferred<Void>();
    prediction_series = Lists.newArrayList();
    
    for (int i = 0; i < result.timeSeries().size(); i++) {
      ((BaseAnomalyFactory) factory).submitJob(
          new Job(result.timeSeries().get(i), result, prediction_index));
    }
    
    return job_deferred.addCallback(new Callback<Void, Void>() {

      @Override
      public Void call(Void arg) throws Exception {
        try {
          for (int i = 0; i < predictions.length; i++) {
            if ((cache_hits.length > 0 && cache_hits[i]) || predictions[i] != null) {
              continue;
            }
            if (cache != null && config.getMode() != ExecutionMode.CONFIG) {
              // need's a clone as we may modify the list when we add thresholds, etc.
              AnomalyPredictionResult anomaly_result = new AnomalyPredictionResult(
                  Prophet.this,
                  new DefaultQueryResultId(config().getId(), 
                      result.dataSource().dataSource()),
                  new SecondTimeStamp(prediction_starts[i]),
                  new SecondTimeStamp(prediction_starts[i] + 
                      (model_units == ChronoUnit.HOURS ? 3600 : 86400)),
                  ds_interval,
                  prediction_series,
                  Const.TS_STRING_ID // TODO - Could be byte
                  );
              writeCache(anomaly_result, prediction_index);
            }
            
            AnomalyPredictionResult anomaly_result = new AnomalyPredictionResult(
                Prophet.this,
                new DefaultQueryResultId(config().getId(), 
                    result.dataSource().dataSource()),
                new SecondTimeStamp(prediction_starts[i]),
                new SecondTimeStamp(prediction_starts[i] + 
                    (model_units == ChronoUnit.HOURS ? 3600 : 86400)),
                ds_interval,
                prediction_series,
                Const.TS_STRING_ID // TODO - Could be byte
                );
            // TODO - shard and properly store
            predictions[i] = anomaly_result;
          }
        } catch (Exception e) {
          LOG.error("Failed to create anomaly results", e);
          throw new QueryDownstreamException("Failed to create anomaly results", e);
        }
        return null;
      }
      
    });
  }

  /**
   * Just decrements the job counter and calls the deferred when we hit zero.
   */
  protected void jobCountdown() {
    int cnt = job_counter.decrementAndGet();
    if (cnt == 0) {
      job_deferred.callback(null);
    }
  }

  /**
   * The job that shells out to Python and runs the training and prediction.
   */
  class Job implements AnomalyPredictionJob {
    final TimeSeries series;
    final QueryResult result;
    final int prediction_idx;
    
    protected Job(final TimeSeries series, 
                  final QueryResult result, 
                  final int prediction_idx) {
      this.series = series;
      this.result = result;
      this.prediction_idx = prediction_idx;
    }
    
    @Override
    public void run() {
      try {
        if (failed.get()) {
          return;
        }
        if (context.queryContext().isClosed()) {
          return;
        }
        
        InputStream config = serializeConfigAndData();
        if (config == null) {
          // nothing was serialized (bad data or all NaNs so don't bother).
          if (LOG.isDebugEnabled()) {
            LOG.debug("No training data for time series " + series.id());
          }
          jobCountdown();
          return;
        }
        
        final ByteArrayOutputStream std_out = new ByteArrayOutputStream();
        final ByteArrayOutputStream std_err = new ByteArrayOutputStream();
        
        final CommandLine cmd = ((ProphetFactory) factory).commandLine();
        PumpStreamHandler stream = new PumpStreamHandler(std_out, std_err, config);
        DefaultExecutor executor = new DefaultExecutor();
        executor.setStreamHandler(stream);
        executor.setExitValues(new int[] { 0, 1 });
        // TODO - timeout
        final int ec = executor.execute(cmd);
        // response!
        if (ec != 0) {
          String response = new String(std_out.toByteArray());
          if (LOG.isTraceEnabled()) {
            LOG.trace("STDOUT: " + response);
            LOG.trace("STDERR: " + new String(std_err.toByteArray()));
          }
          
          // doh
          if (failed.compareAndSet(false, true)) {
            QueryExecutionException ex = new QueryExecutionException(
                "Failed to execute Prophet. Exit code: " + ec + " StdErr: " 
                    + new String(std_err.toByteArray()), ec);
            LOG.error("Unexpected exit code", ex);
            onError(ex);
          }
        } else {
          // TODO - work off the byte array.
          // TODO - check indices
          String response = new String(std_out.toByteArray());
          if (LOG.isTraceEnabled()) {
            LOG.trace("STDOUT: " + response);
            LOG.trace("STDERR: " + new String(std_err.toByteArray()));
          }
          int forecast_idx = response.indexOf("Forecast:");
          String debug = response.substring(0, forecast_idx);
          
          int forecast_end = response.indexOf("}\n", forecast_idx);
          if (forecast_end < 0) {
            throw new RuntimeException("Couldn't find the forecast closing "
                + "bracket after: " + forecast_idx);
          }
          forecast_idx += 10;
          String forecast = response.substring(forecast_idx, forecast_end + 1);
          
          int model_idx = response.indexOf("Model:") + 7;
          String model = response.substring(model_idx);
          
          if (LOG.isTraceEnabled()) {
            LOG.trace("FORECAST: " + forecast);
            LOG.trace("MODEL: " + model);
          }
          
          Series s = new Series(series, forecast, prediction_idx);
          synchronized (prediction_series) {
            prediction_series.add(s);
          }
        }
        jobCountdown();
      } catch (Throwable t) {
        if (failed.compareAndSet(false, true)) {
          LOG.error("Failed to run prediction on " + series.id(), t);
          onError(t);
        } else {
          LOG.error("Failed to run prediction on " + series.id(), t);
        }
      }
    }
    
    /**
     * Takes the training data and serializes a Pandas data frame compatible JSON
     * payload for Prophet to parse.
     * 
     * TODO - check time ranges.
     * TODO - cache the series for config mode.
     * 
     * @return A non-null input stream if serialization was successful, null if
     * there wasn't any data.
     */
    protected InputStream serializeConfigAndData() {
      try {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final JsonGenerator json = JSON.getFactory().createGenerator(stream);
        json.writeStartArray();
        
        boolean wrote_data = false;
        // find the proper format
        Optional<TypedTimeSeriesIterator<?>> op = series.iterator(NumericType.TYPE);
        if (op.isPresent()) {
          // Numeric
          TypedTimeSeriesIterator<NumericType> iterator = 
              (TypedTimeSeriesIterator<NumericType>) op.get();
          while (iterator.hasNext()) {
            TimeSeriesValue<NumericType> value = iterator.next();
            if (value != null && value.value() != null) {
              if (!value.value().isInteger() && 
                  Double.isNaN(value.value().doubleValue())) {
                continue;
              }
              
              json.writeStartObject();
              json.writeStringField(DS, FORMATTER.format(
                  Instant.ofEpochSecond(value.timestamp().epoch())));
              if (value.value().isInteger()) {
                json.writeNumberField(Y, value.value().longValue());
              } else {
                json.writeNumberField(Y, value.value().doubleValue());
              }
              json.writeEndObject();
              wrote_data = true;
            }
          }
        } else {
          // Array
          op = series.iterator(NumericArrayType.TYPE);
          if (op.isPresent()) {
            TypedTimeSeriesIterator<NumericArrayType> iterator = 
                (TypedTimeSeriesIterator<NumericArrayType>) op.get();
            if (iterator.hasNext()) {
              TimeStamp timestamp = result.timeSpecification().start().getCopy();
              TimeSeriesValue<NumericArrayType> value = iterator.next();
              for (int i = value.value().offset(); i < value.value().end(); i++) {
                json.writeStartObject();
                json.writeStringField(DS, FORMATTER.format(
                    Instant.ofEpochSecond(timestamp.epoch())));
                if (value.value().isInteger()) {
                  json.writeNumberField(Y, value.value().longArray()[i]);
                } else {
                  json.writeNumberField(Y, value.value().doubleArray()[i]);
                }
                json.writeEndObject();
                wrote_data = true;
                timestamp.add(result.timeSpecification().interval());
              }
            }
          } else {
            // summary
            op = series.iterator(NumericSummaryType.TYPE);
            if (op.isPresent()) {
              TypedTimeSeriesIterator<NumericSummaryType> iterator = 
                  (TypedTimeSeriesIterator<NumericSummaryType>) op.get();
              int summary = -1;
              while (iterator.hasNext()) {
                TimeSeriesValue<NumericSummaryType> value = iterator.next();
                if (value != null && value.value() != null) {
                  if (summary == -1) {
                    summary = value.value().summariesAvailable().iterator().next();
                  }
                  
                  NumericType dp = value.value().value(summary);
                  if (!dp.isInteger() && Double.isNaN(dp.doubleValue())) {
                    continue;
                  }
                  
                  json.writeStartObject();
                  json.writeStringField(DS, FORMATTER.format(
                      Instant.ofEpochSecond(value.timestamp().epoch())));
                  if (dp.isInteger()) {
                    json.writeNumberField(Y, dp.longValue());
                  } else {
                    json.writeNumberField(Y, dp.doubleValue());
                  }
                  json.writeEndObject();
                  wrote_data = true;
                }
              }
            }
          }
        }
        
        json.writeEndArray();
        if (wrote_data) {
          json.writeRaw("\n");
          // add the config
          json.writeRaw(JSON.serializeToString(config()));
        }
        
        // TODO - serialize the saved model in the future as well.
        json.close();
        
        if (wrote_data) {
          return new ByteArrayInputStream(stream.toByteArray());
        }
        return null;
      } catch (Exception e) {
        // TODO - proper wrapping
        LOG.error("Failed to serialize training data", e);
        throw new QueryDownstreamException("Failed to serialize training data", e);
      }
    }

    
    @Override
    public int jobThreshold() {
      return 0;
    }
  }
  
  /**
   * The prediction time series.
   */
  class Series implements TimeSeries {
    final TimeSeries source;
    final String json;
    final int prediction_idx;
    
    Series(final TimeSeries source, final String json, final int prediction_idx) {
      this.source = source;
      this.json = json;
      this.prediction_idx = prediction_idx;
    }
    
    @Override
    public TimeSeriesId id() {
      return source.id();
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        TypeToken<? extends TimeSeriesDataType> type) {
      return Optional.of(new PredictionArrayIterator(json, prediction_idx));
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> list = 
          Lists.newArrayList();
      list.add(new PredictionArrayIterator(json, prediction_idx));
      return list;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return source.types();
    }

    @Override
    public void close() {
      source.close();
    }
    
  }
  
  class PredictionArrayIterator implements TypedTimeSeriesIterator<NumericArrayType>, 
      TimeSeriesValue<NumericArrayType>, NumericArrayType {
    final int prediction_idx;
    double[] array = new double[2048];
    int idx = 0;
    boolean has_next;
    TimeStamp timestamp;
    
    protected PredictionArrayIterator(final String payload, 
                                      final int prediction_idx) {
      this.prediction_idx = prediction_idx;
      // TODO - proper
      timestamp = new SecondTimeStamp(prediction_starts[prediction_idx]);
      int read_idx = 0;
      try {
        JsonNode root = JSON.getMapper().readTree(payload);
        JsonNode ds = root.get("ds");
        JsonNode yhat = root.get("yhat");
        
        // TODO - may be something goofy here.
        long start = prediction_starts[prediction_idx];
        long end = config.getMode() != ExecutionMode.CONFIG ? 
            prediction_starts[prediction_idx] + 
              (model_units == ChronoUnit.HOURS ? 3600 : 86400) : 
                context.query().endTime().epoch();
        
        while (true) {
          JsonNode ts = ds.get(Integer.toString(read_idx));
          if (ts == null) {
            // done
            break;
          }
          
          long timestamp = ts.asLong();
          if (timestamp < start) {
            read_idx++;
            continue;
          }
          if (timestamp >= end) {
            break;
          }
          
          JsonNode val = yhat.get(Integer.toString(read_idx));
          array[idx++] = val.asDouble();
          read_idx++;
        }
        if (idx > 0) {
          has_next = true;
        }
      } catch (Exception e) {
        LOG.error("Failed to parse the JSON?", e);
        throw new QueryDownstreamException("Failed to parse prophet result", e);
      }
      
    }
    
    @Override
    public TypeToken getType() {
      return NumericArrayType.TYPE;
    }

    @Override
    public boolean hasNext() {
      return has_next;
    }

    @Override
    public TimeSeriesValue<NumericArrayType> next() {
      has_next = false;
      return this;
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public TimeStamp timestamp() {
      return timestamp;
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
      return false;
    }
    
    @Override
    public long[] longArray() {
      return null;
    }
    
    @Override
    public double[] doubleArray() {
      return array;
    }
    
  }
}
