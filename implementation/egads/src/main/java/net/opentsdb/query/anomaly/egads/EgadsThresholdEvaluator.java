// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.anomaly.egads;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.alert.AlertValue;
import net.opentsdb.data.types.alert.AlertType.State;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.anomaly.BaseAnomalyConfig;

/**
 * A class that takes a time series to evaluate and the matching prediction
 * time series. It then iterates over the time series to generate alerts based
 * on the configured thresholds as well as optionally recording the computed
 * thresholds.
 * 
 * TODO - grow the threshold arrays as needed.
 * TODO - clean this up, the ctor is ugly as all get-out.
 * 
 * @since 3.0
 */
public class EgadsThresholdEvaluator {
  static final Logger LOG = LoggerFactory.getLogger(
      EgadsThresholdEvaluator.class);
  
  public static final String UPPER_BAD = "upperBad";
  public static final String UPPER_WARN = "upperWarn";
  public static final String LOWER_BAD = "lowerBad";
  public static final String LOWER_WARN = "lowerWarn";
  
  protected final BaseAnomalyConfig config;
  protected final TimeSeries current;
  protected final QueryResult current_result;
  protected final TimeSeries[] predictions;
  protected final QueryResult[] prediction_results;
  
  protected int idx;
  protected double[] upper_bad_thresholds;
  protected double[] upper_warn_thresholds;
  protected double[] lower_bad_thresholds;
  protected double[] lower_warn_thresholds;
  protected double[] deltas;
  protected List<AlertValue> alerts;
  protected int prediction_index;
  protected long threshold_base;
  
  public EgadsThresholdEvaluator(final BaseAnomalyConfig config, 
                                 final int threshold_dps,
                                 final TimeSeries current,
                                 final QueryResult current_result,
                                 final TimeSeries[] predictions,
                                 final QueryResult[] prediction_results) {
    this.config = config;
    if (config.getSerializeThresholds()) {
      if (config.getUpperThresholdBad() != 0) {
        upper_bad_thresholds = new double[threshold_dps];
        Arrays.fill(upper_bad_thresholds, Double.NaN);
      }
      if (config.getUpperThresholdWarn() != 0) {
        upper_warn_thresholds = new double[threshold_dps];
        Arrays.fill(upper_warn_thresholds, Double.NaN);
      }
      if (config.getLowerThresholdBad() != 0) {
        lower_bad_thresholds = new double[threshold_dps];
        Arrays.fill(lower_bad_thresholds, Double.NaN);
      }
      if (config.getLowerThresholdWarn() != 0) {
        lower_warn_thresholds = new double[threshold_dps];
        Arrays.fill(lower_warn_thresholds, Double.NaN);
      }
    }
    if (config.getSerializeDeltas()) {
      deltas = new double[threshold_dps];
      Arrays.fill(deltas, Double.NaN);
    }
    this.current = current;
    this.current_result = current_result;
    this.predictions = predictions;
    this.prediction_results = prediction_results;
  }
  
  public boolean evaluate() {
    if (predictions == null || predictions.length == 0) {
      // TODO - meh
      LOG.warn("Predictions was null or empty.");
      return false;
    }

    TypedTimeSeriesIterator<? extends TimeSeriesDataType> pred_it = null;
    while (prediction_index < predictions.length) {
      if (predictions[prediction_index] == null) {
        prediction_index++;
        if (threshold_base == 0) {
          threshold_base = -1;
        }
        continue;
      }
      
      final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> pred_op = 
          predictions[prediction_index].iterator(NumericArrayType.TYPE);
      if (!pred_op.isPresent()) {
        LOG.warn("No array iterators for prediction at: " + prediction_index);
        continue;
      }
      
      pred_it = pred_op.get();
      if (!pred_it.hasNext()) {
        LOG.warn("No data in the prediction array at: " + prediction_index);
        continue;
      }
      
      // set the threshold base
      if (threshold_base == 0) {
        threshold_base = prediction_results[prediction_index]
            .timeSpecification().start().epoch();
      } else if (threshold_base < 0) {
        // need to set the expected threshold start time based on the interval;
        threshold_base = prediction_results[prediction_index]
            .timeSpecification().start().epoch();
        for (int i = 0; i < prediction_index; i++) {
          threshold_base -= (prediction_results[prediction_index]
            .timeSpecification().end().epoch() - 
              prediction_results[prediction_index]
                .timeSpecification().start().epoch());
        }
      }
      break;
    }

    if (pred_it == null) {
      LOG.warn("No prediction data.");
      return false;
    }
    
    final TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) pred_it.next();
    if (value.value() == null) {
      LOG.warn("Null value?");
      return false;
    }
    
    TypeToken<? extends TimeSeriesDataType> current_type = null;
    for (final TypeToken<? extends TimeSeriesDataType> type : current.types()) {
      if (type == NumericType.TYPE ||
          type == NumericArrayType.TYPE ||
          type == NumericSummaryType.TYPE) {
        current_type = type;
        break;
      }
    }
    
    if (current_type == null) {
      LOG.warn("No type for current?");
      return false;
    }
    
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> current_op =
        current.iterator(current_type);
    if (!current_op.isPresent()) {
      LOG.warn("No data for type: " + current_type + " in current?");
      return false;
    }
    
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
        current_op.get();
    if (!iterator.hasNext()) {
      LOG.warn("No next type: " + current_type + " in current?");
      return false;
    }
    
    if (current_type == NumericType.TYPE) {
      runNumericType(iterator, value);
    } else if (current_type == NumericArrayType.TYPE) {
      runNumericArrayType(iterator, value);
    } else if (current_type == NumericSummaryType.TYPE) {
      runNumericSummaryType(iterator, value);
    } else {
      LOG.warn("Ummm, don't handle this type?");
      return false;
    }
    return true;
  }
  
  void runNumericType(
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      TimeSeriesValue<NumericArrayType> prediction) {
    long prediction_base = prediction_results[prediction_index]
        .timeSpecification().start().epoch();
    // TODO - won't work for biiiiig time ranges
    long prediction_interval = prediction_results[prediction_index].timeSpecification()
        .interval().get(ChronoUnit.SECONDS);
    
    while (iterator.hasNext()) {
      final TimeSeriesValue<NumericType> value = 
          (TimeSeriesValue<NumericType>) iterator.next();
      if (value.timestamp().compare(Op.GTE, prediction_results[prediction_index]
          .timeSpecification().end())) {
        while (++prediction_index < predictions.length && 
               predictions[prediction_index] == null) {
          continue;
        }
        
        if (prediction_index >= predictions.length) {
          // out of bounds now
          return;
        }
        
        Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> pred_op = 
            predictions[prediction_index].iterator(NumericArrayType.TYPE);
        if (!pred_op.isPresent()) {
          return;
        }
        
        TypedTimeSeriesIterator<? extends TimeSeriesDataType> pred_it = 
            pred_op.get();
        if (!pred_it.hasNext()) {
          return;
        }
        prediction = (TimeSeriesValue<NumericArrayType>) pred_it.next();
        prediction_base = prediction_results[prediction_index]
            .timeSpecification().start().epoch();
      }
      
      if (value.timestamp().compare(Op.LT, prediction_results[prediction_index]
          .timeSpecification().start())) {
        continue;
      }
      
      int idx = (int) ((value.timestamp().epoch() - prediction_base) / prediction_interval);
      if (idx + prediction.value().offset() >= prediction.value().end()) {
        LOG.warn(idx + " beyond the prediction range.");
      }
      
      AlertValue av = eval(value.timestamp(), value.value().toDouble(), 
          (prediction.value().isInteger() ? (double) prediction.value().longArray()[idx] :
            prediction.value().doubleArray()[idx]),
          (int) ((value.timestamp().epoch() - threshold_base) / prediction_interval));
      if (av != null) {
        if (alerts == null) {
          alerts = Lists.newArrayList();
        }
        alerts.add(av);
      }
    }
  }
  
  void runNumericArrayType(
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      TimeSeriesValue<NumericArrayType> prediction) {
    long prediction_base = prediction_results[prediction_index]
        .timeSpecification().start().epoch();
    
    // TODO - won't work for biiiiig time ranges
    long prediction_interval = prediction_results[prediction_index].timeSpecification()
        .interval().get(ChronoUnit.SECONDS);
    final TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    final TimeStamp ts = current_result.timeSpecification().start().getCopy();
    for (int i = value.value().offset(); i < value.value().end(); i++) {
      if (ts.compare(Op.GTE, prediction_results[prediction_index]
            .timeSpecification().end())) {
        while (++prediction_index < predictions.length && 
               predictions[prediction_index] == null) {
          continue;
        }
        
        if (prediction_index >= predictions.length) {
          // out of bounds now
          return;
        }
        
        Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> pred_op = 
            predictions[prediction_index].iterator(NumericArrayType.TYPE);
        if (!pred_op.isPresent()) {
          return;
        }
        
        TypedTimeSeriesIterator<? extends TimeSeriesDataType> pred_it = 
            pred_op.get();
        if (!pred_it.hasNext()) {
          return;
        }
        prediction = (TimeSeriesValue<NumericArrayType>) pred_it.next();
        prediction_base = prediction_results[prediction_index]
            .timeSpecification().start().epoch();
      }
      
      if (ts.compare(Op.LT, prediction_results[prediction_index]
            .timeSpecification().start())) {
        ts.add(current_result.timeSpecification().interval());
        continue;
      }
      
      int idx = (int) ((ts.epoch() - prediction_base) / prediction_interval);
      if (idx + prediction.value().offset() >= prediction.value().end()) {
        ts.add(current_result.timeSpecification().interval());
        continue;
      }
      
      final AlertValue av = eval(ts, 
          (value.value().isInteger() ? (double) value.value().longArray()[i] :
            value.value().doubleArray()[i]), 
          (prediction.value().isInteger() ? (double) prediction.value().longArray()[idx] :
            prediction.value().doubleArray()[idx]),
          (int) ((ts.epoch() - threshold_base) / prediction_interval));
      if (av != null) {
        if (alerts == null) {
          alerts = Lists.newArrayList();
        }
        alerts.add(av);
      }
      
      ts.add(current_result.timeSpecification().interval());
    }
    
  }
  
  void runNumericSummaryType(
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      TimeSeriesValue<NumericArrayType> prediction) {
    long prediction_base = prediction_results[prediction_index]
        .timeSpecification().start().epoch();
    // TODO - won't work for biiiiig time ranges
    long prediction_interval = prediction_results[prediction_index].timeSpecification()
        .interval().get(ChronoUnit.SECONDS);
    int summary = -1;
    
    while (iterator.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> value = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      if (value.timestamp().compare(Op.GTE, prediction_results[prediction_index]
          .timeSpecification().end())) {
        while (++prediction_index < predictions.length && 
                predictions[prediction_index] == null) {
           continue;
         }
        
        Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> pred_op = 
            predictions[prediction_index].iterator(NumericArrayType.TYPE);
        if (!pred_op.isPresent()) {
          return;
        }
        
        TypedTimeSeriesIterator<? extends TimeSeriesDataType> pred_it = 
            pred_op.get();
        if (!pred_it.hasNext()) {
          return;
        }
        prediction = (TimeSeriesValue<NumericArrayType>) pred_it.next();
        prediction_base = prediction_results[prediction_index]
            .timeSpecification().start().epoch();
      }
      
      if (value.timestamp().compare(Op.LT, prediction_results[prediction_index]
          .timeSpecification().start())) {
        continue;
      }
      
      int idx = (int) ((value.timestamp().epoch() - prediction_base) / 
          prediction_interval);
      if (idx + prediction.value().offset() >= prediction.value().end()) {
        LOG.warn(idx + " beyond the prediction range.");
      }
      
      if (summary < 0) {
        summary = value.value().summariesAvailable().iterator().next();
      }
      AlertValue av = eval(value.timestamp(), value.value().value(summary)
          .toDouble(), 
          (prediction.value().isInteger() ? (double) prediction.value().longArray()[idx] :
            prediction.value().doubleArray()[idx]),
          (int) ((value.timestamp().epoch() - threshold_base) / prediction_interval));
      if (av != null) {
        if (alerts == null) {
          alerts = Lists.newArrayList();
        }
        alerts.add(av);
      }
    }
  }
  
  AlertValue eval(final TimeStamp timestamp, 
                  final double current, 
                  final double prediction,
                  final int threshold_idx) {
    AlertValue result = null;
    if (config.getUpperThresholdBad() != 0) {
      final double threshold;
      if (config.isUpperIsScalar()) {
        threshold = prediction + config.getUpperThresholdBad();
      } else {
        threshold = prediction + Math.abs((prediction * (
            config.getUpperThresholdBad() / 100)));
      }
      if (config.getSerializeAlerts() && 
          config.isUpperIsScalar() && current > threshold) {
        result = AlertValue.newBuilder()
            .setState(State.BAD)
            .setDataPoint(current)
            .setMessage("** TEMP " + current + " is > " + threshold)
            .setTimestamp(timestamp)
            .setThreshold(threshold)
            .setThresholdType(UPPER_BAD)
            .build();
      } else if (config.getSerializeAlerts() && current > threshold) {
        result = AlertValue.newBuilder()
            .setState(State.BAD)
            .setDataPoint(current)
            .setMessage("** TEMP " + current + " is greater than " + threshold 
                + " which is > than " + config.getUpperThresholdBad() + "%")
            .setTimestamp(timestamp)
            .setThreshold(threshold)
            .setThresholdType(UPPER_BAD)
            .build();
      }
      
      if (config.getSerializeThresholds()) {
        if (threshold_idx >= upper_bad_thresholds.length) {
          throw new IllegalStateException("Attempted to write too many upper "
              + "thresholds [" + threshold_idx + "]. Make sure to set the threshold_dps ["
              + upper_bad_thresholds.length + "] properly in the ctor.");
        }
        upper_bad_thresholds[threshold_idx] = threshold;
        if (threshold_idx > idx) {
          idx = threshold_idx;
        }
      }
    }
    
    if (config.getUpperThresholdWarn() != 0) {
      final double threshold;
      if (config.isUpperIsScalar()) {
        threshold = prediction + config.getUpperThresholdWarn();
      } else {
        threshold = prediction + Math.abs((prediction * (
            config.getUpperThresholdWarn() / 100)));
      }
      if (config.getSerializeAlerts() && 
          config.isUpperIsScalar() && current > threshold) {
        result = AlertValue.newBuilder()
            .setState(State.WARN)
            .setDataPoint(current)
            .setMessage("** TEMP " + current + " is > " + threshold)
            .setTimestamp(timestamp)
            .setThreshold(threshold)
            .setThresholdType(UPPER_WARN)
            .build();
      } else if (config.getSerializeAlerts() && current > threshold) {
        result = AlertValue.newBuilder()
            .setState(State.WARN)
            .setDataPoint(current)
            .setMessage("** TEMP " + current + " is greater than " + threshold 
                + " which is > than " + config.getUpperThresholdWarn() + "%")
            .setTimestamp(timestamp)
            .setThreshold(threshold)
            .setThresholdType(UPPER_WARN)
            .build();
      }
      
      if (config.getSerializeThresholds()) {
        if (threshold_idx >= upper_warn_thresholds.length) {
          throw new IllegalStateException("Attempted to write too many upper "
              + "thresholds [" + idx + "]. Make sure to set the report_len "
                  + "properly in the ctor.");
        }
        upper_warn_thresholds[threshold_idx] = threshold;
        if (threshold_idx > idx) {
          idx = threshold_idx;
        }
      }
    }
    
    if (config.getLowerThresholdBad() != 0) {
      final double threshold;
      if (config.isLowerIsScalar()) {
        threshold = prediction - config.getLowerThresholdBad();
      } else {
        threshold = prediction - Math.abs((prediction * (
            config.getLowerThresholdBad() / (double) 100)));
      }
      if (config.getSerializeAlerts() && 
          config.isLowerIsScalar() && current < threshold) {
        if (result == null) {
          result = AlertValue.newBuilder()
              .setState(State.BAD)
              .setDataPoint(current)
              .setMessage("** TEMP " + current + " is < " + threshold)
              .setTimestamp(timestamp)
              .setThreshold(threshold)
              .setThresholdType(LOWER_BAD)
              .build();
        }
      } else if (config.getSerializeAlerts() && current < threshold) {
        if (result == null) {
          result = AlertValue.newBuilder()
              .setState(State.BAD)
              .setDataPoint(current)
              .setMessage("** TEMP " + current + " is less than " + threshold 
                  + " which is < than " + config.getLowerThresholdBad() + "%")
              .setTimestamp(timestamp)
              .setThreshold(threshold)
              .setThresholdType(LOWER_BAD)
              .build();
        }
      }
      if (config.getSerializeThresholds()) {
        if (threshold_idx >= lower_bad_thresholds.length) {
          throw new IllegalStateException("Attempted to write too many lower "
              + "thresholds [" + idx + "]. Make sure to set the report_len "
              + "properly in the ctor.");
        }
        lower_bad_thresholds[threshold_idx] = threshold;
        if (threshold_idx > idx) {
          idx = threshold_idx;
        }
      }
    }
    
    if (config.getLowerThresholdWarn() != 0) {
      final double threshold;
      if (config.isLowerIsScalar()) {
        threshold = prediction - config.getLowerThresholdWarn();
      } else {
        threshold = prediction - Math.abs((prediction * (
            config.getLowerThresholdWarn() / (double) 100)));
      }
      if (config.getSerializeAlerts() && 
          config.isLowerIsScalar() && current < threshold) {
        if (result == null) {
          result = AlertValue.newBuilder()
              .setState(State.WARN)
              .setDataPoint(current)
              .setMessage("** TEMP " + current + " is < " + threshold)
              .setTimestamp(timestamp)
              .setThreshold(threshold)
              .setThresholdType(LOWER_WARN)
              .build();
        }
      } else if (config.getSerializeAlerts() && current < threshold) {
        if (result == null) {
          result = AlertValue.newBuilder()
              .setState(State.WARN)
              .setDataPoint(current)
              .setMessage("** TEMP " + current + " is less than " + threshold 
                  + " which is < than " + config.getLowerThresholdWarn() + "%")
              .setTimestamp(timestamp)
              .setThreshold(threshold)
              .setThresholdType(LOWER_WARN)
              .build();
        }
      }
      if (config.getSerializeThresholds()) {
        if (threshold_idx >= lower_warn_thresholds.length) {
          throw new IllegalStateException("Attempted to write too many lower "
              + "thresholds [" + idx + "]. Make sure to set the report_len "
              + "properly in the ctor.");
        }
        lower_warn_thresholds[threshold_idx] = threshold;
        if (threshold_idx > idx) {
          idx = threshold_idx;
        }
      }
    }
    
    if (deltas != null) {
      if (threshold_idx >= deltas.length) {
        throw new IllegalStateException("Attempted to write too many deltas [" 
            + idx + "]. Make sure to set the report_len "
            + "properly in the ctor.");
      }
      deltas[threshold_idx] = current - prediction; 
      if (threshold_idx > idx) {
        idx = threshold_idx;
      }
    }
    
    idx++;
    return result;
  }
  
  public double[] upperBadThresholds() {
    return upper_bad_thresholds;
  }
  
  public double[] upperWarnThresholds() {
    return upper_warn_thresholds;
  }
  
  public double[] lowerBadThresholds() {
    return lower_bad_thresholds;
  }
  
  public double[] lowerWarnThresholds() {
    return lower_warn_thresholds;
  }
  
  public double[] deltas() {
    return deltas;
  }
  
  public int index() {
    return idx;
  }
  
  public List<AlertValue> alerts() {
    return alerts;
  }
}