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
  
  private final double upper_bad;
  private final double upper_warn;
  private final boolean upper_is_scalar;
  private final double lower_bad;
  private final double lower_warn;
  private final boolean lower_is_scalar;
  private final boolean report_thresholds;
  private final TimeSeries current;
  private final QueryResult current_result;
  private final TimeSeries prediction;
  private final QueryResult prediction_result;
  
  private int idx;
  private double[] upper_bad_thresholds;
  private double[] upper_warn_thresholds;
  private double[] lower_bad_thresholds;
  private double[] lower_warn_thresholds;
  private List<AlertValue> alerts;
  
  public EgadsThresholdEvaluator(final double upper_bad,
                                 final double upper_warn, 
                                 final boolean upper_is_scalar,
                                 final double lower_bad,
                                 final double lower_warn,
                                 final boolean lower_is_scalar, 
                                 final int report_len,
                                 final TimeSeries current,
                                 final QueryResult current_result,
                                 final TimeSeries prediction,
                                 final QueryResult prediction_result) {
    this.upper_bad = upper_bad;
    this.upper_warn = upper_warn;
    this.upper_is_scalar = upper_is_scalar;
    this.lower_bad = lower_bad;
    this.lower_warn = lower_warn;
    this.lower_is_scalar = lower_is_scalar;
    if (report_len > 0) {
      this.report_thresholds = true;
      if (upper_bad != 0) {
        upper_bad_thresholds = new double[report_len];
        Arrays.fill(upper_bad_thresholds, Double.NaN);
      }
      if (upper_warn != 0) {
        upper_warn_thresholds = new double[report_len];
        Arrays.fill(upper_warn_thresholds, Double.NaN);
      }
      if (lower_bad != 0) {
        lower_bad_thresholds = new double[report_len];
        Arrays.fill(lower_bad_thresholds, Double.NaN);
      }
      if (lower_warn != 0) {
        lower_warn_thresholds = new double[report_len];
        Arrays.fill(lower_warn_thresholds, Double.NaN);
      }
    } else {
      this.report_thresholds = false;
    }
    this.current = current;
    this.current_result = current_result;
    this.prediction = prediction;
    this.prediction_result = prediction_result;
  }
  
  public boolean evaluate() {
    if (prediction == null) {
      // TODO - meh
      LOG.warn("Prediction was null.");
      return false;
    }
    
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> pred_op = 
        prediction.iterator(NumericArrayType.TYPE);
    if (!pred_op.isPresent()) {
      LOG.warn("No array iterator for prediction.");
      return false;
    }
    
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> pred_it = pred_op.get();
    if (!pred_it.hasNext()) {
      LOG.warn("No data in the prediction array.");
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
      final TimeSeriesValue<NumericArrayType> prediction) {
    final long prediction_base = prediction_result.timeSpecification().start().epoch();
    // TODO - won't work for biiiiig time ranges
    final long prediction_interval = prediction_result.timeSpecification()
        .interval().get(ChronoUnit.SECONDS);
    
    while (iterator.hasNext()) {
      final TimeSeriesValue<NumericType> value = 
          (TimeSeriesValue<NumericType>) iterator.next();
      if (value.timestamp().compare(Op.LT, prediction_result.timeSpecification().start())) {
        continue;
      }
      
      int idx = (int) ((value.timestamp().epoch() - prediction_base) / prediction_interval);
      if (idx + prediction.value().offset() >= prediction.value().end()) {
        LOG.warn(idx + " beyond the prediction range.");
      }
      
      AlertValue av = eval(value.timestamp(), value.value().toDouble(), 
          (prediction.value().isInteger() ? (double) prediction.value().longArray()[idx] :
            prediction.value().doubleArray()[idx]),
          idx);
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
      final TimeSeriesValue<NumericArrayType> prediction) {
    final long prediction_base = prediction_result.timeSpecification().start().epoch();
    // TODO - won't work for biiiiig time ranges
    final long prediction_interval = prediction_result.timeSpecification()
        .interval().get(ChronoUnit.SECONDS);
    final TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    
    final TimeStamp ts = current_result.timeSpecification().start().getCopy();
    int wrote = 0;
    for (int i = value.value().offset(); i < value.value().end(); i++) {
      if (ts.compare(Op.LT, prediction_result.timeSpecification().start())) {
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
          idx);
      if (av != null) {
        if (alerts == null) {
          alerts = Lists.newArrayList();
        }
        alerts.add(av);
      }
      
      ts.add(current_result.timeSpecification().interval());
      wrote++;
    }
  }
  
  void runNumericSummaryType(
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final TimeSeriesValue<NumericArrayType> prediction) {
    final long prediction_base = prediction_result.timeSpecification().start().epoch();
    // TODO - won't work for biiiiig time ranges
    final long prediction_interval = prediction_result.timeSpecification()
        .interval().get(ChronoUnit.SECONDS);
    int summary = -1;
    
    while (iterator.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> value = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      if (value.timestamp().compare(Op.LT, prediction_result.timeSpecification().start())) {
        continue;
      }
      
      int idx = (int) ((value.timestamp().epoch() - prediction_base) / prediction_interval);
      if (idx + prediction.value().offset() >= prediction.value().end()) {
        LOG.warn(idx + " beyond the prediction range.");
      }
      
      if (summary < 0) {
        summary = value.value().summariesAvailable().iterator().next();
      }
      AlertValue av = eval(value.timestamp(), value.value().value(summary).toDouble(), 
          (prediction.value().isInteger() ? (double) prediction.value().longArray()[idx] :
            prediction.value().doubleArray()[idx]),
          idx);
      if (av != null) {
        if (alerts == null) {
          alerts = Lists.newArrayList();
        }
        alerts.add(av);
      }
    }
  }
  
  public AlertValue eval(final TimeStamp timestamp, 
                         final double current, 
                         final double prediction,
                         final int threshold_idx) {
    AlertValue result = null;
    if (upper_bad != 0) {
      final double threshold;
      if (upper_is_scalar) {
        threshold = prediction + upper_bad;
      } else {
        threshold = prediction + Math.abs((prediction * (upper_bad / 100)));
      }
      if (upper_is_scalar && current > threshold) {
        result = AlertValue.newBuilder()
            .setState(State.BAD)
            .setDataPoint(current)
            .setMessage("** TEMP " + current + " is > " + threshold)
            .setTimestamp(timestamp)
            .setThreshold(threshold)
            .setThresholdType(UPPER_BAD)
            .build();
      } else if (current > threshold) {
        result = AlertValue.newBuilder()
            .setState(State.BAD)
            .setDataPoint(current)
            .setMessage("** TEMP " + current + " is greater than " + threshold + " which is > than " + upper_bad + "%")
            .setTimestamp(timestamp)
            .setThreshold(threshold)
            .setThresholdType(UPPER_BAD)
            .build();
      }
      
      if (report_thresholds) {
        if (threshold_idx >= upper_bad_thresholds.length) {
          throw new IllegalStateException("Attempted to write too many upper "
              + "thresholds [" + idx + "]. Make sure to set the report_len "
                  + "properly in the ctor.");
        }
        upper_bad_thresholds[threshold_idx] = threshold;
        if (threshold_idx > idx) {
          idx = threshold_idx;
        }
      }
    }
    
    if (upper_warn != 0) {
      final double threshold;
      if (upper_is_scalar) {
        threshold = prediction + upper_warn;
      } else {
        threshold = prediction + Math.abs((prediction * (upper_warn / 100)));
      }
      if (upper_is_scalar && current > threshold) {
        result = AlertValue.newBuilder()
            .setState(State.WARN)
            .setDataPoint(current)
            .setMessage("** TEMP " + current + " is > " + threshold)
            .setTimestamp(timestamp)
            .setThreshold(threshold)
            .setThresholdType(UPPER_WARN)
            .build();
      } else if (current > threshold) {
        result = AlertValue.newBuilder()
            .setState(State.WARN)
            .setDataPoint(current)
            .setMessage("** TEMP " + current + " is greater than " + threshold + " which is > than " + upper_warn + "%")
            .setTimestamp(timestamp)
            .setThreshold(threshold)
            .setThresholdType(UPPER_WARN)
            .build();
      }
      
      if (report_thresholds) {
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
    
    if (lower_bad != 0) {
      final double threshold;
      if (lower_is_scalar) {
        threshold = prediction - lower_bad;
      } else {
        threshold = prediction - Math.abs((prediction * (lower_bad / (double) 100)));
      }
      if (lower_is_scalar && current < threshold) {
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
      } else if (current < threshold) {
        if (result == null) {
          result = AlertValue.newBuilder()
              .setState(State.BAD)
              .setDataPoint(current)
              .setMessage("** TEMP " + current + " is less than " + threshold + " which is < than " + lower_bad + "%")
              .setTimestamp(timestamp)
              .setThreshold(threshold)
              .setThresholdType(LOWER_BAD)
              .build();
        }
      }
      if (report_thresholds) {
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
    
    if (lower_warn != 0) {
      final double threshold;
      if (lower_is_scalar) {
        threshold = prediction - lower_warn;
      } else {
        threshold = prediction - Math.abs((prediction * (lower_warn / (double) 100)));
      }
      if (lower_is_scalar && current < threshold) {
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
      } else if (current < threshold) {
        if (result == null) {
          result = AlertValue.newBuilder()
              .setState(State.WARN)
              .setDataPoint(current)
              .setMessage("** TEMP " + current + " is less than " + threshold + " which is < than " + lower_warn + "%")
              .setTimestamp(timestamp)
              .setThreshold(threshold)
              .setThresholdType(LOWER_WARN)
              .build();
        }
      }
      if (report_thresholds) {
        if (threshold_idx >= lower_warn_thresholds.length) {
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
  
  public int index() {
    return idx;
  }
  
  public List<AlertValue> alerts() {
    return alerts;
  }
}