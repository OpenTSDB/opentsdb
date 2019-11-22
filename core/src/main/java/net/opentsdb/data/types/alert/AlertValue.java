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
package net.opentsdb.data.types.alert;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;

/**
 * A specific instance of an alert value. Just a simple object with a builder
 * to fill it out.
 * 
 * @since 3.0
 */
public class AlertValue implements AlertType, TimeSeriesValue<AlertType> {
  private State state;
  private TimeStamp timestamp;
  private MutableNumericType data_point;
  private String message;
  private MutableNumericType threshold;
  private String threshold_type;
  
  protected AlertValue(final Builder builder) {
    state = builder.state;
    timestamp = builder.timestamp;
    data_point = builder.data_point;
    message = builder.message;
    threshold = builder.threshold;
    threshold_type = builder.threshold_type;
  }
  
  @Override
  public State state() {
    return state;
  }
  
  @Override
  public String message() {
    return message;
  }

  @Override
  public NumericType dataPoint() {
    return data_point;
  }

  @Override
  public NumericType threshold() {
    return threshold;
  }
  
  @Override
  public String thresholdType() {
    return threshold_type;
  }
  
  @Override
  public TypeToken<AlertType> type() {
    return AlertType.TYPE;
  }

  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public AlertType value() {
    return this;
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private State state;
    private TimeStamp timestamp;
    private MutableNumericType data_point;
    private String message;
    private MutableNumericType threshold;
    private String threshold_type;
    
    public Builder setState(final State state) {
      this.state = state;
      return this;
    }
    
    public Builder setTimestamp(final TimeStamp timestamp) {
      this.timestamp = timestamp.getCopy();
      return this;
    }
    
    public Builder setDataPoint(final NumericType value) {
      data_point = new MutableNumericType(value);
      return this;
    }
    
    public Builder setDataPoint(final long value) {
      data_point = new MutableNumericType(value);
      return this;
    }
    
    public Builder setDataPoint(final double value) {
      data_point = new MutableNumericType(value);
      return this;
    }
    
    public Builder setMessage(final String message) {
      this.message = message;
      return this;
    }
    
    public Builder setThreshold(final long value) {
      threshold = new MutableNumericType(value);
      return this;
    }
    
    public Builder setThreshold(final double value) {
      threshold = new MutableNumericType(value);
      return this;
    }
    
    public Builder setThresholdType(final String threshold_type) {
      this.threshold_type = threshold_type;
      return this;
    }
    
    public AlertValue build() {
      return new AlertValue(this);
    }
  }
}