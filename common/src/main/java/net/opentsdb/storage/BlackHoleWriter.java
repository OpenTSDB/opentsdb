// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.data.LowLevelTimeSeriesData;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;

/**
 * Simple writer that just dumps the data into the bit-bucket in the sky.
 * 
 * @since 3.0
 */
public class BlackHoleWriter extends BaseTSDBPlugin implements
        TimeSeriesDataConsumer,
        TimeSeriesDataConsumerFactory {

  public static final String TYPE = "BlackHoleWriter";
  
  /**
   * Default ctor.
   */
  public BlackHoleWriter() {
    
  }
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public TimeSeriesDataConsumer consumer() {
    return this;
  }

  @Override
  public void write(final AuthState state,
                    final TimeSeriesDatum datum,
                    final WriteCallback callback) {
    if (callback != null) {
      callback.success();
    }
  }

  @Override
  public void write(final AuthState state,
                    final TimeSeriesSharedTagsAndTimeData data,
                    final WriteCallback callback) {
    if (callback != null) {
      callback.success();
    }
  }

  @Override
  public void write(final AuthState state,
                    final LowLevelTimeSeriesData data,
                    final WriteCallback callback) {
    if (callback != null) {
      callback.success();
    }
  }
}
