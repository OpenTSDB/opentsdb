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
package net.opentsdb.query.anomaly;

import java.util.Objects;

import net.opentsdb.utils.JSON;

/**
 * A state object used to track the state of prediction generation in the 
 * system.
 * 
 * TODO - proper getters and builder.
 * 
 * @since 3.0
 */
public class AnomalyPredictionState {

  public static enum State {
    RUNNING,
    COMPLETE,
    ERROR
  }
  
  public String host;
  public long startTime;
  public long predictionStartTime;
  public long lastUpdateTime;
  public State state;
  public long hash;
  public String exception;
  
  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof AnomalyPredictionState)) {
      return false;
    }
    
    AnomalyPredictionState other = (AnomalyPredictionState) o;
    return Objects.equals(host, other.host) &&
        Objects.equals(startTime, other.startTime) &&
        Objects.equals(predictionStartTime, other.predictionStartTime) &&
        Objects.equals(lastUpdateTime, other.lastUpdateTime) &&
        Objects.equals(state, other.state) &&
        Objects.equals(hash, other.hash);
    // purposely leaving out the exception.
  }
  
  @Override
  public String toString() {
    return JSON.serializeToString(this);
  }
}