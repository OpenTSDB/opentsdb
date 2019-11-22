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

import net.opentsdb.query.QueryNodeConfig;

/**
 * The base interface for an anomaly model configuration.
 *
 * @since 3.0
 */
public interface AnomalyConfig extends QueryNodeConfig {

  public static enum ExecutionMode {
    /** Generates the prediction if necessary and evaluates, returning only the 
      * prediction, observed, alerts, upper and lower thresholds. */
    EVALUATE,
    
    /** Just generates the prediction and caches it if appropriate. Returns an
     * empty data set with the HTTP status code indicating success or failure. */
    PREDICT,
    
    /** Does not cache the predictions but is used in building a query in that
     * it will return all of the data including base lines. */
    CONFIG,
  }
  
  /** @return The non-null execution mode for this config. */
  public ExecutionMode getMode();
  
  /** @return Whether or not to serialize the observed data in the query result
   * class. */
  public boolean getSerializeObserved();
  
  /** @return Whether or not to serialize the computed thresholds in the query
   * result.  */
  public boolean getSerializeThresholds();
  
}