// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;

/**
 * The interface used for implementing a data store that can accept 
 * writes via the OpenTSDB APIs.
 * 
 * @since 3.0
 */
public interface WritableTimeSeriesDataStoreFactory extends TSDBPlugin{
  
  /**
   * Returns a reference to a specific data store instance with the 
   * given ID. If the ID is null or empty it's the "default" instance.
   * @param tsdb A non-null TSD to pull configs from.
   * @param id An optional ID.
   * @return An instantiated time series data store.
   */
  public WritableTimeSeriesDataStore newStoreInstance(final TSDB tsdb, 
                                                      final String id);
  
}
