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
package net.opentsdb.storage.schemas.tsdb1x;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;

/**
 * The interface for a time series data store factory to spawn instances
 * of a data store.
 * 
 * @since 3.0
 */
public interface Tsdb1xDataStoreFactory extends TSDBPlugin {

  /**
   * Returns a new (or shared) instance of the data store. 
   * @param tsdb A non-null TSDB instance to pull the config from.
   * @param id An optional (may be null or empty) ID for the instance.
   * @param schema A non-null schema to use for encoding and decoding
   * data with the store.
   * @return A non-null data store.
   */
  public Tsdb1xDataStore newInstance(final TSDB tsdb, 
                                     final String id, 
                                     final Schema schema);
}
