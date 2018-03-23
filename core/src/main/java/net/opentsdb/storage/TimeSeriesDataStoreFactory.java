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

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

/**
 * A factory used for instantiating data stores.
 * 
 * @since 3.0
 */
public abstract class TimeSeriesDataStoreFactory extends BaseTSDBPlugin {

  /**
   * Returns a new instance of the data store created by this factory if
   * successful. On instantiation the store may throw 
   * {@link IllegalArgumentException}s if configuration was invalid.
   * 
   * @param tsdb A non-null TSDB instance to pull the config from.
   * @param id An optional ID for the instance. If null or empty then 
   * it's the default instance.
   * @return A data store instance.
   * @throws IllegalArgumentException if a required configuration setting
   * was missing.
   */
  public abstract TimeSeriesDataStore newInstance(final TSDB tsdb, 
                                                  final String id);
}
