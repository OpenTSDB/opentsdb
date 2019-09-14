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
package net.opentsdb.query.readcache;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryResult;

/**
 * Interface for results returned from a read cache.
 * 
 * @since 3.0
 */
public interface ReadCacheQueryResult extends QueryResult {
  
  /** @return The non-null timestamp of the last value from all time series
   * in the cached result. May be 0. */
  public TimeStamp lastValueTimestamp();

}
