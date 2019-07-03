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

import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.storage.schemas.tsdb1x.Schema;

/**
 * An interface denoting the Query node as a storage layer node.
 * 
 * @since 3.0
 */
public interface SourceNode<T extends TimeSeriesDataSourceConfig> extends TimeSeriesDataSource<T> {

  /**
   * A timestamp representing the current end of a slice of time to stop
   * fetching data and return results.
   * @return A non-null timestamp.
   */
  public TimeStamp sequenceEnd();
  
  /** @return The schema used by this query node. */
  public Schema schema();
  
}
