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
package net.opentsdb.meta;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.TimeSeriesQuery;

/**
 * Handles querying a meta data store for time series identifiers given
 * the query. Not used for the actual time series data.
 * 
 * @since 3.0
 */
public interface MetaDataStorageSchema {

  public Deferred<MetaDataStorageResult> runQuery(final TimeSeriesQuery query);

}