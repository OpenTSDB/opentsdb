// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query;

/**
 * A query result ID. This should be set during configuration node setup so that
 * upstream nodes in the graph can find out how much data to expect.
 * 
 * @since 3.0
 */
public interface QueryResultId {

  /** @return The non-null and non-empty ID of the node the result came from. */
  public String nodeID();
  
  /** @return The non-null and non-empty data source ID for the result. */
  public String dataSource();
  
}
