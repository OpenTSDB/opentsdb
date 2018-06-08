// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import com.google.common.hash.HashCode;

/**
 * The configuration interface for a particular query node. Queries will populate
 * the configs when instantiating a DAG.
 * 
 * @since 3.0
 */
public interface QueryNodeConfig extends Comparable<QueryNodeConfig> {

  /**
   * @return The ID of the node in this config.
   */
  public String getId();
  
  /** @return A hash code for this configuration. */
  public HashCode buildHashCode();
  
}
