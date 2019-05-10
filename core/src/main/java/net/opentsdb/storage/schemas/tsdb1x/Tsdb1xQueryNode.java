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
package net.opentsdb.storage.schemas.tsdb1x;

import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.storage.SourceNode;

/** 
 * The start of an interface or base class for 1x query nodes.
 * 
 * @since 3.0
 */
public interface Tsdb1xQueryNode extends TimeSeriesDataSource, SourceNode {

  /** Sets the sentData flag. */
  public void setSentData();
  
  /** @return Whether or not the node sent data in push node. */
  public boolean sentData();
  
}
