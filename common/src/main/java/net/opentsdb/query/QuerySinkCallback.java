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
package net.opentsdb.query;

import net.opentsdb.data.PartialTimeSeries;

/**
 * A callback passed to the QuerySink so that the query context knows when a 
 * sink is finished with a particular partial time series and can close it out
 * and determine when it should close out the query. This is used instead of
 * deferreds or futures to avoid creating tons of objects.
 * 
 * @since 3.0
 */
public interface QuerySinkCallback {

  /**
   * Called by the sink when it has finished dealing with the partial.
   * @param pts The non-null partial the sink is done with.
   */
  public void onComplete(final PartialTimeSeries pts);
  
  /**
   * Called by the sink if an error happens when dealing with a partial.
   * @param pts The non-null partial the sink was working on when an error
   * happened.
   * @param t The error.
   */
  public void onError(final PartialTimeSeries pts, final Throwable t);
  
}
