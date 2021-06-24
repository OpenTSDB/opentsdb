// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.data;

import net.opentsdb.storage.TimeSeriesDataConsumer.WriteCallback;
import net.opentsdb.storage.WriteStatus;

/**
 * This is a fun one in that it will take a given LLTSD and while iterating, if
 * one or more of the series are to be dropped or ignored on forwarding, simply
 * tombstones that value so that on calls to {@link LowLevelTimeSeriesData#advance()},
 * the tomestoned values are skipped.
 *
 * @since 3.0
 */
public interface LowLevelTimeSeriesDataDropWrapper {

  /**
   * Used on a reset to set the data and callback.
   * @param data The non-null data to wrap.
   * @param writeCallback An optional callback.
   */
  public void setData(final LowLevelTimeSeriesData data,
                      final WriteCallback writeCallback);

  /**
   * Marks an entry in the data payload for dropping/ignoring when passed
   * downstream. This simply means the calls to {@link LowLevelTimeSeriesData#advance()}
   * will skip the dropped records.
   * @param index The index of the record to drop, 0 based.
   * @param status The status to drop the record with, will be returned to the
   *               original write callback if set.
   */
  public void drop(final int index, final WriteStatus status);

  /** @return The callback for this wrapper to be passed downstream. If the
   * callback in {@link #setData(LowLevelTimeSeriesData, WriteCallback)} was
   * not null, this MUST return a non-null callback. */
  public WriteCallback wrapperCallback();
}
