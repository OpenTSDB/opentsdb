// This file is part of OpenTSDB.
// Copyright (C) 2018-2021  The OpenTSDB Authors.
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

import net.opentsdb.auth.AuthState;
import net.opentsdb.data.LowLevelTimeSeriesData;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;

/**
 * This is the base interface for a component that will receive and write or
 * route incoming time series data asynchronously. It could be the final storage
 * destination, a filter to make sure the data is valid or a router that stashes
 * the data in a queue of some kind. Implementations can load other consumers
 * and pass the data down a chain.
 * </p>
 * For data that has to be closed, like the {@link LowLevelTimeSeriesData}
 * instances, implementations that receive a {@link WriteCallback} reference
 * must NOT close the data. Whatever instance is responsible for passing in a
 * callback is responsible for closing the data. Similarly, if a consumer gets
 * a callback, it must pass a callback downstream. However if the callback was
 * null the implementation can assume it is safe to close the data.
 *
 * @since 3.0
 */
public interface TimeSeriesDataConsumer {

  /**
   * Consumes or writes the given datum.
   * @param state A required state to use for authorization, filtering
   * and routing.
   * @param datum A single value.
   * @param callback An optional callback to respond to with results of the write.
   */
  public void write(final AuthState state,
                    final TimeSeriesDatum datum,
                    final WriteCallback callback);

  /**
   * Consumes or writes the given data.
   * @param state A required state to use for authorization, filtering
   * and routing.
   * @param data A set of values to store.
   * @param callback An optional callback to respond to with results of the write.
   */
  public void write(final AuthState state,
                    final TimeSeriesSharedTagsAndTimeData data,
                    final WriteCallback callback);

  /**
   * Consumes or writes the given data.
   * @param state A required state to use for authorization, filtering
   * and routing.
   * @param data A set of values to store.
   * @param callback An optional callback to respond to with results of the write.
   *                 If the callback is null then the data is closed when the
   *                 consumer is finished with it.
   */
  public void write(final AuthState state,
                    final LowLevelTimeSeriesData data,
                    final WriteCallback callback);

  /**
   * A callback triggered when the asynchronous write has completed. Various
   * options are present to avoid passing objects in the calls when possible.
   * <b>NOTE:</b> The callback provider is responsible for closing data objects
   * when a callback is passed to a write function.
   */
  public interface WriteCallback {
    /**
     * Entirely successful, all data handled ok.
     */
    public void success();

    /**
     * One or more datum failed the call and the caller can decide how to
     * handle the results. E.g. some data may fail to assign UIDs and should
     * be retried whereas the others were successful. In that case the failed
     * entries would have a {@link WriteStatus#RETRY} associated with their
     * index and others would likely have {@link WriteStatus#OK}. The caller
     * should retry just those marked for retries either with a wrapper or by
     * requeuing the data.
     *
     * @param status A non-null array of status enums at least the length of
     *               the data sent in. It may be more which is why the caller
     *               should iterate up to #length. All values less than #length
     *               are gauranteed to not be null.
     * @param length The length of the valid status entries.
     */
    public void partialSuccess(final WriteStatus[] status, final int length);

    /**
     * In this case all data failed to write and are eligible for retries. This
     * could happen, for example, if many metrics share the same tag set and one
     * or more of the tags need UID resolution. In that case the caller can
     * simply retry the message after a backoff or write it to a retry queue.
     */
    public void retryAll();

    /**
     * All data failed the write for the given reason. It's up to the caller to
     * decide what to do with the data.
     * @param status The non-null status enum describing the reason for the
     *               failure.
     */
    public void failAll(final WriteStatus status);

    /**
     * A non-recoverable exception was thrown and will be passed to the caller.
     * @param t The exception that was thrown.
     */
    public void exception(final Throwable t);
  }

}
