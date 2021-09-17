/*
 * This file is part of OpenTSDB.
 * Copyright (C) 2021  The OpenTSDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.opentsdb.query;

/**
 * An enum that represents various common node parameters, e.g. whether or not
 * an expression node joins data sources to produce a new one (it does) or
 * if a node's results can be cached.
 *
 * TODO - the types aren't particularly useful yet. Particularly since the
 * generics don't support primitivs.
 *
 * @since 3.0
 */
public enum QueryNodeConfigOptions {

  /** Whether or not the node supports pushedown to the data source. */
  SUPPORTS_PUSH_DOWN(boolean.class),

  /** Whether or not the node joins two or more data sources and produces a
   * new data source. E.g. Expression `e1` may receive metric sources "m1:m1"
   * and "m2:m2" but produces "e1:e1" as it's data.
   */
  JOINS(boolean.class),

  /** For nodes like the rate node, how many previous intervaled data points
   * does it need to compute a proper result. E.g. rate needs one previous value
   * so the source should pad back at least one value or interval.
   */
  PREVIOUS_INTERVALS(int.class),

  /**
   * For nodes that include windowd results, like sliding windows, how far back
   * the sources should look to fully satisfy the first real value. E.g. if a
   * sliding window of 15m is configured, the source should search 15m _before_
   * the query start time so a full window could be evaluated.
   */
  PADDING_WINDOW(String.class),

  /** Whether or not data from the node can be cached. */
  READ_CACHEABLE(boolean.class);

  private Class<?> type;

  /**
   * Private ctor to set the type.
   * @param type The non-null type to set.
   */
  private QueryNodeConfigOptions(final Class<?> type) {
    this.type = type;
  }

  /** @return The type of data the param converts to. */
  public Class<?> type() {
    return type;
  }

}
