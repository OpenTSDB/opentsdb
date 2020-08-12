// This file is part of OpenTSDB.
// Copyright (C) 2019-2020  The OpenTSDB Authors.
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

import java.util.Collection;
import java.util.Map;

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;

/**
 * A serdes plugin for serializing and deserializing read cache data.
 * 
 * @since 3.0
 */
public interface ReadCacheSerdes {

  /**
   * Converts a single query result collection into a byte array as-is, no
   * time parsing. Assumes this is a full cache segment.
   * @param results A collection of non-null results. May be empty.
   * @return A non-null byte array populated with the serialized results.
   */
  public byte[] serialize(final Collection<QueryResult> results);

  /**
   * Slices a query result collection by the timestamps and returns segmented
   * cache results serialized in the proper format.
   * @param timestamps A non-null and non-empty array of timestamps in Unix Epoch
   * format.
   * @param results A non-null collection of results. May be empty.
   * @return A non-null byte array of non-null arrays populated with the
   * serialized results and of the same length as the timestamps array.
   */
  public byte[][] serialize(final int[] timestamps, 
                            final Collection<QueryResult> results);

  /**
   * Deserializes the given result collection.
   * @param context The Query pipeline context.
   * @param data A non-null byte array.
   * @return The deserialized cache result map, may be empty, keyed on the
   * result ID.
   */
  public Map<QueryResultId, ReadCacheQueryResult> deserialize(
      final QueryNode node, 
      final byte[] data);

}