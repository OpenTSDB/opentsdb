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
package net.opentsdb.uid;

import java.util.List;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.pojo.Filter;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.ResolvedFilter;
import net.opentsdb.uid.UniqueId.UniqueIdType;

/**
 * An interface used to make calls to storage for resolving Strings to
 * UIDs and vice-versa.
 * 
 * @since 3.0
 */
public interface UniqueIdStore {

  /**
   * Resolves the given V2 filter, parsing the tag keys and optional tag
   * value literals. The resulting list is populated in the same order as
   * the TagVFilters in the given filter set.
   * @param filter A non-null filter to resolve.
   * @param span An optional tracing span.
   * @return A deferred resolving to a non-null list of resolved filters
   * if successful or an exception if something went wrong. The list may
   * be empty if the filter didn't have any TagVFilters. (In which case
   * this shouldn't be called.)
   * @throws IllegalArgumentException if the filter was null.
   */
  public Deferred<List<ResolvedFilter>> resolveUids(final Filter filter, 
                                                    final Span span);
  
  /**
   * Converts the given string to it's UID value based on the type.
   * @param type A non-null UID type.
   * @param id A non-null and non-empty string.
   * @param span An optional tracing span.
   * @return A deferred resolving to the UID if successful or an exception.
   * @throws IllegalArgumentException if the type was null or the string
   * was null or empty.
   */
  public Deferred<byte[]> stringToId(final UniqueIdType type, 
                                     final String id,
                                     final Span span);
  
  /**
   * Converts the list of strings to their IDs, maintaining order.
   * @param type A non-null UID type.
   * @param ids A non-null and non-empty list of strings.
   * @param span An optional tracing span.
   * @return A deferred resolving to the list of UIDs in order if 
   * successful or an exception.
   * @throws IllegalArgumentException if the type was null or the
   * IDs was null or an ID in the list was null or empty.
   */
  public Deferred<List<byte[]>> stringsToId(final UniqueIdType type, 
                                            final List<String> ids,
                                            final Span span);
  
  /**
   * Converts the UID to the equivalent string name.
   * @param type A non-null UID type.
   * @param id A non-null and non-empty byte array UID.
   * @param span An optional tracing span.
   * @return A deferred resolving to the string if successful or an
   * exception.
   * @throws IllegalArgumentException if the type was null or the ID 
   * null or empty.
   */
  public Deferred<String> idToString(final UniqueIdType type, 
                                     final byte[] id,
                                     final Span span);
  
  /**
   * Converts the list of UIDs to the equivalent string name maintaining
   * order.
   * @param type A non-null UID type.
   * @param ids A deferred resolving to a list of the strings in order
   * if successful or an exception.
   * @param span An optional tracing span.
   * @throws IllegalArgumentException if the type was null or the strings
   * list was null or any string in the list was null or empty.
   * @return
   */
  public Deferred<List<String>> idsToString(final UniqueIdType type, 
                                            final List<byte[]> ids,
                                            final Span span);

}
