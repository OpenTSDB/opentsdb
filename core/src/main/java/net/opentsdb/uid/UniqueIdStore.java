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

import java.nio.charset.Charset;
import java.util.List;

import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.data.TimeSeriesDatumId;
import net.opentsdb.stats.Span;

/**
 * An interface used to make calls to storage for resolving Strings to
 * UIDs and vice-versa.
 * 
 * @since 3.0
 */
public interface UniqueIdStore {
  
  /**
   * Converts the given string to it's UID value based on the type.
   * 
   * @param type A non-null UID type.
   * @param name A non-null and non-empty string.
   * @param span An optional tracing span.
   * @return A deferred resolving to the UID if successful or an exception.
   * @throws IllegalArgumentException if the type was null or the string
   * was null or empty.
   */
  public Deferred<byte[]> getId(final UniqueIdType type, 
                                final String name,
                                final Span span);
  
  /**
   * Converts the list of strings to their IDs, maintaining order.
   * 
   * @param type A non-null UID type.
   * @param names A non-null and non-empty list of strings.
   * @param span An optional tracing span.
   * @return A deferred resolving to the list of UIDs in order if 
   * successful or an exception.
   * @throws IllegalArgumentException if the type was null or the
   * IDs was null or an ID in the list was null or empty.
   */
  public Deferred<List<byte[]>> getIds(final UniqueIdType type, 
                                       final List<String> names,
                                       final Span span);
  
  /**
   * Converts the given string to it's UID value based on the type. If 
   * the string didn't have an assigned UID in storage, attempts to
   * create it given that the user has permission.
   * 
   * @param auth The non-null auth state to parse.
   * @param type A non-null UID type.
   * @param name A non-null and non-empty string.
   * @param id A non-null ID for logging and abuse prevention purposes.   
   * @param span An optional tracing span.
   * @return A deferred resolving to an IdOrError object reflecting if 
   * the resolution/creation was successful or not.
   * @throws IllegalArgumentException if the type was null or the string
   * was null or empty.
   */
  public Deferred<IdOrError> getOrCreateId(final AuthState auth,
                                           final UniqueIdType type, 
                                           final String name,
                                           final TimeSeriesDatumId id,
                                           final Span span);
  
  /**
   * Converts the given strings to their UID values based on the type. If 
   * the strings didn't have assigned UIDs in storage, they're assigned
   * if the user has permission.
   * 
   * @param auth The non-null auth state to parse.
   * @param type A non-null UID type.
   * @param names A non-null and non-empty list of strings.
   * @param id A non-null ID for logging and abuse prevention purposes.   
   * @param span An optional tracing span.
   * @return A deferred resolving to IdOrError objectss reflecting if 
   * the resolution/creation was successful or not.
   * @throws IllegalArgumentException if the type was null or the string
   * was null or empty.
   */
  public Deferred<List<IdOrError>> getOrCreateIds(final AuthState auth,
                                                  final UniqueIdType type, 
                                                  final List<String> names,
                                                  final TimeSeriesDatumId id,
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
  public Deferred<String> getName(final UniqueIdType type, 
                                  final byte[] id,
                                  final Span span);
  
  /**
   * Converts the list of UIDs to the equivalent string name maintaining
   * order. <b>Note</b> that if a UID in the source list was not found in 
   * storage, it's corresponding entry in the results will be null.
   * 
   * @param type A non-null UID type.
   * @param ids A deferred resolving to a list of the strings in order
   * if successful or an exception.
   * @param span An optional tracing span.
   * @throws IllegalArgumentException if the type was null or the strings
   * list was null or any string in the list was null or empty.
   * @return A deferred resolving to list of names corresponding with
   * the same index in the given ids list or an exception if one or more
   * of the UID resolutions failed with an exception.
   */
  public Deferred<List<String>> getNames(final UniqueIdType type, 
                                         final List<byte[]> ids,
                                         final Span span);
  
  /**
   * Fetch the character set for the given type.
   * @param type A non-null ID type.
   * @return The non-null character set.
   */
  public Charset characterSet(final UniqueIdType type);
}
