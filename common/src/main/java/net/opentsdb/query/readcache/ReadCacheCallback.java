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
package net.opentsdb.query.readcache;

/**
 * An async callback executed by the cache plugin when a result is read or 
 * an exception is caught.
 * 
 * @since 3.0
 */
public interface ReadCacheCallback {
  
  /**
   * Called with a successful result.
   * @param result The non-null result.
   */
  public void onCacheResult(final ReadCacheQueryResultSet result);

  /**
   * Called when an exception or error handles. It's up to the cache caller to
   * decide how to handle this.
   * @param index The index into the original cache key array for alignment, 
   * 0 by deault.
   * @param t The exception caught.
   */
  public void onCacheError(final int index, final Throwable t);

}
