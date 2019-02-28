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
package net.opentsdb.pools;

/**
 * An object that can be stored in an object pool. This will be returned by
 * the pool when a claim is made and it <b>MUST</b> be {@link #release()}d when
 * the object can be put back in the pool. To make it easier for users, the 
 * object should maintain a reference to this instance and in a {@code close()}
 * method it should call {@link #release()}. See {@link CloseablePooledObject}.
 *
 * @since 3.0
 */
public interface PooledObject {

  /** @return The non-null object. */
  public Object object();
  
  /** Called to release the object back to the pool for reuse. */
  public void release();
  
}
