// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
 * An object pool of fixed size arrays that allows for allocating new arrays of
 * a larger length without being pooled.
 * 
 * @since 3.0
 */
public interface ArrayObjectPoolAllocator extends ObjectPoolAllocator {

  public static final String LENGTH_KEY = "array.length";
  
  /**
   * Return an object of the given length. 
   * @param length the length of the array.
   * @return A new object of the type {@link #dataType()}.
   */
  public Object allocate(final int length);
  
  /** @return The length of arrays in the pool. */
  public int pooledLength();

}
