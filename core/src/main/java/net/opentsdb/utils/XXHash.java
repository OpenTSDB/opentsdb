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
package net.opentsdb.utils;

import net.openhft.hashing.LongHashFunction;

/**
 * Utility to wrap the XX hash from OpenHFT and allow for combining hashes.
 * 
 * NOTE: We may move this to Common eventually if we find it's ok to include the
 * OpenHFT library there.
 * 
 * @since 3.0
 */
public class XXHash {
  private static LongHashFunction HASHER = LongHashFunction.xx();
  
  private XXHash() {
    // I'm static!
  }
  
  public static long hash(final byte[] value) {
    return HASHER.hashBytes(value);
  }
  
  public static long hash(final byte[] value, final int offset, final int length) {
    return HASHER.hashBytes(value, offset, length);
  }
  
  public static long hash(final String value) {
    return HASHER.hashChars(value);
  }
  
  public static long updateHash(final long hash, final byte[] value) {
    return 2251 * hash ^ 37 * HASHER.hashBytes(value);
  }
  
  public static long updateHash(final long hash, 
                                final byte[] value, 
                                final int offset, 
                                final int length) {
    return 2251 * hash ^ 37 * HASHER.hashBytes(value, offset, length);
  }
  
  public static long updateHash(final long hash, final String value) {
    return 2251 * hash ^ 37 * HASHER.hashChars(value);
  }
  
  public static long combineHashes(final long hash_a, final long hash_b) {
    return 2251 * hash_a ^ 37 * hash_b;
  }
  
}
