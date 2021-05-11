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
package net.opentsdb.hashing;

/**
 * Fast prime multiplication based hash function - weak but really fast.
 * WARNING: This is not cryptographically secure!
 */
public class PrimeMultiplicationHash implements HashFunction {

  @Override
  public long hash(byte[] value) {
    return hash(value, 0, value.length);
  }

  @Override
  public long hash(byte[] value, int offset, int length) {
    return update(0, value, offset, length);
  }

  @Override
  public long update(long h, byte[] value) {
    return update(h, value, 0, value.length);
  }

  @Override
  public long update(long h, byte[] value, int offset, int length) {
    for (int i = 0; i < length; i++) {
      h = value[i + offset] + 12289L * h;
    }

    return h;
  }

  @Override
  public long update(long h, long h2) {
    throw new UnsupportedOperationException();
  }
}
