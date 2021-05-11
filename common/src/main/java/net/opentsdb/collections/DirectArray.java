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
package net.opentsdb.collections;

import sun.misc.Unsafe;

public interface DirectArray {

  Unsafe unsafe = UnsafeHelper.unsafe;

  int getCapacity();

  long getAddress();

  void free();

  static long malloc(final long bytes) {
    long address = unsafe.allocateMemory(bytes);
    unsafe.setMemory(address, bytes, (byte) 0);
    return address;
  }
}
