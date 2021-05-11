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

import java.lang.reflect.Field;

/**
 * An internal helper class to access {@link Unsafe}.
 */
public class UnsafeHelper {

  public static final Unsafe unsafe = getUnsafe();

  static Unsafe getUnsafe() {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      return (Unsafe) f.get(null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to get unsafe instance, are you running Oracle JDK?", e);
    }
  }
}