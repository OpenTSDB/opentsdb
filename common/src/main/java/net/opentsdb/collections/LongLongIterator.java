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

import java.util.Iterator;

/**
 * Iterator for {@link LongLongHashTable}.
 *
 * <pre>{@code
 * // accessing keys, values through an iterator:
 * LongLongIterator itr = table.iterator();
 * while (itr.hasNext()) {
 *   itr.next();
 *
 *   long key = itr.key();
 *   if (itr.key() == expected) {
 *     long value = itr.value();
 *     doSomething(value);
 *   }
 * }
 * }</pre>
 */
public interface LongLongIterator extends Iterator<Void> {

  /** @return the current key */
  long key();

  /** @return the current value */
  long value();
}
