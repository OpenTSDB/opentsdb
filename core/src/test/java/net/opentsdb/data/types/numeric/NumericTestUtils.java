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
package net.opentsdb.data.types.numeric;

import static org.junit.Assert.assertEquals;

/**
 * Helper class for numeric tests.
 *
 * @since 3.0
 */
public class NumericTestUtils {

  private NumericTestUtils() {
    // you shall not instantiate me!
  }

  /**
   * Same as the regular assertArrayEquals but accounts for NaNs in either array.
   * @param expected The non-null expected array.
   * @param test The non-null test array.
   * @param epsilon The epsilon for comparing doubles.
   */
  public static void assertArrayEqualsNaNs(final double[] expected,
                                           final double[] test,
                                           final double epsilon) {
    if (test == null) {
      throw new AssertionError("Test array was null.");
    }
    if (expected.length != test.length) {
      throw new AssertionError("Array lengths differ. Expected "
              + expected.length + " but found " + test.length);
    }
    for (int i = 0; i < expected.length; i++) {
      if (Double.isNaN(expected[i]) && !Double.isNaN(test[i])) {
        throw new AssertionError("Values differed at [" + i + "]. " +
                "Expected NaN but found " + test[i]);
      } else {
        assertEquals(expected[i], test[i], epsilon);
      }
    }
  }
}
