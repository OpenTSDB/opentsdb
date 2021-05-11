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

import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNotNull;

public class TestUnsafeHelper {

  private UnsafeHelper dummy;

//  @Test
//  public void testIllegalAccessException() throws Exception {
//    assertNotNull(UnsafeHelper.unsafe);
//
//    new MockUp<Field>() {
//      @Mock
//      public Object get(Object obj) throws IllegalAccessException {
//        throw new IllegalAccessException("test");
//      }
//    };
//
//    try {
//      UnsafeHelper.getUnsafe();
//      fail("No exception thrown");
//    } catch (Throwable t) {
//      // Intellij gives different exception
//      if (t instanceof RuntimeException) {
//        assertTrue(t.getCause() instanceof IllegalAccessException);
//        assertEquals("test", t.getCause().getMessage());
//      } else {
//        assertTrue(t.getCause().getCause() instanceof IllegalAccessException);
//        assertEquals("test", t.getCause().getCause().getMessage());
//      }
//    }
//  }

}
