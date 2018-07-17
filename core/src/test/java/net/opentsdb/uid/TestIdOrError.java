// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.uid;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestIdOrError {

  @Test
  public void wrapId() throws Exception {
    IdOrError ioe = IdOrError.wrapId(new byte[] { 0, 0, 1 });
    assertArrayEquals(new byte[] { 0, 0, 1 }, ioe.id());
    assertNull(ioe.error());
    
    try {
      IdOrError.wrapId(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      IdOrError.wrapId(new byte[0]);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void wrapError() throws Exception {
    IdOrError ioe = IdOrError.wrapError("Ooops!");
    assertNull(ioe.id());
    assertEquals("Ooops!", ioe.error());
    
    try {
      IdOrError.wrapError(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      IdOrError.wrapError("");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
