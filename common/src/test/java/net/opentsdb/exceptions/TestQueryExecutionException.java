// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.exceptions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Lists;

public class TestQueryExecutionException {

  @Test
  public void ctors() throws Exception {
    QueryExecutionException e = new QueryExecutionException("Boo!", 404);
    assertEquals("Boo!", e.getMessage());
    assertEquals(404, e.getStatusCode());
    assertEquals(-1, e.getOrder());
    assertNull(e.getCause());
    assertTrue(e.getExceptions().isEmpty());
    
    e = new QueryExecutionException("Boo!", 0);
    assertEquals("Boo!", e.getMessage());
    assertEquals(0, e.getStatusCode());
    assertEquals(-1, e.getOrder());
    assertNull(e.getCause());
    assertTrue(e.getExceptions().isEmpty());
    
    e = new QueryExecutionException("Boo!", 404, 42);
    assertEquals("Boo!", e.getMessage());
    assertEquals(404, e.getStatusCode());
    assertEquals(42, e.getOrder());
    assertNull(e.getCause());
    assertTrue(e.getExceptions().isEmpty());
    
    final IllegalStateException cause = new IllegalStateException("Boo!");
    e = new QueryExecutionException("Boo!", 404, 
        Lists.<Exception>newArrayList(cause));
    assertEquals("Boo!", e.getMessage());
    assertEquals(404, e.getStatusCode());
    assertEquals(-1, e.getOrder());
    assertNull(e.getCause());
    assertEquals(1, e.getExceptions().size());
    assertSame(cause, e.getExceptions().get(0));
    
    e = new QueryExecutionException("Boo!", 404, cause);
    assertEquals("Boo!", e.getMessage());
    assertEquals(404, e.getStatusCode());
    assertEquals(-1, e.getOrder());
    assertSame(cause, e.getCause());
    assertTrue(e.getExceptions().isEmpty());
    
    e = new QueryExecutionException("Boo!", 404, 42,
        Lists.<Exception>newArrayList(cause));
    assertEquals("Boo!", e.getMessage());
    assertEquals(404, e.getStatusCode());
    assertEquals(42, e.getOrder());
    assertNull(e.getCause());
    assertEquals(1, e.getExceptions().size());
    assertSame(cause, e.getExceptions().get(0));
    
    e = new QueryExecutionException("Boo!", 404, 42, cause);
    assertEquals("Boo!", e.getMessage());
    assertEquals(404, e.getStatusCode());
    assertEquals(42, e.getOrder());
    assertSame(cause, e.getCause());
    assertTrue(e.getExceptions().isEmpty());
  }
}
