// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
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
