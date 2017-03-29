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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import com.stumbleupon.async.TimeoutException;

import net.opentsdb.query.pojo.Query;

public class TestQueryExecution {

  private Query query;
  
  @Before
  public void before() throws Exception {
    query = mock(Query.class);
  }
  
  @Test
  public void ctor() throws Exception {
    final TestImp exec = new TestImp(query);
    assertNotNull(exec.deferred());
    assertSame(query, exec.query());
    assertFalse(exec.completed());
    
    try {
      exec.deferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    
    try {
      new TestImp(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void callback() throws Exception {
    TestImp exec = new TestImp(query);
    assertFalse(exec.completed());
    
    exec.callback(42L);
    assertEquals(42L, (long) exec.deferred().join());
    assertTrue(exec.completed());
    
    exec = new TestImp(query);
    assertFalse(exec.completed());
    
    exec.callback(new IllegalArgumentException("Boo!"));
    try {
      exec.deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertTrue(exec.completed());
    
    try {
      exec.callback(42L);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void cancel() throws Exception {
    TestImp exec = new TestImp(query);
    assertFalse(exec.completed());
    
    exec.cancel();
    
    assertEquals(-1L, (long) exec.deferred().join());
    assertTrue(exec.completed());
  }
  
  class TestImp extends QueryExecution<Long> {

    public TestImp(Query query) {
      super(query);
    }

    @Override
    public void cancel() {
      callback(-1L);
    }
    
  }
}
