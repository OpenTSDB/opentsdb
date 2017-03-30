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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.query.pojo.TimeSeriesQuery;

public class TestFailedQueryExecution {
  private TimeSeriesQuery query;
  private IllegalArgumentException ex;
  
  @Before
  public void before() throws Exception {
    query = mock(TimeSeriesQuery.class);
    ex = new IllegalArgumentException("Boo!");
  }
  
  @Test
  public void ctor() throws Exception {
    final FailedQueryExecution<Long> exec = 
        new FailedQueryExecution<Long>(query, ex);
    assertSame(query, exec.query());
    assertTrue(exec.completed());
    
    try {
      exec.deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { 
      assertSame(ex, e);
    }
    
    try {
      new FailedQueryExecution<Long>(null, ex);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new FailedQueryExecution<Long>(query, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void callback() throws Exception {
    final FailedQueryExecution<Long> exec = 
        new FailedQueryExecution<Long>(query, ex);
    assertTrue(exec.completed());

    try {
      exec.callback(42L);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
}
