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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertNull;
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
    FailedQueryExecution<Long> exec = 
        new FailedQueryExecution<Long>(query, ex);
    assertSame(query, exec.query());
    assertTrue(exec.completed());
    
    try {
      exec.deferred().join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { 
      assertSame(ex, e);
    }
    
    exec = new FailedQueryExecution<Long>(null, ex);
    assertNull(exec.query());
    assertTrue(exec.completed());
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
