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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.query.context.QueryExecutorContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Registry.class, Executors.class })
public class TestRegistry {

  private TSDB tsdb;
  private ExecutorService cleanup_pool;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    cleanup_pool = mock(ExecutorService.class);
    
    PowerMockito.mockStatic(Executors.class);
    PowerMockito.when(Executors.newFixedThreadPool(1))
      .thenReturn(cleanup_pool);
  }
  
  @Test
  public void ctor() throws Exception {
    Registry registry = new Registry(tsdb);
    assertTrue(registry.dataMergers().isEmpty());
    assertSame(cleanup_pool, registry.cleanupPool());
    assertNull(registry.tracer());
    
    try {
      new Registry(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void queryExecutorContexts() throws Exception {
    Registry registry = new Registry(tsdb);
    
    QueryExecutorContext context = mock(QueryExecutorContext.class);
    when(context.id()).thenReturn("MyContext");
    
    registry.registerQueryExecutorContext(context, true);
    assertSame(context, registry.getQueryExecutorContext(null));
    assertSame(context, registry.getQueryExecutorContext(""));
    assertSame(context, registry.getQueryExecutorContext("MyContext"));
    
    context = mock(QueryExecutorContext.class);
    when(context.id()).thenReturn("Context2");
    registry.registerQueryExecutorContext(context, false);
    assertSame(context, registry.getQueryExecutorContext("Context2"));
    
    assertNull(registry.getQueryExecutorContext("doesnotexist"));
    
    // clear
    registry = new Registry(tsdb);
    assertNull(registry.getQueryExecutorContext(null));
    assertNull(registry.getQueryExecutorContext(""));
    
    try {
      registry.registerQueryExecutorContext(null, true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    context = mock(QueryExecutorContext.class);
    when(context.id()).thenReturn(null);
    try {
      registry.registerQueryExecutorContext(context, true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    when(context.id()).thenReturn("");
    try {
      registry.registerQueryExecutorContext(context, true);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
