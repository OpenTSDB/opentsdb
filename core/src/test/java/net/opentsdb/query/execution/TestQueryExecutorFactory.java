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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Constructor;

import org.junit.Before;
import org.junit.Test;

import io.opentracing.Span;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.pojo.TimeSeriesQuery;

public class TestQueryExecutorFactory {

  private QueryContext context;
  
  @Before
  public void before() throws Exception {
    context = mock(QueryContext.class);
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void ctor() throws Exception {
    Constructor<?> ctor = TestExec.class.getDeclaredConstructor(
        QueryContext.class, QueryExecutorConfig.class);
    
    QueryExecutorFactory<Long> factory = 
        new DefaultQueryExecutorFactory(ctor, null);
    QueryExecutor<Long> executor = factory.newExecutor(context);
    assertNull(((TestExec<Long>) executor).config);
    assertSame(context, ((TestExec<Long>) executor).context);
    
    final TestConfig<Long> config = new TestConfig<Long>();
    factory = 
        new DefaultQueryExecutorFactory(ctor, config);
    executor = factory.newExecutor(context);
    assertSame(config, ((TestExec<Long>) executor).config);
    assertSame(context, ((TestExec<Long>) executor).context);
    
    try {
      new DefaultQueryExecutorFactory(null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    ctor = TestExec.class.getDeclaredConstructor();
    assertNotNull(ctor);
    try {
      new DefaultQueryExecutorFactory(null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    ctor = TestExec.class.getDeclaredConstructor(QueryContext.class);
    assertNotNull(ctor);
    try {
      new DefaultQueryExecutorFactory(null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    ctor = TestExec.class.getDeclaredConstructor(QueryContext.class, 
        QueryExecutorConfig.class, long.class);
    assertNotNull(ctor);
    try {
      new DefaultQueryExecutorFactory(null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  private static class TestExec<T> extends QueryExecutor<T> {
    public TestExec() {
      super(mock(QueryContext.class), null);
    }
    
    public TestExec(final QueryContext context) {
      super(context, null);
    }
    
    public TestExec(final QueryContext context, 
        final QueryExecutorConfig config) { 
      super(context, config); 
    }
    
    public TestExec(final QueryContext context, 
        final QueryExecutorConfig config, final long something) { 
      super(context, config); 
    }
    
    @Override
    public QueryExecution<T> executeQuery(final TimeSeriesQuery query,
        final Span upstream_span) { return null; }
  }
  
  private static class TestConfig<T> implements QueryExecutorConfig {
    
  }
}
