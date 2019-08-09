// This file is part of OpenTSDB.
// Copyright (C) 2019 The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.threadpools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import net.opentsdb.auth.AuthState;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryContext;
import net.opentsdb.threadpools.UserAwareThreadPoolExecutor.QCFutureWrapper;
import net.opentsdb.threadpools.UserAwareThreadPoolExecutor.QCRunnableWrapper;

public class TestUserAwareThreadPoolExecutor {

  private ExecutorService service;
  private TSDB tsdb;

  @Before
  public void extracted() {
    tsdb = mock(TSDB.class);
    Registry registry = mock(Registry.class);

    Configuration config = UnitTestConfiguration.getConfiguration();
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);
  }

  @Test
  public void initializeTest() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    assertNull(executor.initialize(tsdb, null).join());
    assertEquals(null, executor.id());
    assertNull(executor.shutdown().join());
  }

  @Test
  public void taskSubmitTest() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    assertNull(executor.initialize(tsdb, null).join());

    service = mock(ThreadPoolExecutor.class);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ((Runnable) invocation.getArguments()[0]).run();
        return null;
      }
    }).when(service).submit(any(Runnable.class));

    Runnable task = new Runnable() {

      @Override
      public void run() {

      }
    };
    service.submit(task);

    Thread.sleep(1000);

    verify(service, times(1)).submit(any(Runnable.class));

  }

  @Test
  public void addToQueryTaskTest() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());

    Runnable r = getRunnable();

    QueryContext qctx = getQctx();
    QCRunnableWrapper wrapper = executor.new QCRunnableWrapper(r, qctx, TSDTask.QUERY);

    wrapper.run();

    int size = executor.getCurrentTaskExecutions().get(TSDTask.QUERY).get();
    assertEquals(size, 1);

    size = executor.getCurrentExecutions().size();
    assertEquals(size, 0);

  }

  @Test
  public void addToQueryCloseTest() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());

    Runnable r = getRunnable();

    QueryContext qctx = getQctx();
    QCRunnableWrapper queryStart = executor.new QCRunnableWrapper(r, qctx, TSDTask.QUERY);
    queryStart.run();

    int size = executor.getCurrentTaskExecutions().get(TSDTask.QUERY).get();
    assertEquals(size, 1);

    QCRunnableWrapper closeTask = executor.new QCRunnableWrapper(r, qctx, TSDTask.QUERY_CLOSE);
    closeTask.run();

    size = executor.getCurrentTaskExecutions().get(TSDTask.QUERY).get();
    assertEquals(size, 0);

    size = executor.getCurrentExecutions().size();
    assertEquals(size, 0);

  }

  @Test
  public void addToQueryCloseWOQCtxTest() throws Exception {
    
    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());
    
    Runnable r = getRunnable();
    
    QueryContext qctx = getQctx();
    QCRunnableWrapper queryStart = executor.new QCRunnableWrapper(r, qctx, TSDTask.QUERY);
    queryStart.run();
    
    int size = executor.getCurrentTaskExecutions().get(TSDTask.QUERY).get();
    assertEquals(size, 1);
    
    QCRunnableWrapper closeTask = executor.new QCRunnableWrapper(r, null, TSDTask.QUERY_CLOSE);
    closeTask.run();
    
    size = executor.getCurrentTaskExecutions().get(TSDTask.QUERY).get();
    assertEquals(size, 0);
    
    size = executor.getCurrentExecutions().size();
    assertEquals(size, 0);
    
  }

  @Test
  public void addToQueryTaskTestCallable() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());

    Callable<Runnable> r = getCallable();

    QueryContext qctx = getQctx();
    QCFutureWrapper<Runnable> wrapper = executor.new QCFutureWrapper(r, qctx, TSDTask.QUERY);

    wrapper.run();

    int size = executor.getCurrentTaskExecutions().get(TSDTask.QUERY).get();
    assertEquals(size, 1);

    size = executor.getCurrentExecutions().size();
    assertEquals(size, 0);

  }

  @Test
  public void addToQueryQueryCloseCallable() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());

    Callable<Runnable> r = getCallable();

    QueryContext qctx = getQctx();
    QCFutureWrapper<Runnable> wrapper = executor.new QCFutureWrapper(r, qctx, TSDTask.QUERY);

    wrapper.run();

    int size = executor.getCurrentTaskExecutions().get(TSDTask.QUERY).get();
    assertEquals(size, 1);

    wrapper = executor.new QCFutureWrapper(r, qctx, TSDTask.QUERY_CLOSE);

    wrapper.run();

    size = executor.getCurrentTaskExecutions().get(TSDTask.QUERY).get();
    assertEquals(size, 0);

    size = executor.getCurrentExecutions().size();
    assertEquals(size, 0);

  }

  @Test
  public void addToQueryQueryCloseWOQCtxCallable() throws Exception {
    
    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());
    
    Callable<Runnable> r = getCallable();
    
    QueryContext qctx = getQctx();
    QCFutureWrapper<Runnable> wrapper = executor.new QCFutureWrapper(r, qctx, TSDTask.QUERY);
    
    wrapper.run();
    
    int size = executor.getCurrentTaskExecutions().get(TSDTask.QUERY).get();
    assertEquals(size, 1);
    
    wrapper = executor.new QCFutureWrapper(r, null, TSDTask.QUERY_CLOSE);
    
    wrapper.run();
    
    size = executor.getCurrentTaskExecutions().get(TSDTask.QUERY).get();
    assertEquals(size, 0);
    
    size = executor.getCurrentExecutions().size();
    assertEquals(size, 0);
    
  }

  @Test
  public void addToStateTest() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());

    Runnable r = getRunnable();

    QueryContext qctx = getQctx();
    QCRunnableWrapper wrapper = executor.new QCRunnableWrapper(r, qctx, null);

    wrapper.run();

    int size = executor.getCurrentExecutions().size();

    assertEquals(size, 1);

  }

  @Test
  public void disableStateTest() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.setDisableScheduling(false);
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());

    Runnable r = getRunnable();

    QueryContext qctx = getQctx();
    QCRunnableWrapper wrapper = executor.new QCRunnableWrapper(r, qctx, null);

    wrapper.run();

    int size = executor.getCurrentExecutions().size();

    assertEquals(size, 1);

  }

  @Test
  public void statePurgeTest() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());

    Runnable r = getRunnable();

    QueryContext qctx = getQctx();
    QCRunnableWrapper wrapper = executor.new QCRunnableWrapper(r, qctx, null);

    wrapper.run();

    int size = executor.getCurrentExecutions().size();
    assertEquals(size, 1);

    executor.getCountToPurge().set(0);
    wrapper.run();

    size = executor.getCurrentExecutions().size();
    assertEquals(size, 0);

  }

  private QueryContext getQctx() {
    QueryContext qctx = mock(QueryContext.class);
    when(qctx.authState()).thenReturn(mock(AuthState.class));
    when(qctx.authState().getUser()).thenReturn("TSD_User");
    return qctx;
  }

  private Runnable getRunnable() {
    Runnable r = new Runnable() {
      public void run() {

      }
    };
    return r;
  }

  @Test
  public void addToStateFromFutureTaskTest() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());

    QueryContext qctx = getQctx();
    Callable<Runnable> task = getCallable();
    QCFutureWrapper<Runnable> wrapper = executor.new QCFutureWrapper<Runnable>(task, qctx, null);

    wrapper.run();

    int size = executor.getCurrentExecutions().size();

    assertEquals(size, 1);

  }

  @Test
  public void purgeStateFromFutureTaskTest() throws Exception {

    UserAwareThreadPoolExecutor executor = new UserAwareThreadPoolExecutor();
    executor.getCurrentExecutions().clear();
    assertNull(executor.initialize(tsdb, null).join());

    QueryContext qctx = getQctx();
    Callable<Runnable> task = getCallable();
    QCFutureWrapper<Runnable> wrapper = executor.new QCFutureWrapper<Runnable>(task, qctx, null);

    wrapper.run();

    int size = executor.getCurrentExecutions().size();

    assertEquals(size, 1);

    executor.getCountToPurge().set(0);
    wrapper.run();

    size = executor.getCurrentExecutions().size();
    assertEquals(size, 0);

  }

  private Callable<Runnable> getCallable() {
    Callable<Runnable> task = new Callable<Runnable>() {

      @Override
      public Runnable call() throws Exception {
        return null;
      }
    };
    return task;
  }

}
