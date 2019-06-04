package net.opentsdb.threadpools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;

public class TestFixedThreadPoolExecutor {

  private ExecutorService service;

  @Test
  public void initializeTest() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    Registry registry = mock(Registry.class);

    Configuration config = UnitTestConfiguration.getConfiguration();
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);

    FixedThreadPoolExecutor executor = new FixedThreadPoolExecutor();
    assertNull(executor.initialize(tsdb, null).join());
    assertEquals(null, executor.id());
    assertNull(executor.shutdown().join());
  }

  @Test
  public void taskSubmitTest() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    Registry registry = mock(Registry.class);

    Configuration config = UnitTestConfiguration.getConfiguration();
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);

    FixedThreadPoolExecutor executor = new FixedThreadPoolExecutor();
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

    verify(service, times(1)).submit(any(Runnable.class));

  }

}
