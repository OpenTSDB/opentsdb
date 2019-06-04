package net.opentsdb.threadpools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.UnitTestConfiguration;
import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;

public class TestRoundRobinThreadPoolExecutor {

  @Test
  public void factory() throws Exception {
    TSDB tsdb = mock(TSDB.class);
    Registry registry = mock(Registry.class);

    Configuration config = UnitTestConfiguration.getConfiguration();
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);

    RoundRobinThreadPoolExecutor executor = new RoundRobinThreadPoolExecutor();
    assertNull(executor.initialize(tsdb, null).join());
    assertEquals(null, executor.id());
    assertNull(executor.shutdown().join());
  }

}
