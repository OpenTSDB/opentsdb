package net.opentsdb.storage.cassandra;

import net.opentsdb.core.CoreModule;
import net.opentsdb.storage.TsdbStoreTimeSeriesQueryTest;

import com.typesafe.config.ConfigFactory;
import org.junit.Before;

public class CassandraTimeSeriesQueryTest extends TsdbStoreTimeSeriesQueryTest {
  @Override
  @Before
  public void setUp() throws Exception {
    DaggerCassandraTestComponent.builder()
        .coreModule(new CoreModule(ConfigFactory.load("cassandra")))
        .build()
        .inject(this);

    super.setUp();
  }
}
