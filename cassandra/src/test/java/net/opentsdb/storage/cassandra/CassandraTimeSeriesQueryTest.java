package net.opentsdb.storage.cassandra;

import net.opentsdb.storage.TsdbStoreTimeSeriesQueryTest;

import org.junit.Before;

public class CassandraTimeSeriesQueryTest extends TsdbStoreTimeSeriesQueryTest {
  @Override
  @Before
  public void setUp() throws Exception {
    DaggerCassandraTestComponent.create()
        .inject(this);

    super.setUp();
  }
}
