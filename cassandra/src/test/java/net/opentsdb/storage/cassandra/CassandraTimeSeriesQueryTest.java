
package net.opentsdb.storage.cassandra;

import net.opentsdb.storage.TsdbStoreTimeSeriesQueryTest;

import dagger.ObjectGraph;
import org.junit.Before;

public class CassandraTimeSeriesQueryTest extends TsdbStoreTimeSeriesQueryTest {
  @Override
  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new CassandraTestModule()).inject(this);
    super.setUp();
  }
}
