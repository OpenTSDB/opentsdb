
package net.opentsdb.storage.cassandra;

import dagger.ObjectGraph;
import net.opentsdb.storage.TsdbStoreTimeSeriesQueryTest;
import org.junit.Before;

public class CassandraTimeSeriesQueryTest extends TsdbStoreTimeSeriesQueryTest {
  @Override
  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new CassandraTestModule()).inject(this);
    super.setUp();
  }
}
