package net.opentsdb.storage.hbase;

import dagger.ObjectGraph;
import net.opentsdb.storage.TsdbStoreTimeSeriesQueryTest;
import org.junit.Before;

public class HBaseTimeSeriesQueryTest extends TsdbStoreTimeSeriesQueryTest {
  @Override
  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new HBaseTestModule()).inject(this);
    super.setUp();
  }
}
