package net.opentsdb.storage.hbase;

import dagger.ObjectGraph;
import net.opentsdb.storage.DatabaseTests;
import net.opentsdb.storage.TsdbStoreTimeSeriesQueryTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;

@Category(DatabaseTests.class)
public class HBaseTimeSeriesQueryTest extends TsdbStoreTimeSeriesQueryTest {
  @Override
  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new HBaseTestModule()).inject(this);
    super.setUp();
  }
}
