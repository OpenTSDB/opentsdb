package net.opentsdb.storage;

import net.opentsdb.utils.Config;
import org.hbase.async.HBaseClient;
import org.junit.Before;
import org.junit.Ignore;

import java.io.IOException;

import static org.mockito.Mockito.mock;

@Ignore
public class TestHBaseStore extends TestTsdbStore {
  private HBaseClient client;

  @Before
  public void setUp() throws IOException {
    // why.... why..... why final!?
    client = mock(HBaseClient.class);
    tsdb_store = new HBaseStore(client, new Config(false));
  }
}
