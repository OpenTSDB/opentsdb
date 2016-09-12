package net.opentsdb.tsd;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTsdbTest;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TestTSUIDQuery;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.BlacklistManager;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, HBaseClient.class, Config.class, HttpQuery.class,
        Query.class, Deferred.class, UniqueId.class, DateTime.class, KeyValue.class,
        Scanner.class })
public class TestReactiveBlacklisting extends BaseTsdbTest {
  private QueryRpc rpc;

  private Map<String, String> host1tags;
  private Map<String, String> host2tags;

  private final int rowCountThreshold = 0;
  private final int blockTimeInSeconds = 10;

  private final String allHostQuery = "/api/query?start=1h-ago&m=sum:sys.cpu.user";
  private final String host1Query1 = "/api/query?start=1h-ago&m=sum:sys.cpu.user{host=web01,datacenter=dc01}";
  private final String host1Query2 = "/api/query?start=1h-ago&m=sum:sys.cpu.user{datacenter=dc01,host=web01}";
  private final String host2Query1 = "/api/query?start=1h-ago&m=sum:sys.cpu.user{host=web02,datacenter=dc01}";

  @Before
  public void beforeLocal() throws Exception {
    Whitebox.setInternalState(config, "enable_tsuid_incrementing", true);
    Whitebox.setInternalState(config, "enable_realtime_ts", true);
    rpc = new QueryRpc();
    storage = new MockBase(tsdb, client, true, true, true, true);
    TestTSUIDQuery.setupStorage(tsdb, storage);
    host1tags = new HashMap<String, String>();
    host1tags.put("host", "web01");
    host1tags.put("owner", "web02");
    host2tags = new HashMap<String, String>();
    host2tags.put("host", "web02");
    host2tags.put("owner", "web01");

  }

  private void executeQueryWith200Response(String queryString) throws IOException {
    final HttpQuery query = NettyMocks.getQuery(tsdb, queryString);
    rpc.execute(tsdb, query);
    assert (query.response().getStatus().getCode() == 200);
  }

  private void executeQueryWith400Blacklist(String queryString) throws IOException {
    final HttpQuery query = NettyMocks.getQuery(tsdb, queryString);
    boolean exception = false;
    try {
      rpc.execute(tsdb, query);
    } catch (BadRequestException be) {
      exception = true;
      assert (be.getMessage().startsWith("Metric Blacklisted"));
    }
    assert (exception == true);
  }

  private void toggleReactiveBlacklisting(boolean enable) {
    BlacklistManager.initBlockListConfiguration(enable, rowCountThreshold, blockTimeInSeconds);
  }

  @Test
  public void testWithNoBlacklisting() throws Exception {
    toggleReactiveBlacklisting(false);
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1461924148000L);
    tsdb.addPoint("sys.cpu.user", 1461924131, 1, host1tags);
    tsdb.addPoint("sys.cpu.user", 1461924141, 1, host1tags);
    tsdb.addPoint("sys.cpu.user", 1461924131, 1, host2tags);
    tsdb.addPoint("sys.cpu.user", 1461924141, 1, host2tags);

    executeQueryWith200Response(allHostQuery);
    executeQueryWith200Response(allHostQuery);
    executeQueryWith200Response(host1Query1);
    executeQueryWith200Response(host1Query2);
    executeQueryWith200Response(host2Query1);
    Thread.sleep(blockTimeInSeconds * 1000);
    executeQueryWith200Response(allHostQuery);
    executeQueryWith200Response(host1Query2);
  }

  @Test
  public void testWithBlacklisting() throws Exception {
    toggleReactiveBlacklisting(true);
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.when(DateTime.currentTimeMillis()).thenReturn(1461924148000L);
    tsdb.addPoint("sys.cpu.user", 1461924131, 1, host1tags);
    tsdb.addPoint("sys.cpu.user", 1461924141, 1, host1tags);
    tsdb.addPoint("sys.cpu.user", 1461924131, 1, host2tags);
    tsdb.addPoint("sys.cpu.user", 1461924141, 1, host2tags);

    executeQueryWith200Response(allHostQuery);
    executeQueryWith400Blacklist(allHostQuery);
    executeQueryWith200Response(host1Query1);
    executeQueryWith400Blacklist(host1Query2);
    executeQueryWith200Response(host2Query1);
    Thread.sleep(blockTimeInSeconds * 1000);
    executeQueryWith200Response(allHostQuery);
    executeQueryWith200Response(host1Query2);
  }

}
