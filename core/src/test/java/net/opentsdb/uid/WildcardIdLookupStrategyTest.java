package net.opentsdb.uid;

import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class WildcardIdLookupStrategyTest {
  private TsdbStore client;
  private UniqueId uid;
  private IdLookupStrategy lookupStrategy;

  @Before
  public void setUp() throws IOException {
    ObjectGraph objectGraph = ObjectGraph.create(new TestModuleMemoryStore());
    client = objectGraph.get(TsdbStore.class);

    uid = new UniqueId(client, UniqueIdType.METRIC,
        mock(MetricRegistry.class), mock(EventBus.class));

    lookupStrategy = new IdLookupStrategy.WildcardIdLookupStrategy();
  }

  @Test
  public void testResolveIdWildcardNull() throws Exception {
    assertNull(lookupStrategy.getId(uid, null).joinUninterruptibly());
  }

  @Test
  public void testResolveIdWildcardEmpty() throws Exception {
    assertNull(lookupStrategy.getId(uid, "").joinUninterruptibly());
  }

  @Test
  public void testResolveIdWildcardStar() throws Exception {
    assertNull(lookupStrategy.getId(uid, "*").joinUninterruptibly());
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void testResolveIdGetsId() throws Exception {
    LabelId id = client.allocateUID("nameexists", UniqueIdType.METRIC).join();
    assertEquals(id, lookupStrategy.getId(uid, "*").joinUninterruptibly());
  }

  @Test(expected = NoSuchUniqueName.class)
  public void testResolveIdGetsMissingId() throws Exception {
    lookupStrategy.getId(uid, "nosuchname").joinUninterruptibly();
  }
}