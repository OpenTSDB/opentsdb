package net.opentsdb.uid;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import net.opentsdb.DaggerTestComponent;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.utils.TestUtil;

import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import javax.inject.Inject;

public class WildcardIdLookupStrategyTest {
  @Inject TsdbStore client;
  @Inject MetricRegistry metricRegistry;
  @Inject EventBus eventBus;

  private UniqueId uid;
  private IdLookupStrategy lookupStrategy;

  @Rule
  public final Timeout timeout = Timeout.millis(TestUtil.TIMEOUT);

  @Before
  public void setUp() throws IOException {
    DaggerTestComponent.create().inject(this);

    uid = new UniqueId(client, UniqueIdType.METRIC, metricRegistry, eventBus);
    lookupStrategy = new IdLookupStrategy.WildcardIdLookupStrategy();
  }

  @Test
  public void testResolveIdWildcardNull() throws Exception {
    assertNull(lookupStrategy.getId(uid, null).get());
  }

  @Test
  public void testResolveIdWildcardEmpty() throws Exception {
    assertNull(lookupStrategy.getId(uid, "").get());
  }

  @Test
  public void testResolveIdWildcardStar() throws Exception {
    assertNull(lookupStrategy.getId(uid, "*").get());
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void testResolveIdGetsId() throws Exception {
    LabelId id = client.allocateLabel("nameexists", UniqueIdType.METRIC).get();
    assertEquals(id, lookupStrategy.getId(uid, "nameexists").get());
  }

  @Test(expected = NoSuchUniqueName.class)
  public void testResolveIdGetsMissingId() throws Exception {
    lookupStrategy.getId(uid, "nosuchname").get();
  }
}