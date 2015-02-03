package net.opentsdb.core;

import com.google.common.eventbus.EventBus;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdCreatedEvent;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MetaClientUIDMetaTest {
  private Config config;
  private EventBus idEventBus;

  @Mock private TsdbStore store;
  @Mock private SearchPlugin searchPlugin;

  @Before
  public void setUp() throws Exception {
    config = new Config(false);
    idEventBus = new EventBus();
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void createdIdEventCreatesUIDMetaWhenEnabled() {
    config.overrideConfig("tsd.core.meta.enable_realtime_uid", "true");
    new MetaClient(store, idEventBus, searchPlugin, config);
    idEventBus.post(new IdCreatedEvent(new byte[] {0, 0, 1}, "test", UniqueIdType.METRIC));
    verify(store).add(any(UIDMeta.class));
    verify(searchPlugin).indexUIDMeta(any(UIDMeta.class));
  }

  @Test
  public void createdIdEventCreatesUIDMetaWhenDisabled() {
    new MetaClient(store, idEventBus, searchPlugin, config);
    idEventBus.post(new IdCreatedEvent(new byte[] {0, 0, 1}, "test", UniqueIdType.METRIC));
    verify(store, never()).add(any(UIDMeta.class));
    verify(searchPlugin, never()).indexUIDMeta(any(UIDMeta.class));
  }
}
