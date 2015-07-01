package net.opentsdb.search;

import static net.opentsdb.uid.IdType.METRIC;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.opentsdb.DaggerTestComponent;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelCreatedEvent;
import net.opentsdb.uid.LabelDeletedEvent;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.IdType;

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.Futures;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.inject.Inject;

public class IdChangeIndexerListenerTest {
  @Inject EventBus idEventBus;

  @Mock private TsdbStore store;
  @Mock private SearchPlugin searchPlugin;

  @Before
  public void setUp() throws Exception {
    DaggerTestComponent.create().inject(this);
    MockitoAnnotations.initMocks(this);

    IdChangeIndexerListener idChangeIndexer = new IdChangeIndexerListener(store, searchPlugin);
    idEventBus.register(idChangeIndexer);
  }

  @Test
  public void createdLabelEventIndexesLabelMeta() {
    final LabelId id = mock(LabelId.class);
    LabelMeta labelMeta = LabelMeta.create(id, METRIC, "sys.cpu.0", "Description", 1328140801);
    when(store.getMeta(any(LabelId.class), METRIC)).thenReturn(Futures.immediateFuture(labelMeta));

    idEventBus.post(new LabelCreatedEvent(id, "test", IdType.METRIC));
    verify(searchPlugin).indexLabelMeta(labelMeta);
  }

  @Test
  public void deletedLabelEventRemovesLabelMeta() {
    final LabelDeletedEvent event =
        new LabelDeletedEvent(mock(LabelId.class), "test", IdType.METRIC);
    idEventBus.post(event);
    verify(searchPlugin).deleteLabelMeta(any(LabelId.class), event.getType());
  }
}