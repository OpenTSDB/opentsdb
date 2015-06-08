package net.opentsdb.search;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import net.opentsdb.TestModule;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelCreatedEvent;
import net.opentsdb.uid.LabelDeletedEvent;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;

import com.google.common.eventbus.EventBus;
import com.stumbleupon.async.Deferred;
import dagger.ObjectGraph;
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
    ObjectGraph.create(new TestModule()).inject(this);
    MockitoAnnotations.initMocks(this);

    IdChangeIndexerListener idChangeIndexer = new IdChangeIndexerListener(store, searchPlugin);
    idEventBus.register(idChangeIndexer);
  }

  @Test
  public void createdLabelEventIndexesLabelMeta() {
    final LabelId id = mock(LabelId.class);
    LabelMeta labelMeta = LabelMeta.create(id, METRIC, "sys.cpu.0", "Description", 1328140801);
    when(store.getMeta(any(LabelId.class), METRIC)).thenReturn(Deferred.fromResult(labelMeta));

    idEventBus.post(new LabelCreatedEvent(id, "test", UniqueIdType.METRIC));
    verify(searchPlugin).indexLabelMeta(labelMeta);
  }

  @Test
  public void deletedLabelEventRemovesLabelMeta() {
    final LabelDeletedEvent event =
        new LabelDeletedEvent(mock(LabelId.class), "test", UniqueIdType.METRIC);
    idEventBus.post(event);
    verify(searchPlugin).deleteLabelMeta(any(LabelId.class), event.getType());
  }
}