package net.opentsdb.search;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.stumbleupon.async.Callback;
import net.opentsdb.core.MetaClient;
import net.opentsdb.core.PluginError;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelCreatedEvent;
import net.opentsdb.uid.LabelDeletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Guava {@link com.google.common.eventbus.EventBus} listener that listens
 * for label changes and notifies the {@link SearchPlugin}
 * when appropriate.
 */
public class IdChangeIndexerListener {
  private static final Logger LOG = LoggerFactory.getLogger(IdChangeIndexerListener.class);

  private final TsdbStore store;
  private SearchPlugin searchPlugin;

  public IdChangeIndexerListener(final TsdbStore store,
                                 final SearchPlugin searchPlugin) {
    this.store = store;
    this.searchPlugin = searchPlugin;
  }

  /**
   * The method that subscribes to {@link LabelCreatedEvent}s. You should not
   * call this directly, post messages to the event bus that this listener is
   * registered to instead.
   *
   * @param event The published event.
   */
  @Subscribe
  @AllowConcurrentEvents
  public final void recordLabelCreated(LabelCreatedEvent event) {
    store.getMeta(event.getId(), event.getType()).addCallback(
        new Callback<Void, LabelMeta>() {
          @Override
          public Void call(final LabelMeta meta) {
            LOG.info("Indexing {}", meta);
            searchPlugin.indexLabelMeta(meta)
                .addErrback(new PluginError(searchPlugin));
            return null;
          }
        });
  }

  /**
   * The method that subscribes to {@link LabelDeletedEvent}s. You should not
   * call this directly, post messages to the event bus that this listener is
   * registered to instead.
   *
   * @param event The published event.
   */
  @Subscribe
  @AllowConcurrentEvents
  public final void recordLabelDeleted(LabelDeletedEvent event) {
    LOG.info("Removing label with id {}, type {} and name {} from search index",
        event.getId(), event.getType(), event.getName());
    searchPlugin.deleteLabelMeta(event.getId(), event.getType())
        .addErrback(new PluginError(searchPlugin));
  }
}
