package net.opentsdb.search;

import static com.google.common.util.concurrent.Futures.addCallback;

import net.opentsdb.meta.LabelMeta;
import net.opentsdb.plugins.PluginError;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelCreatedEvent;
import net.opentsdb.uid.LabelDeletedEvent;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.stumbleupon.async.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Guava {@link com.google.common.eventbus.EventBus} listener that listens for label changes and
 * notifies the {@link SearchPlugin} when appropriate.
 */
public class IdChangeIndexerListener {
  private static final Logger LOG = LoggerFactory.getLogger(IdChangeIndexerListener.class);

  private final TsdbStore store;
  private final SearchPlugin searchPlugin;

  public IdChangeIndexerListener(final TsdbStore store,
                                 final SearchPlugin searchPlugin) {
    this.store = store;
    this.searchPlugin = searchPlugin;
  }

  /**
   * The method that subscribes to {@link LabelCreatedEvent}s. You should not call this directly,
   * post messages to the event bus that this listener is registered to instead.
   *
   * @param event The published event.
   */
  @Subscribe
  @AllowConcurrentEvents
  public final void recordLabelCreated(final LabelCreatedEvent event) {
    addCallback(store.getMeta(event.getId(), event.getType()),
        new FutureCallback<LabelMeta>() {
          @Override
          public void onSuccess(final LabelMeta meta) {
            LOG.info("Indexing {}", meta);
            addCallback(searchPlugin.indexLabelMeta(meta), new PluginError(searchPlugin));
          }

          @Override
          public void onFailure(final Throwable t) {
            LOG.error("Unable to fetch LabelMeta object for {}[{}]",
                event.getId(), event.getType(), t);
          }
        });
  }

  /**
   * The method that subscribes to {@link LabelDeletedEvent}s. You should not call this directly,
   * post messages to the event bus that this listener is registered to instead.
   *
   * @param event The published event.
   */
  @Subscribe
  @AllowConcurrentEvents
  public final void recordLabelDeleted(LabelDeletedEvent event) {
    LOG.info("Removing label with id {}, type {} and name {} from search index",
        event.getId(), event.getType(), event.getName());
    addCallback(searchPlugin.deleteLabelMeta(event.getId(), event.getType()),
        new PluginError(searchPlugin));
  }
}
