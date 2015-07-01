package net.opentsdb.search;

import static com.google.common.base.Preconditions.checkNotNull;

import net.opentsdb.BuildData;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdType;
import net.opentsdb.uid.Label;
import net.opentsdb.uid.LabelId;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * A default search plugin to use when no other search plugin has been configured. This search
 * plugin will just discard all data given to it.
 */
public class DefaultSearchPlugin extends SearchPlugin {
  private final TsdbStore store;

  public DefaultSearchPlugin(final TsdbStore store) {
    this.store = checkNotNull(store);
  }

  @Override
  public void close() {
  }

  @Override
  public String version() {
    return BuildData.version();
  }

  @Override
  public ListenableFuture<Void> indexLabelMeta(final LabelMeta meta) {
    return Futures.immediateFuture(null);
  }

  @Nonnull
  @Override
  public ListenableFuture<Void> deleteLabelMeta(final LabelId id,
                                                final IdType type) {
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Void> indexAnnotation(final Annotation note) {
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Void> deleteAnnotation(final Annotation note) {
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<SearchQuery> executeQuery(final SearchQuery query) {
    throw new IllegalStateException("The default search plugin does "
                                    + "not support executing search queries");
  }

  @Override
  public ListenableFuture<List<Label>> executeIdQuery(final IdQuery query) {
    return store.executeIdQuery(query);
  }
}
