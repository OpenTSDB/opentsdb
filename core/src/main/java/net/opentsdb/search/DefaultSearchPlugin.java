package net.opentsdb.search;

import static com.google.common.base.Preconditions.checkNotNull;

import net.opentsdb.BuildData;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;

import com.stumbleupon.async.Deferred;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * A default search plugin to use when no other search plugin has been
 * configured. This search plugin will just discard all data given to it.
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
  public Deferred<Void> indexLabelMeta(final LabelMeta meta) {
    return Deferred.fromResult(null);
  }

  @Nonnull
  @Override
  public Deferred<Void> deleteLabelMeta(@Nonnull final LabelId id,
                                        @Nonnull final UniqueIdType type) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> indexAnnotation(final Annotation note) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> deleteAnnotation(final Annotation note) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<SearchQuery> executeQuery(final SearchQuery query) {
    throw new IllegalStateException("The default search plugin does " +
            "not support executing search queries");
  }

  @Override
  public Deferred<List<IdentifierDecorator>> executeIdQuery(final IdQuery query) {
    return store.executeIdQuery(query);
  }
}
