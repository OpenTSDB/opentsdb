package net.opentsdb.search;

import net.opentsdb.BuildData;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.meta.TSMeta;

import com.stumbleupon.async.Deferred;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

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
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return BuildData.version();
  }

  @Override
  public Deferred<Object> indexTSMeta(final TSMeta meta) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> deleteTSMeta(final String tsuid) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> indexUIDMeta(final LabelMeta meta) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> deleteUIDMeta(final LabelMeta meta) {
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
