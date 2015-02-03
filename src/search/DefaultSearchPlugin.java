package net.opentsdb.search;

import net.opentsdb.BuildData;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;

import com.stumbleupon.async.Deferred;

/**
 * A default search plugin to use when no other search plugin has been
 * configured. This search plugin will just discard all data given to it.
 * @see net.opentsdb.core.TSDB
 * @see net.opentsdb.core.TsdbBuilder
 * @see net.opentsdb.search.SearchPlugin
 */
public class DefaultSearchPlugin extends SearchPlugin {
  @Override
  public void initialize(final TSDB tsdb) {
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
  public Deferred<Object> indexUIDMeta(final UIDMeta meta) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> deleteUIDMeta(final UIDMeta meta) {
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
}
