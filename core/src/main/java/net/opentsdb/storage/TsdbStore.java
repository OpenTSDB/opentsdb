
package net.opentsdb.storage;

import net.opentsdb.core.DataPoints;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.uid.IdQuery;
import net.opentsdb.uid.IdentifierDecorator;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.TimeseriesId;
import net.opentsdb.uid.UniqueIdType;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Deferred;

import java.io.Closeable;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * An abstract class defining the functions any database used with TSDB must
 * implement. Another requirement is tha the database connection has to be
 * asynchronous.
 */
public abstract class TsdbStore implements Closeable {
  //
  // Identifier management
  //
  @Nonnull
  public abstract Deferred<LabelId> allocateUID(@Nonnull final String name,
                                                @Nonnull final UniqueIdType type);

  @Nonnull
  public abstract Deferred<LabelId> allocateUID(@Nonnull final String name,
                                                @Nonnull final LabelId uid,
                                                @Nonnull final UniqueIdType type);

  public abstract Deferred<Void> deleteUID(String name, UniqueIdType type);

  /**
   * Lookup time series related to a metric, tagk, tagv or any combination
   * thereof. See {@link net.opentsdb.core.UniqueIdClient#executeTimeSeriesQuery}
   * for a more formal specification how the query language and logic.
   *
   * @param query The query that filters out which TSUIDs to lookup
   * @return All TSUIDs that matches the provided query
   */
  public abstract Deferred<List<byte[]>> executeTimeSeriesQuery(final ResolvedSearchQuery query);

  /**
   * Lookup all IDs that matches the provided {@link net.opentsdb.uid.IdQuery}.
   * There are no demands on how the exact the results are but the lookup should
   * be efficient. In fact, the provided should be viewed as a hint about what
   * should be returned but in reality all IDs or nothing at all may be
   * returned.
   *
   * @param query An object that describes the query parameters
   * @return A deferred with a list of matching IDs
   */
  public abstract Deferred<List<IdentifierDecorator>> executeIdQuery(final IdQuery query);

  @Nonnull
  public abstract Deferred<Optional<LabelId>> getId(@Nonnull final String name,
                                                    @Nonnull final UniqueIdType type);

  @Nonnull
  public abstract Deferred<Optional<String>> getName(@Nonnull final LabelId id,
                                                     @Nonnull final UniqueIdType type);

  //
  // Datapoints
  //
  @Nonnull
  public abstract Deferred<Void> addPoint(@Nonnull final TimeseriesId tsuid,
                                          final long timestamp,
                                          final float value);

  @Nonnull
  public abstract Deferred<Void> addPoint(@Nonnull final TimeseriesId tsuid,
                                          final long timestamp,
                                          final double value);

  @Nonnull
  public abstract Deferred<Void> addPoint(@Nonnull final TimeseriesId tsuid,
                                          final long timestamp,
                                          final long value);

  /**
   * Should execute the provided {@link net.opentsdb.core.Query} and return a
   * deferred. Every single item in the returned iterator may contain multiple
   * datapoints but every single instance must only contain the datapoints for a
   * single TSUID. The iterator may return multiple items for the same TSUID.
   *
   * @param query The query to execute
   */
  // TODO
  public abstract Deferred<ImmutableList<DataPoints>> executeQuery(final Object query);

  //
  // Annotations
  //

  /**
   * Attempts to mark an Annotation object for deletion. Note that if the
   * annoation does not exist in storage, this delete call will not throw an
   * error.
   *
   * @param annotation@return A meaningless Deferred for the caller to wait on
   *                          until the call is complete. The value may be
   *                          null.
   */
  public abstract Deferred<Void> delete(Annotation annotation);

  public abstract Deferred<Integer> deleteAnnotationRange(final byte[] tsuid, final long start_time, final long end_time);

  /**
   * Attempts to fetch a global or local annotation from storage
   *
   * @param tsuid      The TSUID as a byte array. May be null if retrieving a
   *                   global annotation
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  public abstract Deferred<Annotation> getAnnotation(byte[] tsuid, long start_time);

  public abstract Deferred<List<Annotation>> getGlobalAnnotations(final long start_time, final long end_time);

  public abstract Deferred<Boolean> updateAnnotation(Annotation original, Annotation annotation);

  //
  // LabelMeta
  //
  @Nonnull
  public abstract Deferred<LabelMeta> getMeta(@Nonnull final LabelId uid,
                                              @Nonnull final UniqueIdType type);

  public abstract Deferred<Boolean> updateMeta(final LabelMeta meta);
}
