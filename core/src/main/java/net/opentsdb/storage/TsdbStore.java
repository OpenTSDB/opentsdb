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
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * An abstract class defining the functions any database used with TSDB must implement. Another
 * requirement is tha the database connection has to be asynchronous.
 */
public abstract class TsdbStore implements Closeable {
  //
  // Identifier management
  //
  @Nonnull
  public abstract ListenableFuture<LabelId> allocateUID(@Nonnull final String name,
                                                        @Nonnull final UniqueIdType type);

  @Nonnull
  public abstract ListenableFuture<LabelId> allocateUID(@Nonnull final String name,
                                                        @Nonnull final LabelId uid,
                                                        @Nonnull final UniqueIdType type);

  public abstract ListenableFuture<Void> deleteUID(String name, UniqueIdType type);

  /**
   * Lookup time series related to a metric, tagk, tagv or any combination thereof. See {@link
   * net.opentsdb.core.UniqueIdClient#executeTimeSeriesQuery} for a more formal specification how
   * the query language and logic.
   *
   * @param query The query that filters out which TSUIDs to lookup
   * @return All TSUIDs that matches the provided query
   */
  public abstract ListenableFuture<List<byte[]>> executeTimeSeriesQuery(final ResolvedSearchQuery query);

  /**
   * Lookup all IDs that matches the provided {@link net.opentsdb.uid.IdQuery}. There are no demands
   * on how the exact the results are but the lookup should be efficient. In fact, the provided
   * should be viewed as a hint about what should be returned but in reality all IDs or nothing at
   * all may be returned.
   *
   * @param query An object that describes the query parameters
   * @return A future that on completion will contain with a list of matching IDs
   */
  public abstract ListenableFuture<List<IdentifierDecorator>> executeIdQuery(final IdQuery query);

  @Nonnull
  public abstract ListenableFuture<Optional<LabelId>> getId(@Nonnull final String name,
                                                            @Nonnull final UniqueIdType type);

  @Nonnull
  public abstract ListenableFuture<Optional<String>> getName(@Nonnull final LabelId id,
                                                             @Nonnull final UniqueIdType type);

  //
  // Datapoints
  //
  @Nonnull
  public abstract ListenableFuture<Void> addPoint(@Nonnull final TimeseriesId tsuid,
                                                  final long timestamp,
                                                  final float value);

  @Nonnull
  public abstract ListenableFuture<Void> addPoint(@Nonnull final TimeseriesId tsuid,
                                                  final long timestamp,
                                                  final double value);

  @Nonnull
  public abstract ListenableFuture<Void> addPoint(@Nonnull final TimeseriesId tsuid,
                                                  final long timestamp,
                                                  final long value);

  /**
   * Should execute the provided {@link net.opentsdb.core.Query} and return a deferred. Every single
   * item in the returned iterator may contain multiple datapoints but every single instance must
   * only contain the datapoints for a single TSUID. The iterator may return multiple items for the
   * same TSUID.
   *
   * @param query The query to execute
   */
  // TODO
  public abstract ListenableFuture<ImmutableList<DataPoints>> executeQuery(final Object query);

  //
  // Annotations
  //

  /**
   * Attempts to mark an Annotation object for deletion. Note that if the annoation does not exist
   * in storage, this delete call will not throw an error.
   *
   * @param annotation@return A meaningless Deferred for the caller to wait on until the call is
   * complete. The value may be null.
   */
  public abstract ListenableFuture<Void> delete(Annotation annotation);

  public abstract ListenableFuture<Integer> deleteAnnotationRange(final byte[] tsuid,
                                                                  final long startTime,
                                                                  final long endTime);

  /**
   * Attempts to fetch a global or local annotation from storage
   *
   * @param tsuid The TSUID as a byte array. May be null if retrieving a global annotation
   * @param startTime The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  public abstract ListenableFuture<Annotation> getAnnotation(byte[] tsuid, long startTime);

  public abstract ListenableFuture<List<Annotation>> getGlobalAnnotations(final long startTime,
                                                                          final long endTime);

  public abstract ListenableFuture<Boolean> updateAnnotation(Annotation annotation);

  //
  // LabelMeta
  //
  @Nonnull
  public abstract ListenableFuture<LabelMeta> getMeta(@Nonnull final LabelId uid,
                                                      @Nonnull final UniqueIdType type);

  public abstract ListenableFuture<Boolean> updateMeta(final LabelMeta meta);
}
