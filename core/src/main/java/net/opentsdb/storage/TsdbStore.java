package net.opentsdb.storage;

import net.opentsdb.core.DataPoints;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.LabelType;
import net.opentsdb.uid.TimeSeriesId;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
  public abstract ListenableFuture<LabelId> allocateLabel(final String name,
                                                          final LabelType type);

  @Nonnull
  public abstract ListenableFuture<LabelId> allocateLabel(final String name,
                                                          final LabelId id,
                                                          final LabelType type);

  @Nonnull
  public abstract ListenableFuture<Void> deleteLabel(final String name, final LabelType type);

  /**
   * Lookup time series related to a metric, tagk, tagv or any combination thereof. See {@link
   * net.opentsdb.core.LabelClient#executeTimeSeriesQuery} for a more formal specification how the
   * query language and logic.
   *
   * @param query The query that filters out which TSUIDs to lookup
   * @return All TSUIDs that matches the provided query
   */
  public abstract ListenableFuture<List<byte[]>> executeTimeSeriesQuery(
      final ResolvedSearchQuery query);

  @Nonnull
  public abstract ListenableFuture<Optional<LabelId>> getId(final String name,
                                                            final LabelType type);

  @Nonnull
  public abstract ListenableFuture<Optional<String>> getName(final LabelId id,
                                                             final LabelType type);

  //
  // Datapoints
  //
  @Nonnull
  public abstract ListenableFuture<Void> addPoint(final TimeSeriesId tsuid,
                                                  final long timestamp,
                                                  final float value);

  @Nonnull
  public abstract ListenableFuture<Void> addPoint(final TimeSeriesId tsuid,
                                                  final long timestamp,
                                                  final double value);

  @Nonnull
  public abstract ListenableFuture<Void> addPoint(final TimeSeriesId tsuid,
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
   * Delete the annotation with the provided metric, tags and start time. This method will silently
   * ignore any missing annotation.
   *
   * @param metric A non-null label that represents the metric
   * @param tags A non-null and non-empty map of the labels for the tags
   * @param startTime The exact set start time of the annotation
   * @return A future that indicates the completion of the request
   */
  @Nonnull
  public abstract ListenableFuture<Void> deleteAnnotation(final LabelId metric,
                                                          final ImmutableMap<LabelId, LabelId> tags,
                                                          final long startTime);

  /**
   * Delete all annotations specified by the provided metric and tags within the provided time
   * range.
   *
   * @param metric A non-null label that represents the metric
   * @param tags A non-null and non-empty map of the labels for the tags
   * @param startTime The lower time bound to to remove annotations within
   * @param endTime the upper time bound to remove annotations within
   * @return A future that on completion contains the number of annotations removed
   */
  @Nonnull
  public abstract ListenableFuture<Integer> deleteAnnotations(
      final LabelId metric,
      final ImmutableMap<LabelId, LabelId> tags,
      final long startTime,
      final long endTime);

  /**
   * Fetch the information for the annotation behind the provided metric, tags and start time.
   *
   * @param metric A non-null label that represents the metric
   * @param tags A non-null and non-empty map of the labels for the tags
   * @param startTime The exact set start time of the annotation
   * @return A future that on completion contains an annotation with the stored information
   */
  @Nonnull
  public abstract ListenableFuture<Annotation> getAnnotation(
      final LabelId metric,
      final ImmutableMap<LabelId, LabelId> tags,
      final long startTime);

  /**
   * Update the stored information of the annotation with the same metric, tags and start time of
   * the provided annotation.
   *
   * @param annotation The new annotation information
   * @return A future that on completion contains a boolean that indicates whether a change was made
   * or not.
   */
  @Nonnull
  public abstract ListenableFuture<Boolean> updateAnnotation(final Annotation annotation);

  //
  // LabelMeta
  //
  @Nonnull
  public abstract ListenableFuture<LabelMeta> getMeta(final LabelId uid,
                                                      final LabelType type);

  @Nonnull
  public abstract ListenableFuture<Boolean> updateMeta(final LabelMeta meta);
}
