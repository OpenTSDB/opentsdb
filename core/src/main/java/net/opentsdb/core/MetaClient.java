package net.opentsdb.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.transform;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.plugins.PluginError;
import net.opentsdb.plugins.RealTimePublisher;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.LabelType;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * The exposed interface for managing meta objects.
 */
@Singleton
public class MetaClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetaClient.class);

  private final TsdbStore store;
  private final LabelClient labelClient;
  private final SearchPlugin searchPlugin;
  private final RealTimePublisher realTimePublisher;

  /**
   * Create a new instance using the given arguments to configure itself.
   */
  @Inject
  public MetaClient(final TsdbStore store,
                    final SearchPlugin searchPlugin,
                    final LabelClient labelClient,
                    final RealTimePublisher realTimePublisher) {
    this.store = checkNotNull(store);
    this.labelClient = checkNotNull(labelClient);
    this.searchPlugin = checkNotNull(searchPlugin);
    this.realTimePublisher = checkNotNull(realTimePublisher);
  }

  /**
   * Delete the annotation object from the search index
   *
   * @param note The annotation object to delete
   * @since 2.0
   */
  public void deleteAnnotation(final Annotation note) {
    addCallback(searchPlugin.deleteAnnotation(note), new PluginError(searchPlugin));
  }

  /**
   * Perform a query using the configured search plugin and get the matching label meta objects.
   *
   * @param query A plugin specific query as received without modification
   * @return A future that on completion contains an {@link Iterable} of label meta objects
   * @see SearchPlugin#findLabels(String) for information about the format of the query
   */
  @Nonnull
  public ListenableFuture<Iterable<LabelMeta>> findLabels(final String query) {
    return searchPlugin.findLabels(query);
  }

  /**
   * Fetch an annotation.
   *
   * @param metric The metric associated with the annotation
   * @param tags The tags associated with the annotation
   * @param startTime The exact start time of the annotation
   * @return A future that on completion contains an annotation object if found, null if not
   */
  @Nonnull
  public ListenableFuture<Annotation> getAnnotation(final LabelId metric,
                                                    final ImmutableMap<LabelId, LabelId> tags,
                                                    final long startTime) {
    checkNotNull(metric);
    checkArgument(!tags.isEmpty());
    checkArgument(startTime > 0L);

    return store.getAnnotation(metric, tags, startTime);
  }

  /**
   * Delete the annotation with the given metric, tags and start time.
   *
   * @param metric The metric associated with the annotation
   * @param tags The tags associated with the annotation
   * @param startTime The start time of the annotation
   * @return A future that indicates the completion of the call
   */
  @Nonnull
  public ListenableFuture<Void> delete(final LabelId metric,
                                       final ImmutableMap<LabelId, LabelId> tags,
                                       final long startTime) {
    checkNotNull(metric);
    checkArgument(!tags.isEmpty(), "At least one tag is required");
    checkArgument(startTime > 0L);

    return store.deleteAnnotation(metric, tags, startTime);
  }

  /**
   * Verifies the UID object exists, then attempts to fetch the meta from storage and if not found,
   * returns a default object.
   *
   * <p>The reason for returning a default object (with the type, identifier and name set) is due to
   * users who may have just enabled meta data or have upgraded; we want to return valid data. If
   * they modify the entry, it will write to storage. You can tell it's a default if the {@code
   * created} value is 0. If the meta was generated at UID assignment or updated by the meta sync
   * CLI command, it will have a valid created timestamp.
   *
   * @param type The type of UID to fetch
   * @param uid The ID of the meta to fetch
   * @return A UIDMeta from storage or a default
   */
  public ListenableFuture<LabelMeta> getLabelMeta(final LabelType type,
                                                  final LabelId uid) {
    // Verify that the identifier exists before fetching the meta object. The future that
    // #getLabelName returns will contain an exception if it does not exist.
    return transform(labelClient.getLabelName(type, uid), new AsyncFunction<String, LabelMeta>() {
      @Override
      public ListenableFuture<LabelMeta> apply(final String name) throws Exception {
        return store.getMeta(uid, type);
      }
    });
  }

  /**
   * Index the given Annotation object via the configured search plugin
   *
   * @param note The annotation object to index
   * @since 2.0
   */
  public void indexAnnotation(final Annotation note) {
    addCallback(searchPlugin.indexAnnotation(note), new PluginError(searchPlugin));
    addCallback(realTimePublisher.publishAnnotation(note), new PluginError(realTimePublisher));
  }

  /**
   * Attempts to update the information of the stored Annotation object with the same {@code
   * timeSeriesId} and {@code startTime} as the provided object. The provided object will be checked
   * for changes against the stored object before saving anything.
   *
   * @param annotation The annotation with the updated information.
   * @return True if the updates were saved successfully. False if there were no changes to make.
   */
  public ListenableFuture<Boolean> updateAnnotation(final Annotation annotation) {
    return transform(
        store.getAnnotation(annotation.metric(), annotation.tags(), annotation.startTime()),
        new AsyncFunction<Annotation, Boolean>() {
          @Override
          public ListenableFuture<Boolean> apply(final Annotation storedAnnotation)
              throws Exception {
            if (!storedAnnotation.equals(annotation)) {
              return store.updateAnnotation(annotation);
            }

            LOG.debug("{} does not have any changes, skipping update", annotation);
            return Futures.immediateFuture(Boolean.FALSE);
          }
        });
  }

  /**
   * Delete all annotations associated with the non-null and non-empty metric and tags within the
   * given time bounds.
   *
   * @param metric The metric associated with the annotations to delete
   * @param tags The tags associated with the annotations to delete
   * @param startTime The lower bound where to start deleting
   * @param endTime The upper bound where to stop deleting
   * @return A future that on completion contains the number of annotations that were deleted
   */
  @Nonnull
  public ListenableFuture<Integer> deleteRange(final LabelId metric,
                                               final ImmutableMap<LabelId, LabelId> tags,
                                               final long startTime,
                                               final long endTime) {
    checkNotNull(metric, "Missing a metric", metric, tags);
    checkArgument(!tags.isEmpty(), "At least one tag is required", metric, tags);
    checkArgument(startTime > 0L, "The start time must be lager than zero", startTime);
    checkArgument(startTime <= endTime, "The end timestamp cannot be less than the start timestamp",
        startTime, endTime);

    return store.deleteAnnotations(metric, tags, startTime, endTime);
  }

  /**
   * Attempts to update the information of the stored LabelMeta object with the same {@code
   * identifier} and {@code type} as the provided meta object. The provided meta object will be
   * checked for changes against the stored object before saving anything.
   *
   * @param meta The LabelMeta with the updated information.
   * @return A future that on completion contains a {@code true} if the update was successful or
   * {@code false} if the meta object had no changes to save.
   * @throws net.opentsdb.uid.NoSuchUniqueId If the UID does not exist
   */
  public ListenableFuture<Boolean> update(final LabelMeta meta) {
    return transform(getLabelMeta(meta.type(), meta.identifier()),
        new AsyncFunction<LabelMeta, Boolean>() {
          @Override
          public ListenableFuture<Boolean> apply(final LabelMeta storedMeta) throws Exception {
            if (!storedMeta.equals(meta)) {
              return store.updateMeta(meta);
            }

            LOG.debug("{} does not have any changes, skipping update", meta);
            return Futures.immediateFuture(Boolean.FALSE);
          }
        });
  }
}
