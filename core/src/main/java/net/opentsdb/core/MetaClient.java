package net.opentsdb.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.transform;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.plugins.PluginError;
import net.opentsdb.plugins.RTPublisher;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * The exposed interface for managing meta objects.
 */
@Singleton
public class MetaClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetaClient.class);

  private final Config config;
  private final TsdbStore store;
  private final IdClient idClient;
  private final SearchPlugin searchPlugin;
  private final RTPublisher realtimePublisher;

  @Inject
  public MetaClient(final TsdbStore store,
                    final SearchPlugin searchPlugin,
                    final Config config,
                    final IdClient idClient, final RTPublisher realtimePublisher) {
    this.config = checkNotNull(config);
    this.store = checkNotNull(store);
    this.idClient = checkNotNull(idClient);
    this.searchPlugin = checkNotNull(searchPlugin);
    this.realtimePublisher = checkNotNull(realtimePublisher);
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
   * Executes a search query using the search plugin
   *
   * @param query The query to execute
   * @return A deferred object to wait on for the results to be fetched
   * @throws IllegalStateException if the search plugin has not been enabled or configured
   * @since 2.0
   */
  public ListenableFuture<SearchQuery> executeSearch(final SearchQuery query) {
    return searchPlugin.executeQuery(query);
  }

  /**
   * Fetch an annotation.
   *
   * @param tsuid The time series ID as a String.
   * @param startTime The start time of the annotation
   * @return A deferred that on completion contains an annotation object if found, null if not
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
   * Attempts to mark an Annotation object for deletion. Note that if the annotation does not exist
   * in storage, this delete call will not throw an error.
   *
   * @param annotation The Annotation we want to store.
   * @return A meaningless Deferred for the caller to wait on until the call is complete. The value
   * may be null.
   */
  public ListenableFuture<Void> delete(Annotation annotation) {
    return store.delete(annotation.metric(), annotation.tags(), annotation.startTime());
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
   * @throws net.opentsdb.uid.NoSuchUniqueId If the UID does not exist
   */
  public ListenableFuture<LabelMeta> getLabelMeta(final UniqueIdType type,
                                                  final LabelId uid) {
    // Verify that the identifier exists before fetching the meta object. #getUidName will throw an
    // exception if it does not exist.
    return transform(idClient.getUidName(type, uid), new AsyncFunction<String, LabelMeta>() {
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
    addCallback(realtimePublisher.publishAnnotation(note), new PluginError(realtimePublisher));
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
   * Deletes global or TSUID associated annotiations for the given time range.
   *
   * @param tsuid An optional TSUID. If set to null, then global annotations for the given range
   * will be deleted
   * @param startTime A start timestamp in milliseconds
   * @param endTime An end timestamp in millseconds
   * @return The number of annotations deleted
   * @throws IllegalArgumentException if the timestamps are invalid
   * @since 2.1
   */
  @Nonnull
  public ListenableFuture<Integer> deleteRange(final LabelId metric,
                                               final ImmutableMap<LabelId, LabelId> tags,
                                               final long startTime,
                                               final long endTime) {
    if (endTime < 1) {
      throw new IllegalArgumentException("The end timestamp has not been set");
    }
    if (endTime < startTime) {
      throw new IllegalArgumentException(
          "The end timestamp cannot be less than the start timestamp");
    }

    return store.deleteAnnotationRange(metric, tags, startTime, endTime);
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
