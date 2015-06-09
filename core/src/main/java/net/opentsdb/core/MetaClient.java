package net.opentsdb.core;

import static com.google.common.base.Preconditions.checkNotNull;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.plugins.PluginError;
import net.opentsdb.plugins.RTPublisher;
import net.opentsdb.search.IdChangeIndexerListener;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdUtils;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;

import com.google.common.base.Strings;
import com.google.common.eventbus.EventBus;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
  private final UniqueIdClient uniqueIdClient;
  private final SearchPlugin searchPlugin;
  private final RTPublisher realtimePublisher;

  @Inject
  public MetaClient(final TsdbStore store,
                    final EventBus idEventBus,
                    final SearchPlugin searchPlugin,
                    final Config config,
                    final UniqueIdClient uniqueIdClient, final RTPublisher realtimePublisher) {
    this.config = checkNotNull(config);
    this.store = checkNotNull(store);
    this.uniqueIdClient = checkNotNull(uniqueIdClient);
    this.searchPlugin = checkNotNull(searchPlugin);
    this.realtimePublisher = checkNotNull(realtimePublisher);

    checkNotNull(idEventBus);

    // TODO this should be registered in a dagger module
    idEventBus.register(new IdChangeIndexerListener(store, searchPlugin));
  }

  /**
   * Delete the annotation object from the search index
   *
   * @param note The annotation object to delete
   * @since 2.0
   */
  public void deleteAnnotation(final Annotation note) {
    searchPlugin.deleteAnnotation(note).addErrback(new PluginError(searchPlugin));
  }

  /**
   * Executes a search query using the search plugin
   *
   * @param query The query to execute
   * @return A deferred object to wait on for the results to be fetched
   * @throws IllegalStateException if the search plugin has not been enabled or configured
   * @since 2.0
   */
  public Deferred<SearchQuery> executeSearch(final SearchQuery query) {
    return searchPlugin.executeQuery(query);
  }

  /**
   * Scans through the global annotation storage rows and returns a list of parsed annotation
   * objects. If no annotations were found for the given timespan, the resulting list will be
   * empty.
   *
   * @param startTime Start time to scan from. May be 0
   * @param endTime End time to scan to. Must be greater than 0
   * @return A list with detected annotations. May be empty.
   * @throws IllegalArgumentException if the end timestamp has not been set or the end time is less
   * than the start time
   */
  public Deferred<List<Annotation>> getGlobalAnnotations(final long startTime,
                                                         final long endTime) {
    if (endTime < 1) {
      throw new IllegalArgumentException("The end timestamp has not been set");
    }
    if (endTime < startTime) {
      throw new IllegalArgumentException(
          "The end timestamp cannot be less than the start timestamp");
    }

    return store.getGlobalAnnotations(startTime, endTime);
  }

  /**
   * Attempts to fetch a global or local annotation from storage
   *
   * @param tsuid The TSUID as a string. May be empty if retrieving a global annotation
   * @param startTime The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  public Deferred<Annotation> getAnnotation(final String tsuid, final long startTime) {
    if (Strings.isNullOrEmpty(tsuid)) {
      return store.getAnnotation(null, startTime);
    }

    return store.getAnnotation(IdUtils.stringToUid(tsuid), startTime);
  }

  /**
   * Attempts to mark an Annotation object for deletion. Note that if the annotation does not exist
   * in storage, this delete call will not throw an error.
   *
   * @param annotation The Annotation we want to store.
   * @return A meaningless Deferred for the caller to wait on until the call is complete. The value
   * may be null.
   */
  public Deferred<Void> delete(Annotation annotation) {
    if (annotation.getStartTime() < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    return store.delete(annotation);
  }

  /**
   * Verifies the UID object exists, then attempts to fetch the meta from storage and if not found,
   * returns a default object.
   * <p/>
   * The reason for returning a default object (with the type, identifier and name set) is due to
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
  public Deferred<LabelMeta> getLabelMeta(final UniqueIdType type,
                                          final LabelId uid) {
    /**
     * Callback used to verify that the UID to name mapping exists. Uses the TSD
     * for verification so the name may be cached. If the name does not exist
     * it will throw a NoSuchUniqueId and the meta data will not be returned.
     * This helps in case the user deletes a UID but the meta data is still
     * stored. The fsck utility can be used later to cleanup orphaned objects.
     */
    class NameCB implements Callback<Deferred<LabelMeta>, String> {

      /**
       * Called after verifying that the name mapping exists
       * @return The results of {@link TsdbStore#getMeta(byte[], UniqueIdType)}
       */
      @Override
      public Deferred<LabelMeta> call(final String name) throws Exception {
        return store.getMeta(uid, type);
      }
    }

    // verify that the UID is still in the map before fetching from storage
    return uniqueIdClient.getUidName(type, uid).addCallbackDeferring(new NameCB());
  }

  /**
   * Index the given Annotation object via the configured search plugin
   *
   * @param note The annotation object to index
   * @since 2.0
   */
  public void indexAnnotation(final Annotation note) {
    searchPlugin.indexAnnotation(note).addErrback(new PluginError(searchPlugin));
    realtimePublisher.publishAnnotation(note).addErrback(new PluginError(realtimePublisher));
  }

  /**
   * Attempts a CompareAndSet storage call, loading the object from storage, synchronizing changes,
   * and attempting a put. <b>Note:</b> If the local object didn't have any fields set by the caller
   * or there weren't any changes, then the data will not be written and an exception will be
   * thrown.
   *
   * @param annotation The The Annotation we want to store.
   * @param overwrite When the RPC method is PUT, will overwrite all user accessible fields True if
   * the storage call was successful, false if the object was modified in storage during the CAS
   * call. If false, retry the call. Other failures will result in an exception being thrown.
   * @throws IllegalArgumentException if required data was missing such as the {@code #start_time}
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  public Deferred<Boolean> syncToStorage(final Annotation annotation,
                                         final boolean overwrite) {
    if (annotation.getStartTime() < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    if (!annotation.hasChanges()) {
      LOG.debug("{} does not have changes, skipping sync to storage", annotation);
      throw new IllegalStateException("No changes detected in Annotation data");
    }

    final class StoreCB implements Callback<Deferred<Boolean>, Annotation> {
      @Override
      public Deferred<Boolean> call(final Annotation storedNote)
          throws Exception {
        if (storedNote != null) {
          annotation.syncNote(storedNote, overwrite);
        }

        return store.updateAnnotation(storedNote, annotation);
      }
    }

    final byte[] tsuid;
    if (Strings.isNullOrEmpty(annotation.getTSUID())) {
      tsuid = null;
    } else {
      tsuid = IdUtils.stringToUid(annotation.getTSUID());
    }

    return store.getAnnotation(tsuid, annotation.getStartTime())
        .addCallbackDeferring(new StoreCB());
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
  public Deferred<Integer> deleteRange(final byte[] tsuid,
                                       final long startTime,
                                       final long endTime) {
    if (endTime < 1) {
      throw new IllegalArgumentException("The end timestamp has not been set");
    }
    if (endTime < startTime) {
      throw new IllegalArgumentException(
          "The end timestamp cannot be less than the start timestamp");
    }

    return store.deleteAnnotationRange(tsuid, startTime, endTime);
  }

  /**
   * Attempts to update the information of the stored LabelMeta object with the same {@code
   * identifier} and {@type} as the provided meta object. The stored LabelMeta will be fetched first
   * and checked for equality before it tried to save anything.
   *
   * @param meta The LabelMeta with the updated information.
   * @return True if the updates were saved successfully. False if there were no changes to make.
   * @throws net.opentsdb.uid.NoSuchUniqueId If the UID does not exist
   */
  public Deferred<Boolean> update(final LabelMeta meta) {
    return getLabelMeta(meta.type(), meta.identifier()).addCallbackDeferring(
        new Callback<Deferred<Boolean>, LabelMeta>() {
          @Override
          public Deferred<Boolean> call(final LabelMeta storedMeta) {
            if (!storedMeta.equals(meta)) {
              return store.updateMeta(meta);
            }

            LOG.debug("{} does not have any changes, skipping update", meta);
            return Deferred.fromResult(Boolean.FALSE);
          }
        });
  }
}
