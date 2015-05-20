package net.opentsdb.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.eventbus.EventBus;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.plugins.PluginError;
import net.opentsdb.plugins.RTPublisher;
import net.opentsdb.search.IdChangeIndexerListener;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdUtils;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.uid.LabelId;
import net.opentsdb.uid.UniqueIdType;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkNotNull;

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
   * Determines if an entry exists in storage or not.
   * This is used by the UID Manager tool to determine if we need to write a
   * new TSUID entry or not. It will not attempt to verify if the stored data is
   * valid, just checks to see if something is stored in the proper column.
   * @param tsuid The UID of the meta to verify
   * @return True if data was found, false if not
   */
  public Deferred<Boolean> TSMetaExists(final String tsuid) {
    return store.TSMetaExists(tsuid);
  }

  /**
   * Attempts to store a new, blank timeseries meta object via a Put
   * <b>Note:</b> This should not be called by user accessible methods as it will
   * overwrite any data already in the column.
   * <b>Note:</b> This call does not guarantee that the UIDs exist before
   * storing as it should only be called *after* a data point has been recorded
   * or during a meta sync.
   * @param tsMeta The TSMeta to be stored in the database
   * @return A meaningless deferred.
   * @throws IllegalArgumentException if parsing failed
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  public Deferred<Boolean> create(final TSMeta tsMeta) {
    tsMeta.checkTSUI();
    return store.create(tsMeta);
  }

  /**
   * Attempts to delete the meta object from storage
   * @param tsMeta The TSMeta to be removed.
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws IllegalArgumentException if data was missing (identifier and type)
   */
  public Deferred<Void> delete(final TSMeta tsMeta) {
    tsMeta.checkTSUI();
    return store.delete(tsMeta);
  }

  /**
   * Delete the annotation object from the search index
   * @param note The annotation object to delete
   * @since 2.0
   */
  public void deleteAnnotation(final Annotation note) {
    searchPlugin.deleteAnnotation(note).addErrback(new PluginError(searchPlugin));
  }

  /**
   * Delete the timeseries meta object from the search index
   * @param tsuid The TSUID to delete
   * @since 2.0
   */
  public void deleteTSMeta(final String tsuid) {
    searchPlugin.deleteTSMeta(tsuid).addErrback(new PluginError(searchPlugin));
  }

  /**
   * Executes a search query using the search plugin
   * @param query The query to execute
   * @return A deferred object to wait on for the results to be fetched
   * @throws IllegalStateException if the search plugin has not been enabled or
   * configured
   * @since 2.0
   */
  public Deferred<SearchQuery> executeSearch(final SearchQuery query) {
    return searchPlugin.executeQuery(query);
  }

  /**
   * Scans through the global annotation storage rows and returns a list of
   * parsed annotation objects. If no annotations were found for the given
   * timespan, the resulting list will be empty.
   * @param start_time Start time to scan from. May be 0
   * @param end_time End time to scan to. Must be greater than 0
   * @return A list with detected annotations. May be empty.
   * @throws IllegalArgumentException if the end timestamp has not been set or
   * the end time is less than the start time
   */
  public Deferred<List<Annotation>> getGlobalAnnotations(final long start_time, final long end_time) {
    if (end_time < 1) {
      throw new IllegalArgumentException("The end timestamp has not been set");
    }
    if (end_time < start_time) {
      throw new IllegalArgumentException(
          "The end timestamp cannot be less than the start timestamp");
    }

    return store.getGlobalAnnotations(start_time, end_time);
  }

  /**
   * Attempts to fetch a global or local annotation from storage
   * @param tsuid The TSUID as a string. May be empty if retrieving a global
   * annotation
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  public Deferred<Annotation> getAnnotation(final String tsuid, final long start_time) {
    if (Strings.isNullOrEmpty(tsuid)) {
      return store.getAnnotation(null, start_time);
    }

    return store.getAnnotation(IdUtils.stringToUid(tsuid), start_time);
  }

  /**
   * Attempts to mark an Annotation object for deletion. Note that if the
   * annotation does not exist in storage, this delete call will not throw an
   * error.
   *
   * @param annotation The Annotation we want to store.
   * @return A meaningless Deferred for the caller to wait on until the call is
   * complete. The value may be null.
   */
  public Deferred<Void> delete(Annotation annotation) {
    if (annotation.getStartTime() < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    return store.delete(annotation);
  }

  /**
   * Attempts to fetch the timeseries meta data from storage.
   * This method will fetch the {@code counter} and {@code meta} columns.
   * If load_uids is false this method will not load the UIDMeta objects.
   * <b>Note:</b> Until we have a caching layer implemented, this will make at
   * least 4 reads to the storage system, 1 for the TSUID meta, 1 for the
   * metric UIDMeta and 1 each for every tagk/tagv UIDMeta object.
   * <p>
   * @param tsuid The UID of the meta to fetch
   * @param load_uids Set this you true if you also want to load the UIDs
   * @return A TSMeta object if found, null if not
   * @throws IllegalArgumentException if parsing failed
   * @throws net.opentsdb.utils.JSONException if the data was corrupted
   */
  public Deferred<TSMeta> getTSMeta(final String tsuid, final boolean load_uids) {
    return  store.getTSMeta(IdUtils.stringToUid(tsuid));

  }

  /**
   * Verifies the UID object exists, then attempts to fetch the meta from
   * storage and if not found, returns a default object.
   * <p>
   * The reason for returning a default object (with the type, identifier and name set)
   * is due to users who may have just enabled meta data or have upgraded; we
   * want to return valid data. If they modify the entry, it will write to
   * storage. You can tell it's a default if the {@code created} value is 0. If
   * the meta was generated at UID assignment or updated by the meta sync CLI
   * command, it will have a valid created timestamp.
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
   * @param note The annotation object to index
   * @since 2.0
   */
  public void indexAnnotation(final Annotation note) {
    searchPlugin.indexAnnotation(note).addErrback(new PluginError(searchPlugin));
    realtimePublisher.publishAnnotation(note).addErrback(new PluginError(realtimePublisher));
  }

  /**
   * Index the given timeseries meta object via the configured search plugin
   * @param meta The meta data object to index
   * @since 2.0
   */
  public void indexTSMeta(final TSMeta meta) {
    searchPlugin.indexTSMeta(meta).addErrback(new PluginError(searchPlugin));
  }

  /**
   * Attempts a CompareAndSet storage call, loading the object from storage,
   * synchronizing changes, and attempting a put.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * or there weren't any changes, then the data will not be written and an
   * exception will be thrown.
   * @param annotation The The Annotation we want to store.
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * True if the storage call was successful, false if the object was
   * modified in storage during the CAS call. If false, retry the call. Other
   * failures will result in an exception being thrown.
   * @throws IllegalArgumentException if required data was missing such as the
   * {@code #start_time}
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
      public Deferred<Boolean> call(final Annotation stored_note)
        throws Exception {
        if (stored_note != null) {
          annotation.syncNote(stored_note, overwrite);
        }

        return store.updateAnnotation(stored_note, annotation);
      }
    }

    final byte[] tsuid;
    if (Strings.isNullOrEmpty(annotation.getTSUID())) {
      tsuid = null;
    } else {
      tsuid = IdUtils.stringToUid(annotation.getTSUID());
    }

    return store.getAnnotation(tsuid, annotation.getStartTime()).addCallbackDeferring(new StoreCB());
  }

  /**
   * Deletes global or TSUID associated annotiations for the given time range.
   * @param tsuid An optional TSUID. If set to null, then global annotations for
   * the given range will be deleted
   * @param start_time A start timestamp in milliseconds
   * @param end_time An end timestamp in millseconds
   * @return The number of annotations deleted
   * @throws IllegalArgumentException if the timestamps are invalid
   * @since 2.1
   */
  public Deferred<Integer> deleteRange(final byte[] tsuid, final long start_time, final long end_time) {
    if (end_time < 1) {
      throw new IllegalArgumentException("The end timestamp has not been set");
    }
    if (end_time < start_time) {
      throw new IllegalArgumentException(
          "The end timestamp cannot be less than the start timestamp");
    }

    return store.deleteAnnotationRange(tsuid, start_time, end_time);
  }

  /**
   * Attempts a CompareAndSet storage call, loading the object from storage,
   * synchronizing changes, and attempting a put. Also verifies that associated
   * UID name mappings exist before merging.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * or there weren't any changes, then the data will not be written and an
   * exception will be thrown.
   * <b>Note:</b> We do not store the UIDMeta information with TSMeta's since
   * users may change a single UIDMeta object and we don't want to update every
   * TSUID that includes that object with the new data. Instead, UIDMetas are
   * merged into the TSMeta on retrieval so we always have canonical data. This
   * also saves space in storage.
   * @param tsMeta The TSMeta to stored
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * @return True if the storage call was successful, false if the object was
   * modified in storage during the CAS call. If false, retry the call. Other
   * failures will result in an exception being thrown.
   * @throws IllegalArgumentException if parsing failed
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  public Deferred<Boolean> syncToStorage(final TSMeta tsMeta,
                                         final boolean overwrite) {
    tsMeta.checkTSUI();

    if (!tsMeta.hasChanges()) {
      LOG.debug("{} does not have changes, skipping sync to storage", tsMeta);
      throw new IllegalStateException("No changes detected in TSUID meta data");
    }

    /**
     * Callback used to verify that the UID name mappings exist. We don't need
     * to process the actual name, we just want it to throw an error if any
     * of the UIDs don't exist.
     */
    class UidCB implements Callback<Object, String> {

      @Override
      public Object call(String name) throws Exception {
        // nothing to do as missing mappings will throw a NoSuchUniqueId
        return null;
      }

    }

    // parse out the tags from the tsuid
    final List<byte[]> parsed_tags = IdUtils.getTagsFromTSUID(tsMeta.getTSUID());

    // Deferred group used to accumulate UidCB callbacks so the next call
    // can wait until all of the UIDs have been verified
    ArrayList<Deferred<Object>> uid_group =
        new ArrayList<>(parsed_tags.size() + 1);

    final LabelId metric = tsMeta.metric();
    uid_group.add(uniqueIdClient.getUidName(UniqueIdType.METRIC, metric)
            .addCallback(new UidCB()));

    for (final Map.Entry<LabelId, LabelId> tag : tsMeta.tags().entrySet()) {
      // TODO
      //uid_group.add(uniqueIdClient.getUidName(UniqueIdType.TAGK, tag.getKey()));
      //uid_group.add(uniqueIdClient.getUidName(UniqueIdType.TAGV, tag.getValue()));
    }

    return store.syncToStorage(tsMeta, Deferred.group(uid_group), overwrite);
  }

  /**
   * Returns all TSMeta objects stored for timeseries defined by this query. The
   * query is similar to TsdbQuery without any aggregations. Returns an empty
   * list, when no TSMetas are found. Only returns stored TSMetas.
   *
   * @return A list of existing TSMetas for the timeseries covered by the query.
   * @throws IllegalArgumentException When either no metric was specified or the
   *                                  tag map was null (Empty map is OK).
   */
  public Deferred<List<TSMeta>> executeTimeseriesMetaQuery(final SearchQuery query) {
    return uniqueIdClient.resolve(query).addCallbackDeferring(
        new Callback<Deferred<List<TSMeta>>, ResolvedSearchQuery>() {
          @Override
          public Deferred<List<TSMeta>> call(final ResolvedSearchQuery arg) {
            return store.executeTimeseriesMetaQuery(arg);
          }
        });
  }

  /**
   * Attempts to update the information of the stored LabelMeta object with the
   * same {@code identifier} and {@type} as the provided meta object. The stored
   * LabelMeta will be fetched first and checked for equality before it tried to
   * save anything.
   *
   * @param meta The LabelMeta with the updated information.
   * @return True if the updates were saved successfully. False if there were no
   * changes to make.
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
