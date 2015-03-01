package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.ResolvedSearchQuery;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tree.TreeBuilder;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.uid.IdCreatedEvent;
import net.opentsdb.uid.IdUtils;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.uid.UniqueIdType;
import com.typesafe.config.Config;
import net.opentsdb.utils.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The exposed interface for managing meta objects.
 */
public class MetaClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetaClient.class);

  private final Config config;
  private final TsdbStore store;
  private final UniqueIdClient uniqueIdClient;
  private final SearchPlugin searchPlugin;
  private final TreeClient treeClient;
  private final RTPublisher realtimePublisher;

  @Inject
  public MetaClient(final TsdbStore store,
                    final EventBus idEventBus,
                    final SearchPlugin searchPlugin,
                    final Config config,
                    final UniqueIdClient uniqueIdClient, final TreeClient treeClient, final RTPublisher realtimePublisher) {
    this.config = checkNotNull(config);
    this.store = checkNotNull(store);
    this.uniqueIdClient = checkNotNull(uniqueIdClient);
    this.treeClient = checkNotNull(treeClient);
    this.searchPlugin = checkNotNull(searchPlugin);
    this.realtimePublisher = checkNotNull(realtimePublisher);

    checkNotNull(idEventBus);

    if (config.getBoolean("tsd.core.meta.enable_realtime_uid")) {
      idEventBus.register(new IdChangeListener(store, searchPlugin));
    }
  }

  /**
   * Determines if the counter column exists for the TSUID.
   * This is used by the UID Manager tool to determine if we need to write a
   * new TSUID entry or not. It will not attempt to verify if the stored data is
   * valid, just checks to see if something is stored in the proper column.
   * @param tsuid The UID of the meta to verify
   * @return True if data was found, false if not
   * @throws org.hbase.async.HBaseException if there was an issue fetching
   */
  public Deferred<Boolean> TSMetaCounterExists(final byte[] tsuid) {
    return store.TSMetaCounterExists(tsuid);
  }

  /**
   * Determines if an entry exists in storage or not.
   * This is used by the UID Manager tool to determine if we need to write a
   * new TSUID entry or not. It will not attempt to verify if the stored data is
   * valid, just checks to see if something is stored in the proper column.
   * @param tsuid The UID of the meta to verify
   * @return True if data was found, false if not
   * @throws org.hbase.async.HBaseException if there was an issue fetching
   */
  public Deferred<Boolean> TSMetaExists(final String tsuid) {
    return store.TSMetaExists(tsuid);
  }

  /**
   * Attempts to store a blank, new UID meta object in the proper location.
   * <b>Warning:</b> This should not be called by user accessible methods as it
   * will overwrite any data already in the column. This method does not use
   * a CAS, instead it uses a PUT to overwrite anything in the column.
   * @param meta The meta object to store
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws IllegalArgumentException if data was missing
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  public Deferred<Object> add(final UIDMeta meta) {
    if (Strings.isNullOrEmpty(meta.getName())) {
      throw new IllegalArgumentException("Missing name");
    }

    return store.add(meta);
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
   * Create the counter for a timeseries meta object.
   * @param ts The Timeseries meta object to create the counter for
   * @return A deferred that indicates the completion of the request
   */
  public Deferred<Object> createTimeseriesCounter(final TSMeta ts) {
    ts.checkTSUI();
    return store.setTSMetaCounter(IdUtils.stringToUid(ts.getTSUID()), 0);
  }

  /**
   * Attempts to delete the meta object from storage
   * @param tsMeta The TSMeta to be removed.
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws IllegalArgumentException if data was missing (uid and type)
   */
  public Deferred<Object> delete(final TSMeta tsMeta) {
    tsMeta.checkTSUI();
    return store.delete(tsMeta);
  }

  /**
   * Attempts to delete the meta object from storage
   *
   * @param meta The meta object to delete
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws IllegalArgumentException if data was missing (uid and type)
   */
  public Deferred<Object> delete(final UIDMeta meta) {
    return store.delete(meta);
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

  public Deferred<Object> deleteTimeseriesCounter(final TSMeta ts) {
    ts.checkTSUI();
    return store.deleteTimeseriesCounter(ts);
  }

  /**
   * Delete the UID meta object from the search index
   * @param meta The UID meta object to delete
   * @since 2.0
   */
  public void deleteUIDMeta(final UIDMeta meta) {
    searchPlugin.deleteUIDMeta(meta).addErrback(new PluginError(searchPlugin));
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
  public Deferred<Object> delete(Annotation annotation) {
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
   * @throws org.hbase.async.HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws net.opentsdb.utils.JSONException if the data was corrupted
   */
  public Deferred<TSMeta> getTSMeta(final String tsuid, final boolean load_uids) {
    if (load_uids)
    return store.getTSMeta(IdUtils.stringToUid(tsuid))
            .addCallbackDeferring(new LoadUIDs(tsuid));

    return  store.getTSMeta(IdUtils.stringToUid(tsuid));

  }

  /**
   * Convenience overload of {@code getUIDMeta(UniqueIdType, byte[])}
   * @param type The type of UID to fetch
   * @param uid The ID of the meta to fetch
   * @return A UIDMeta from storage or a default
   * @throws org.hbase.async.HBaseException if there was an issue fetching
   * @throws net.opentsdb.uid.NoSuchUniqueId If the UID does not exist
   */
  public Deferred<UIDMeta> getUIDMeta(final UniqueIdType type,
                                      final String uid) {
    return getUIDMeta(type, IdUtils.stringToUid(uid));
  }

  /**
   * Verifies the UID object exists, then attempts to fetch the meta from
   * storage and if not found, returns a default object.
   * <p>
   * The reason for returning a default object (with the type, uid and name set)
   * is due to users who may have just enabled meta data or have upgraded; we
   * want to return valid data. If they modify the entry, it will write to
   * storage. You can tell it's a default if the {@code created} value is 0. If
   * the meta was generated at UID assignment or updated by the meta sync CLI
   * command, it will have a valid created timestamp.
   * @param type The type of UID to fetch
   * @param uid The ID of the meta to fetch
   * @return A UIDMeta from storage or a default
   * @throws org.hbase.async.HBaseException if there was an issue fetching
   * @throws net.opentsdb.uid.NoSuchUniqueId If the UID does not exist
   */
  public Deferred<UIDMeta> getUIDMeta(final UniqueIdType type,
                                      final byte[] uid) {
    /**
     * Callback used to verify that the UID to name mapping exists. Uses the TSD
     * for verification so the name may be cached. If the name does not exist
     * it will throw a NoSuchUniqueId and the meta data will not be returned.
     * This helps in case the user deletes a UID but the meta data is still
     * stored. The fsck utility can be used later to cleanup orphaned objects.
     */
    class NameCB implements Callback<Deferred<UIDMeta>, String> {

      /**
       * Called after verifying that the name mapping exists
       * @return The results of {@link net.opentsdb.storage.TsdbStore#getMeta(
       *      byte[], String, net.opentsdb.uid.UniqueIdType)}
       */
      @Override
      public Deferred<UIDMeta> call(final String name) throws Exception {
        return store.getMeta(uid, name, type);
      }
    }

    // verify that the UID is still in the map before fetching from storage
    return uniqueIdClient.getUidName(type, uid).addCallbackDeferring(new NameCB());
  }

  /**
   * Increments the tsuid datapoint counter or creates a new counter. Also
   * creates a new meta data entry if the counter did not exist.
   * <b>Note:</b> This method also:
   * <ul><li>Passes the new TSMeta object to the Search plugin after loading
   * UIDMeta objects</li>
   * <li>Passes the new TSMeta through all configured trees if enabled</li></ul>
   * @param tsuid The TSUID to increment or create
   * @return 0 if the put failed, a positive LONG if the put was successful
   * @throws org.hbase.async.HBaseException if there was a storage issue
   * @throws net.opentsdb.utils.JSONException if the data was corrupted
   */
  public Deferred<Long> incrementAndGetCounter(final byte[] tsuid) {

    /**
     * Callback that will create a new TSMeta if the increment result is 1 or
     * will simply return the new value.
     */
    final class TSMetaCB implements Callback<Deferred<Long>, Long> {

      /**
       * Called after incrementing the counter and will create a new TSMeta if
       * the returned value was 1 as well as pass the new meta through trees
       * and the search indexer if configured.
       * @return 0 if the put failed, a positive LONG if the put was successful
       */
      @Override
      public Deferred<Long> call(final Long incremented_value)
              throws Exception {

        if (incremented_value > 1) {
          // TODO - maybe update the search index every X number of increments?
          // Otherwise the search engine would only get last_updated/count
          // whenever the user runs the full sync CLI
          return Deferred.fromResult(incremented_value);
        }

        // create a new meta object with the current system timestamp. Ideally
        // we would want the data point's timestamp, but that's much more data
        // to keep track of and may not be accurate.
        final TSMeta meta = new TSMeta(tsuid,
                System.currentTimeMillis() / 1000);

        /**
         * Called after the meta has been passed through tree processing. The
         * result of the processing doesn't matter and the user may not even
         * have it enabled, so we'll just return the counter.
         */
        final class TreeCB implements Callback<Deferred<Long>, Boolean> {

          @Override
          public Deferred<Long> call(Boolean success) throws Exception {
            return Deferred.fromResult(incremented_value);
          }

        }

        /**
         * Called after retrieving the newly stored TSMeta and loading
         * associated UIDMeta objects. This class will also pass the meta to the
         * search plugin and run it through any configured trees
         */
        final class FetchNewCB implements Callback<Deferred<Long>, TSMeta> {

          @Override
          public Deferred<Long> call(TSMeta stored_meta) throws Exception {

            // pass to the search plugin
            indexTSMeta(stored_meta);

            // pass through the trees
            return processTSMetaThroughTrees(stored_meta)
                    .addCallbackDeferring(new TreeCB());
          }

        }

        /**
         * Called after the CAS to store the new TSMeta object. If the CAS
         * failed then we return immediately with a 0 for the counter value.
         * Otherwise we keep processing to load the meta and pass it on.
         */
        final class StoreNewCB implements Callback<Deferred<Long>, Boolean> {

          @Override
          public Deferred<Long> call(Boolean success) throws Exception {
            if (!success) {
              LOG.warn("Unable to save metadata: {}", meta);
              return Deferred.fromResult(0L);
            }

            LOG.info("Successfullly created new TSUID entry for: {}", meta);
            final Deferred<TSMeta> meta = store.getTSMeta(tsuid)
                    .addCallbackDeferring(
                            new LoadUIDs(IdUtils.uidToString(tsuid)));
            return meta.addCallbackDeferring(new FetchNewCB());
          }

        }

        // store the new TSMeta object and setup the callback chain
        return create(meta).addCallbackDeferring(new StoreNewCB());
      }

    }

    Deferred<Long> res = store.incrementAndGetCounter(tsuid);
    if (!config.getBoolean("tsd.core.meta.enable_realtime_ts"))
      return res;
    return res.addCallbackDeferring(
            new TSMetaCB());
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
   * Index the given UID meta object via the configured search plugin
   * @param meta The meta data object to index
   * @since 2.0
   */
  public void indexUIDMeta(final UIDMeta meta) {
    searchPlugin.indexUIDMeta(meta).addErrback(new PluginError(searchPlugin));
  }

  /**
   * Parses a TSMeta object from the given column, optionally loading the
   * UIDMeta objects
   *
   * @param column_key The KeyValue.key() of the column to parse also know as uid
   * @param column_value The KeyValue.value() of the column to parse
   * @param load_uidmetas Whether or not UIDmeta objects should be loaded
   * @return A TSMeta if parsed successfully
   * @throws net.opentsdb.utils.JSONException if the data was corrupted
   */
  public Deferred<TSMeta> parseFromColumn(final byte[] column_key,
                                          final byte[] column_value,
                                          final boolean load_uidmetas) {
    if (column_value == null || column_value.length < 1) {
      throw new IllegalArgumentException("Empty column value");
    }

    final TSMeta meta = JSON.parseToObject(column_value, TSMeta.class);

    // fix in case the tsuid is missing
    if (meta.getTSUID() == null || meta.getTSUID().isEmpty()) {
      meta.setTSUID(IdUtils.uidToString(column_key));
    }

    if (!load_uidmetas) {
      return Deferred.fromResult(meta);
    }

    final LoadUIDs deferred = new LoadUIDs(meta.getTSUID());
    try {
      return deferred.call(meta);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Processes the TSMeta through all of the trees if configured to do so
   * @param meta The meta data to process
   * @since 2.0
   */
  public Deferred<Boolean> processTSMetaThroughTrees(final TSMeta meta) {
    if (config.getBoolean("tsd.core.tree.enable_processing")) {
      return TreeBuilder.processAllTrees(treeClient, store, meta);
    }
    return Deferred.fromResult(false);
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
            new ArrayList<Deferred<Object>>(parsed_tags.size() + 1);

    // calculate the metric UID and fetch it's name mapping
    final byte[] metric_uid = IdUtils.stringToUid(
        tsMeta.getTSUID().substring(0, Const.METRICS_WIDTH * 2));
    uid_group.add(uniqueIdClient.getUidName(UniqueIdType.METRIC, metric_uid)
            .addCallback(new UidCB()));

    int idx = 0;
    for (byte[] tag : parsed_tags) {
      if (idx % 2 == 0) {
        uid_group.add(uniqueIdClient.getUidName(UniqueIdType.TAGK, tag)
                .addCallback(new UidCB()));
      } else {
        uid_group.add(uniqueIdClient.getUidName(UniqueIdType.TAGV, tag)
                .addCallback(new UidCB()));
      }
      idx++;
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
   * Attempts a CompareAndSet storage call, loading the object from storage,
   * synchronizing changes, and attempting a put.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * then the data will not be written.
   *
   * @param meta      The UIDMeta to store.
   * @param overwrite When the RPC method is PUT, will overwrite all user
   *                  accessible fields
   * @return True if the storage call was successful, false if the object
   * was
   * modified in storage during the CAS call. If false, retry the call. Other
   * failures will result in an exception being thrown.
   * @throws org.hbase.async.HBaseException     If there was an issue fetching
   * @throws IllegalArgumentException           If parsing failed
   * @throws net.opentsdb.uid.NoSuchUniqueId    If the UID does not exist
   * @throws IllegalStateException              If the data hasn't changed. This is OK!
   * @throws net.opentsdb.utils.JSONException   If the object could not be serialized
   */
  public Deferred<Boolean> syncUIDMetaToStorage(final UIDMeta meta,
                                                final boolean overwrite) {
    if (!meta.hasChanges()) {
      LOG.debug("{} does not have changes, skipping sync to storage", meta);
      throw new IllegalStateException("No changes detected in UID meta data");
    }

    return uniqueIdClient.getUidName(meta.getType(), meta.getUID()).addCallbackDeferring(
      new Callback<Deferred<Boolean>, String>() {
        @Override
        public Deferred<Boolean> call(String arg) {
          return store.updateMeta(meta, overwrite);
        }
      }
    );
  }




  /**
   * Asynchronously loads the UIDMeta objects into the given TSMeta object. Used
   * by multiple methods so it's broken into it's own class here.
   */
  private class LoadUIDs implements Callback<Deferred<TSMeta>, TSMeta> {

    private final byte[] metric_uid;
    private final List<byte[]> tags;

    public LoadUIDs(final String tsuid) {

      final byte[] byte_tsuid = IdUtils.stringToUid(tsuid);

      metric_uid = Arrays.copyOfRange(byte_tsuid, 0, Const.METRICS_WIDTH);
      tags = IdUtils.getTagsFromTSUID(tsuid);
    }

    /**
     * @return A TSMeta object loaded with UIDMetas if successful
     * @throws org.hbase.async.HBaseException if there was a storage issue
     * @throws net.opentsdb.utils.JSONException if the data was corrupted
     */
    @Override
    public Deferred<TSMeta> call(final TSMeta meta) throws Exception {
      if (meta == null) {
        return Deferred.fromResult(null);
      }

      final ArrayList<Deferred<UIDMeta>> uid_group =
              new ArrayList<Deferred<UIDMeta>>(tags.size());

      Iterator<byte[]> tag_iter = tags.iterator();

      while(tag_iter.hasNext()) {
        uid_group.add(getUIDMeta(UniqueIdType.TAGK, tag_iter.next()));
        uid_group.add(getUIDMeta(UniqueIdType.TAGV, tag_iter.next()));
      }

      /**
       * A callback that will place the loaded UIDMeta objects for the tags in
       * order on meta.tags.
       */
      final class UIDMetaTagsCB implements Callback<TSMeta, ArrayList<UIDMeta>> {
        @Override
        public TSMeta call(final ArrayList<UIDMeta> uid_metas) {
          meta.setTags(uid_metas);
          return meta;
        }
      }

      /**
       * A callback that will place the loaded UIDMeta object for the metric
       * UID on meta.metric.
       */
      class UIDMetaMetricCB implements Callback<Deferred<TSMeta>, UIDMeta> {
        @Override
        public Deferred<TSMeta> call(UIDMeta uid_meta) {
          meta.setMetric(uid_meta);

          // This will chain the UIDMetaTagsCB on this callback which is what
          // allows us to just return the result of the callback chain bellow
          // . groupInOrder is used so that the resulting list will be in the
          // same order as they were added to uid_group.
          return Deferred.groupInOrder(uid_group)
                  .addCallback(new UIDMetaTagsCB());
        }
      }

      return getUIDMeta(UniqueIdType.METRIC, metric_uid)
              .addCallbackDeferring(new UIDMetaMetricCB());
    }
  }


  /**
   * A Guava {@link com.google.common.eventbus.EventBus} listener that listens
   * for ID changes and creates UIDMeta objects and indexes them with the
   * {@link net.opentsdb.search.SearchPlugin} when appropriate.
   */
  private static class IdChangeListener {
    private final TsdbStore store;
    private SearchPlugin searchPlugin;

    public IdChangeListener(final TsdbStore store,
                            final SearchPlugin searchPlugin) {
      this.store = store;
      this.searchPlugin = searchPlugin;
    }

    /**
     * The method that subscribes to {@link net.opentsdb.uid.IdCreatedEvent}s.
     * You should not call this directly, post messages to the event bus that
     * this listener is registered to instead.
     * @param event The published event.
     */
    @Subscribe
    @AllowConcurrentEvents
    public final void recordIdCreated(IdCreatedEvent event) {
      UIDMeta meta = new UIDMeta(event.getType(), event.getId(), event.getName());
      store.add(meta);
      LOG.info("Wrote UIDMeta for: {}", event.getName());
      searchPlugin.indexUIDMeta(meta);
    }
  }
}
