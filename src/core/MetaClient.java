package net.opentsdb.core;

import java.util.List;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdCreatedEvent;
import net.opentsdb.uid.UniqueId;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The exposed interface for managing meta objects.
 */
public class MetaClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetaClient.class);

  private final TsdbStore store;

  public MetaClient(final TsdbStore store,
                    final EventBus idEventBus,
                    final SearchPlugin searchPlugin,
                    final Config config) {
    this.store = checkNotNull(store);

    checkNotNull(idEventBus);
    checkNotNull(searchPlugin);

    if (config.enable_realtime_uid()) {
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
    return store.setTSMetaCounter(UniqueId.stringToUid(ts.getTSUID()), 0);
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

  public Deferred<Object> deleteTimeseriesCounter(final TSMeta ts) {
    ts.checkTSUI();
    return store.deleteTimeseriesCounter(ts);
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

    return store.getAnnotation(UniqueId.stringToUid(tsuid), start_time);
  }

  /**
   * Attempts to mark an Annotation object for deletion. Note that if the
   * annotation does not exist in storage, this delete call will not throw an
   * error.
   *
   * @param annotation The Annotation we want to store.
   * @param tsdb
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
   * @param tsdb
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
      tsuid = UniqueId.stringToUid(annotation.getTSUID());
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
