// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

/**
 * Tool helper class used to generate or update meta data for UID names and 
 * timeseries. This class should only be used by CLI tools as it can take a long
 * time to complete.
 * A scanner is opened on the data table and it scans the entire thing looking
 * for timeseries that are missing TSMeta objects or may have the wrong "created"
 * time. Each timeseries also causes a check on the UIDMeta objects to verify
 * they have values and have a proper "Created" time as well.
 * <b>Note:</b> This class will also update configured search plugins with 
 * meta data generated or updated
 */
final class MetaSync extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(MetaSync.class);
  
  /** TSDB to use for storage access */
  final TSDB tsdb;

  /** A shared list of TSUIDs that have been processed by this or other 
   * threads. It stores hashes instead of the bytes or strings to save
   * on space */
  final Set<Integer> processed_tsuids;
  
  /** List of metric UIDs and their earliest detected timestamp */
  final ConcurrentHashMap<String, Long> metric_uids;
  
  /** List of tagk UIDs and their earliest detected timestamp */
  final ConcurrentHashMap<String, Long> tagk_uids;
  
  /** List of tagv UIDs and their earliest detected timestamp */
  final ConcurrentHashMap<String, Long> tagv_uids;
  
  /** Diagnostic ID for this thread */
  final int thread_id;
  
  /** The scanner for this worker */
  final Scanner scanner;
  
  /**
   * Constructor that sets local variables
   * @param tsdb The TSDB to process with
   * @param scanner The scanner to use for this worker
   * @param processed_tsuids TSUIDs that have been processed already
   * @param metric_uids List of metric UIDs
   * @param tagk_uids List of tag key UIDs
   * @param tagv_uids List of tag value UIDs
   * @param thread_id The ID of this thread (starts at 0)
   */
  public MetaSync(final TSDB tsdb, final Scanner scanner, 
      final Set<Integer> processed_tsuids,
      ConcurrentHashMap<String, Long> metric_uids,
      ConcurrentHashMap<String, Long> tagk_uids,
      ConcurrentHashMap<String, Long> tagv_uids,
      final int thread_id) {
    this.tsdb = tsdb;
    this.scanner = scanner;
    this.processed_tsuids = processed_tsuids;
    this.metric_uids = metric_uids;
    this.tagk_uids = tagk_uids;
    this.tagv_uids = tagv_uids;
    this.thread_id = thread_id;
  }
  
  /**
   * Loops through the entire TSDB data set and exits when complete.
   */
  public void run() {
    // list of deferred calls used to act as a buffer
    final ArrayList<Deferred<Boolean>> storage_calls = 
      new ArrayList<Deferred<Boolean>>();
    final Deferred<Object> result = new Deferred<Object>();

    final class ErrBack implements Callback<Object, Exception> {
      @Override
      public Object call(Exception e) throws Exception {
        Throwable ex = e;
        while (ex.getClass().equals(DeferredGroupException.class)) {
          if (ex.getCause() == null) {
            LOG.warn("Unable to get to the root cause of the DGE");
            break;
          }
          ex = ex.getCause();
        }
        LOG.error("Sync thread failed with exception", ex);
        result.callback(null);
        return null;
      }
    }
    final ErrBack err_back = new ErrBack();
    
    /**
     * Called when we have encountered a previously un-processed UIDMeta object.
     * This callback will update the "created" timestamp of the UIDMeta and
     * store the update, replace corrupted metas and update search plugins.
     */
    final class UidCB implements Callback<Deferred<Boolean>, UIDMeta> {

      private final UniqueIdType type;
      private final byte[] uid;
      private final long timestamp;
      
      /**
       * Constructor that initializes the local callback
       * @param type The type of UIDMeta we're dealing with
       * @param uid The UID of the meta object as a byte array
       * @param timestamp The timestamp of the timeseries when this meta
       * was first detected
       */
      public UidCB(final UniqueIdType type, final byte[] uid, 
          final long timestamp) {
        this.type = type;
        this.uid = uid;
        this.timestamp = timestamp;
      }
      
      /**
       * A nested class called after fetching a UID name to use when creating a
       * new UIDMeta object if the previous object was corrupted. Also pushes
       * the meta off to the search plugin.
       */
      final class UidNameCB implements Callback<Deferred<Boolean>, String> {

        @Override
        public Deferred<Boolean> call(final String name) throws Exception {
          UIDMeta new_meta = new UIDMeta(type, uid, name);
          new_meta.setCreated(timestamp);
          tsdb.indexUIDMeta(new_meta);
          LOG.info("Replacing corrupt UID [" + UniqueId.uidToString(uid) + 
            "] of type [" + type + "]");
          
          return new_meta.syncToStorage(tsdb, true);
        }
        
      }
      
      @Override
      public Deferred<Boolean> call(final UIDMeta meta) throws Exception {

        // we only want to update the time if it was outside of an hour
        // otherwise it's probably an accurate timestamp
        if (meta.getCreated() > (timestamp + 3600) || 
            meta.getCreated() == 0) {
          LOG.info("Updating UID [" + UniqueId.uidToString(uid) + 
              "] of type [" + type + "]");
          meta.setCreated(timestamp);
          
          // if the UIDMeta object was missing any of these fields, we'll
          // consider it corrupt and replace it with a new object
          if (meta.getUID() == null || meta.getUID().isEmpty() || 
              meta.getType() == null) {
            return tsdb.getUidName(type, uid)
              .addCallbackDeferring(new UidNameCB());
          } else {
            // the meta was good, just needed a timestamp update so sync to
            // search and storage
            tsdb.indexUIDMeta(meta);
            LOG.info("Syncing valid UID [" + UniqueId.uidToString(uid) + 
              "] of type [" + type + "]");
            return meta.syncToStorage(tsdb, false);
          }
        } else {
          LOG.debug("UID [" + UniqueId.uidToString(uid) + 
              "] of type [" + type + "] is up to date in storage");
          return Deferred.fromResult(true);
        }
      }
      
    }
    
    /**
     * Called to handle a previously unprocessed TSMeta object. This callback
     * will update the "created" timestamp, create a new TSMeta object if
     * missing, and update search plugins.
     */
    final class TSMetaCB implements Callback<Deferred<Boolean>, TSMeta> {
      
      private final String tsuid_string;
      private final byte[] tsuid;
      private final long timestamp;
      
      /**
       * Default constructor
       * @param tsuid ID of the timeseries
       * @param timestamp The timestamp when the first data point was recorded
       */
      public TSMetaCB(final byte[] tsuid, final long timestamp) {
        this.tsuid = tsuid;
        tsuid_string = UniqueId.uidToString(tsuid);
        this.timestamp = timestamp;
      }

      @Override
      public Deferred<Boolean> call(final TSMeta meta) throws Exception {
        
        /** Called to process the new meta through the search plugin and tree code */
        final class IndexCB implements Callback<Deferred<Boolean>, TSMeta> {
          @Override
          public Deferred<Boolean> call(final TSMeta new_meta) throws Exception {
            tsdb.indexTSMeta(new_meta);
            // pass through the trees
            return tsdb.processTSMetaThroughTrees(new_meta);
          }
        }
        
        /** Called to load the newly created meta object for passage onto the
         * search plugin and tree builder if configured
         */
        final class GetCB implements Callback<Deferred<Boolean>, Boolean> {
          @Override
          public final Deferred<Boolean> call(final Boolean exists)
              throws Exception {
            if (exists) {
              return TSMeta.getTSMeta(tsdb, tsuid_string)
                  .addCallbackDeferring(new IndexCB());
            } else {
              return Deferred.fromResult(false);
            }
          }
        }
        
        /** Errback on the store new call to catch issues */
        class ErrBack implements Callback<Object, Exception> {
          public Object call(final Exception e) throws Exception {
            LOG.warn("Failed creating meta for: " + tsuid + 
                " with exception: ", e);
            return null;
          }
        }
        
        // if we couldn't find a TSMeta in storage, then we need to generate a
        // new one
        if (meta == null) {
          
          /**
           * Called after successfully creating a TSMeta counter and object,
           * used to convert the deferred long to a boolean so it can be
           * combined with other calls for waiting.
           */
          final class CreatedCB implements Callback<Deferred<Boolean>, Long> {

            @Override
            public Deferred<Boolean> call(Long value) throws Exception {
              LOG.info("Created counter and meta for timeseries [" + 
                  tsuid_string + "]");
              return Deferred.fromResult(true);
            }
            
          }
          
          /**
           * Called after checking to see if the counter exists and is used
           * to determine if we should create a new counter AND meta or just a
           * new meta
           */
          final class CounterCB implements Callback<Deferred<Boolean>, Boolean> {
            
            @Override
            public Deferred<Boolean> call(final Boolean exists) throws Exception {
              if (!exists) {
                // note that the increment call will create the meta object
                // and send it to the search plugin so we don't have to do that
                // here or in the local callback
                return TSMeta.incrementAndGetCounter(tsdb, tsuid)
                  .addCallbackDeferring(new CreatedCB());
              } else {
                TSMeta new_meta = new TSMeta(tsuid, timestamp);
                tsdb.indexTSMeta(new_meta);
                LOG.info("Counter exists but meta was null, creating meta data "
                    + "for timeseries [" + tsuid_string + "]");
                return new_meta.storeNew(tsdb)
                    .addCallbackDeferring(new GetCB())
                    .addErrback(new ErrBack());    
              }
            }
          }
          
          // Take care of situations where the counter is created but the
          // meta data is not. May happen if the TSD crashes or is killed
          // improperly before the meta is flushed to storage.
          return TSMeta.counterExistsInStorage(tsdb, tsuid)
            .addCallbackDeferring(new CounterCB());
        }

        // verify the tsuid is good, it's possible for this to become 
        // corrupted
        if (meta.getTSUID() == null || 
            meta.getTSUID().isEmpty()) {
          LOG.warn("Replacing corrupt meta data for timeseries [" + 
              tsuid_string + "]");
          TSMeta new_meta = new TSMeta(tsuid, timestamp);
          tsdb.indexTSMeta(new_meta);
          return new_meta.storeNew(tsdb)
              .addCallbackDeferring(new GetCB())
              .addErrback(new ErrBack());
        } else {
          // we only want to update the time if it was outside of an 
          // hour otherwise it's probably an accurate timestamp
          if (meta.getCreated() > (timestamp + 3600) || 
              meta.getCreated() == 0) {
            meta.setCreated(timestamp);
            tsdb.indexTSMeta(meta);
            LOG.info("Updated created timestamp for timeseries [" + 
                tsuid_string + "]");
            return meta.syncToStorage(tsdb, false);
          }
          
          LOG.debug("TSUID [" + tsuid_string + "] is up to date in storage");
          return Deferred.fromResult(false);
        }
      }
      
    }
    
    /**
     * Scanner callback that recursively loops through all of the data point
     * rows. Note that we don't process the actual data points, just the row
     * keys.
     */
    final class MetaScanner implements Callback<Object, 
      ArrayList<ArrayList<KeyValue>>> {
      
      private byte[] last_tsuid = null;
      private String tsuid_string = "";

      /**
       * Fetches the next set of rows from the scanner and adds this class as
       * a callback
       * @return A meaningless deferred to wait on until all data rows have
       * been processed.
       */
      public Object scan() {
        return scanner.nextRows().addCallback(this).addErrback(err_back);
      }

      @Override
      public Object call(ArrayList<ArrayList<KeyValue>> rows)
          throws Exception {
        if (rows == null) {
          result.callback(null);
          return null;
        }
        
        for (final ArrayList<KeyValue> row : rows) {
          try {
            final byte[] tsuid = UniqueId.getTSUIDFromKey(row.get(0).key(), 
                TSDB.metrics_width(), Const.TIMESTAMP_BYTES);
            
            // if the current tsuid is the same as the last, just continue
            // so we save time
            if (last_tsuid != null && Arrays.equals(last_tsuid, tsuid)) {
              continue;
            }
            last_tsuid = tsuid;
            
            // see if we've already processed this tsuid and if so, continue
            if (processed_tsuids.contains(Arrays.hashCode(tsuid))) {
              continue;
            }
            tsuid_string = UniqueId.uidToString(tsuid);
            
            /**
             * An error callback used to catch issues with a particular timeseries
             * or UIDMeta such as a missing UID name. We want to continue
             * processing when this happens so we'll just log the error and
             * the user can issue a command later to clean up orphaned meta
             * entries.
             */
            final class RowErrBack implements Callback<Object, Exception> {
              @Override
              public Object call(Exception e) throws Exception {
                Throwable ex = e;
                while (ex.getClass().equals(DeferredGroupException.class)) {
                  if (ex.getCause() == null) {
                    LOG.warn("Unable to get to the root cause of the DGE");
                    break;
                  }
                  ex = ex.getCause();
                }
                if (ex.getClass().equals(IllegalStateException.class)) {
                  LOG.error("Invalid data when processing TSUID [" + 
                      tsuid_string + "]: " + ex.getMessage());
                } else if (ex.getClass().equals(IllegalArgumentException.class)) {
                  LOG.error("Invalid data when processing TSUID [" + 
                      tsuid_string + "]: " + ex.getMessage());
                } else if (ex.getClass().equals(NoSuchUniqueId.class)) {
                  LOG.warn("Timeseries [" + tsuid_string + 
                      "] includes a non-existant UID: " + ex.getMessage());
                } else {
                  LOG.error("Unknown exception processing row: " + row, ex);
                }
                return null;
              }
            }
            
            // add tsuid to the processed list
            processed_tsuids.add(Arrays.hashCode(tsuid));
            
            // we may have a new TSUID or UIDs, so fetch the timestamp of the 
            // row for use as the "created" time. Depending on speed we could 
            // parse datapoints, but for now the hourly row time is enough
            final long timestamp = Bytes.getUnsignedInt(row.get(0).key(), 
                Const.SALT_WIDTH() + TSDB.metrics_width());
            
            LOG.debug("[" + thread_id + "] Processing TSUID: " + tsuid_string + 
                "  row timestamp: " + timestamp);
            
            // now process the UID metric meta data
            final byte[] metric_uid_bytes = 
              Arrays.copyOfRange(tsuid, 0, TSDB.metrics_width());
            final String metric_uid = UniqueId.uidToString(metric_uid_bytes);
            Long last_get = metric_uids.get(metric_uid);
            
            if (last_get == null || last_get == 0 || timestamp < last_get) {
              // fetch and update. Returns default object if the meta doesn't
              // exist, so we can just call sync on this to create a missing
              // entry
              final UidCB cb = new UidCB(UniqueIdType.METRIC, 
                  metric_uid_bytes, timestamp);
              final Deferred<Boolean> process_uid = UIDMeta.getUIDMeta(tsdb, 
                  UniqueIdType.METRIC, metric_uid_bytes)
                  .addCallbackDeferring(cb)
                  .addErrback(new RowErrBack());
              storage_calls.add(process_uid);
              metric_uids.put(metric_uid, timestamp);
            }
            
            // loop through the tags and process their meta
            final List<byte[]> tags = UniqueId.getTagsFromTSUID(tsuid_string);
            int idx = 0;
            for (byte[] tag : tags) {
              final UniqueIdType type = (idx % 2 == 0) ? UniqueIdType.TAGK : 
                UniqueIdType.TAGV;
              idx++;
              final String uid = UniqueId.uidToString(tag);
              
              // check the maps to see if we need to bother updating
              if (type == UniqueIdType.TAGK) {
                last_get = tagk_uids.get(uid);
              } else {
                last_get = tagv_uids.get(uid);
              }
              if (last_get != null && last_get != 0 && last_get <= timestamp) {
                continue;
              }
  
              // fetch and update. Returns default object if the meta doesn't
              // exist, so we can just call sync on this to create a missing
              // entry
              final UidCB cb = new UidCB(type, tag, timestamp);
              final Deferred<Boolean> process_uid = 
                  UIDMeta.getUIDMeta(tsdb, type, tag)
                  .addCallbackDeferring(cb)
                  .addErrback(new RowErrBack());
              storage_calls.add(process_uid);
              if (type == UniqueIdType.TAGK) {
                tagk_uids.put(uid, timestamp);
              } else {
                tagv_uids.put(uid, timestamp);
              }
            }
            
            // handle the timeseries meta last so we don't record it if one
            // or more of the UIDs had an issue
            final Deferred<Boolean> process_tsmeta = 
              TSMeta.getTSMeta(tsdb, tsuid_string)
                .addCallbackDeferring(new TSMetaCB(tsuid, timestamp))
                .addErrback(new RowErrBack());
            storage_calls.add(process_tsmeta);
          } catch (RuntimeException e) {
            LOG.error("Processing row " + row + " failed with exception: " 
                + e.getMessage());
            LOG.debug("Row: " + row + " stack trace: ", e);
          }
        }
        
        /**
         * A buffering callback used to avoid StackOverflowError exceptions
         * where the list of deferred calls can exceed the limit. Instead we'll
         * process the Scanner's limit in rows, wait for all of the storage
         * calls to complete, then continue on to the next set.
         */
        final class ContinueCB implements Callback<Object, ArrayList<Boolean>> {

          @Override
          public Object call(ArrayList<Boolean> puts)
              throws Exception {
            storage_calls.clear();
            return scan();
          }
          
        }
        
        /**
         * Catch exceptions in one of the grouped calls and continue scanning.
         * Without this the user may not see the exception and the thread will
         * just die silently.
         */
        final class ContinueEB implements Callback<Object, Exception> {
          @Override
          public Object call(Exception e) throws Exception {
            
            Throwable ex = e;
            while (ex.getClass().equals(DeferredGroupException.class)) {
              if (ex.getCause() == null) {
                LOG.warn("Unable to get to the root cause of the DGE");
                break;
              }
              ex = ex.getCause();
            }
            LOG.error("[" + thread_id + "] Upstream Exception: ", ex);
            return scan();
          }
        }
        
        // call ourself again but wait for the current set of storage calls to
        // complete so we don't OOM
        Deferred.group(storage_calls).addCallback(new ContinueCB())
          .addErrback(new ContinueEB());
        return null;
      }
      
    }

    final MetaScanner scanner = new MetaScanner();
    try {
      scanner.scan();
      result.joinUninterruptibly();
      LOG.info("[" + thread_id + "] Complete");
    } catch (Exception e) {
      LOG.error("[" + thread_id + "] Scanner Exception", e);
      throw new RuntimeException("[" + thread_id + "] Scanner exception", e);
    }
  }

}
