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
import java.util.HashMap;

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
 * Tool helper class that removes known UIDs from in-memory structure.
 * This class should only be used by CLI tools as it can take a long
 * time to complete.
 */
final class UidGarbageCollector extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(UidGarbageCollector.class);
  
  /** TSDB to use for storage access */
  final TSDB tsdb;

  /** Map type of UID to class containing it. */
  final HashMap<String,Uids> name2uids;

  /** Diagnostic ID for this thread */
  final int thread_id;
  
  /** The scanner for this worker */
  final Scanner scanner;
  
  /**
   * Constructor that sets local variables
   * @param tsdb The TSDB to process with
   * @param scanner The scanner to use for this worker
   * @param thread_id The ID of this thread (starts at 0)
   * @param name2uids The currently known UIDs.
   */
  public UidGarbageCollector(final TSDB tsdb,
                             final Scanner scanner,
                             final int thread_id,
                             HashMap<String,Uids> name2uids
                             ) {
    this.tsdb = tsdb;
    this.scanner = scanner;
    this.thread_id = thread_id;
    this.name2uids = name2uids;
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

        final Uids metricsUids = name2uids.get(CliUtils.METRICS);
        final Uids tagkUids = name2uids.get(CliUtils.TAGK);
        final Uids tagvUids = name2uids.get(CliUtils.TAGV);

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

            LOG.debug("[" + thread_id + "] Processing TSUID: " + tsuid_string);

            // now mark the UIDs as present:
            final byte[] metric_uid_bytes = 
              Arrays.copyOfRange(tsuid, 0, TSDB.metrics_width());
            final String metric_uid = UniqueId.uidToString(metric_uid_bytes);
            metricsUids.localRemoveUid(metric_uid);

            // loop through the tags and mark them as present too:
            final List<byte[]> tags = UniqueId.getTagsFromTSUID(tsuid_string);
            int idx = 0;
            for (byte[] tag : tags) {
              final UniqueIdType type = (idx % 2 == 0) ? UniqueIdType.TAGK : 
                UniqueIdType.TAGV;
              idx++;
              final String uid = UniqueId.uidToString(tag);
              if (type == UniqueIdType.TAGK) {
                tagkUids.localRemoveUid(uid);
              } else {
                tagvUids.localRemoveUid(uid);
              }
            }
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
  }

}
