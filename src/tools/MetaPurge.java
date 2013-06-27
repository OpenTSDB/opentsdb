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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Tool helper class used to delete all TSMeta and UIDMeta entries from the
 * UID table. 
 * <b>Note:</b> After you execute this, you may want to perform a "flush" on
 * the UID table in HBase so that the data doesn't mysteriously come back.
 */
final class MetaPurge extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(MetaPurge.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** Name of the CF where trees and branches are stored */
  private static final byte[] NAME_FAMILY = "name".getBytes(CHARSET);
  
  /** TSDB to use for storage access */
  private final TSDB tsdb;

  /** Number of columns deleted */
  private long columns;
  
  /** The ID to start the sync with for this thread */
  final long start_id;
  
  /** The end of the ID block to work on */
  final long end_id;
  
  /** Diagnostic ID for this thread */
  final int thread_id;
  
  /**
   * Constructor that sets local variables
   * @param tsdb The TSDB to process with
   * @param start_id The starting ID of the block we'll work on
   * @param quotient The total number of IDs in our block
   * @param thread_id The ID of this thread (starts at 0)
   */
  public MetaPurge(final TSDB tsdb, final long start_id, final double quotient, 
      final int thread_id) {
    this.tsdb = tsdb;
    this.start_id = start_id;
    this.end_id = start_id + (long) quotient + 1; // teensy bit of overlap
    this.thread_id = thread_id;
  }
  
  /**
   * Loops through the entire tsdb-uid table and exits when complete.
   */
  public void run() {
    long purged_columns;
    try {
      purged_columns = purge().joinUninterruptibly();
      LOG.info("Thread [" + thread_id + "] finished. Purged [" + purged_columns + "] columns from storage");
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
    }
    
  }
  
  /**
   * Scans the entire UID table and removes any TSMeta or UIDMeta objects
   * found.
   * @return The total number of columns deleted
   */
  public Deferred<Long> purge() {
    
    // a list to store all pending deletes so we don't exit before they've 
    // completed
    final ArrayList<Deferred<Object>> delete_calls = 
      new ArrayList<Deferred<Object>>();
    final Deferred<Long> result = new Deferred<Long>();
    
    /**
     * Scanner callback that will recursively call itself and loop through the
     * rows of the UID table, issuing delete requests for all of the columns in
     * a row that match a meta qualifier.
     */
    final class MetaScanner implements Callback<Deferred<Long>, 
      ArrayList<ArrayList<KeyValue>>> {

      final Scanner scanner;
      
      public MetaScanner() {
        scanner = getScanner();
      }
      
      /**
       * Fetches the next group of rows from the scanner and sets this class as
       * a callback
       * @return The total number of columns deleted after completion
       */
      public Deferred<Long> scan() {
        return scanner.nextRows().addCallbackDeferring(this);
      }
      
      @Override
      public Deferred<Long> call(ArrayList<ArrayList<KeyValue>> rows)
          throws Exception {
        if (rows == null) {
          result.callback(columns);
          return null;
        }
        
        for (final ArrayList<KeyValue> row : rows) {
          // one delete request per row. We'll almost always delete the whole
          // row, so preallocate some ram.
          ArrayList<byte[]> qualifiers = new ArrayList<byte[]>(row.size());
          
          for (KeyValue column : row) {
            if (Bytes.equals(TSMeta.META_QUALIFIER(), column.qualifier())) {
              qualifiers.add(column.qualifier());
            } else if (Bytes.equals(TSMeta.COUNTER_QUALIFIER(), 
                column.qualifier())) {
              qualifiers.add(column.qualifier());
            } else if (Bytes.equals("metric_meta".getBytes(CHARSET), 
                column.qualifier())) {
              qualifiers.add(column.qualifier());
            } else if (Bytes.equals("tagk_meta".getBytes(CHARSET), 
                column.qualifier())) {
              qualifiers.add(column.qualifier());
            } else if (Bytes.equals("tagv_meta".getBytes(CHARSET), 
                column.qualifier())) {
              qualifiers.add(column.qualifier());
            }
          }
          
          if (qualifiers.size() > 0) {
            columns += qualifiers.size();
            final DeleteRequest delete = new DeleteRequest(tsdb.uidTable(), 
                row.get(0).key(), NAME_FAMILY, 
                qualifiers.toArray(new byte[qualifiers.size()][]));
            delete_calls.add(tsdb.getClient().delete(delete));
          }
        }
        
        /**
         * Buffer callback used to wait on all of the delete calls for the
         * last set of rows returned from the scanner so we don't fill up the
         * deferreds array and OOM out.
         */
        final class ContinueCB implements Callback<Deferred<Long>, 
          ArrayList<Object>> {

          @Override
          public Deferred<Long> call(ArrayList<Object> deletes)
              throws Exception {
            LOG.debug("[" + thread_id + "] Processed [" + deletes.size() 
                + "] delete calls");
            delete_calls.clear();
            return scan();
          }
          
        }
        
        // fetch the next set of rows after waiting for current set of delete
        // requests to complete
        Deferred.group(delete_calls).addCallbackDeferring(new ContinueCB());
        return null;
      }
      
    }
    
    // start the scan
    new MetaScanner().scan();
    return result;
  }
  
  /**
   * Returns a scanner to run over the UID table starting at the given row
   * @return A scanner configured for the entire table
   * @throws HBaseException if something goes boom
   */
  private Scanner getScanner() throws HBaseException {
    short metric_width = TSDB.metrics_width();
    final byte[] start_row = 
      Arrays.copyOfRange(Bytes.fromLong(start_id), 8 - metric_width, 8);
    final byte[] end_row = 
      Arrays.copyOfRange(Bytes.fromLong(end_id), 8 - metric_width, 8);
    final Scanner scanner = tsdb.getClient().newScanner(tsdb.uidTable());
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily(NAME_FAMILY);
    return scanner;
  }
}
