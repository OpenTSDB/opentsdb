// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

package net.opentsdb.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

/**
 * Examples for how to add points to the tsdb.
 * 
 * @author Bryan Hernandez
 * 
 */
public class AddDataExample {

  public static void main(String[] args) throws Exception {

    // Set these as arguments so you don't have to keep path information in
    // source files
    String pathToConfigFile = args[0]; // e.g.
                                       // "/User/thisUser/opentsdb/src/opentsdb.conf"

    // Create a config object with a path to the file for parsing. Or manually
    // override settings.
    // e.g. config.overrideConfig("tsd.storage.hbase.zk_quorum", "localhost");
    Config config = new Config(pathToConfigFile);
    final TSDB tsdb = new TSDB(config);

    // Declare new metric
    String metricName = "dummyFromjavaAPI";
    // First check to see it doesn't already exist
    byte[] byteMetricUID; // we don't actually need this for the first
                          // .addPoint() call below.
    // TODO: Ideally we could just call a not-yet-implemented tsdb.uIdExists()
    // function.
    try {
      byteMetricUID = tsdb.getUID(UniqueIdType.METRIC, metricName);
    } catch (IllegalArgumentException iae) {
      System.out.println("Metric name not valid.");
      iae.printStackTrace();
    } catch (NoSuchUniqueName nsune) {
      // If not, great. Create it.
      byteMetricUID = tsdb.assignUid("metric", metricName);
    }

    // Make a single datum
    long timestamp = System.currentTimeMillis();
    long value = 314159;
    // Make key-val
    Map<String, String> tags = new HashMap<String, String>(1);
    tags.put("dummy-key", "dummy-val1");

    // This is an optional errorback to handle when there is a failure.
    class errBack implements Callback<String, Exception> {
      public String call(final Exception e) throws Exception {
        String message = ">>>>>>>>>>>Failure!>>>>>>>>>>>";
        System.out.println(message);
        return message;
      }
    }
    ;

    // Start timer
    long startTime1 = System.nanoTime();
    // Write datum to tsdb
    Deferred<Object> deferred = tsdb.addPoint(metricName, timestamp, value,
        tags);

    // Add the callbacks to the deferred object. (They might have already
    // returned, btw)
    // This will cause the calling thread to wait until the add has completed.
    deferred.addErrback(new errBack());

    // End timer.
    long elapsedTime1 = (System.nanoTime() - startTime1) / (1000 * 1000);
    System.out.println("\nAdding 1 point took: " + elapsedTime1
        + " milliseconds.\n");

    /*
     * Now let's input a stream of data
     */

    // create an array for storing the deferreds of each addPoint() call
    final int nDataPoints = 100;
    final ArrayList<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(
        nDataPoints);
    // Initialize which time series you're going to write to
    WritableDataPoints wdp = tsdb.newDataPoints();
    wdp.setSeries(metricName, tags); // you can recall this at any time to
                                     // change the ts.
    // Make some data
    Random rand = new Random();
    long[] timestamps1 = makeTimestamps(nDataPoints, rand);
    long[] values1 = makeRandomValues(nDataPoints, rand);
    // Start timer
    long startTime2 = System.nanoTime();
    // Now write to tsdb
    for (int i = 0; i < nDataPoints; i++) {
      // write the data and add the returned Deferred to our ArrayList
      deferreds.add(wdp.addPoint(timestamps1[i], values1[i]));
    }

    try {
      Deferred.groupInOrder(deferreds).addErrback(new errBack())
          .joinUninterruptibly();
    } catch (Exception e) {
      e.printStackTrace();
    }
    // End timer.
    long elapsedTime2 = (System.nanoTime() - startTime2) / (1000 * 1000);
    System.out.println("\nAdding " + nDataPoints
        + " points individually took: " + elapsedTime2 + " milliseconds.\n");

    /*
     * Now try writing in batch import mode
     */
    wdp.setBatchImport(true);
    wdp.setBufferingTime((short) 50);
    // Make some data
    long[] timestamps3 = makeTimestamps(nDataPoints, rand);
    long[] values3 = makeRandomValues(nDataPoints, rand);
    // Start timer
    long startTime3 = System.nanoTime();
    // Now write to tsdb
    for (int i = 0; i < nDataPoints; i++) {
      // write the data and add the returned Deferred to our ArrayList
      deferreds.add(wdp.addPoint(timestamps3[i], values3[i]));
    }

    try {
      Deferred.groupInOrder(deferreds).addErrback(new errBack())
          .joinUninterruptibly();
    } catch (Exception e) {
      e.printStackTrace();
    }
    // End timer.
    long elapsedTime3 = (System.nanoTime() - startTime3) / (1000 * 1000);
    System.out.println("\nAdding " + nDataPoints
        + " points in batch mode took: " + elapsedTime3 + " milliseconds.\n");
    
    
    /*
     * Writing in batch import mode with reinstantiation of WriteableDataPoints connection
     * This is needed if you want to write datapoints with the same time to the same metrics but
     * that have different tags.  
     */
    
    // Make some data
    long timestamp4 = System.currentTimeMillis(); // one time
    long[] values4 = makeRandomValues(nDataPoints, rand);
    // Start timer
    long startTime4 = System.nanoTime();
    // Now write to tsdb
    for (int i = 0; i < nDataPoints; i++) {
      WritableDataPoints wdp2 = tsdb.newDataPoints();
      wdp2.setBatchImport(true);
      wdp2.setBufferingTime((short) 50);
      Map<String, String> tags2 = new HashMap<String, String>(1);
      tags2.put("some-tag", "version-" + i);
      wdp2.setSeries(metricName, tags2);
      // write the data and add the returned Deferred to our ArrayList
      deferreds.add(wdp2.addPoint(timestamp4, values3[i]));
    }

    try {
      Deferred.groupInOrder(deferreds).addErrback(new errBack())
          .joinUninterruptibly();
    } catch (Exception e) {
      e.printStackTrace();
    }
    // End timer.
    long elapsedTime4 = (System.nanoTime() - startTime3) / (1000 * 1000);
    System.out.println("\nAdding " + nDataPoints
        + " points in batch mode with WDP reinstantiation took: " + elapsedTime4 + " milliseconds.\n");
    

    // Gracefully shutdown connection to TSDB
    tsdb.shutdown();

  }

  public static long[] makeRandomValues(int num, Random rand) {
    long[] values = new long[num];
    for (int i = 0; i < num; i++) {
      // define datum
      values[i] = rand.nextInt();
    }
    return values;
  }

  public static long[] makeTimestamps(int num, Random rand) {

    long[] timestamps = new long[num];
    for (int i = 0; i < num; i++) {
      // ensures that we won't try to put two data points in the same
      // millisecond
      try {
        Thread.sleep(1); // in millis
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      // define datum
      timestamps[i] = System.currentTimeMillis();
    }
    return timestamps;
  }

}
