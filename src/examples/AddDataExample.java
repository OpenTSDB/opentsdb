// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

/**
 * Examples for how to add points to the tsdb.
 * 
 */
public class AddDataExample {
  private static String pathToConfigFile;
  
  public static void processArgs(String[] args) {
 // Set these as arguments so you don't have to keep path information in
    // source files
    if (args == null) {
      System.err.println("First (and only) argument must be the full path to the opentsdb.conf file. (e.g. /User/thisUser/opentsdb/src/opentsdb.conf");
    } else {
      pathToConfigFile = args[0];
    }
  }

  public static void main(String[] args) throws Exception {

    
    
    processArgs(args);
    
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
    // Note, however, that this is optional. If autometric is enabled,  the UID will be assigned in call to addPoint().
    try {
      byteMetricUID = tsdb.getUID(UniqueIdType.METRIC, metricName);
    } catch (IllegalArgumentException iae) {
      System.out.println("Metric name not valid.");
      iae.printStackTrace();
      System.exit(1);
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

   

    
    // Start timer
    long startTime1 = System.currentTimeMillis();
    
    int n = 100;
    ArrayList<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(n);
    for (int i = 0; i < n; i++) {
      Deferred<Object> deferred = tsdb.addPoint(metricName, timestamp, value + i, tags);
      deferreds.add(deferred);
      
    }
    
    // Add the callbacks to the deferred object. (They might have already
    // returned, btw)
    // This will cause the calling thread to wait until the add has completed.
    
    System.out.println("Waiting for deferred result to return...");
    Deferred.groupInOrder(deferreds)
        .addErrback(new AddDataExample().new errBack())
        .addCallback(new AddDataExample().new succBack())
        .join();
    
    // Block the thread until the deferred returns it's result.
//    deferred.join();
    
    // End timer.
    long elapsedTime1 = System.currentTimeMillis() - startTime1;
    System.out.println("\nAdding " + n + " points took: " + elapsedTime1
        + " milliseconds.\n");


    // Gracefully shutdown connection to TSDB
    tsdb.shutdown();

    
  }

  // This is an optional errorback to handle when there is a failure.
  class errBack implements Callback<String, Exception> {
    public String call(final Exception e) throws Exception {
      String message = ">>>>>>>>>>>Failure!>>>>>>>>>>>";
      System.err.println(message);
      return message;
    }
  };

  // This is an optional success callback to handle when there is a success.
  class succBack implements Callback<Object, ArrayList<Object>> {
    public Object call(ArrayList<Object> results) {
      for (Object res : results) {
        if (res != null) {
          if (res.toString().equals("MultiActionSuccess")) {
            System.err.println(">>>>>>>>>>>Success!>>>>>>>>>>>");
          }
        } else {
          System.err.println(">>>>>>>>>>>" + res.getClass() + ">>>>>>>>>>>");
        }
      }
      return null;
    }
  };
  
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
