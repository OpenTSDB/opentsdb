package net.opentsdb.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.utils.Config;

import com.stumbleupon.async.Deferred;

public class RetryAddPointExample {
	private static String pathToConfigFile;

	public static void processArgs(String[] args) {
		/*
		 * Set these as arguments so you don't have to keep path information in source files
		 */
		if (args.length == 0) {
			System.err
					.println("First (and only) argument must be the full path to the opentsdb.conf file. (e.g. /User/thisUser/opentsdb/src/opentsdb.conf");
			System.exit(1);
		} else {
			pathToConfigFile = args[0];
		}
	}

	public static void main(String[] args) throws Exception {

		processArgs(args);

		/*
		 * Create a config object with a path to the file for parsing. Or
		 * manually override settings. e.g.
		 * config.overrideConfig("tsd.storage.hbase.zk_quorum", "localhost");
		 */
		Config config = new Config(pathToConfigFile);
		final TSDB tsdb = new TSDB(config);

		// Declare new metric
		String metricName = "dummyFromjavaAPI";

		long initValue = 314159;
		Map<String, String> tags = new HashMap<String, String>(1);
		tags.put("dummy-key", "dummy-val1");

		TimeSeriesManager tsm = new TimeSeriesManager(tsdb, false, (short) 50);

		// Start timer
		long startTime1 = System.currentTimeMillis();

		int m = 10;
		int n = 1000;

		ArrayList<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(n * m);
		for (int j = 0; j < m; j++) {
			System.out.println("Saving set: " + j);
			for (int i = 0; i < n; i++) {
				long timestamp = System.currentTimeMillis();
				Deferred<Object> deferred = tsm.addPoint(metricName, tags, timestamp, initValue + i);
				deferreds.add(deferred);
			}
		}
		
		System.out.println("Waiting for deferred result to return...");
		if (deferreds.size() > 1) {
			Deferred.groupInOrder(deferreds).join();
		} else {
			deferreds.get(0).join();
		}

		// End timer.
		long elapsedTime1 = System.currentTimeMillis() - startTime1;
		System.out.println("\nAdding " + n * m + " points took: " + elapsedTime1 + " milliseconds.\n");

		// Gracefully shutdown connection to TSDB
		tsdb.shutdown();

	}

}
