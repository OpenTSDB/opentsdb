package net.opentsdb.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.Internal;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.HBaseRpc;
import org.hbase.async.KeyValue;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Tool to migrate the data from one tsdb instance to another.
 */
public class MigrateSeries {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateSeries.class);
 
  /** Function used to convert a String to a byte[]. */
  private static final Method toBytes;
  /** Function used to convert a byte[] to a String. */
  private static final Method fromBytes;
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET;
  /** The single column family used by this class. */
  private static final byte[] ID_FAMILY;
  /** The single column family used by this class. */
  private static final byte[] NAME_FAMILY;
  /** Row key of the special row used to track the max ID already assigned. */
  private static final byte[] MAXID_ROW;

  static {
    final Class<UniqueId> uidclass = UniqueId.class;
    try {
      // Those are all implementation details so they're not part of the
      // interface.  We access them anyway using reflection.  I think this
      // is better than marking those public and adding a javadoc comment
      // "THIS IS INTERNAL DO NOT USE".  If only Java had C++'s "friend" or
      // a less stupid notion of a package.
      Field f;
      f = uidclass.getDeclaredField("CHARSET");
      f.setAccessible(true);
      CHARSET = (Charset) f.get(null);
      f = uidclass.getDeclaredField("ID_FAMILY");
      f.setAccessible(true);
      ID_FAMILY = (byte[]) f.get(null);
      f = uidclass.getDeclaredField("NAME_FAMILY");
      f.setAccessible(true);
      NAME_FAMILY = (byte[]) f.get(null);
      f = uidclass.getDeclaredField("MAXID_ROW");
      f.setAccessible(true);
      MAXID_ROW = (byte[]) f.get(null);
      toBytes = uidclass.getDeclaredMethod("toBytes", String.class);
      toBytes.setAccessible(true);
      fromBytes = uidclass.getDeclaredMethod("fromBytes", byte[].class);
      fromBytes.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("static initializer failed", e);
    }
  }  

  /** Prints usage and exits with the given retval. */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: migrate "
        + "START-DATE END-DATE aggregator metric_pattern [excluded_metrics_list]\n"
        + "Migrates the metric for a specific set of dates to another TSDB instance.");
    System.err.print(argp.usage());
    System.exit(retval);
  }
  
  /**
   * @param args Arguments.
   * @throws Exception Generic Exception
   */
  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    String[] myArgs = args;
    CliOptions.addCommon(argp);
    CliOptions.addAutoMetricFlag(argp);
    argp.addOption("--src-zkquorum", "Sets the source Zookeeper quorum");
    argp.addOption("--src-uidtable", "Source UID table");
    argp.addOption("--src-table", "Source data table");
    myArgs = CliOptions.parse(argp, myArgs);
    if (myArgs == null) {
      usage(argp, "Invalid usage.", 1);
    } else if (myArgs.length < 4) {
      usage(argp, "Not enough arguments.", 2);
    }

    final String srcTable = argp.get("--src-table", "src-tsdb");
    final String dstTable = argp.get("--table", "tsdb");
    
    final String srcUidTable = argp.get("--src-uidtable", "src-tsdb-uid");
    final String dstUidTable = argp.get("--uidtable", "tsdb-uid");
    
    final String srcZk = argp.get("--src-zkquorum","localhost");
    final String dstZk = argp.get("--zkquorum","localhost");
    
    if (argp.optionExists("--auto-metric") && argp.has("--auto-metric")) {
      System.setProperty("tsd.core.auto_create_metrics", "true");
    }
    
    HBaseClient srcClient = null;
    HBaseClient dstClient = null;
    
    try {
      srcClient = new HBaseClient(srcZk);
    } catch (Exception e) {
      System.out.println("Couldn't create source hbase client");
      System.out.println(e.getStackTrace());
      System.exit(255);
    }
    
    try {
      dstClient = new HBaseClient(dstZk);  
    } catch (Exception e) {
      System.out.println("Couldn't create destination hbase client");
      System.out.println(e.getStackTrace());
      System.exit(255);      
    }
    
    argp = null;
    try {
      doMigration(srcClient,dstClient,srcUidTable,srcTable,dstUidTable,dstTable,myArgs);
      LOG.info("Migration complete.");
    } finally {
      if (srcClient != null) {
        srcClient.shutdown().joinUninterruptibly();
      }
      if (dstClient != null) {
        dstClient.shutdown().joinUninterruptibly();
      }
    }
  }

  private static void doMigration (HBaseClient srcClient, HBaseClient dstClient, 
                              String srcUid, String srcData,
                              String dstUid, String dstData, String[] args) {

    String agg = null;
    String metric = null;
    String excludedMetrics = null;
    ArrayList<String> em = null;
    String start_date = "1970/01/01-00:00";
    String end_date = "2099/12/31-23:59";
    
    if (args.length <= 4) {
      start_date = args[0];
      end_date = args[1];
      agg = args[2];
      metric = args[3];
    } else {
      start_date = args[0];
      end_date = args[1];
      agg = args[2];
      metric = args[3];
      excludedMetrics = args[4];
    }
    
    LOG.info("Got the following parameters: start_date=" + start_date + " end_date=" + end_date + " metric=" + metric + " excludedMetrics='" + excludedMetrics + "'");
    
    if (excludedMetrics != null && excludedMetrics.contains(",")) {
      em = new ArrayList<String>(Arrays.asList(excludedMetrics.split(",")));
      LOG.info("Exclusion list contains " + em.size() + " elements.");
      LOG.info("Exclusion list is: " + em.toString());
    }
    
    final HashMap<String,byte[]> srcMetricsList = readUidList(srcClient,srcUid,"metrics",metric);
    LOG.info("We read " + String.valueOf(srcMetricsList.size()) + " metrics from the source cluster.");
    ArrayList<String> metricsList = new ArrayList<String>();

    LOG.debug("*****************************************************************");
    metric_loop:
    for (final String metricItem : srcMetricsList.keySet()) {
      if (em != null) {
        for (final String emi : em) {
          if (metricItem.matches("^" + emi + ".*")) {
            continue metric_loop;
          }
        }
      }
      metricsList.add(metricItem);
      LOG.debug("Would parse the following metric: " + metricItem); 
    }
    srcMetricsList.clear();
    LOG.debug("*****************************************************************");
    LOG.info("We will process " + metricsList.size() + " metrics.");
    Collections.sort(metricsList);
    
    for (final String metricItem : metricsList) {
      final String[] computedArgs = {start_date,end_date,agg,metricItem};
      try {
        if (computedArgs[3].length() > 1) {
          LOG.info("Issuing the following query: " + start_date + " " + end_date + " " + agg + " " + metricItem);
          convertTsdData(srcClient,dstClient,srcUid,srcData,dstUid,dstData,computedArgs);
        }
      } catch (Exception e) {
        LOG.warn(e.getMessage());
        System.exit(255);
      }
    }
  }
  
  private static void convertTsdData (HBaseClient srcClient, HBaseClient dstClient, 
      String srcUid, String srcData,
      String dstUid, String dstData, String[] args) throws Exception {
    final TSDB tsdb = new TSDB(srcClient, srcData,srcUid);
    final TSDB dstTsdb = new TSDB(dstClient,dstData,dstUid);
    final ArrayList<Query> queries = new ArrayList<Query>();
    CliQuery.parseCommandLineQuery(args, tsdb, queries, null, null);

    for (final Query query : queries) {
      final Scanner scanner = Internal.getScanner(query);
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          final byte[] key = row.get(0).key();
          final long base_time = Internal.baseTime(tsdb, key);
          final String metric = Internal.metricName(tsdb, key);

          for (final KeyValue kv : row) {
            parseInsertKeyValue(dstClient, tsdb, dstTsdb, kv, base_time, metric);
          }
        }
      }
    }
  }

  static volatile boolean throttle = false;
  
  private static void parseInsertKeyValue(final HBaseClient client,
              final TSDB srcTsdb,
              final TSDB dstTsdb,
              final KeyValue kv,
              final long base_time,
              final String metric) {
    int points = 0;
    final long start_time = System.nanoTime();
    long ping_start_time = start_time;
    final class Errback implements Callback<Object, Exception> {
      private final Logger ZELOG = LoggerFactory.getLogger(Errback.class);
      @Override
      public Object call(final Exception arg) {
        if (arg instanceof PleaseThrottleException) {
          final PleaseThrottleException e = (PleaseThrottleException) arg;
          ZELOG.warn("parseInsertKeyValue: Need to throttle, HBase isn't keeping up.", e);
          throttle = true;
          final HBaseRpc rpc = e.getFailedRpc();
          if (rpc instanceof PutRequest) {
            client.put((PutRequest) rpc);  // Don't lose edits.
          }
          return null;
        }
        ZELOG.error("parseInsertKeyValue: Exception caught while processing input" + arg);
        System.exit(2);
        return arg;
      }
      @Override
      public String toString() {
        return "parseInsertKeyValue errback";
      }
    }
    final byte[] qualifier = kv.qualifier();
    final byte[] cell = kv.value();
    if (qualifier.length != 2 && cell[cell.length - 1] != 0) {
      throw new IllegalDataException("Don't know how to read this value:"
          + Arrays.toString(cell) + " found in " + kv
          + " -- this compacted value might have been written by a future"
          + " version of OpenTSDB, or could be corrupt.");
    }
    final int nvalues = qualifier.length / 2;

    final HashMap<String,String> tags = new HashMap<String,String>(Internal.getTags(srcTsdb, kv.key()));

    int value_offset = 0;
    final Errback errback = new Errback();
    for (int i = 0; i < nvalues; i++) {
      final short qual = Bytes.getShort(qualifier, i * 2);
      final byte flags = (byte) qual;
      final int value_len = (flags & 0x7) + 1;
      final short delta = (short) ((0x0000FFFF & qual) >>> 4);
      final long timestamp = base_time + delta;
      final WritableDataPoints dp = getDataPoints(dstTsdb, metric, tags);
      Deferred<Object> d;
      if ((qual & 0x8) == 0x8) {
        if (cell.length == 8 && value_len == 4
            && cell[0] == 0 && cell[1] == 0 && cell[2] == 0 && cell[3] == 0) {
          // Incorrect encoded floating point value.
          // See CompactionQueue.fixFloatingPointValue() for more details.
          value_offset += 4;
        }
        d = dp.addPoint(timestamp, (float)Internal.extractFloatingPointValue(cell, value_offset, flags));
      } else {
        d = dp.addPoint(timestamp, Internal.extractIntegerValue(cell, value_offset, flags));
      }
      value_offset += value_len;
      d.addErrback(errback);
      points++;
      if (points % 1000000 == 0) {
        final long now = System.nanoTime();
        ping_start_time = (now - ping_start_time) / 1000000;
        LOG.info(String.format("... %d data points in %dms (%.1f points/s)",
                               points, ping_start_time,
                               (1000000 * 1000.0 / ping_start_time)));
        ping_start_time = now;
      }
      if (throttle) {
        LOG.info("Throttling...");
        long throttle_time = System.nanoTime();
        try {
          d.joinUninterruptibly();
        } catch (Exception e) {
          throw new RuntimeException("Should never happen", e);
        }
        throttle_time = System.nanoTime() - throttle_time;
        if (throttle_time < 1000000000L) {
          LOG.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
          try { Thread.sleep(1000); } catch (InterruptedException e) { throw new RuntimeException("interrupted", e); }
        }
        LOG.info("Done throttling...");
        throttle = false;
      }
    }
  }

  /** Transforms a UNIX timestamp into a human readable date.  */
  static String date(final long timestamp) {
    return new Date(timestamp * 1000).toString();
  }

  private static HashMap<String,byte[]> readUidList (final HBaseClient client, final String uidTable, final String type, final String metric) {
    final int maxItems = 1024;
    final Scanner scanner = client.newScanner(uidTable);
    final HashMap<String,byte[]> results = new HashMap<String,byte[]>(maxItems);
    scanner.setMaxNumRows(maxItems);
    scanner.setQualifier(toBytes(type));
    String regexp = metric + "(?i)";
    scanner.setFamily(ID_FAMILY);
    scanner.setKeyRegexp(regexp, CHARSET);
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          for (final KeyValue kv : row) {
            results.put(fromBytes(kv.key()),kv.value());
          }
        }
      }
    } catch (HBaseException e) {
      LOG.error("Error while scanning HBase, scanner=" + scanner, e);
      throw e;
    } catch (Exception e) {
      LOG.error("WTF?  Unexpected exception type, scanner=" + scanner, e);
      throw new AssertionError("Should never happen");
    }
    return results;
  }

  private static byte[] toBytes(final String s) {
    try {
      return (byte[]) toBytes.invoke(null, s);
    } catch (Exception e) {
      throw new RuntimeException("toBytes=" + toBytes, e);
    }
  }

  private static String fromBytes(final byte[] b) {
    try {
      return (String) fromBytes.invoke(null, b);
    } catch (Exception e) {
      throw new RuntimeException("fromBytes=" + fromBytes, e);
    }
  }

  private static final HashMap<String, WritableDataPoints> datapoints =
      new HashMap<String, WritableDataPoints>();

  private static
    WritableDataPoints getDataPoints(final TSDB tsdb,
                                     final String metric,
                                     final HashMap<String, String> tags) {
    final String key = metric + tags;
    WritableDataPoints dp = datapoints.get(key);
    if (dp != null) {
      return dp;
    }
    dp = tsdb.newDataPoints();
    dp.setSeries(metric, tags);
    dp.setBatchImport(true);
    datapoints.put(key, dp);
    return dp;
  }
}
