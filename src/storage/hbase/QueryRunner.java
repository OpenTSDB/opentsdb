package net.opentsdb.storage.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.opentsdb.core.AsyncIterator;
import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.Span;
import net.opentsdb.core.TsdbQuery;
import net.opentsdb.uid.UniqueId;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyRegexpFilter;
import org.hbase.async.KeyValue;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryRunner implements AsyncIterator<DataPoints> {
  private static final Logger LOG = LoggerFactory.getLogger(QueryRunner.class);

  private final TsdbQuery tsdb_query;
  private final HBaseClient client;

  private final Scanner scanner;

  private final ConcurrentLinkedQueue<DataPoints> result_rows;
  private final Iterator<DataPoints> result_iterator;

  public QueryRunner(final TsdbQuery tsdb_query,
                     final HBaseClient client,
                     final byte[] table,
                     final byte[] family) {
    this.tsdb_query = checkNotNull(tsdb_query);
    this.client = checkNotNull(client);

    this.result_rows = Queues.newConcurrentLinkedQueue();
    this.result_iterator = result_rows.iterator();

    scanner = getScannerForQuery(tsdb_query, checkNotNull(table),
            checkNotNull(family));

    scanner.nextRows().addCallback(new ScannerCB());
  }

  @Override
  public Deferred<Boolean> hasMore() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public DataPoints next() {
    return result_iterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("#remove() is not supported by QueryRunner");
  }

  /**
   * Returns a scanner set for the given metric (from {@link #metric} or from
   * the first TSUID in the {@link #tsuids}s list. If one or more tags are
   * provided, it calls into {@link #buildMetricFilter} to setup a row key
   * filter. If one or more TSUIDs have been provided, it calls into
   * {@link #buildTSUIDFilter} to setup a row key filter.
   * @return A scanner to use for fetching data points
   * @param tsdbQuery
   * @param table
   * @param family
   */
  private Scanner getScannerForQuery(final TsdbQuery tsdbQuery,
                                     final byte[] table,
                                     final byte[] family) {
    final byte[] start_row = buildRowKey(tsdbQuery.getMetric(),
            getScanStartTimeSeconds(tsdbQuery));
    final byte[] end_row = buildRowKey(tsdbQuery.getMetric(),
            getScanEndTimeSeconds(tsdbQuery));

    final Scanner scanner = client.newScanner(table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily(family);
    scanner.setFilter(buildRowKeyFilter(tsdbQuery));

    return scanner;
  }

  private byte[] buildRowKey(final byte[] metric_id, final int timestamp) {
    final byte[] row = new byte[Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES];

    Bytes.setInt(row, timestamp, Const.METRICS_WIDTH);
    System.arraycopy(metric_id, 0, row, 0, Const.METRICS_WIDTH);

    return row;
  }

  /** Returns the UNIX timestamp from which we must start scanning.
   * @param tsdbQuery*/
  private int getScanStartTimeSeconds(TsdbQuery tsdbQuery) {
    // The reason we look before by `MAX_TIMESPAN * 2' seconds is because of
    // the following.  Let's assume MAX_TIMESPAN = 600 (10 minutes) and the
    // start_time = ... 12:31:00.  If we initialize the scanner to look
    // only 10 minutes before, we'll start scanning at time=12:21, which will
    // give us the row that starts at 12:30 (remember: rows are always aligned
    // on MAX_TIMESPAN boundaries -- so in this example, on 10m boundaries).
    // But we need to start scanning at least 1 row before, so we actually
    // look back by twice MAX_TIMESPAN.  Only when start_time is aligned on a
    // MAX_TIMESPAN boundary then we'll mistakenly scan back by an extra row,
    // but this doesn't really matter.
    // Additionally, in case our sample_interval_ms is large, we need to look
    // even further before/after, so use that too.
    long start = tsdbQuery.getStartTimeSeconds();
    final long ts = start - Const.MAX_TIMESPAN * 2 - tsdbQuery.getSampleInterval() / 1000;
    return ts > 0 ? (int) ts : 0;
  }

  /** Returns the UNIX timestamp at which we must stop scanning.
   * @param tsdbQuery*/
  private int getScanEndTimeSeconds(TsdbQuery tsdbQuery) {
    // For the end_time, we have a different problem.  For instance if our
    // end_time = ... 12:30:00, we'll stop scanning when we get to 12:40, but
    // once again we wanna try to look ahead one more row, so to avoid this
    // problem we always add 1 second to the end_time.  Only when the end_time
    // is of the form HH:59:59 then we will scan ahead an extra row, but once
    // again that doesn't really matter.
    // Additionally, in case our sample_interval_ms is large, we need to look
    // even further before/after, so use that too.
    Optional<Long> end = tsdbQuery.getEndTimeSeconds();

    if (end.isPresent()) {
      return (int) (end.get() + Const.MAX_TIMESPAN + 1 +
              tsdbQuery.getSampleInterval() / 1000);
    }

    // Returning -1 will make us scan to the end (0xFFF...)
    return -1;
  }

  private ScanFilter buildRowKeyFilter(final TsdbQuery tsdbQuery) {
    if (tsdbQuery.getTSUIDS() != null && !tsdbQuery.getTSUIDS().isEmpty()) {
      return buildTSUIDFilter(tsdbQuery);
    } else if (!tsdbQuery.getTags().isEmpty() || tsdbQuery.getGroupBys() != null) {
      return buildMetricFilter(tsdbQuery);
    }

    // TODO(luuse): Is it even possible to get here, not how can we rewrite
    // the above conditional so we don't have to throw this?
    throw new IllegalStateException("Missing both metric and tsuids " +
            "information so unable to build a row key filter. How the hell " +
            "did we get here?!");
  }

  /**
   * Sets the server-side regexp filter on the scanner.
   * This will compile a list of the tagk/v pairs for the TSUIDs to prevent
   * storage from returning irrelevant rows.
   * @param tsdbQuery The query that describes what parameters to build a
   *                  filter for
   */
  private KeyRegexpFilter buildTSUIDFilter(final TsdbQuery tsdbQuery) {
    List<String> tsuids = tsdbQuery.getTSUIDS();
    Collections.sort(tsuids);

    // first, convert the tags to byte arrays and count up the total length
    // so we can allocate the string builder
    final short metric_width = Const.METRICS_WIDTH;
    int tags_length = 0;

    final ArrayList<byte[]> tag_uids = new ArrayList<byte[]>(tsuids.size());
    for (final String tsuid : tsuids) {
      final String tags = tsuid.substring(metric_width * 2);
      final byte[] tag_bytes = UniqueId.stringToUid(tags);
      tags_length += tag_bytes.length;
      tag_uids.add(tag_bytes);
    }

    // Generate a regexp for our tags based on any metric and timestamp (since
    // those are handled by the row start/stop) and the list of TSUID tagk/v
    // pairs. The generated regex will look like: ^.{7}(tags|tags|tags)$
    // where each "tags" is similar to \\Q\000\000\001\000\000\002\\E
    final StringBuilder buf = new StringBuilder(
            13  // "(?s)^.{N}(" + ")$"
                    + (tsuids.size() * 11) // "\\Q" + "\\E|"
                    + tags_length); // total # of bytes in tsuids tagk/v pairs

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)")  // Ensure we use the DOTALL flag.
       .append("^.{")
       // ... start by skipping the metric ID and timestamp.
       .append(Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES)
       .append("}(");

    for (final byte[] tags : tag_uids) {
      // quote the bytes
      buf.append("\\Q");
      addId(buf, tags);
      buf.append('|');
    }

    // Replace the pipe of the last iteration, close and set
    buf.setCharAt(buf.length() - 1, ')');
    buf.append("$");

    return new KeyRegexpFilter(buf.toString(), HBaseConst.CHARSET);
  }

  /**
   * Sets the server-side regexp filter on the scanner.
   * In order to find the rows with the relevant tags, we use a
   * server-side filter that matches a regular expression on the row key.
   * @param tsdbQuery The query that describes what parameters to build a
   *                  filter for
   */
  private KeyRegexpFilter buildMetricFilter(final TsdbQuery tsdbQuery) {
    List<byte[]> group_bys1 = tsdbQuery.getGroupBys();
    ArrayList<byte[]> tags1 = tsdbQuery.getTags();
    Map<byte[], ArrayList<byte[]>> groupByValues = tsdbQuery.getGroupByValues();

    if (group_bys1 != null) {
      Collections.sort(group_bys1, Bytes.MEMCMP);
    }

    final short name_width = Const.TAG_NAME_WIDTH;
    final short value_width = Const.TAG_VALUE_WIDTH;
    final short tagsize = (short) (name_width + value_width);
    // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
    // and { 4 5 6 9 8 7 }, the regexp will be:
    // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
    final StringBuilder buf = new StringBuilder(
            15  // "^.{N}" + "(?:.{M})*" + "$"
                    + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
                    * (tags1.size() + (group_bys1 == null ? 0 : group_bys1.size() * 3))));
    // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)")  // Ensure we use the DOTALL flag.
       .append("^.{")
       // ... start by skipping the metric ID and timestamp.
       .append(Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES)
       .append("}");

    final Iterator<byte[]> tags = tags1.iterator();
    final Iterator<byte[]> group_bys = (group_bys1 == null
            ? new ArrayList<byte[]>(0).iterator()
            : group_bys1.iterator());
    byte[] tag = tags.hasNext() ? tags.next() : null;
    byte[] group_by = group_bys.hasNext() ? group_bys.next() : null;
    // Tags and group_bys are already sorted.  We need to put them in the
    // regexp in order by ID, which means we just merge two sorted lists.
    do {
      // Skip any number of tags.
      buf.append("(?:.{")
         .append(tagsize)
         .append("})*\\Q");

      if (isTagNext(name_width, tag, group_by)) {
        addId(buf, tag);
        tag = tags.hasNext() ? tags.next() : null;
      } else {  // Add a group_by.
        addId(buf, group_by);
        final ArrayList<byte[]> value_ids = (groupByValues == null
                ? null
                : groupByValues.get(group_by));
        if (value_ids == null) {  // We don't want any specific ID...
          buf.append(".{").append(value_width).append('}');  // Any value ID.
        } else {  // We want specific IDs.  List them: /(AAA|BBB|CCC|..)/
          buf.append("(?:");
          for (final byte[] value_id : value_ids) {
            buf.append("\\Q");
            addId(buf, value_id);
            buf.append('|');
          }
          // Replace the pipe of the last iteration.
          buf.setCharAt(buf.length() - 1, ')');
        }
        group_by = group_bys.hasNext() ? group_bys.next() : null;
      }
    } while (tag != group_by);  // Stop when they both become null.

    // Skip any number of tags before the end.
    buf.append("(?:.{").append(tagsize).append("})*$");

    return new KeyRegexpFilter(buf.toString(), HBaseConst.CHARSET);
  }

  /**
   * Appends the given ID to the given buffer, followed by "\\E".
   */
  private static void addId(final StringBuilder buf, final byte[] id) {
    boolean backslash = false;
    for (final byte b : id) {
      buf.append((char) (b & 0xFF));
      if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
        // So we just terminated the quoted section because we just added \E
        // to `buf'.  So let's put a litteral \E now and start quoting again.
        buf.append("\\\\E\\Q");
      } else {
        backslash = b == '\\';
      }
    }
    buf.append("\\E");
  }

  /**
   * Helper comparison function to compare tag name IDs.
   * @param name_width Number of bytes used by a tag name ID.
   * @param tag A tag (array containing a tag name ID and a tag value ID).
   * @param group_by A tag name ID.
   * @return {@code true} number if {@code tag} should be used next (because
   * it contains a smaller ID), {@code false} otherwise.
   */
  private boolean isTagNext(final short name_width,
                            final byte[] tag,
                            final byte[] group_by) {
    if (tag == null) {
      return false;
    } else if (group_by == null) {
      return true;
    }
    final int cmp = Bytes.memcmp(tag, group_by, 0, name_width);
    if (cmp == 0) {
      throw new AssertionError("invariant violation: tag ID "
              + Arrays.toString(group_by) + " is both in 'tags' and"
              + " 'group_bys' in " + this);
    }
    return cmp < 0;
  }

  private class ScannerCB implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
    int nrows = 0;
    boolean seenAnnotation = false;
    int hbase_time = 0; // milliseconds.
    long starttime = System.nanoTime();

    @Override
    public Object call(final ArrayList<ArrayList<KeyValue>> rows) {
      hbase_time += (System.nanoTime() - starttime) / 1000000;
      try {
        if (rows == null) {
          hbase_time += (System.nanoTime() - starttime) / 1000000;
          TsdbQuery.scanlatency.add(hbase_time);
          LOG.info("{} matched {} rows in {} spans in {}ms",
                  tsdbQuery, nrows, spans.size(), hbase_time);
          if (nrows < 1 && !seenAnnotation) {
            results.callback(null);
          } else {
            results.callback(spans);
          }
          scanner.close();
          return null;
        }

        final byte[] metric = tsdb_query.getMetric();

        for (final ArrayList<KeyValue> row : rows) {
          final byte[] key = row.get(0).key();

          if (Bytes.memcmp(metric, key, 0, Const.METRICS_WIDTH) != 0) {
            scanner.close();
            throw new IllegalDataException(
                    "HBase returned a row that doesn't match"
                            + " our scanner (" + scanner + ")! " + row + " does not start"
                            + " with " + Arrays.toString(metric));
          }

          Span datapoints = spans.get(key);
          if (datapoints == null) {
            datapoints = new Span();
            spans.put(key, datapoints);
          }

          final KeyValue compacted = compact(row, datapoints.getAnnotations());
          seenAnnotation |= !datapoints.getAnnotations().isEmpty();
          if (compacted != null) { // Can be null if we ignored all KVs.
            datapoints.addRow(compacted);
            nrows++;
          }
        }

        return scan();
      } catch (Exception e) {
        scanner.close();
        results.callback(e);
        return null;
      }
    }
  }
}
