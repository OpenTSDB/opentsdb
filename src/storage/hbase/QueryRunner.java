package net.opentsdb.storage.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import net.opentsdb.core.AsyncIterator;
import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TsdbQuery;
import net.opentsdb.uid.UniqueId;

import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.Scanner;

public class QueryRunner implements AsyncIterator<DataPoints> {
  private final TsdbQuery tsdb_query;
  private final HBaseClient client;

  public QueryRunner(TsdbQuery tsdb_query, HBaseClient client) {
    this.tsdb_query = tsdb_query;
    this.client = client;
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
    return null;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("#remove() is not supported by QueryRunner");
  }

  /**
   * Returns a scanner set for the given metric (from {@link #metric} or from
   * the first TSUID in the {@link #tsuids}s list. If one or more tags are
   * provided, it calls into {@link #createAndSetFilter} to setup a row key
   * filter. If one or more TSUIDs have been provided, it calls into
   * {@link #createAndSetTSUIDFilter} to setup a row key filter.
   * @return A scanner to use for fetching data points
   * @param tsdbQuery
   */
  private Scanner getScannerForQuery(final TsdbQuery tsdbQuery) throws
          HBaseException {

    final byte[] start_row = buildRowKey(getScanStartTimeSeconds(tsdbQuery));
    final byte[] end_row = buildRowKey();
    // We search at least one row before and one row after the start & end
    // time we've been given as it's quite likely that the exact timestamp
    // we're looking for is in the middle of a row.  Plus, a number of things
    // rely on having a few extra data points before & after the exact start
    // & end dates in order to do proper rate calculation or downsampling near
    // the "edges" of the graph.
    Bytes.setInt(start_row, (int) getScanStartTimeSeconds(tsdbQuery), metric_width);
    Bytes.setInt(end_row, (tsdbQuery.end_time == TsdbQuery.UNSET
                    ? -1  // Will scan until the end (0xFFF...).
                    : (int) getScanEndTimeSeconds(tsdbQuery)),
            metric_width);

    // set the metric UID based on the TSUIDs if given, or the metric UID
    if (tsdbQuery.tsuids != null && !tsdbQuery.tsuids.isEmpty()) {
      final String tsuid = tsdbQuery.tsuids.get(0);
      final String metric_uid = tsuid.substring(0, Const.METRICS_WIDTH * 2);
      tsdbQuery.metric = UniqueId.stringToUid(metric_uid);


      System.arraycopy(tsdbQuery.metric, 0, start_row, 0, metric_width);
      System.arraycopy(tsdbQuery.metric, 0, end_row, 0, metric_width);
    } else {
      System.arraycopy(tsdbQuery.metric, 0, start_row, 0, metric_width);
      System.arraycopy(tsdbQuery.metric, 0, end_row, 0, metric_width);
    }

    final Scanner scanner = client.newScanner(table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    if (tsdbQuery.tsuids != null && !tsdbQuery.tsuids.isEmpty()) {
      tsdbQuery.createAndSetTSUIDFilter(scanner);
    } else if (tsdbQuery.tags.size() > 0 || tsdbQuery.group_bys != null) {
      tsdbQuery.createAndSetFilter(scanner);
    }
    scanner.setFamily(FAMILY);
    return scanner;
  }

  private byte[] buildRowKey(int timestamp) {
    final short metric_width = Const.METRICS_WIDTH;
    final byte[] row = new byte[metric_width + Const.TIMESTAMP_BYTES];

    Bytes.setInt(row, timestamp, metric_width);

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
    long end = tsdbQuery.getEndTimeSeconds();
    return (int) (end + Const.MAX_TIMESPAN + 1 + tsdbQuery.getSampleInterval
            () / 1000);
  }

  /**
   * Sets the server-side regexp filter on the scanner.
   * This will compile a list of the tagk/v pairs for the TSUIDs to prevent
   * storage from returning irrelevant rows.
   * @param scanner The scanner on which to add the filter.
   * @param tsdbQuery
   * @since 2.0
   */
  private void createAndSetTSUIDFilter(final Scanner scanner, TsdbQuery tsdbQuery) {
    Collections.sort(tsdbQuery.tsuids);

    // first, convert the tags to byte arrays and count up the total length
    // so we can allocate the string builder
    final short metric_width = metrics.width();
    int tags_length = 0;
    final ArrayList<byte[]> uids = new ArrayList<byte[]>(tsdbQuery.tsuids.size());
    for (final String tsuid : tsdbQuery.tsuids) {
      final String tags = tsuid.substring(metric_width * 2);
      final byte[] tag_bytes = UniqueId.stringToUid(tags);
      tags_length += tag_bytes.length;
      uids.add(tag_bytes);
    }

    // Generate a regexp for our tags based on any metric and timestamp (since
    // those are handled by the row start/stop) and the list of TSUID tagk/v
    // pairs. The generated regex will look like: ^.{7}(tags|tags|tags)$
    // where each "tags" is similar to \\Q\000\000\001\000\000\002\\E
    final StringBuilder buf = new StringBuilder(
            13  // "(?s)^.{N}(" + ")$"
                    + (tsdbQuery.tsuids.size() * 11) // "\\Q" + "\\E|"
                    + tags_length); // total # of bytes in tsuids tagk/v pairs

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
            + "^.{")
            // ... start by skipping the metric ID and timestamp.
            .append(metrics.width() + Const.TIMESTAMP_BYTES)
            .append("}(");

    for (final byte[] tags : uids) {
      // quote the bytes
      buf.append("\\Q");
      HBaseStore.addId(buf, tags);
      buf.append('|');
    }

    // Replace the pipe of the last iteration, close and set
    buf.setCharAt(buf.length() - 1, ')');
    buf.append("$");
    scanner.setKeyRegexp(buf.toString(), HBaseConst.CHARSET);
  }

  /**
   * Sets the server-side regexp filter on the scanner.
   * In order to find the rows with the relevant tags, we use a
   * server-side filter that matches a regular expression on the row key.
   * @param scanner The scanner on which to add the filter.
   */
  private void createAndSetFilter(final Scanner scanner) {
    if (group_bys != null) {
      Collections.sort(group_bys, Bytes.MEMCMP);
    }
    final short name_width = tsdb.tag_names.width();
    final short value_width = tsdb.tag_values.width();
    final short tagsize = (short) (name_width + value_width);
    // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
    // and { 4 5 6 9 8 7 }, the regexp will be:
    // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
    final StringBuilder buf = new StringBuilder(
            15  // "^.{N}" + "(?:.{M})*" + "$"
                    + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
                    * (tags.size() + (group_bys == null ? 0 : group_bys.size() * 3))));
    // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
            + "^.{")
            // ... start by skipping the metric ID and timestamp.
            .append(tsdb.metrics.width() + Const.TIMESTAMP_BYTES)
            .append("}");
    final Iterator<byte[]> tags = this.tags.iterator();
    final Iterator<byte[]> group_bys = (this.group_bys == null
            ? new ArrayList<byte[]>(0).iterator()
            : this.group_bys.iterator());
    byte[] tag = tags.hasNext() ? tags.next() : null;
    byte[] group_by = group_bys.hasNext() ? group_bys.next() : null;
    // Tags and group_bys are already sorted.  We need to put them in the
    // regexp in order by ID, which means we just merge two sorted lists.
    do {
      // Skip any number of tags.
      buf.append("(?:.{").append(tagsize).append("})*\\Q");
      if (isTagNext(name_width, tag, group_by)) {
        HBaseStore.addId(buf, tag);
        tag = tags.hasNext() ? tags.next() : null;
      } else {  // Add a group_by.
        HBaseStore.addId(buf, group_by);
        final byte[][] value_ids = (group_by_values == null
                ? null
                : group_by_values.get(group_by));
        if (value_ids == null) {  // We don't want any specific ID...
          buf.append(".{").append(value_width).append('}');  // Any value ID.
        } else {  // We want specific IDs.  List them: /(AAA|BBB|CCC|..)/
          buf.append("(?:");
          for (final byte[] value_id : value_ids) {
            buf.append("\\Q");
            HBaseStore.addId(buf, value_id);
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
    scanner.setKeyRegexp(buf.toString(), CHARSET);
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
}
