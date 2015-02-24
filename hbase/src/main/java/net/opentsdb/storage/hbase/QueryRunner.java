package net.opentsdb.storage.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.Const;
import net.opentsdb.core.Query;
import net.opentsdb.core.RowKey;
import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.IdUtils;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
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

public class QueryRunner {
  private static final Logger LOG = LoggerFactory.getLogger(QueryRunner.class);

  private final Query tsdb_query;
  private final HBaseClient client;

  private final Scanner scanner;

  private final ImmutableList.Builder<CompactedRow> result_rows;
  private final CompactionQueue compactionq;

  public QueryRunner(final Query tsdb_query,
                     final HBaseClient client,
                     final CompactionQueue compactionq,
                     final byte[] table,
                     final byte[] family) {
    this.tsdb_query = checkNotNull(tsdb_query);
    this.client = checkNotNull(client);
    this.compactionq = checkNotNull(compactionq);

    this.result_rows = ImmutableList.builder();

    scanner = getScannerForQuery(tsdb_query, checkNotNull(table),
            checkNotNull(family));
  }

  public Deferred<ImmutableList<CompactedRow>> run() {
    return scanner.nextRows()
            .addErrback(new ScannerEB())
            .addCallbackDeferring(new ScannerCB());
  }

  /**
   * Returns a scanner set for the given metric (from {@link #metric} or from
   * the first TSUID in the {@link #tsuids}s list. If one or more tags are
   * provided, it calls into {@link #buildMetricFilter} to setup a row key
   * filter. If one or more TSUIDs have been provided, it calls into
   * {@link #buildTSUIDFilter} to setup a row key filter.
   * @return A scanner to use for fetching data points
   * @param query
   * @param table
   * @param family
   */
  private Scanner getScannerForQuery(final Query query,
                                     final byte[] table,
                                     final byte[] family) {
    final byte[] start_row = buildRowKey(query.getMetric(),
            getScanStartTimeSeconds(query));
    final byte[] end_row = buildRowKey(query.getMetric(),
            getScanEndTimeSeconds(query));

    final Scanner scanner = client.newScanner(table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily(family);
    scanner.setFilter(buildRowKeyFilter(query));

    return scanner;
  }

  private byte[] buildRowKey(final byte[] metric_id, final int timestamp) {
    final byte[] row = new byte[Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES];

    Bytes.setInt(row, timestamp, Const.METRICS_WIDTH);
    System.arraycopy(metric_id, 0, row, 0, Const.METRICS_WIDTH);

    return row;
  }

  /** Returns the UNIX timestamp from which we must start scanning.
   * @param query*/
  private int getScanStartTimeSeconds(Query query) {
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
    long start = query.getStartTimeSeconds();
    final long ts = start - Const.MAX_TIMESPAN * 2 - query.getSampleInterval() / 1000;
    return ts > 0 ? Ints.checkedCast(ts) : 0;
  }

  /** Returns the UNIX timestamp at which we must stop scanning.
   * @param query*/
  private int getScanEndTimeSeconds(Query query) {
    // For the end_time, we have a different problem.  For instance if our
    // end_time = ... 12:30:00, we'll stop scanning when we get to 12:40, but
    // once again we wanna try to look ahead one more row, so to avoid this
    // problem we always add 1 second to the end_time.  Only when the end_time
    // is of the form HH:59:59 then we will scan ahead an extra row, but once
    // again that doesn't really matter.
    // Additionally, in case our sample_interval_ms is large, we need to look
    // even further before/after, so use that too.
    Optional<Long> end = query.getEndTimeSeconds();

    if (end.isPresent()) {
      return Ints.checkedCast(end.get() + Const.MAX_TIMESPAN + 1 +
              query.getSampleInterval() / 1000);
    }

    // Returning -1 will make us scan to the end (0xFFF...)
    return -1;
  }

  private ScanFilter buildRowKeyFilter(final Query query) {
    if (query.getTSUIDS() != null && !query.getTSUIDS().isEmpty()) {
      return buildTSUIDFilter(query);
    } else if (!query.getTags().isEmpty() || query.getGroupBys() != null) {
      return buildMetricFilter(query);
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
   * @param query The query that describes what parameters to build a
   *                  filter for
   */
  private KeyRegexpFilter buildTSUIDFilter(final Query query) {
    List<String> tsuids = query.getTSUIDS();
    Collections.sort(tsuids);

    // first, convert the tags to byte arrays and count up the total length
    // so we can allocate the string builder
    final short metric_width = Const.METRICS_WIDTH;
    int tags_length = 0;

    final ArrayList<byte[]> tag_uids = new ArrayList<byte[]>(tsuids.size());
    for (final String tsuid : tsuids) {
      final String tags = tsuid.substring(metric_width * 2);
      final byte[] tag_bytes = IdUtils.stringToUid(tags);
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
   * @param query The query that describes what parameters to build a
   *                  filter for
   */
  private KeyRegexpFilter buildMetricFilter(final Query query) {
    List<byte[]> group_bys1 = query.getGroupBys();
    ArrayList<byte[]> tags1 = query.getTags();
    Map<byte[], ArrayList<byte[]>> groupByValues = query.getGroupByValues();

    if (group_bys1 != null) {
      Collections.sort(group_bys1, Bytes.MEMCMP);
    } else {
      group_bys1 = Collections.emptyList();
    }

    if (groupByValues == null) {
      groupByValues = Maps.newHashMap();
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
                    * (tags1.size() + group_bys1.size() * 3)));
    // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)")  // Ensure we use the DOTALL flag.
       .append("^.{")
       // ... start by skipping the metric ID and timestamp.
       .append(Const.METRICS_WIDTH + Const.TIMESTAMP_BYTES)
       .append("}");

    final Iterator<byte[]> tags = tags1.iterator();
    final Iterator<byte[]> group_bys = group_bys1.iterator();

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
        final ArrayList<byte[]> value_ids = groupByValues.get(group_by);
        if (value_ids == null) {  // We don't want any specific ID...
          buf.append(".{")
             .append(value_width)
             .append('}');  // Any value ID.
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
    buf.append("(?:.{")
       .append(tagsize)
       .append("})*$");

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

  private class ScannerCB implements Callback<Deferred<ImmutableList<CompactedRow>>, ArrayList<ArrayList<KeyValue>>> {
    int nrows = 0;
    boolean seenAnnotation = false;
    int hbase_time = 0; // milliseconds.
    long starttime = System.nanoTime();

    @Override
    public Deferred<ImmutableList<CompactedRow>> call(final
                                                ArrayList<ArrayList<KeyValue>> rows) {
      hbase_time += (System.nanoTime() - starttime) / 1000000;

      // If we're done `rows` will be null and we should finally return the
      // result as a deferred.
      if (rows == null) {
        LOG.info("{} matched {} rows in in {}ms",
                tsdb_query, nrows, hbase_time);
        scanner.close();
        return Deferred.fromResult(result_rows.build());
      }

      final byte[] metric = tsdb_query.getMetric();

      // We've gotten a bunch of new rows so now we need to to some basic
      // preprocessing.
      for (final ArrayList<KeyValue> row : rows) {
        RowKey.checkMetric(row.get(0).key(), metric);

        // TODO(luuse): Previously AggregationIterator checked so that every
        // datapoint was within the time bounds however we saw no use in that
        // and removed that since we can check it easier here. It might be
        // possible that the aggregation and interpolation does some funky
        // stuff that creates datapoints outside the bounds though but
        // wouldn't it be better to consider that a programmer error and ask
        // them to guarantee to not do that. This also makes sense if you
        // consider that only happens when we read so it will never trash any
        // data perf
        // RowKey.checkBaseTime(row.get(0).key(), start, end);

        List<Annotation> annotations = Lists.newArrayList();
        final KeyValue compacted = compactionq.compact(row, annotations);
        seenAnnotation |= !annotations.isEmpty();
        if (compacted != null) { // Can be null if we ignored all KVs.
          result_rows.add(new CompactedRow(compacted, annotations));
          nrows++;
        }
      }

      return scanner.nextRows().addCallbackDeferring(this);
    }
  }

  private class ScannerEB implements Callback<Exception, Exception> {
    @Override
    public Exception call(final Exception e) throws Exception {
      scanner.close();
      return e;
    }
  }
}
