
package net.opentsdb.storage.hbase;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import net.opentsdb.core.Const;
import net.opentsdb.uid.IdUtils;
import net.opentsdb.utils.ByteArrayPair;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.regex.Pattern;

/**
 * Lookup time series associated with the given metric, tagk, tagv or tag pairs.
 * If no metric is given, a full table scan must be performed and this call may
 * take a long time to complete.
 *
 * To avoid running performance degrading regexes in HBase regions, we'll double
 * filter when necessary. If tagks are present, those are used in the rowkey
 * filter and a secondary filter is applied in the TSD with remaining tagvs.
 * E.g. the query "host=web01 =lga" will issue a rowkey filter with "host=web01"
 * then within the TSD scanner, we'll filter out only the rows that contain an
 * "lga" tag value. We don't know where in a row key the tagv may fall, so we
 * would have to first match on the pair, then backtrack to find the value and
 * make sure the pair is skipped. Thus its easier on the region server to
 * execute a simpler rowkey regex, pass all the results to the TSD, then let us
 * filter on tag values only when necessary. (if a query only has tag values,
 * then this is moot and we can pass them in a rowkey filter since they're
 * OR'd).
 *
 * @see net.opentsdb.core.UniqueIdClient#executeTimeSeriesQuery
 */
class TimeseriesQueryRunner extends RowProcessor<List<byte[]>> {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimeseriesQueryRunner.class);

  private final Pattern tagKeyRegex;
  private final List<byte[]> tsuids;

  /**
   * Appends the given ID to the given buffer, escaping where appropriate
   *
   * @param buf The string buffer to append to
   * @param id  The ID to append
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
  }

  /**
   * Configure the start and stop keys on the scanner based on the provided
   * metric id. If no metric is provided no keys will be configured and a full
   * table scan will be done.
   *
   * @param metric  The metric to base the start and stop keys on
   */
  static void configureMetric(final Scanner scanner, final byte[] metric) {
    // if a metric id is given, we need to set the start key to the UID and the
    // stop key to the next row by incrementing the UID.
    if (metric != null) {
      scanner.setStartKey(metric);

      // TODO - see what happens when this rolls over
      final long stopKey = IdUtils.uidToLong(metric, Const.METRICS_WIDTH) + 1;
      scanner.setStopKey(IdUtils.longToUID(stopKey, Const.METRICS_WIDTH));
    } else {
      LOG.info("Performing full table scan, no metric provided");
    }
  }

  /**
   * Configure a {@link org.hbase.async.KeyRegexpFilter} on the scanner based on
   * the provided tags and also return the built {@link java.util.regex.Pattern}
   * so that we can perform an additional check when processing the row.
   *
   * @param tags The tags to base the pattern on
   * @return A {@link java.util.regex.Pattern} that is configured to match
   * TSUIDs that match the query
   */
  static Pattern configureTags(final Scanner scanner,
                               final SortedSet<ByteArrayPair> tags) {
    final StringBuilder tagv_filter = new StringBuilder();

    if (!tags.isEmpty()) {
      // remember, tagks are sorted in the row key so we need to supply a sorted
      // regex or matching will fail.

      final short name_width = Const.TAG_NAME_WIDTH;
      final short value_width = Const.TAG_VALUE_WIDTH;
      final short tagsize = (short) (name_width + value_width);

      boolean isFirstTag = true;

      final StringBuilder buf = new StringBuilder(
          22  // "^.{N}" + "(?:.{M})*" + "$" + wiggle
              + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
              * (tags.size())));
      buf.append("(?s)^.{").append(Const.METRICS_WIDTH)
          .append("}");
      buf.append("(?:.{").append(tagsize).append("})*");

      final PeekingIterator<ByteArrayPair> tagIterator = Iterators.peekingIterator(tags.iterator());

      // at the top of the list will be the null=tagv pairs. We want to compile
      // a separate regex for them.
      while (tagIterator.hasNext() && tagIterator.peek().getKey() == null) {
        if (!isFirstTag) {
          buf.append("|");
        }
        buf.append("(?:.{").append(name_width).append("})");
        buf.append("\\Q");
        addId(buf, tagIterator.next().getValue());
        buf.append("\\E");

        isFirstTag = false;
      }
      buf.append("(?:.{").append(tagsize).append("})*")
          .append("$");

      if (!isFirstTag && tagIterator.hasNext()) {
        // we had one or more tagvs to lookup AND we have tagk or tag pairs to
        // filter on, so we dump the previous regex into the tagv_filter and
        // continue on with a row key
        tagv_filter.append(buf);
        LOG.debug("Setting tagv filter: {}", buf);
      } else if (!tagIterator.hasNext()) {
        // in this case we don't have any tagks to deal with so we can just
        // pass the previously compiled regex to the rowkey filter of the
        // scanner
        scanner.setKeyRegexp(buf.toString(), HBaseConst.CHARSET);
        LOG.debug("Setting scanner row key filter with tagvs only: {}", buf);
      }

      // catch any left over tagk/tag pairs
      if (tagIterator.hasNext()) {
        buf.setLength(0);
        buf.append("(?s)^.{").append(Const.METRICS_WIDTH)
            .append("}");

        ByteArrayPair last_pair = null;
        while (tagIterator.hasNext()) {
          final ByteArrayPair tag = tagIterator.next();

          if (last_pair != null && last_pair.getValue() == null &&
              Bytes.memcmp(last_pair.getKey(), tag.getKey()) == 0) {
            // tagk=null is a wildcard so we don't need to bother adding
            // tagk=tagv pairs with the same tagk.
            LOG.debug("Skipping pair due to wildcard: {}", tag);
          } else if (last_pair != null &&
              Bytes.memcmp(last_pair.getKey(), tag.getKey()) == 0) {
            // in this case we're ORing e.g. "host=web01|host=web02"
            buf.append("|\\Q");
            addId(buf, tag.getKey());
            addId(buf, tag.getValue());
            buf.append("\\E");
          } else {
            if (last_pair != null) {
              buf.append(")");
            }
            // moving on to the next tagk set
            buf.append("(?:.{6})*"); // catch tag pairs in between
            buf.append("(?:");
            if (tag.getKey() != null &&
                tag.getValue() != null) {
              buf.append("\\Q");
              addId(buf, tag.getKey());
              addId(buf, tag.getValue());
              buf.append("\\E");
            } else {
              buf.append("\\Q");
              addId(buf, tag.getKey());
              buf.append("\\E");
              buf.append("(?:.{").append(value_width).append("})+");
            }
          }
          last_pair = tag;
        }
        buf.append(")(?:.{").append(tagsize).append("})*").append("$");

        scanner.setKeyRegexp(buf.toString(), HBaseConst.CHARSET);
        LOG.debug("Setting scanner row key filter: {}", buf);
      }
    }

    return tagv_filter.length() > 1 ?
        Pattern.compile(tagv_filter.toString()) : null;
  }

  public TimeseriesQueryRunner(final Pattern tagKeyRegex) {
    this.tagKeyRegex = tagKeyRegex;
    this.tsuids = Lists.newArrayList();
  }

  /**
   * Return the list of all matching TSUIDs. This is called once the {@link
   * org.hbase.async.Scanner} is exhausted.
   *
   * @return the list of matching TSUIDs
   */
  @Override
  protected List<byte[]> getResult() {
    return tsuids;
  }

  /**
   * Process each row returned by the scanner and add all TSUIDs that matches
   * the {@link #tagKeyRegex} to the list of matching TSUIDs.
   *
   * @param row A row as returned by the {@link org.hbase.async.Scanner}
   */
  @Override
  protected void processRow(final ArrayList<KeyValue> row) {
    final byte[] tsuid = row.get(0).key();

    // TODO - there MUST be a better way than creating a ton of temp
    // string objects.
    if (tagKeyRegex == null ||
        tagKeyRegex.matcher(new String(tsuid, HBaseConst.CHARSET)).find()) {
      tsuids.add(tsuid);
    }
  }
}
