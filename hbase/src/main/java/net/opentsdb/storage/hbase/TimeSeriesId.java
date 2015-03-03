package net.opentsdb.storage.hbase;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import net.opentsdb.core.Const;
import net.opentsdb.uid.IdUtils;
import net.opentsdb.utils.ByteArrayPair;
import org.hbase.async.Bytes;

import java.util.SortedSet;

class TimeSeriesId {
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
   * Configure a {@link org.hbase.async.KeyRegexpFilter} on the scanner based on
   * the provided tags and also return the built {@link java.util.regex.Pattern}
   * so that we can perform an additional check when processing the row.
   *
   * @param tags The tags to base the pattern on
   * @return A {@link java.util.regex.Pattern} that is configured to match
   * TSUIDs that match the query
   */
  static String scanRegexp(final SortedSet<ByteArrayPair> tags) {
    if (!tags.isEmpty()) {
      // remember, tagks are sorted in the row key so we need to supply a sorted
      // regex or matching will fail.

      final short name_width = Const.TAG_NAME_WIDTH;
      final short value_width = Const.TAG_VALUE_WIDTH;
      final short tagsize = (short) (name_width + value_width);

      boolean isFirstTag = true;

      final PeekingIterator<ByteArrayPair> tagIterator = Iterators.peekingIterator(tags.iterator());

      final StringBuilder buf = new StringBuilder(
          22  // "^.{N}" + "(?:.{M})*" + "$" + wiggle
              + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
              * (tags.size())));

      buf.append("(?s)^.{")
          .append(Const.METRICS_WIDTH)
          .append("}")
          .append("(?:.{")
          .append(tagsize)
          .append("})*");

      // At the top of the list will be the null=tagv pairs. We want to compile
      // a separate regex for them.
      while (tagIterator.hasNext() && tagIterator.peek().getKey() == null) {
        if (!isFirstTag) {
          buf.append("|");
        }

        // Skip the tag key
        buf.append("(?:.{")
            .append(name_width)
            .append("})");

        buf.append("\\Q");
        addId(buf, tagIterator.next().getValue());
        buf.append("\\E");

        isFirstTag = false;
      }

      buf.append("(?:.{")
          .append(tagsize)
          .append("})*")
          .append("$");

      final StringBuilder tagv_filter = new StringBuilder();

      if (!isFirstTag) {
        // we had one or more tagvs to lookup AND we have tagk or tag pairs to
        // filter on, so we dump the previous regex into the tagv_filter and
        // continue on with a row key
        tagv_filter.append(buf);
      }

      // catch any left over tagk/tag pairs
      if (tagIterator.hasNext()) {
        // Clear the buffer and start over.
        buf.setLength(0);

        buf.append("(?s)^.{")
            .append(Const.METRICS_WIDTH)
            .append("}");

        ByteArrayPair last_pair = null;
        while (tagIterator.hasNext()) {
          final ByteArrayPair tag = tagIterator.next();

          if (last_pair != null &&
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
      }

      return tagv_filter.toString();
    }

    return null;
  }

  /**
   * Configure the start and stop keys on the scanner based on the provided
   * metric id. If no metric is provided no keys will be configured and a full
   * table scan will be done.
   *
   * @param metric  The metric to base the start and stop keys on
   */
  static byte[] startKey(final byte[] metric) {
    return metric;
  }

  /**
   * Configure the start and stop keys on the scanner based on the provided
   * metric id. If no metric is provided no keys will be configured and a full
   * table scan will be done.
   *
   * @param metric  The metric to base the start and stop keys on
   */
  static byte[] stopKey(final byte[] metric) {
    if (metric != null) {
      // TODO - see what happens when this rolls over
      final long stop = IdUtils.uidToLong(metric, Const.METRICS_WIDTH) + 1;
      return IdUtils.longToUID(stop, Const.METRICS_WIDTH);
    }

    return null;
  }
}
