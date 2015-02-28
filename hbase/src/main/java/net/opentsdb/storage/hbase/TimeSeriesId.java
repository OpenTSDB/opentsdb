package net.opentsdb.storage.hbase;

import com.google.common.primitives.Bytes;
import net.opentsdb.core.Const;
import net.opentsdb.uid.IdUtils;
import org.hbase.async.KeyRegexpFilter;
import org.hbase.async.ScanFilter;

import java.util.Map;

class TimeSeriesId {
  static ScanFilter scanFilter(final Map<byte[], byte[]> tags) {
    if (!tags.isEmpty()) {
      final short name_width = Const.TAG_NAME_WIDTH;
      final short value_width = Const.TAG_VALUE_WIDTH;
      final short tagsize = (short) (name_width + value_width);

      // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
      // and { 4 5 6 9 8 7 }, the regexp will be:
      // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
      final StringBuilder buf = new StringBuilder(
          15  // "^.{N}" + "(?:.{M})*" + "$"
              + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
              * (tags.size())));

      // Alright, let's build this regexp.  From the beginning...
      buf.append("(?s)"  // Ensure we use the DOTALL flag.
          + "^.{")
          // ... start by skipping the metric ID.
          .append(Const.METRICS_WIDTH)
          .append("}");

      // Tags and group_bys are already sorted.  We need to put them in the
      // regexp in order by ID, which means we just merge two sorted lists.
      for (final Map.Entry<byte[], byte[]> tag : tags.entrySet()) {
        // Skip any number of tags.
        buf.append("(?:.{").append(tagsize).append("})*\\Q");
        IdUtils.addIdToRegexp(buf, Bytes.concat(tag.getKey(), tag.getValue()));
      }

      // Skip any number of tags before the end.
      buf.append("(?:.{").append(tagsize).append("})*$");

      return new KeyRegexpFilter(buf.toString(), HBaseConst.CHARSET);
    }

    return null;
  }

  static byte[] startKey(final byte[] metric) {
    return metric;
  }

  static byte[] stopKey(final byte[] metric) {
    final long stop = IdUtils.uidToLong(metric, Const.METRICS_WIDTH) + 1;
    return IdUtils.longToUID(stop, Const.METRICS_WIDTH);
  }
}
