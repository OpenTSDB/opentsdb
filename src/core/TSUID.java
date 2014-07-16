package net.opentsdb.core;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by andreas on 7/16/14.
 */
public class TSUID {
  /**
   * Validates the given metric and tags.
   * @throws IllegalArgumentException if any of the arguments aren't valid.
   */
  static void checkMetricAndTags(final String metric, final Map<String, String> tags) {
    if (tags.size() <= 0) {
      throw new IllegalArgumentException("Need at least one tags (metric="
          + metric + ", tags=" + tags + ')');
    } else if (tags.size() > Const.MAX_NUM_TAGS) {
      throw new IllegalArgumentException("Too many tags: " + tags.size()
          + " maximum allowed: " + Const.MAX_NUM_TAGS + ", tags: " + tags);
    }

    Tags.validateString("metric name", metric);
    for (final Map.Entry<String, String> tag : tags.entrySet()) {
      Tags.validateString("tag name", tag.getKey());
      Tags.validateString("tag value", tag.getValue());
    }
  }

  /**
  * Returns a partially initialized row key for this metric and these tags.
  * The only thing left to fill in is the base timestamp.
  */
  static byte[] rowKeyTemplate(final TSDB tsdb,
                               final String metric,
                               final Map<String, String> tags) {
    final short metric_width = tsdb.metrics.width();
    final short tag_name_width = tsdb.tag_names.width();
    final short tag_value_width = tsdb.tag_values.width();
    final short num_tags = (short) tags.size();

    int row_size = (metric_width + Const.TIMESTAMP_BYTES
                    + tag_name_width * num_tags
                    + tag_value_width * num_tags);
    final byte[] row = new byte[row_size];

    short pos = 0;

    copyInRowKey(row, pos, (tsdb.config.auto_metric() ?
        tsdb.metrics.getOrCreateId(metric) : tsdb.metrics.getId(metric)));
    pos += metric_width;

    pos += Const.TIMESTAMP_BYTES;

    for(final byte[] tag : Tags.resolveOrCreateAll(tsdb, tags)) {
      copyInRowKey(row, pos, tag);
      pos += tag.length;
    }
    return row;
  }

  /**
   * Returns a partially initialized row key for this metric and these tags.
   * The only thing left to fill in is the base timestamp.
   * @since 2.0
   */
  static Deferred<byte[]> rowKeyTemplateAsync(final TSDB tsdb,
                                         final String metric,
                                         final Map<String, String> tags) {
    final short metric_width = tsdb.metrics.width();
    final short tag_name_width = tsdb.tag_names.width();
    final short tag_value_width = tsdb.tag_values.width();
    final short num_tags = (short) tags.size();

    int row_size = (metric_width + Const.TIMESTAMP_BYTES
                    + tag_name_width * num_tags
                    + tag_value_width * num_tags);
    final byte[] row = new byte[row_size];

    // Lookup or create the metric ID.
    final Deferred<byte[]> metric_id;
    if (tsdb.config.auto_metric()) {
      metric_id = tsdb.metrics.getOrCreateIdAsync(metric);
    } else {
      metric_id = tsdb.metrics.getIdAsync(metric);
    }

    // Copy the metric ID at the beginning of the row key.
    class CopyMetricInRowKeyCB implements Callback<byte[], byte[]> {
      public byte[] call(final byte[] metricid) {
        copyInRowKey(row, (short) 0, metricid);
        return row;
      }
    }

    // Copy the tag IDs in the row key.
    class CopyTagsInRowKeyCB
      implements Callback<Deferred<byte[]>, ArrayList<byte[]>> {
      public Deferred<byte[]> call(final ArrayList<byte[]> tags) {
        short pos = metric_width;
        pos += Const.TIMESTAMP_BYTES;
        for (final byte[] tag : tags) {
          copyInRowKey(row, pos, tag);
          pos += tag.length;
        }
        // Once we've resolved all the tags, schedule the copy of the metric
        // ID and return the row key we produced.
        return metric_id.addCallback(new CopyMetricInRowKeyCB());
      }
    }

    // Kick off the resolution of all tags.
    return Tags.resolveOrCreateAllAsync(tsdb, tags)
      .addCallbackDeferring(new CopyTagsInRowKeyCB());
  }

  /**
   * Copies the specified byte array at the specified offset in the row key.
   * @param row The row key into which to copy the bytes.
   * @param offset The offset in the row key to start writing at.
   * @param bytes The bytes to copy.
   */
  private static void copyInRowKey(final byte[] row, final short offset, final byte[] bytes) {
    System.arraycopy(bytes, 0, row, offset, bytes.length);
  }
}
