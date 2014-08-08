package net.opentsdb.storage.hbase;

import java.util.Comparator;

import net.opentsdb.core.Const;

/**
 * Comparator that ignores timestamps in row keys.
 * TODO SpanCMP is a really bad name now.
 */
public final class SpanCmp implements Comparator<byte[]> {

  private final short metric_width;

  public SpanCmp(final short metric_width) {
    this.metric_width = metric_width;
  }

  @Override
  public int compare(final byte[] a, final byte[] b) {
    final int length = Math.min(a.length, b.length);
    if (a == b) {  // Do this after accessing a.length and b.length
      return 0;    // in order to NPE if either a or b is null.
    }
    int i;
    // First compare the metric ID.
    for (i = 0; i < metric_width; i++) {
      if (a[i] != b[i]) {
        return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
      }
    }
    // Then skip the timestamp and compare the rest.
    for (i += Const.TIMESTAMP_BYTES; i < length; i++) {
      if (a[i] != b[i]) {
        return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
      }
    }
    return a.length - b.length;
  }

}
