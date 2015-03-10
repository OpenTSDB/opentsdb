package net.opentsdb.uid;

import java.util.List;

public abstract class TimeseriesId {
  public abstract byte[] metric();
  public abstract List<byte[]> tags();

  /**
   * @see #toHBaseTSUID(byte[], java.util.List)
   */
  @Deprecated
  public static byte[] toHBaseTSUID(final TimeseriesId timeseriesId) {
    return toHBaseTSUID(timeseriesId.metric(), timeseriesId.tags());
  }

  /**
   * Build a HBase TSUID based on the properties of the provided timeseries id.
   *
   * Note: This is here for legacy reasons and to aid migration. Everything that
   * returns the result of this method should be refactored to return a
   * timeseries id instead and a everything that accepts the result of this
   * method as an argument should be refactored to accept the metric and tags
   * separately instead. You should not use {@link net.opentsdb.uid.TimeseriesId}
   * instances as an argument, rather you should use the components that make
   * them up (metric and tags).
   *
   * @param metric The metric to use in the TSUID
   * @param tags   The tags to use in the TSUID
   * @return The byte array representation of a timeseries id as the HBase store
   * used to expect them.
   */
  @Deprecated
  public static byte[] toHBaseTSUID(final byte[] metric,
                                    final List<byte[]> tags) {
    final short metric_width = UniqueIdType.METRIC.width;
    final short tag_name_width = UniqueIdType.TAGK.width;
    final short tag_value_width = UniqueIdType.TAGV.width;
    final short num_tags = (short) tags.size();

    final int size = (metric_width
        + tag_name_width * num_tags
        + tag_value_width * num_tags);

    final byte[] tsuid = new byte[size];

    int pos = 0;

    pos += copyInTSUID(tsuid, pos, metric);

    for (final byte[] tag : tags) {
      pos += copyInTSUID(tsuid, pos, tag);
    }

    return tsuid;
  }

  /**
   * Copies the specified byte array at the specified offset into the tsuid.
   *
   * @param tsuid  The tsuid into which to copy the bytes.
   * @param offset The offset in the tsuid to start writing at.
   * @param bytes  The bytes to copy.
   * @return The number of bytes copied into the tsuid
   */
  private static int copyInTSUID(final byte[] tsuid, final int offset, final byte[] bytes) {
    System.arraycopy(bytes, 0, tsuid, offset, bytes.length);
    return bytes.length;
  }
}
