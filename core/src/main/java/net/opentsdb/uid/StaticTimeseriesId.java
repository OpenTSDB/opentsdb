package net.opentsdb.uid;

import java.util.List;

/**
 * An implementation of {@link net.opentsdb.uid.TimeseriesId} that is built up
 * by the plain metric and tags.
 *
 * Note: This is here for legacy reasons and to aid migration. Everything that
 * accepts instances of this as an argument should be refactored to accept the
 * metric and tags separately instead
 */
@Deprecated
public class StaticTimeseriesId extends TimeseriesId {
  private final byte[] metric;
  private final List<byte[]> tags;

  public StaticTimeseriesId(final byte[] metric,
                            final List<byte[]> tags) {
    this.metric = metric;
    this.tags = tags;
  }

  @Override
  public byte[] metric() {
    return metric;
  }

  @Override
  public List<byte[]> tags() {
    return tags;
  }
}
