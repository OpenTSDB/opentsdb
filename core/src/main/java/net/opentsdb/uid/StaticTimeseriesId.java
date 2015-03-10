package net.opentsdb.uid;

import java.util.List;

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
