package net.opentsdb.core;

public class LongHistogramDataPointForTestDecoder implements HistogramDataPointDecoder {

  @Override
  public HistogramDataPoint decode(byte[] raw_data, long timestamp) {
    return new LongHistogramDataPointForTest(timestamp, raw_data);
  }

}
