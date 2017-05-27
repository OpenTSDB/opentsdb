package net.opentsdb.core;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hbase.async.Bytes.ByteMap;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.meta.Annotation;

public class HistogramBucketDataPointsAdaptor implements DataPoints {
  private final HistogramDataPoints hist_data_points;
  private final HistogramDataPoint.HistogramBucket bucket;

  HistogramBucketDataPointsAdaptor(final HistogramDataPoints hists, final HistogramDataPoint.HistogramBucket bucket) {
    this.hist_data_points = hists;
    this.bucket = bucket;
  }

  @Override
  public String metricName() {
    return (this.hist_data_points.metricName() + metricNamePostfix());
  }

  @Override
  public Deferred<String> metricNameAsync() {
    return this.hist_data_points.metricNameAsync().addCallback(new Callback<String, String>() {
      public String call(final String name) {
        return name + metricNamePostfix();
      }
    });
  }

  @Override
  public byte[] metricUID() {
    return this.hist_data_points.metricUID();
  }

  @Override
  public Map<String, String> getTags() {
    return this.hist_data_points.getTags();
  }

  @Override
  public Deferred<Map<String, String>> getTagsAsync() {
    return this.hist_data_points.getTagsAsync();
  }

  @Override
  public ByteMap<byte[]> getTagUids() {
    return this.hist_data_points.getTagUids();
  }

  @Override
  public List<String> getAggregatedTags() {
    return this.hist_data_points.getAggregatedTags();
  }

  @Override
  public Deferred<List<String>> getAggregatedTagsAsync() {
    return this.hist_data_points.getAggregatedTagsAsync();
  }

  @Override
  public List<byte[]> getAggregatedTagUids() {
    return this.hist_data_points.getAggregatedTagUids();
  }

  @Override
  public List<String> getTSUIDs() {
    return this.hist_data_points.getTSUIDs();
  }

  @Override
  public List<Annotation> getAnnotations() {
    return this.hist_data_points.getAnnotations();
  }
  
  @Override
  public int size() {
    return this.hist_data_points.size();
  }

  @Override
  public int aggregatedSize() {
    return this.hist_data_points.aggregatedSize();
  }

  @Override
  public SeekableView iterator() {
    return internalIterator();
  }

  private HistogramDataPoint getHistogramDataPoint(int i) {
    if (i < 0) {
      throw new IndexOutOfBoundsException("negative index: " + i);
    }
    final int saved_i = i;
    final HistogramSeekableView it = this.hist_data_points.iterator();
    HistogramDataPoint dp = null;
    while (it.hasNext() && i >= 0) {
      dp = it.next();
      i--;
    }
    if (i != -1 || dp == null) {
      throw new IndexOutOfBoundsException("index " + saved_i + " too large (it's >= " + size() + ") for " + this);
    }
    return dp;
  }

  @Override
  public long timestamp(int i) {
    return getHistogramDataPoint(i).timestamp();
  }

  @Override
  public boolean isInteger(int i) {
    return true;
  }

  @Override
  public long longValue(int i) {
    HistogramDataPoint hdp = this.getHistogramDataPoint(i);

    try {
      Map<HistogramDataPoint.HistogramBucket, Long> buckets = hdp.getHistogramBucketsIfHas();
      if (null != buckets && buckets.containsKey(bucket)) {
        return buckets.get(bucket).longValue();
      }
    } catch (UnsupportedOperationException e) {
      // Just ignore
    }

    return 0;
  }

  @Override
  public double doubleValue(int i) {
    return this.longValue(i);
  }

  @Override
  public int getQueryIndex() {
    return this.hist_data_points.getQueryIndex();
  }

  @Override
  public boolean isPercentile() {
    return false;
  }

  @Override
  public float getPercentile() {
    return 0;
  }

  private String metricNamePostfix() {
    if (this.bucket.bucketType() == HistogramDataPoint.HistogramBucket.BucketType.UNDERFLOW) {
      return "_UNDERFLOW";
    } else if (this.bucket.bucketType() == HistogramDataPoint.HistogramBucket.BucketType.OVERFLOW) {
      return "_OVERFLOW";
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append("_").append(this.bucket.getLowerBound()).append("_")
          .append(this.bucket.getUpperBound());
      return sb.toString();
    }
  }

  private Iterator internalIterator() {
    return new Iterator();
  }

  //////////////////////////////////////////////////////////////////////////////////
  // internal iterator
  /////////////////////////////////////////////////////////////////////////////////
  final class Iterator implements SeekableView, DataPoint {
    final private HistogramSeekableView source;
    private long value;
    private long timestamp;

    public Iterator() {
      this.source = hist_data_points.iterator();
    }

    @Override
    public boolean hasNext() {
      return this.source.hasNext();
    }

    @Override
    public DataPoint next() {
      HistogramDataPoint hdp = this.source.next();

      this.value = 0;
      try {
        Map<HistogramDataPoint.HistogramBucket, Long> buckets = hdp.getHistogramBucketsIfHas();
        if (null != buckets && buckets.containsKey(bucket)) {
          this.value = buckets.get(bucket).longValue();
        }
      } catch (UnsupportedOperationException e) {
        // Just ignore
      }
      
      this.timestamp = hdp.timestamp();
      return this;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove is not supported here");
    }

    @Override
    public void seek(long timestamp) {
      this.source.seek(timestamp);
    }

    @Override
    public long timestamp() {
      return this.timestamp;
    }

    @Override
    public boolean isInteger() {
      return true;
    }

    @Override
    public long longValue() {
      return this.value;
    }

    @Override
    public double doubleValue() {
      throw new ClassCastException("value #" + " is not a long in " + this);
    }

    @Override
    public double toDouble() {
      return this.value;
    }

    @Override
    public long valueCount() {
      return 0;
    }
  }
}
